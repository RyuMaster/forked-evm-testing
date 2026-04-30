import logging
import traceback
import time
from datetime import datetime

from db_manager import CHARSET, COLLATION
from updaters.datadump import (
    _calculate_roi_by_payback,
    THRESHOLDS
)

logger = logging.getLogger(__name__)

def calculate_smart_match_time_percentage(player_rating, club_baseline_rating):
    """Calculate smart match time percentage based on player rating vs club's 11th best player

    The baseline is the rating of the 11th best player (lowest of starting 11).
    This represents the threshold to be in the starting lineup.

    Args:
        player_rating: Player's rating (0-100)
        club_baseline_rating: Rating of club's 11th best player (starting lineup threshold)

    Returns:
        Match time percentage (0-100):
        - 100% if player_rating >= club_baseline_rating (good enough to start)
        - 0% if player_rating <= (club_baseline_rating - 10)
        - Linear interpolation in between
    """
    if player_rating >= club_baseline_rating:
        return 100

    diff = club_baseline_rating - player_rating
    if diff >= 10:
        return 0

    # Linear interpolation: 0% at diff=10, 100% at diff=0
    percentage = 100 * (10 - diff) / 10
    return percentage


class PlayerEarningsCalculator:
    """Calculate player earnings for influence holders"""

    def __init__(self, game_params):
        self.ECONOMY_PLAYER_WAGES_PAYOUT_BP = game_params['payout-player-influence-dividend-bp']
        self.ECONOMY_PLAYER_BONUS_PER_MATCH_BP = game_params['payout-player-playing-bonus-bp']
        self.ECONOMY_PLAYER_BONUS_PER_GOAL_BP = game_params['payout-player-score-bonus-bp']
        self.ECONOMY_PLAYER_BONUS_PER_ASSIST_BP = game_params['payout-player-assist-bonus-bp']
        self.ECONOMY_PLAYER_BONUS_PER_CLEANSHEET_BP = game_params['payout-player-clean-sheet-bonus-bp']
        self.ECONOMY_PLAYER_SHAREHOLDERS_PRIZEPOT_LEAGUE_AMOUNT_BP = game_params['economy-player-shareholders-prizepot-league-amount-bp']
        self.ECONOMY_PLAYER_SHAREHOLDERS_PRIZEPOT_CUP_AMOUNT_BP = game_params['economy-player-shareholders-prizepot-cup-amount-bp']
        self.MAX_INFLUENCE = 1000000

    def calculate_club_total_prize(self, prize_pot, position, num_teams):
        """Calculate TOTAL club prize money for a specific position (before shareholder split)

        This matches the club earnings calculator but returns the club's total, not shareholder portion.
        Used for player prize calculations.
        """
        # Base prize money - half the pot divided equally
        base_prize = (prize_pot / 2) / num_teams

        # Position-based calculation
        equal = 100 / num_teams
        perc_due = (num_teams - position) * equal
        pot_perc = (perc_due * 256 / 100) * (2 * equal)

        # Handle edge case for last position
        if pot_perc == 0:
            perc_due = 100 - (num_teams - 1) * equal
            pot_perc = (perc_due * 256 / 100) * (2 * equal)

        # Calculate position-based prize money
        prize_money = (pot_perc / 100) * prize_pot
        prize_money = prize_money / 256
        prize_money = (prize_money * 5000) / 10000  # 50% to clubs
        prize_money = prize_money + base_prize

        # Return TOTAL club prize (NOT the shareholder portion)
        return prize_money

    def calculate_season_earnings(self, player, league, club_position, influence_amount=1, match_time_percentage=100):
        """Calculate full season earnings for a player

        Returns dict with:
        - wages: Season wages
        - play_bonus: Playing bonus (paid per match)
        - league_prize: League prize earnings
        - total: Total season earnings
        """
        results = {
            'wages': 0,
            'play_bonus': 0,
            'league_prize': 0,
            'total': 0,
            'influence_amount': influence_amount
        }

        if not player or not league:
            return results

        wages = player.get('wages', 0)
        rating = player.get('rating', 0)

        if not wages or not rating:
            return results

        # Calculate matches played (num_rounds is the total matches per season)
        matches_played = league.get('num_rounds')
        num_teams = league.get('num_teams', 20)

        # 1. Calculate wages (0.2% per match, full payment regardless of match time)
        wages_per_match = (wages * self.ECONOMY_PLAYER_WAGES_PAYOUT_BP) / 10000
        season_wages = wages_per_match * matches_played

        # 2. Calculate play bonus (25bp per match, scaled by match time)
        bonus_per_match = (wages * self.ECONOMY_PLAYER_BONUS_PER_MATCH_BP) / 10000
        season_play_bonus = bonus_per_match * matches_played * (match_time_percentage / 100)

        # 3. Calculate league prize
        prize_pot = league.get('prize_money_pot', 0)
        if prize_pot and club_position:
            # Get club's TOTAL prize (not shareholder portion)
            club_prize = self.calculate_club_total_prize(prize_pot, club_position, num_teams)

            # Apply player shareholder percentage (10bp for league)
            prize_share = (club_prize * self.ECONOMY_PLAYER_SHAREHOLDERS_PRIZEPOT_LEAGUE_AMOUNT_BP) / 10000

            # Discount by rating
            prize_share_from_rating = (prize_share * rating) / 100

            # Discount by match time percentage (linear scaling)
            discount = match_time_percentage / 100
            prize_share_from_rating = prize_share_from_rating * discount

            season_league_prize = prize_share_from_rating
        else:
            season_league_prize = 0

        # Scale to actual influence amount (all calculations above are for 1M influence)
        influence_scale = influence_amount / self.MAX_INFLUENCE
        results['wages'] = season_wages * influence_scale
        results['play_bonus'] = season_play_bonus * influence_scale
        results['league_prize'] = season_league_prize * influence_scale
        results['total'] = results['wages'] + results['play_bonus'] + results['league_prize']

        return results


class PlayerEarningsUpdater:
    UPDATE_INTERVAL = 120  # seconds

    def __init__(self, db_manager):
        self.db = db_manager
        self.main_table = "dc_player_earnings"
        self.staging_table = "dc_player_earnings_staging"
        self._create_main_table()

    def _create_main_table(self):
        """Create the dc_player_earnings table if it doesn't exist."""
        # Base columns
        columns = [
            "`player_id` BIGINT PRIMARY KEY",
            "`club_id` BIGINT",
            "`club_country` VARCHAR(3)",
            "`club_division` INT",
            "`club_position` INT",
            "`player_nationality` VARCHAR(3)",
            "`player_age` INT",
            "`player_rating` INT",
            "`player_position` INT",
            "`match_time_percentage` DOUBLE",
            "`current_earnings` DOUBLE"
        ]

        # Add threshold columns
        for t in THRESHOLDS:
            columns.append(f"`buyable_{t}s` BIGINT")
            columns.append(f"`cost_{t}s` BIGINT")

        columns.append("`updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP")

        # Indexes for fast filtering
        indexes = [
            "INDEX `idx_club_country` (`club_country`)",
            "INDEX `idx_club_division` (`club_division`)",
            "INDEX `idx_player_position` (`player_position`)",
            "INDEX `idx_player_age` (`player_age`)",
            "INDEX `idx_player_rating` (`player_rating`)"
        ]

        columns_sql = ",\n            ".join(columns + indexes)

        create_query = f"""
        CREATE TABLE IF NOT EXISTS `{self.main_table}` (
            {columns_sql}
        ) ENGINE=InnoDB DEFAULT CHARSET={CHARSET} COLLATE={COLLATION}
        """
        self.db.execute_query(self.db.dest_conn, create_query)
        logger.info(f"Ensured table `{self.main_table}` exists.")

    def _create_staging_table(self):
        """Create an empty staging table mirroring the main table."""
        cursor = self.db.dest_conn.cursor()
        try:
            cursor.execute(f"DROP TABLE IF EXISTS `{self.staging_table}`")
            cursor.execute(f"CREATE TABLE `{self.staging_table}` LIKE `{self.main_table}`")
            self.db.dest_conn.commit()
            logger.info(f"Created empty staging table `{self.staging_table}`")
        except Exception as e:
            logger.error(f"Error creating staging table: {e}")
            self.db.dest_conn.rollback()
            raise
        finally:
            cursor.close()

    def _atomic_swap_tables(self):
        """Atomically swap the staging table with the main table."""
        cursor = self.db.dest_conn.cursor()
        try:
            swap_query = f"""
            RENAME TABLE
                `{self.main_table}` TO `{self.main_table}_old`,
                `{self.staging_table}` TO `{self.main_table}`
            """
            cursor.execute(swap_query)
            cursor.execute(f"DROP TABLE IF EXISTS `{self.main_table}_old`")
            self.db.dest_conn.commit()
            logger.info(f"Atomically swapped staging table to `{self.main_table}`")
        except Exception as e:
            logger.error(f"Error swapping tables: {e}")
            self.db.dest_conn.rollback()
            raise
        finally:
            cursor.close()

    def _load_game_params(self):
        """Load game parameters from the SQLite parameters table."""
        rows = self.db.execute_query('sqlite', 'SELECT name, value FROM parameters')
        return {row['name']: row['value'] for row in rows}

    def update(self):
        """Run the full update process for player earnings."""
        try:
            logger.info("Starting player earnings update (MySQL)...")
            start_time = time.time()

            # 1. Fetch required data
            game_params = self._load_game_params()
            player_calculator = PlayerEarningsCalculator(game_params)

            max_season_result = self.db.execute_query(
                'sqlite', "SELECT MAX(season_id) AS max_season FROM leagues"
            )
            if not max_season_result or max_season_result[0]['max_season'] is None:
                logger.warning("No seasons found, skipping player earnings update.")
                return
            max_season_id = max_season_result[0]['max_season']

            leagues_rows = self.db.execute_query(
                'sqlite',
                "SELECT * FROM leagues WHERE comp_type = 0 AND season_id = ?",
                (max_season_id,),
            )
            if not leagues_rows:
                logger.warning("No leagues found, skipping player earnings update.")
                return

            # Group leagues by league_id
            leagues_by_id = {row['league_id']: row for row in leagues_rows}

            # Only fetch table_rows for national leagues in the current season so that
            # cup competition rows never shadow a club's national league entry.
            league_id_placeholders = ','.join('?' * len(leagues_by_id))
            table_rows = self.db.execute_query(
                'sqlite',
                f"SELECT * FROM table_rows WHERE league_id IN ({league_id_placeholders})",
                tuple(leagues_by_id.keys()),
            )

            # Build club-to-league and club-position lookups
            club_to_league = {}
            club_positions = {}
            for row in table_rows:
                club_id = row['club_id']
                club_to_league[club_id] = row['league_id']
                club_positions[club_id] = row['new_position'] or row['old_position'] or 1

            # Fetch player orders from share_orders
            orders_rows = self.db.execute_query('sqlite', "SELECT * FROM share_orders WHERE share_type = 'player'")
            orderbook_data = {}
            for row in orders_rows:
                # Format: [order_id, name, is_ask, price, num]
                order = [row['order_id'], row['name'], row['is_ask'], row['price'], row['num']]
                orderbook_data.setdefault(row['share_id'], []).append(order)

            # 2. Pass 1: Calculate baseline ratings (11th best player) for each club
            baseline_query = """
                SELECT club_id, MIN(rating) as baseline_rating
                FROM (
                    SELECT club_id, rating,
                           ROW_NUMBER() OVER (PARTITION BY club_id ORDER BY rating DESC) as rn
                    FROM players
                    WHERE club_id IS NOT NULL AND club_id > 0
                      AND rating IS NOT NULL AND rating > 0
                      AND retired = 0
                ) ranked
                WHERE rn <= 11
                GROUP BY club_id
            """
            baseline_rows = self.db.execute_query('sqlite', baseline_query)
            club_baseline_ratings = {row['club_id']: row['baseline_rating'] for row in baseline_rows}

            # 3. Pass 2: Query active players
            players_query = """
                SELECT player_id, wages, rating, country_id, club_id, dob, position
                FROM players
                WHERE club_id IS NOT NULL AND club_id > 0
                  AND wages IS NOT NULL AND wages > 0
                  AND rating IS NOT NULL AND rating > 0
                  AND retired = 0
            """
            players_rows = self.db.execute_query('sqlite', players_query)

            # Prepare staging table
            self._create_staging_table()

            # 4. Calculate and batch insert
            current_time = datetime.now().timestamp()
            
            insert_columns = [
                "player_id", "club_id", "club_country", "club_division", "club_position",
                "player_nationality", "player_age", "player_rating", "player_position",
                "match_time_percentage", "current_earnings"
            ]
            for t in THRESHOLDS:
                insert_columns.append(f"buyable_{t}s")
                insert_columns.append(f"cost_{t}s")

            insert_sql = f"""
                INSERT INTO `{self.staging_table}` 
                ({", ".join(f"`{c}`" for c in insert_columns)})
                VALUES ({", ".join(["%s"] * len(insert_columns))})
            """

            batch = []
            batch_size = 5000
            total_inserted = 0

            for player_dict in players_rows:
                player_id = player_dict['player_id']
                club_id = player_dict['club_id']
                wages = player_dict['wages']
                rating = player_dict['rating']
                country_id = player_dict['country_id']
                dob = player_dict['dob']
                position = player_dict['position']

                if club_id not in club_to_league:
                    continue

                league_id = club_to_league[club_id]
                league = leagues_by_id.get(league_id)
                if not league:
                    continue

                # Age calculation
                age = 0
                if dob:
                    age_seconds = current_time - dob
                    age = int(age_seconds / (365.25 * 24 * 3600))

                club_position = club_positions.get(club_id, 1)

                club_baseline = club_baseline_ratings.get(club_id, rating)
                smart_match_time = calculate_smart_match_time_percentage(rating, club_baseline)

                player = {
                    'wages': wages,
                    'rating': rating,
                }

                current_result = player_calculator.calculate_season_earnings(
                    player,
                    league,
                    club_position,
                    influence_amount=1,
                    match_time_percentage=smart_match_time
                )

                player_orders = orderbook_data.get(player_id, [])
                current_roi = _calculate_roi_by_payback(player_orders, current_result['total'])

                # Construct row data
                row_data = [
                    player_id,
                    club_id,
                    league['country_id'],
                    int(league['level']),
                    club_position,
                    country_id,
                    age,
                    rating,
                    position,
                    smart_match_time,
                    current_result['total']
                ]

                for t in THRESHOLDS:
                    row_data.append(current_roi[t]['buyable'])
                    row_data.append(current_roi[t]['cost'])

                batch.append(row_data)

                if len(batch) >= batch_size:
                    self.db.execute_many(self.db.dest_conn, insert_sql, batch)
                    total_inserted += len(batch)
                    batch = []

            # Insert remainder
            if batch:
                self.db.execute_many(self.db.dest_conn, insert_sql, batch)
                total_inserted += len(batch)

            # 5. Swap staging table
            self._atomic_swap_tables()

            elapsed = time.time() - start_time
            logger.info(f"Player earnings update completed: {total_inserted} players inserted in {elapsed:.2f}s.")

        except Exception as e:
            logger.error(f"Error updating player earnings: {e}")
            logger.debug(traceback.format_exc())
