# club_info_updater.py
import logging
from datetime import datetime
from .trade_updater import TradeUpdaterBase
from tqdm import tqdm

class ClubInfoUpdater(TradeUpdaterBase):
    def __init__(self, db_manager):
        component_name = 'club_info_updater'
        key_column = 'club_id'
        dest_table = 'dc_club_info'
        update_columns = [
            'balance', 'available', 'country_id', 'avg_wages', 'total_wages',
            'total_player_value', 'avg_player_rating', 'avg_player_rating_top21',  # Changed from avg_player_rating_top25
            'avg_shooting', 'avg_passing', 'avg_tackling', 'gk_rating', 'league_id',
            'manager_name', 'division'
        ]
        super().__init__(db_manager, component_name, key_column, dest_table, update_columns)
    
    def update_club_info(self, shutdown_flag, club_ids_to_update=None):
        try:
            self.get_update_status()
            # Create the table if it doesn't exist
            columns = self.get_columns_for_table()
            self.create_table(columns)

            if club_ids_to_update:
                logging.info(f"Updating club info for club_ids: {club_ids_to_update}")
                self.process_club_info(shutdown_flag, club_ids_to_update=club_ids_to_update)
            else:
                logging.info("Updating club info for all clubs...")
                self.process_club_info(shutdown_flag)
            self.update_status(0, full_update=True)  # Use 0 as we don't track specific heights

        except Exception as e:
            logging.error(f"An error occurred in club_info update: {e}")
            raise

    def process_club_info(self, shutdown_flag, club_ids_to_update=None):
        # Fetch all required data
        league_data = self.get_league_data()  # Adjusted to use table_rows
        if league_data is None:
            logging.error("No league data found. Exiting process_club_info.")
            return

        clubs_data = self.get_clubs_data(club_ids=club_ids_to_update)
        players_data = self.get_players_data(club_ids=club_ids_to_update)

        # Process the data
        rows_to_update = []
        total_clubs = len(clubs_data)
        batch_size = 100  # Process 100 clubs at a time

        with tqdm(total=total_clubs, desc=f"Processing {self.dest_table}", unit="clubs") as pbar:
            for i in range(0, total_clubs, batch_size):
                if shutdown_flag.is_set():
                    logging.info("Shutdown signal received. Exiting process_club_info early.")
                    break

                batch_clubs = clubs_data[i:i+batch_size]
                for club in batch_clubs:
                    club_id = club['club_id']
                    club_players = [p for p in players_data if p['club_id'] == club_id]

                    # Get league_id and division using the latest season
                    league_id, division = self.get_league_id_and_division(club, league_data)

                    # Calculate gk_rating
                    gk_rating = self.get_top_gk_rating(club_players)

                    row = {
                        'club_id': club_id,
                        'balance': club['balance'],
                        'available': self.is_club_available(club),
                        'country_id': club['country_id'],
                        'avg_wages': self.calculate_avg_wages(club_players),
                        'total_wages': self.calculate_total_wages(club_players),
                        'total_player_value': self.calculate_total_player_value(club_players),
                        'avg_player_rating': self.calculate_avg_player_rating(club_players),
                        'avg_player_rating_top21': self.calculate_avg_player_rating_top21(club_players),  # Changed method call
                        'avg_shooting': self.calculate_avg_top5(club_players, 'rating_shooting'),
                        'avg_passing': self.calculate_avg_top5(club_players, 'rating_passing'),
                        'avg_tackling': self.calculate_avg_top5(club_players, 'rating_tackling'),
                        'gk_rating': gk_rating,
                        'league_id': league_id,
                        'manager_name': club['manager_name'],
                        'division': division
                    }
                    rows_to_update.append(row)
                pbar.update(len(batch_clubs))

        # Update the database
        if rows_to_update:
            self.update_club_info_table(rows_to_update, shutdown_flag)
        else:
            logging.info("No clubs to update in update_club_info_table.")

    def get_clubs_data(self, club_ids=None):
        if club_ids:
            placeholders = ','.join(['%s'] * len(club_ids))
            query = f"""
            SELECT c.club_id, c.balance, c.manager_name, c.country_id, u.last_active
            FROM dc_clubs c
            LEFT JOIN dc_users u ON c.manager_name = u.name
            WHERE c.club_id IN ({placeholders})
            """
            params = club_ids
        else:
            query = """
            SELECT c.club_id, c.balance, c.manager_name, c.country_id, u.last_active
            FROM dc_clubs c
            LEFT JOIN dc_users u ON c.manager_name = u.name
            """
            params = ()

        return self.db.execute_query(self.db.dest_conn, query, params)

    def get_players_data(self, club_ids=None):
        if club_ids:
            placeholders = ','.join(['%s'] * len(club_ids))
            query = f"""
            SELECT club_id, wages, value, rating, rating_shooting, rating_passing, rating_tackling, position,
                rating_gk
            FROM dc_players
            WHERE club_id IN ({placeholders})
            """
            params = club_ids
        else:
            query = """
            SELECT club_id, wages, value, rating, rating_shooting, rating_passing, rating_tackling, position,
                rating_gk
            FROM dc_players
            """
            params = ()
        return self.db.execute_query(self.db.dest_conn, query, params)

    def get_league_data(self):
        # Get the maximum season_id from table_rows
        season_id_result = self.db.execute_query('sqlite', "SELECT MAX(season_id) AS max_season_id FROM table_rows")
        max_season_id = season_id_result[0]['max_season_id'] if season_id_result else None
        if max_season_id is None:
            logging.error("No season data found in table_rows table.")
            return None

        # Fetch table_rows data for the latest season_id
        table_rows_query = """
        SELECT club_id, league_id
        FROM table_rows
        WHERE season_id = ?
        """
        table_rows_data = self.db.execute_query('sqlite', table_rows_query, (max_season_id,))

        # Fetch leagues data
        leagues_query = """
        SELECT league_id, level
        FROM leagues
        """
        leagues_data = self.db.execute_query('sqlite', leagues_query)

        return {
            'table_rows': table_rows_data,
            'leagues': leagues_data,
            'season_id': max_season_id
        }

    def get_league_id_and_division(self, club, league_data):
        table_rows = league_data['table_rows']
        leagues = league_data['leagues']

        # Find league_id for the club
        table_row = next((tr for tr in table_rows if tr['club_id'] == club['club_id']), None)
        if not table_row:
            logging.warning(f"No table_row found for club_id: {club['club_id']} in season {league_data['season_id']}.")
            return None, None  # Cannot proceed without league_id

        league_id = table_row['league_id']

        # Find division (level) from leagues table
        league = next((l for l in leagues if l['league_id'] == league_id), None)
        if not league:
            logging.warning(f"No league found for league_id: {league_id}.")
            return league_id, None  # Return league_id but no division

        division = league['level']
        return league_id, division

    def is_club_available(self, club):
        if club['manager_name'] is None:
            return True
        if club['last_active'] is None:
            return False  # Assume unavailable if last_active is not set
        return club['last_active'] < datetime.now().timestamp() - (14 * 24 * 60 * 60)

    def calculate_avg_wages(self, players):
        if not players:
            return 0
        return round(sum(p['wages'] for p in players) / len(players))

    def calculate_total_wages(self, players):
        return sum(p['wages'] for p in players)

    def calculate_total_player_value(self, players):
        return sum(p['value'] for p in players)

    def calculate_avg_player_rating(self, players):
        if not players:
            return 0
        return round(sum(p['rating'] for p in players) / len(players), 2)

    def calculate_avg_player_rating_top21(self, players):  # Changed method name
        if not players:
            return 0
        top_21 = sorted(players, key=lambda p: p['rating'], reverse=True)[:21]  # Changed to top 21 players
        return round(sum(p['rating'] for p in top_21) / len(top_21), 2)

    def calculate_avg_top5(self, players, rating_column):
        if not players:
            return 0
        top_5 = sorted(players, key=lambda p: p[rating_column], reverse=True)[:5]
        return round(sum(p[rating_column] for p in top_5) / len(top_5), 2)

    def get_top_gk_rating(self, players):
        gk_ratings = [p['rating_gk'] for p in players if p['rating_gk'] not in (None, 0)]
        if not gk_ratings:
            logging.debug("No players with rating_gk found.")
            return 0
        top_gk_rating = max(gk_ratings)
        logging.debug(f"Top GK rating: {top_gk_rating}")
        return top_gk_rating

    def update_club_info_table(self, rows_to_update, shutdown_flag):
        columns = ['club_id'] + self.update_columns
        insert_query = self.prepare_insert_query(columns)

        batch_size = 100  # Adjust batch size if needed
        total_rows = len(rows_to_update)

        with tqdm(total=total_rows, desc=f"Updating {self.dest_table}", unit="rows") as pbar:
            for i in range(0, total_rows, batch_size):
                if shutdown_flag.is_set():
                    logging.info("Shutdown signal received. Exiting update_club_info_table early.")
                    break

                batch = rows_to_update[i:i+batch_size]
                try:
                    self.db.execute_many(self.db.dest_conn, insert_query, [tuple(row[col] for col in columns) for row in batch])
                    pbar.update(len(batch))
                except Exception as e:
                    logging.error(f"Error executing batch query: {e}")
                    raise

        logging.info(f"Successfully updated {self.dest_table} table with {total_rows} rows.")

    def prepare_insert_query(self, columns):
        columns_str = ', '.join(f'`{col}`' for col in columns)
        values_str = ', '.join(['%s'] * len(columns))
        update_str = ', '.join(f'`{col}` = VALUES(`{col}`)' for col in columns if col != 'club_id')

        return f"""
        INSERT INTO `{self.dest_table}` ({columns_str})
        VALUES ({values_str})
        ON DUPLICATE KEY UPDATE
        {update_str}
        """

    def get_columns_for_table(self):
        columns = {
            self.key_column: 'INT',
            'balance': 'BIGINT',
            'available': 'TINYINT(1)',
            'country_id': 'VARCHAR(3)',
            'avg_wages': 'BIGINT',
            'total_wages': 'BIGINT',
            'total_player_value': 'BIGINT',
            'avg_player_rating': 'DECIMAL(5, 2)',
            'avg_player_rating_top21': 'DECIMAL(5, 2)',  # Changed from avg_player_rating_top25
            'avg_shooting': 'DECIMAL(5, 2)',
            'avg_passing': 'DECIMAL(5, 2)',
            'avg_tackling': 'DECIMAL(5, 2)',
            'gk_rating': 'DECIMAL(5, 2)',
            'league_id': 'INT',
            'manager_name': 'VARCHAR(255)',
            'division': 'INT',
            'updated_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'
        }
        return columns