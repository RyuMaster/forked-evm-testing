# File: updaters/datadump.py
import csv
import io
import json
import logging
import os
import tempfile
import traceback
from datetime import datetime, timedelta

from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport

from config import SV_SUBGRAPH_URL


logger = logging.getLogger(__name__)

SUBGRAPH_PAGE_SIZE = 1000

# Field lists matching the data_dumps_api format
SHARE_ORDER_FIELDS = ['order_id', 'name', 'is_ask', 'price', 'num']
LEAGUES_FIELDS = [
    'name', 'group', 'level', 'ticket_cost', 'tv_money', 'prize_money_pot',
    'ave_attendance', 'ave_club_rating_start', 'num_teams', 'round', 'num_rounds',
    'comp_type', 'country_id',
]
LEAGUE_TABLES_FIELDS = [
    'club_id', 'club_ix', 'played', 'won', 'drawn', 'lost', 'goals_for',
    'goals_against', 'pts', 'form', 'old_position', 'new_position',
    'fans_start', 'fans_current', 'stadium_size_current', 'manager_name',
]

THRESHOLDS = [2, 3, 4, 5, 6, 7, 8, 10, 12, 15, 20]

# Build field lists for earnings outputs dynamically to avoid repetition
def _earnings_fields(prefix_buyable, prefix_cost):
    fields = ['club_id', 'country', 'division', 'position', 'current_earnings']
    for t in THRESHOLDS:
        fields.append(f'current_{t}s_{prefix_buyable}')
        fields.append(f'current_{t}s_{prefix_cost}')
    fields.append('potential_earnings')
    for t in THRESHOLDS:
        fields.append(f'potential_{t}s_{prefix_buyable}')
        fields.append(f'potential_{t}s_{prefix_cost}')
    fields += ['pack_price_usdc', 'pack_payback_seasons', 'pack_tranche_remaining']
    return fields

CLUB_EARNINGS_FIELDS = _earnings_fields('buyable', 'cost')
CLUB_SELLER_OPPORTUNITIES_FIELDS = _earnings_fields('sellable', 'revenue')

CLUB_PACKS_FIELDS = [
    'club_id', 'country', 'division', 'position', 'current_earnings',
    'pack_price_usdc', 'pack_payback_seasons', 'pack_tranche_remaining',
    'primary_influence_per_pack', 'packs_available',
]


# ---------------------------------------------------------------------------
# Atomic file write helpers
# ---------------------------------------------------------------------------

def _atomic_write_json(path, data):
    """Write JSON data atomically using a temp file + rename."""
    dir_path = os.path.dirname(path)
    os.makedirs(dir_path, exist_ok=True)
    with tempfile.NamedTemporaryFile('w', dir=dir_path, delete=False, suffix='.tmp') as f:
        json.dump(data, f, separators=(',', ':'))
        tmp_path = f.name
    os.replace(tmp_path, path)


def _atomic_write_text(path, text):
    """Write text data atomically using a temp file + rename."""
    dir_path = os.path.dirname(path)
    os.makedirs(dir_path, exist_ok=True)
    with tempfile.NamedTemporaryFile('w', dir=dir_path, delete=False, suffix='.tmp',
                                     encoding='utf-8', newline='') as f:
        f.write(text)
        tmp_path = f.name
    os.replace(tmp_path, path)


# ---------------------------------------------------------------------------
# Earnings calculation (ported from data_dumps_api/earnings.py)
# ---------------------------------------------------------------------------

class EarningsCalculator:
    """Calculate club earnings for influence holders."""

    def __init__(self, game_params):
        self.PAYOUT_CLUB_INFLUENCE_DIVIDEND_BP = game_params['payout-club-influence-dividend-bp']
        self.ECONOMY_CLUB_PRIZEMONEY_PERCENTAGE_BP = game_params['economy-club-prizemoney-percentage-bp']
        self.ECONOMY_CLUB_SHAREHOLDERS_PRIZEPOT_AMOUNT_BP = game_params['economy-club-shareholders-prizepot-amount-bp']

    def calculate_season_earnings(self, club, league, influence_amount=1, position=None):
        results = {'matchday_payout': 0, 'league_prize': 0, 'total': 0}

        if not league:
            return results

        actual_position = position if position is not None else club.get('position', 1)

        if not all([
            club.get('stadium_size_current'),
            club.get('fans_current'),
            league.get('ticket_cost'),
            league.get('num_rounds'),
            league.get('tv_money') is not None,
        ]):
            return results

        avg_attendance = min(club['stadium_size_current'], club['fans_current'])
        ticket_price = league['ticket_cost']
        total_matches = league['num_rounds']
        home_matches = total_matches // 2
        away_matches = total_matches - home_matches

        gate_receipts = avg_attendance * ticket_price * 0.8
        sponsor = avg_attendance * ticket_price * 0.3
        merchandise = avg_attendance * ticket_price * 0.1
        tv_money = league['tv_money']

        home_game_revenue = gate_receipts + sponsor + merchandise + tv_money
        away_game_revenue = tv_money

        home_game_payout = (home_game_revenue * self.PAYOUT_CLUB_INFLUENCE_DIVIDEND_BP) / 10000
        away_game_payout = (away_game_revenue * self.PAYOUT_CLUB_INFLUENCE_DIVIDEND_BP) / 10000
        matchday_for_1m = (home_game_payout * home_matches) + (away_game_payout * away_matches)

        if not league.get('prize_money_pot') or not league.get('num_teams'):
            return results

        prize_for_1m = self._calculate_position_prize(
            league['prize_money_pot'], actual_position, league['num_teams']
        )

        influence_scale = influence_amount / 1000000
        results['matchday_payout'] = matchday_for_1m * influence_scale
        results['league_prize'] = prize_for_1m * influence_scale
        results['total'] = results['matchday_payout'] + results['league_prize']
        return results

    def _calculate_position_prize(self, prize_pot, position, num_teams):
        base_prize = (prize_pot / 2) / num_teams
        equal = 100 / num_teams
        perc_due = (num_teams - position) * equal
        pot_perc = (perc_due * 256 / 100) * (2 * equal)

        if pot_perc == 0:
            perc_due = 100 - (num_teams - 1) * equal
            pot_perc = (perc_due * 256 / 100) * (2 * equal)

        prize_money = (pot_perc / 100) * prize_pot
        prize_money = prize_money / 256
        prize_money = (prize_money * self.ECONOMY_CLUB_PRIZEMONEY_PERCENTAGE_BP) / 10000
        prize_money = prize_money + base_prize
        return (prize_money * self.ECONOMY_CLUB_SHAREHOLDERS_PRIZEPOT_AMOUNT_BP) / 10000

    def adjust_fanbase_for_division(self, club, current_division, target_division, target_league):
        if not club.get('fans_current'):
            return 0
        adjusted_fans = club['fans_current']
        if not target_league or not target_league.get('ave_attendance') or not club.get('fans_start'):
            return adjusted_fans
        avg_attendance = target_league['ave_attendance']
        if target_division == current_division:
            return adjusted_fans
        elif target_division < current_division:
            adjusted_fans = max(club['fans_start'], avg_attendance)
        else:
            if adjusted_fans > avg_attendance:
                diff = adjusted_fans - avg_attendance
                adjusted_fans = int(adjusted_fans - (diff / 2))
            else:
                adjusted_fans = avg_attendance
        return adjusted_fans


def _calculate_roi_by_payback(orders, earnings_per_influence):
    result = {t: {'buyable': 0, 'cost': 0} for t in THRESHOLDS}
    if not earnings_per_influence or earnings_per_influence <= 0:
        return result

    sell_orders = [o for o in orders if o[2] == 1]
    orders_with_payback = sorted(
        [{'price': o[3], 'amount': o[4], 'payback': o[3] / earnings_per_influence} for o in sell_orders],
        key=lambda x: x['payback']
    )

    for threshold in THRESHOLDS:
        buyable = cost = 0
        for o in orders_with_payback:
            if o['payback'] <= threshold:
                buyable += o['amount']
                cost += o['price'] * o['amount']
            else:
                break
        result[threshold] = {'buyable': buyable, 'cost': cost}
    return result


def _calculate_seller_opportunities_by_payback(orders, earnings_per_influence):
    result = {t: {'sellable': 0, 'revenue': 0} for t in THRESHOLDS}
    if not earnings_per_influence or earnings_per_influence <= 0:
        return result

    buy_orders = [o for o in orders if o[2] == 0]
    orders_with_payback = sorted(
        [{'price': o[3], 'amount': o[4], 'payback': o[3] / earnings_per_influence} for o in buy_orders],
        key=lambda x: x['payback'],
        reverse=True
    )

    for threshold in THRESHOLDS:
        sellable = revenue = 0
        for o in orders_with_payback:
            if o['payback'] >= threshold:
                sellable += o['amount']
                revenue += o['price'] * o['amount']
            else:
                break
        result[threshold] = {'sellable': sellable, 'revenue': revenue}
    return result


def _build_club_earnings_lookup(leagues_by_id, table_field_index, tables_data, calculator):
    """Build {club_id: earnings_per_influence} for all clubs."""
    lookup = {}
    for league_id_str, table_rows in tables_data.items():
        league = leagues_by_id.get(int(league_id_str))
        if not league:
            continue
        for club_row in table_rows:
            club_id = club_row[table_field_index['club_id']]
            position = club_row[table_field_index['new_position']] or club_row[table_field_index['old_position']] or 1
            club = {
                'club_id': club_id,
                'fans_current': club_row[table_field_index['fans_current']] or 0,
                'fans_start': club_row[table_field_index['fans_start']] or 0,
                'stadium_size_current': club_row[table_field_index['stadium_size_current']] or 0,
            }
            result = calculator.calculate_season_earnings(club, league, influence_amount=1, position=position)
            lookup[club_id] = result['total']
    return lookup


def _calculate_pack_pricing_data(club_id, pack_pricing, club_earnings_lookup, svc_price):
    """Return (pack_price_usdc, pack_payback_seasons, tranche_remaining, primary_influence_per_pack, packs_available)."""
    if club_id not in pack_pricing:
        return (0, 0, 0, 0, 0)

    pack_data = pack_pricing[club_id]
    pack_price_usdc = pack_data['price_per_pack'] / 1000000
    tranche_remaining = pack_data['tranche_remaining']
    shares = pack_data['shares']

    primary_influence_per_pack = 0
    for share_entry in shares:
        if share_entry.get('club_id') == club_id:
            primary_influence_per_pack = share_entry.get('amount', 0)
            break

    packs_available = int(tranche_remaining / primary_influence_per_pack) if primary_influence_per_pack > 0 else 0

    pack_payback_seasons = 0
    if svc_price:
        total_pack_earnings_raw = sum(
            club_earnings_lookup.get(s.get('club_id'), 0) * s.get('amount', 0)
            for s in shares
        )
        if total_pack_earnings_raw > 0:
            earnings_usdc_per_season = (total_pack_earnings_raw / 10000) * svc_price
            if earnings_usdc_per_season > 0:
                pack_payback_seasons = pack_price_usdc / earnings_usdc_per_season

    return (pack_price_usdc, pack_payback_seasons, tranche_remaining, primary_influence_per_pack, packs_available)


# ---------------------------------------------------------------------------
# DataDumpUpdater
# ---------------------------------------------------------------------------

class DataDumpUpdater:
    UPDATE_INTERVAL = 120  # seconds

    def __init__(self, db_manager, output_folder):
        self.db = db_manager
        self.output_folder = output_folder

    def update(self):
        """Run all dump operations."""
        clubs_data = None
        try:
            clubs_data = self._dump_share_orders()
        except Exception as e:
            logger.error(f"Error dumping share orders: {e}")
            logger.debug(traceback.format_exc())

        try:
            leagues_data, league_tables_data, max_season_id = self._dump_leagues()
        except Exception as e:
            logger.error(f"Error dumping leagues: {e}")
            logger.debug(traceback.format_exc())
            return  # Earnings dumps depend on league data

        # Fetch shared inputs for all earnings dumps
        svc_price = None
        try:
            svc_price = self._fetch_svc_price()
        except Exception as e:
            logger.error(f"Error fetching SVC price: {e}")
            logger.debug(traceback.format_exc())

        pack_pricing = {}
        try:
            pack_pricing = self._fetch_pack_pricing()
        except Exception as e:
            logger.error(f"Error fetching pack pricing: {e}")
            logger.debug(traceback.format_exc())

        game_params = None
        try:
            game_params = self._load_game_params()
        except Exception as e:
            logger.error(f"Error loading game params: {e}")
            logger.debug(traceback.format_exc())
            return  # All earnings dumps need game params

        calculator = EarningsCalculator(game_params)

        # Build shared league lookups (used by all earnings dumps)
        league_field_index = {f: i for i, f in enumerate(LEAGUES_FIELDS)}
        table_field_index = {f: i for i, f in enumerate(LEAGUE_TABLES_FIELDS)}

        leagues_by_id = {}
        for league_id_str, league_array in leagues_data.items():
            leagues_by_id[int(league_id_str)] = {
                'league_id': int(league_id_str),
                'country_id': league_array[league_field_index['name']],
                'division': int(league_array[league_field_index['level']]),
                'ticket_cost': league_array[league_field_index['ticket_cost']],
                'tv_money': league_array[league_field_index['tv_money']],
                'prize_money_pot': league_array[league_field_index['prize_money_pot']],
                'ave_attendance': league_array[league_field_index['ave_attendance']],
                'num_teams': league_array[league_field_index['num_teams']],
                'num_rounds': league_array[league_field_index['num_rounds']],
            }

        leagues_by_country_div = {
            f"{l['country_id']}-{l['division']}": l
            for l in leagues_by_id.values()
        }

        club_earnings_lookup = _build_club_earnings_lookup(
            leagues_by_id, table_field_index, league_tables_data, calculator
        )

        try:
            self._dump_club_earnings(
                clubs_data or {}, leagues_by_id, leagues_by_country_div,
                league_tables_data, table_field_index, calculator,
                club_earnings_lookup, pack_pricing, svc_price,
            )
        except Exception as e:
            logger.error(f"Error dumping club earnings: {e}")
            logger.debug(traceback.format_exc())

        try:
            self._dump_club_seller_opportunities(
                clubs_data or {}, leagues_by_id, leagues_by_country_div,
                league_tables_data, table_field_index, calculator,
                club_earnings_lookup, pack_pricing, svc_price,
            )
        except Exception as e:
            logger.error(f"Error dumping club seller opportunities: {e}")
            logger.debug(traceback.format_exc())

        try:
            self._dump_club_packs(
                leagues_by_id, league_tables_data, table_field_index, calculator,
                club_earnings_lookup, pack_pricing, svc_price,
            )
        except Exception as e:
            logger.error(f"Error dumping club packs: {e}")
            logger.debug(traceback.format_exc())

    # -----------------------------------------------------------------------
    # Shared data fetchers
    # -----------------------------------------------------------------------

    def _load_game_params(self):
        """Load game parameters from the SQLite parameters table."""
        rows = self.db.execute_query('sqlite', 'SELECT name, value FROM parameters')
        params = {row['name']: row['value'] for row in rows}
        logger.info(f"DataDumpUpdater: Loaded {len(params)} game parameters.")
        return params

    def _fetch_svc_price(self):
        """Compute VWAP SVC/USDC price from the svc_trades MySQL table.

        Uses the last 24 hours of trades.  If there are none, falls back to
        all trades on the calendar date of the most recent trade.
        Returns None if no usable trades are found.
        """
        # Try last 24 hours first
        rows = self.db.execute_query(
            self.db.dest_conn,
            """
            SELECT SUM(volume_svc) AS total_svc, SUM(volume_usdc) AS total_usdc
            FROM svc_trades
            WHERE trade_ts >= NOW() - INTERVAL 24 HOUR
              AND price_usdc IS NOT NULL
              AND volume_usdc IS NOT NULL
              AND volume_svc > 0
            """,
        )
        if rows and rows[0]['total_svc'] and rows[0]['total_svc'] > 0:
            vwap = rows[0]['total_usdc'] / rows[0]['total_svc']
            logger.info(f"DataDumpUpdater: SVC VWAP (last 24h) = {vwap:.8f} USDC")
            return vwap

        # Fall back: find the calendar date of the last trade and use that whole day
        date_rows = self.db.execute_query(
            self.db.dest_conn,
            """
            SELECT DATE(trade_ts) AS last_date
            FROM svc_trades
            WHERE price_usdc IS NOT NULL AND volume_usdc IS NOT NULL AND volume_svc > 0
            ORDER BY trade_ts DESC
            LIMIT 1
            """,
        )
        if not date_rows or not date_rows[0]['last_date']:
            logger.warning("DataDumpUpdater: No usable SVC trades found for price calculation.")
            return None

        last_date = date_rows[0]['last_date']
        rows = self.db.execute_query(
            self.db.dest_conn,
            """
            SELECT SUM(volume_svc) AS total_svc, SUM(volume_usdc) AS total_usdc
            FROM svc_trades
            WHERE DATE(trade_ts) = %s
              AND price_usdc IS NOT NULL
              AND volume_usdc IS NOT NULL
              AND volume_svc > 0
            """,
            (last_date,),
        )
        if rows and rows[0]['total_svc'] and rows[0]['total_svc'] > 0:
            vwap = rows[0]['total_usdc'] / rows[0]['total_svc']
            logger.info(f"DataDumpUpdater: SVC VWAP (fallback date {last_date}) = {vwap:.8f} USDC")
            return vwap

        logger.warning("DataDumpUpdater: Could not compute SVC VWAP.")
        return None

    def _fetch_pack_pricing(self):
        """Fetch pack pricing data from The Graph subgraph.

        Returns dict: {club_id: {'price_per_pack': int, 'tranche_remaining': int, 'shares': list}}
        """
        if not SV_SUBGRAPH_URL:
            logger.warning("DataDumpUpdater: SV_SUBGRAPH_URL not configured, skipping pack pricing.")
            return {}

        query = gql(f"""
        query GetPackPricing($lastId: Bytes) {{
          saleClubs(
            first: {SUBGRAPH_PAGE_SIZE}
            where: {{
              tier_not: null
              id_gt: $lastId
            }}
            orderBy: id
          ) {{
            id
            clubId
            remainingInTranche
            primaryPack {{
              cost
              shares {{
                club {{
                  clubId
                }}
                num
              }}
            }}
          }}
        }}
        """)

        transport = RequestsHTTPTransport(url=SV_SUBGRAPH_URL, timeout=10, retries=3)
        client = Client(transport=transport, fetch_schema_from_transport=False)

        pack_pricing = {}
        last_id = "0x"

        while True:
            result = client.execute(query, variable_values={"lastId": last_id})
            sale_clubs = result.get('saleClubs', [])
            if not sale_clubs:
                break

            for club in sale_clubs:
                primary_pack = club.get('primaryPack')
                if not primary_pack:
                    continue
                club_id = club['clubId']
                shares = [
                    {'club_id': s['club']['clubId'], 'amount': s['num']}
                    for s in primary_pack.get('shares', [])
                ]
                pack_pricing[club_id] = {
                    'price_per_pack': int(primary_pack['cost']),
                    'tranche_remaining': club['remainingInTranche'],
                    'shares': shares,
                }
                last_id = club['id']

            if len(sale_clubs) < SUBGRAPH_PAGE_SIZE:
                break

        logger.info(f"DataDumpUpdater: Fetched pack pricing for {len(pack_pricing)} clubs.")
        return pack_pricing

    # -----------------------------------------------------------------------
    # Dump methods
    # -----------------------------------------------------------------------

    def _dump_share_orders(self):
        """Dump share_orders to CSV and combined JSON.  Returns clubs_data dict."""
        logger.info("DataDumpUpdater: Dumping share orders...")

        rows = self.db.execute_query('sqlite', "SELECT * FROM share_orders") or []

        if rows:
            all_columns = list(rows[0].keys())
        else:
            cols_info = self.db.execute_query('sqlite', "PRAGMA table_info(share_orders)")
            all_columns = [c['name'] for c in cols_info]

        clubs_data = {}
        players_data = {}
        all_grouped = {'clubs': {}, 'players': {}}

        for row in rows:
            share_type = row.get('share_type', 'unknown')
            share_id = row['share_id']
            compact_order = [row['order_id'], row['name'], row['is_ask'], row['price'], row['num']]

            if share_type == 'club':
                clubs_data.setdefault(share_id, []).append(compact_order)
                all_grouped['clubs'].setdefault(share_id, []).append(compact_order)
            elif share_type == 'player':
                players_data.setdefault(share_id, []).append(compact_order)
                all_grouped['players'].setdefault(share_id, []).append(compact_order)

        updated = datetime.now().isoformat()
        meta = {'fields': SHARE_ORDER_FIELDS, 'updated': updated}

        csv_path = os.path.join(self.output_folder, 'share_orders.csv')
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(all_columns)
        for row in rows:
            writer.writerow([row.get(col) for col in all_columns])
        _atomic_write_text(csv_path, buf.getvalue())

        json_path = os.path.join(self.output_folder, 'share_orders.json')
        _atomic_write_json(json_path, {'meta': meta, 'data': all_grouped})

        logger.info(
            f"DataDumpUpdater: Share orders dumped ({len(rows)} rows, "
            f"{len(clubs_data)} clubs, {len(players_data)} players)."
        )
        return clubs_data

    def _dump_leagues(self):
        """Dump leagues and league tables to JSON.  Returns (leagues_data, league_tables_data, max_season_id)."""
        logger.info("DataDumpUpdater: Dumping leagues and league tables...")

        max_season_result = self.db.execute_query(
            'sqlite', "SELECT MAX(season_id) AS max_season FROM leagues"
        )
        if not max_season_result or max_season_result[0]['max_season'] is None:
            logger.warning("DataDumpUpdater: No seasons found in leagues table, skipping.")
            return {}, {}, None
        max_season_id = max_season_result[0]['max_season']

        leagues_rows = self.db.execute_query(
            'sqlite',
            "SELECT * FROM leagues WHERE season_id = ? AND comp_type = 0 ORDER BY country_id, level",
            (max_season_id,),
        )

        leagues_data = {}
        league_tables_data = {}

        for league_row in leagues_rows:
            league_id = league_row['league_id']
            leagues_data[league_id] = [
                league_row['name'],
                league_row['group'],
                league_row['level'],
                league_row['ticket_cost'],
                league_row['tv_money'],
                league_row['prize_money_pot'],
                league_row['ave_attendance'],
                league_row['ave_club_rating_start'],
                league_row['num_teams'],
                league_row['round'],
                league_row['num_rounds'],
                league_row['comp_type'],
                league_row['country_id'],
            ]

            table_rows = self.db.execute_query(
                'sqlite',
                """
                SELECT tr.*, c.fans_start, c.fans_current, c.stadium_size_current, c.manager_name
                FROM table_rows tr
                LEFT JOIN clubs c ON tr.club_id = c.club_id
                WHERE tr.league_id = ?
                ORDER BY tr.new_position
                """,
                (league_id,),
            )

            if table_rows:
                league_tables_data[league_id] = [
                    [
                        r['club_id'], r['club_ix'], r['played'], r['won'], r['drawn'], r['lost'],
                        r['goals_for'], r['goals_against'], r['pts'], r['form'],
                        r['old_position'], r['new_position'],
                        r['fans_start'], r['fans_current'], r['stadium_size_current'], r['manager_name'],
                    ]
                    for r in table_rows
                ]

        updated = datetime.now().isoformat()

        _atomic_write_json(os.path.join(self.output_folder, 'leagues.json'), {
            'meta': {'fields': LEAGUES_FIELDS, 'season_id': max_season_id, 'updated': updated},
            'data': leagues_data,
        })

        # league_tables.json contains all divisions in flat format: {league_id: [[row,...], ...]}
        _atomic_write_json(os.path.join(self.output_folder, 'league_tables.json'), {
            'meta': {'fields': LEAGUE_TABLES_FIELDS, 'season_id': max_season_id, 'updated': updated},
            'data': {str(lid): teams for lid, teams in league_tables_data.items()},
        })

        logger.info(
            f"DataDumpUpdater: Leagues dumped ({len(leagues_data)} leagues, "
            f"{len(league_tables_data)} with table rows)."
        )
        return leagues_data, league_tables_data, max_season_id

    def _dump_club_earnings(
        self, clubs_data, leagues_by_id, leagues_by_country_div,
        league_tables_data, table_field_index, calculator,
        club_earnings_lookup, pack_pricing, svc_price,
    ):
        logger.info("DataDumpUpdater: Calculating club earnings...")
        all_club_earnings = {}
        total_clubs = 0

        for league_id_str, table_rows in league_tables_data.items():
            league = leagues_by_id.get(int(league_id_str))
            if not league:
                continue
            d1_league = leagues_by_country_div.get(f"{league['country_id']}-0")

            for club_row in table_rows:
                total_clubs += 1
                club_id = club_row[table_field_index['club_id']]
                position = club_row[table_field_index['new_position']] or club_row[table_field_index['old_position']] or 1
                club = {
                    'club_id': club_id,
                    'country_id': league['country_id'],
                    'division': league['division'],
                    'position': position,
                    'fans_start': club_row[table_field_index['fans_start']] or 0,
                    'fans_current': club_row[table_field_index['fans_current']] or 0,
                    'stadium_size_current': club_row[table_field_index['stadium_size_current']] or 0,
                }

                current_result = calculator.calculate_season_earnings(
                    club, league, influence_amount=1, position=position
                )

                potential_result = {'total': 0}
                if d1_league:
                    adjusted_fans = calculator.adjust_fanbase_for_division(
                        club, league['division'], 0, d1_league
                    )
                    adjusted_club = {
                        **club,
                        'fans_current': adjusted_fans,
                        'stadium_size_current': max(adjusted_fans, club['stadium_size_current']),
                    }
                    potential_result = calculator.calculate_season_earnings(
                        adjusted_club, d1_league, influence_amount=1, position=1
                    )

                club_orders = clubs_data.get(club_id, clubs_data.get(str(club_id), []))
                current_roi = _calculate_roi_by_payback(club_orders, current_result['total'])
                potential_roi = _calculate_roi_by_payback(club_orders, potential_result['total'])

                pack_price_usdc, pack_payback_seasons, tranche_remaining, _, _ = \
                    _calculate_pack_pricing_data(club_id, pack_pricing, club_earnings_lookup, svc_price)

                response = [
                    club_id, league['country_id'], league['division'], position,
                    current_result['total'],
                ]
                for t in THRESHOLDS:
                    response.append(current_roi[t]['buyable'])
                    response.append(current_roi[t]['cost'])
                response.append(potential_result['total'])
                for t in THRESHOLDS:
                    response.append(potential_roi[t]['buyable'])
                    response.append(potential_roi[t]['cost'])
                response += [pack_price_usdc, pack_payback_seasons, tranche_remaining]

                all_club_earnings[str(club_id)] = response

        _atomic_write_json(os.path.join(self.output_folder, 'club_earnings.json'), {
            'meta': {
                'fields': CLUB_EARNINGS_FIELDS,
                'description': (
                    'Pre-calculated club earnings for 1 influence unit per club. '
                    'All earnings and cost values are raw (not divided by 10000). '
                    'Potential earnings assume D1P1 (Division 1, Position 1).'
                ),
                'updated': datetime.now().isoformat(),
                'total_clubs': total_clubs,
            },
            'data': all_club_earnings,
        })
        current_earnings_idx = CLUB_EARNINGS_FIELDS.index('current_earnings')
        all_club_earnings_light = {
            club_id: row[current_earnings_idx]
            for club_id, row in all_club_earnings.items()
        }
        _atomic_write_json(os.path.join(self.output_folder, 'club_earnings_light.json'), {
            'meta': {
                'description': (
                    'SVC generated per influence per season '
                    '(from league matchday revenue + league prize). '
                    'All values are raw (not divided by 10000).'
                ),
                'updated': datetime.now().isoformat(),
                'total_clubs': total_clubs,
            },
            'data': all_club_earnings_light,
        })
        logger.info(f"DataDumpUpdater: Club earnings dumped ({total_clubs} clubs).")

    def _dump_club_seller_opportunities(
        self, clubs_data, leagues_by_id, leagues_by_country_div,
        league_tables_data, table_field_index, calculator,
        club_earnings_lookup, pack_pricing, svc_price,
    ):
        logger.info("DataDumpUpdater: Calculating club seller opportunities...")
        all_seller_opps = {}
        total_clubs = 0

        for league_id_str, table_rows in league_tables_data.items():
            league = leagues_by_id.get(int(league_id_str))
            if not league:
                continue
            d1_league = leagues_by_country_div.get(f"{league['country_id']}-0")

            for club_row in table_rows:
                total_clubs += 1
                club_id = club_row[table_field_index['club_id']]
                position = club_row[table_field_index['new_position']] or club_row[table_field_index['old_position']] or 1
                club = {
                    'club_id': club_id,
                    'country_id': league['country_id'],
                    'division': league['division'],
                    'position': position,
                    'fans_start': club_row[table_field_index['fans_start']] or 0,
                    'fans_current': club_row[table_field_index['fans_current']] or 0,
                    'stadium_size_current': club_row[table_field_index['stadium_size_current']] or 0,
                }

                current_result = calculator.calculate_season_earnings(
                    club, league, influence_amount=1, position=position
                )

                potential_result = {'total': 0}
                if d1_league:
                    adjusted_fans = calculator.adjust_fanbase_for_division(
                        club, league['division'], 0, d1_league
                    )
                    adjusted_club = {
                        **club,
                        'fans_current': adjusted_fans,
                        'stadium_size_current': max(adjusted_fans, club['stadium_size_current']),
                    }
                    potential_result = calculator.calculate_season_earnings(
                        adjusted_club, d1_league, influence_amount=1, position=1
                    )

                club_orders = clubs_data.get(club_id, clubs_data.get(str(club_id), []))
                current_seller = _calculate_seller_opportunities_by_payback(club_orders, current_result['total'])
                potential_seller = _calculate_seller_opportunities_by_payback(club_orders, potential_result['total'])

                pack_price_usdc, pack_payback_seasons, tranche_remaining, _, _ = \
                    _calculate_pack_pricing_data(club_id, pack_pricing, club_earnings_lookup, svc_price)

                response = [
                    club_id, league['country_id'], league['division'], position,
                    current_result['total'],
                ]
                for t in THRESHOLDS:
                    response.append(current_seller[t]['sellable'])
                    response.append(current_seller[t]['revenue'])
                response.append(potential_result['total'])
                for t in THRESHOLDS:
                    response.append(potential_seller[t]['sellable'])
                    response.append(potential_seller[t]['revenue'])
                response += [pack_price_usdc, pack_payback_seasons, tranche_remaining]

                all_seller_opps[str(club_id)] = response

        _atomic_write_json(os.path.join(self.output_folder, 'club_seller_opportunities.json'), {
            'meta': {
                'fields': CLUB_SELLER_OPPORTUNITIES_FIELDS,
                'description': (
                    'Pre-calculated club seller opportunities for 1 influence unit per club. '
                    'Shows buy orders with payback >= threshold. '
                    'All earnings and revenue values are raw (not divided by 10000). '
                    'Potential earnings assume D1P1 (Division 1, Position 1).'
                ),
                'updated': datetime.now().isoformat(),
                'total_clubs': total_clubs,
            },
            'data': all_seller_opps,
        })
        logger.info(f"DataDumpUpdater: Club seller opportunities dumped ({total_clubs} clubs).")

    def _dump_club_packs(
        self, leagues_by_id, league_tables_data, table_field_index, calculator,
        club_earnings_lookup, pack_pricing, svc_price,
    ):
        logger.info("DataDumpUpdater: Calculating club packs...")
        all_club_packs = {}
        total_clubs = 0

        for league_id_str, table_rows in league_tables_data.items():
            league = leagues_by_id.get(int(league_id_str))
            if not league:
                continue

            for club_row in table_rows:
                total_clubs += 1
                club_id = club_row[table_field_index['club_id']]
                position = club_row[table_field_index['new_position']] or club_row[table_field_index['old_position']] or 1
                club = {
                    'club_id': club_id,
                    'fans_current': club_row[table_field_index['fans_current']] or 0,
                    'fans_start': club_row[table_field_index['fans_start']] or 0,
                    'stadium_size_current': club_row[table_field_index['stadium_size_current']] or 0,
                }

                current_result = calculator.calculate_season_earnings(
                    club, league, influence_amount=1, position=position
                )

                pack_price_usdc, pack_payback_seasons, tranche_remaining, primary_influence_per_pack, packs_available = \
                    _calculate_pack_pricing_data(club_id, pack_pricing, club_earnings_lookup, svc_price)

                if pack_price_usdc > 0 and tranche_remaining > 0:
                    all_club_packs[str(club_id)] = [
                        club_id, league['country_id'], league['division'], position,
                        current_result['total'],
                        pack_price_usdc, pack_payback_seasons, tranche_remaining,
                        primary_influence_per_pack, packs_available,
                    ]

        _atomic_write_json(os.path.join(self.output_folder, 'club_packs.json'), {
            'meta': {
                'fields': CLUB_PACKS_FIELDS,
                'description': (
                    'Club pack deals. Only includes clubs with packs currently available '
                    '(tranche_remaining > 0). Pack price is the TOTAL cost in USDC. '
                    'Current earnings are per influence per season (raw, divide by 10000 for display).'
                ),
                'updated': datetime.now().isoformat(),
                'total_packs': len(all_club_packs),
                'total_clubs_scanned': total_clubs,
            },
            'data': all_club_packs,
        })
        logger.info(
            f"DataDumpUpdater: Club packs dumped "
            f"({len(all_club_packs)} packs from {total_clubs} clubs scanned)."
        )
