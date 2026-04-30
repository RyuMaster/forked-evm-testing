import logging
from datetime import datetime, timedelta, date, time
import requests

from config import POLYGON_STATS_SUBGRAPH_URL, GAME_ID
from db_manager import CHARSET, COLLATION
from user_enrichment import UserEnrichmentProvider

class ManagerActivityUpdater:
    def __init__(self, db_manager, user_enrichment_provider: UserEnrichmentProvider):
        self.db = db_manager
        self.user_enrichment_provider = user_enrichment_provider
        self.dest_table = "dc_manager_activity"
        # Backfill starting date is January 9th, 2025
        self.backfill_start_date = date(2025, 1, 9)
        self.subgraph_url = POLYGON_STATS_SUBGRAPH_URL

    def create_table_if_not_exists(self):
        create_query = f"""
        CREATE TABLE IF NOT EXISTS `{self.dest_table}` (
            `activity_date` DATE PRIMARY KEY,
            `total_unlocked` INT,
            `active_last_2_weeks` INT,
            `actual_total_unlocked` INT DEFAULT NULL,
            `actual_active_last_2_weeks` INT DEFAULT NULL,
            `actual_active_users` INT DEFAULT NULL,
            `NumberOfUsers` INT DEFAULT NULL,
            `NumberAgents` INT DEFAULT NULL,
            `NumberManagers` INT DEFAULT NULL,
            `ActiveUsers` INT DEFAULT NULL,
            `ActiveManagers` INT DEFAULT NULL,
            `InactiveManagers` INT DEFAULT NULL,
            `NumberManagersLocked` INT DEFAULT NULL,
            `NumberManagersUnlocked` INT DEFAULT NULL,
            `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET={CHARSET} COLLATE={COLLATION}
        """
        self.db.execute_query(self.db.dest_conn, create_query)
        logging.info(f"Ensured table `{self.dest_table}` exists.")

    def ensure_extra_columns_exist(self):
        existing = self.db.get_existing_columns(self.dest_table)
        extra_columns = {
            "NumberOfUsers": "INT DEFAULT NULL",
            "NumberAgents": "INT DEFAULT NULL",
            "NumberManagers": "INT DEFAULT NULL",
            "ActiveUsers": "INT DEFAULT NULL",
            "ActiveManagers": "INT DEFAULT NULL",
            "InactiveManagers": "INT DEFAULT NULL",
            "NumberManagersLocked": "INT DEFAULT NULL",
            "NumberManagersUnlocked": "INT DEFAULT NULL",
            "actual_total_unlocked": "INT DEFAULT NULL",
            "actual_active_last_2_weeks": "INT DEFAULT NULL",
            "actual_active_users": "INT DEFAULT NULL",
        }
        for col, col_def in extra_columns.items():
            if col not in existing:
                alter_query = f"ALTER TABLE `{self.dest_table}` ADD COLUMN `{col}` {col_def}"
                self.db.execute_query(self.db.dest_conn, alter_query)
                logging.info(f"Added column `{col}` to `{self.dest_table}`.")

    def get_last_activity_date(self):
        query = f"SELECT MAX(activity_date) AS last_date FROM `{self.dest_table}`"
        result = self.db.execute_query(self.db.dest_conn, query)
        if result and result[0]['last_date']:
            last_date = result[0]['last_date']
            # If last_date is a string, convert it; if already a date, return it directly.
            if isinstance(last_date, str):
                return datetime.strptime(last_date, "%Y-%m-%d").date()
            elif isinstance(last_date, (datetime, date)):
                return last_date if isinstance(last_date, date) else last_date.date()
        return None

    def load_unlocked_managers(self):
        # Query the archival (SOURCE) database for unlocked manager messages.
        # messages table holds type, name_1, and height; blocks has height and date.
        query = """
        SELECT m.name_1 AS name, MIN(b.date) AS unlock_date
        FROM messages m
        JOIN blocks b ON m.height = b.height
        WHERE m.type = 504
        GROUP BY m.name_1
        """
        results = self.db.execute_query('source', query)
        unlocked = {}
        for row in results:
            name = row['name']
            try:
                unlock_date = int(row['unlock_date'])
            except Exception:
                continue
            unlocked[name] = unlock_date
        logging.info(f"Loaded {len(unlocked)} unlocked managers from archival.")
        return unlocked



    def query_distinct_names_in_range(self, start_ts, end_ts):
        """
        Query the subgraph for all distinct names that have game moves
        within the given timestamp range [start_ts, end_ts].

        Uses pagination strategy:
        1. Query for rows with timestamp > (last timestamp), ordered by timestamp
        2. Deduplicate names we've already seen (just keep a set of all the distinct names)
        3. Continue with the last entry's timestamp as "last timestamp"
        4. In case a full page is returned with a single timestamp (first and last entries'
           timestamps match), we run a temporary special procedure that queries for all
           entries with this exact timestamp, ordered by ID, and paginates by ID
        """
        names = set()
        page_size = 1000
        last_timestamp = start_ts - 1  # Start just before our range

        while True:
            query = f"""
            {{
              gameMoves(
                first: {page_size},
                orderBy: tx__timestamp,
                orderDirection: asc,
                where: {{
                  tx_: {{ timestamp_gt: "{last_timestamp}", timestamp_lte: "{end_ts}" }},
                  game_: {{ game: "{GAME_ID}" }}
                }}
              ) {{
                id
                tx {{
                  timestamp
                }}
                move {{
                  name {{
                    name
                  }}
                }}
              }}
            }}
            """

            resp = requests.post(self.subgraph_url, json={"query": query}, timeout=30)
            js = resp.json()
            if "errors" in js:
                raise Exception(f"GraphQL error: {js['errors']}")

            game_moves = js["data"]["gameMoves"]
            if not game_moves:
                break

            # Extract names from this page
            for gm in game_moves:
                name = gm["move"]["name"]["name"]
                names.add(name)

            first_ts = int(game_moves[0]["tx"]["timestamp"])
            last_ts = int(game_moves[-1]["tx"]["timestamp"])

            # If we got less than a full page, we're done
            if len(game_moves) < page_size:
                break

            # Check if entire page has the same timestamp
            if first_ts == last_ts:
                # Special procedure: paginate by ID for this exact timestamp
                self._paginate_by_id_for_timestamp(last_ts, names, page_size)

            # Continue from the last timestamp
            last_timestamp = last_ts

        return names

    def _paginate_by_id_for_timestamp(self, exact_ts, names, page_size):
        """
        Handle the case where a full page has a single timestamp.
        Query all entries with this exact timestamp, ordered by ID, paginating by ID.
        """
        last_id = ""

        while True:
            query = f"""
            {{
              gameMoves(
                first: {page_size},
                orderBy: id,
                orderDirection: asc,
                where: {{
                  tx_: {{ timestamp: "{exact_ts}" }},
                  game_: {{ game: "{GAME_ID}" }},
                  id_gt: "{last_id}"
                }}
              ) {{
                id
                move {{
                  name {{
                    name
                  }}
                }}
              }}
            }}
            """

            resp = requests.post(self.subgraph_url, json={"query": query}, timeout=30)
            js = resp.json()
            if "errors" in js:
                raise Exception(f"GraphQL error: {js['errors']}")

            game_moves = js["data"]["gameMoves"]
            if not game_moves:
                break

            for gm in game_moves:
                name = gm["move"]["name"]["name"]
                names.add(name)

            if len(game_moves) < page_size:
                break

            last_id = game_moves[-1]["id"]

    def _query_single_day_names(self, day_date):
        """Query the subgraph for distinct names active on a single day."""
        dt_start = datetime.combine(day_date, time(0, 0, 0))
        dt_end = datetime.combine(day_date, time(23, 59, 59))
        return self.query_distinct_names_in_range(int(dt_start.timestamp()), int(dt_end.timestamp()))

    def update_manager_activity(self):
        logging.info("Starting Manager Activity Update...")
        self.create_table_if_not_exists()
        self.ensure_extra_columns_exist()

        # Process data from the day after the last recorded date up to today.
        last_date = self.get_last_activity_date()
        if last_date:
            start_date = last_date + timedelta(days=1)
        else:
            start_date = self.backfill_start_date
        end_date = date.today()
        logging.info(f"Updating manager activity from {start_date} to {end_date}.")

        # Load unlocked managers from archival
        unlocked_managers = self.load_unlocked_managers()

        # Sliding window: cache names per day to avoid re-querying the
        # overlapping 13/14 days from scratch each iteration. Each new day
        # only fetches one day of subgraph data and drops the oldest.
        daily_names_cache = {}

        current_date = start_date
        while current_date <= end_date:
            # Compute timestamp for the end of current_date (23:59:59)
            dt_end = datetime.combine(current_date, time(23, 59, 59))
            d_ts = int(dt_end.timestamp())

            # Total unlocked: count managers with unlock_date <= d_ts
            total_unlocked = sum(1 for unlock in unlocked_managers.values() if unlock <= d_ts)

            # Define the 14-day window as individual days
            window_start_date = current_date - timedelta(days=13)

            # Fetch any days in the window not yet cached
            day = window_start_date
            while day <= current_date:
                if day not in daily_names_cache:
                    daily_names_cache[day] = self._query_single_day_names(day)
                day += timedelta(days=1)

            # Union all names across the 14-day window
            active_names_from_subgraph = set()
            for day_offset in range(14):
                d = window_start_date + timedelta(days=day_offset)
                if d in daily_names_cache:
                    active_names_from_subgraph |= daily_names_cache[d]

            # Evict days that have fallen out of the window
            for cached_day in list(daily_names_cache.keys()):
                if cached_day < window_start_date:
                    del daily_names_cache[cached_day]

            # Active managers: count those from unlocked managers that had an event in the window
            active_names = {name for name in active_names_from_subgraph
                           if name in unlocked_managers and unlocked_managers[name] <= d_ts}
            active_count = len(active_names)

            # Collect all usernames we need to look up
            unlocked_at_date = {manager for manager, unlock in unlocked_managers.items() if unlock <= d_ts}
            all_usernames_to_consider = unlocked_at_date | active_names_from_subgraph

            # Get user mappings for all relevant usernames
            user_mappings = self.user_enrichment_provider.get_valid_user_mappings(list(all_usernames_to_consider))

            # Actual total unlocked: count distinct groups (via user mapping)
            actual_unlocked_groups = set()
            for manager in unlocked_at_date:
                if manager in user_mappings:
                    actual_unlocked_groups.add(user_mappings[manager])
            actual_total_unlocked = len(actual_unlocked_groups)

            # Actual active managers: deduplicate via group_id
            actual_active_manager_groups = set()
            for manager in active_names:
                if manager in user_mappings:
                    actual_active_manager_groups.add(user_mappings[manager])
            actual_active_last_2_weeks = len(actual_active_manager_groups)

            # Actual active users: count all active users (regardless of manager status) over the last 2 weeks,
            # deduplicated via user_mappings
            actual_active_user_ids = set()
            for username in active_names_from_subgraph:
                if username and username in user_mappings:
                    actual_active_user_ids.add(user_mappings[username])
            actual_active_users = len(actual_active_user_ids)

            # Upsert the daily record WITHOUT touching API columns
            insert_query = f"""
            INSERT INTO `{self.dest_table}` (activity_date, total_unlocked, active_last_2_weeks, actual_total_unlocked, actual_active_last_2_weeks, actual_active_users)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                total_unlocked = VALUES(total_unlocked),
                active_last_2_weeks = VALUES(active_last_2_weeks),
                actual_total_unlocked = VALUES(actual_total_unlocked),
                actual_active_last_2_weeks = VALUES(actual_active_last_2_weeks),
                actual_active_users = VALUES(actual_active_users),
                updated_at = CURRENT_TIMESTAMP
            """
            self.db.execute_query(self.db.dest_conn, insert_query,
                                  (current_date.strftime("%Y-%m-%d"), total_unlocked, active_count, actual_total_unlocked, actual_active_last_2_weeks, actual_active_users))

            logging.info(f"Updated {current_date}: total_unlocked={total_unlocked}, active_last_2_weeks={active_count}, actual_total_unlocked={actual_total_unlocked}, actual_active_last_2_weeks={actual_active_last_2_weeks}, actual_active_users={actual_active_users}")
            current_date += timedelta(days=1)

        # For the current day (today), fetch API market data and update the row.
        target_date = date.today()
        try:
            response = requests.get("https://services.soccerverse.com/api/market", timeout=10)
            if response.ok:
                data = response.json()
                num_users = data.get("NumberOfUsers")
                num_agents = data.get("NumberAgents")
                num_managers = data.get("NumberManagers")
                active_users = data.get("ActiveUsers")
                active_managers = data.get("ActiveManagers")
                inactive_managers = data.get("InactiveManagers")
                num_managers_locked = data.get("NumberManagersLocked")
                num_managers_unlocked = data.get("NumberManagersUnlocked")

                # Always update API columns for today's record
                update_api_query = f"""
                UPDATE `{self.dest_table}`
                SET
                    NumberOfUsers = %s,
                    NumberAgents = %s,
                    NumberManagers = %s,
                    ActiveUsers = %s,
                    ActiveManagers = %s,
                    InactiveManagers = %s,
                    NumberManagersLocked = %s,
                    NumberManagersUnlocked = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE activity_date = %s
                """
                self.db.execute_query(self.db.dest_conn, update_api_query,
                                       (num_users, num_agents, num_managers, active_users,
                                        active_managers, inactive_managers, num_managers_locked,
                                        num_managers_unlocked, target_date.strftime("%Y-%m-%d")))
                logging.info(f"Updated API market data for {target_date}.")
            else:
                logging.error("Failed to fetch market data from API.")
        except Exception as e:
            logging.error(f"Error during API call: {e}")
            logging.exception(e)

        logging.info("Manager Activity Update completed.")
