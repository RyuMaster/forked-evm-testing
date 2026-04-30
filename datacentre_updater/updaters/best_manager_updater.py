import logging, re
from datetime import datetime, timedelta
import traceback
from tqdm import tqdm

from db_manager import CHARSET, COLLATION

class BestManagerUpdater:
    def __init__(self, db_manager):
        self.db = db_manager
        self.component_name = 'best_manager_updater'
        self.last_processed_fixture_id = None
        self.last_full_update = None
        # CHANGED FROM 7 DAYS TO 1 DAY
        self.full_update_interval = timedelta(days=1)  # Full update every day

        # Flag to force full update on startup
        self.perform_full_update_on_startup = True

        # Initialize the status table
        self.initialize_status_table()

    def initialize_status_table(self):
        # Use the same trade_update_status table or create a new one if needed
        create_query = f"""
        CREATE TABLE IF NOT EXISTS `trade_update_status` (
            `component` VARCHAR(50) PRIMARY KEY,
            `last_processed_height` BIGINT,
            `last_full_update` DATETIME
        ) ENGINE=InnoDB DEFAULT CHARSET={CHARSET} COLLATE={COLLATION}
        """
        self.db.execute_query(self.db.dest_conn, create_query)
        logging.info("Initialized trade_update_status table.")

    def get_update_status(self):
        query = "SELECT `last_processed_height`, `last_full_update` FROM `trade_update_status` WHERE `component` = %s"
        result = self.db.execute_query(self.db.dest_conn, query, (self.component_name,))
        if result:
            self.last_processed_fixture_id = result[0]['last_processed_height']
            self.last_full_update = result[0]['last_full_update']
            if self.last_full_update in (None, '0000-00-00 00:00:00', 'None'):
                self.last_full_update = None
                logging.info(f"First run for {self.component_name}. Performing initial full update.")
        else:
            # Insert initial status
            insert_query = "INSERT INTO `trade_update_status` (`component`, `last_processed_height`, `last_full_update`) VALUES (%s, %s, %s)"
            self.db.execute_query(self.db.dest_conn, insert_query, (self.component_name, 0, None))
            self.last_processed_fixture_id = 0
            self.last_full_update = None
            logging.info(f"Inserted initial status for {self.component_name}. Performing initial full update.")

    def update_status(self, last_processed_fixture_id, full_update=False):
        if full_update:
            update_query = """
            UPDATE `trade_update_status`
            SET `last_processed_height` = %s, `last_full_update` = %s
            WHERE `component` = %s
            """
            last_full_update_time = datetime.now()
            params = (last_processed_fixture_id, last_full_update_time, self.component_name)
            self.last_full_update = last_full_update_time
        else:
            update_query = """
            UPDATE `trade_update_status`
            SET `last_processed_height` = %s
            WHERE `component` = %s
            """
            params = (last_processed_fixture_id, self.component_name)

        self.db.execute_query(self.db.dest_conn, update_query, params)
        self.last_processed_fixture_id = last_processed_fixture_id

    def update_best_managers(self, shutdown_flag):
        try:
            self.get_update_status()
            now = datetime.now()
            perform_full_update = False

            # Always perform full update on startup
            if self.perform_full_update_on_startup:
                logging.info(f"Performing full best managers update on startup.")
                perform_full_update = True
                self.perform_full_update_on_startup = False  # Reset the flag after first use
            elif self.last_full_update is None:
                logging.info(f"First run for {self.component_name}. Performing initial full update.")
                perform_full_update = True
            elif now - self.last_full_update >= self.full_update_interval:
                perform_full_update = True

            # Get the maximum fixture_id from the source database
            max_fixture_id_result = self.db.execute_query('sqlite', "SELECT MAX(fixture_id) as max_fixture_id FROM fixtures")
            max_fixture_id = max_fixture_id_result[0]['max_fixture_id'] if max_fixture_id_result[0]['max_fixture_id'] else 0

            if perform_full_update:
                logging.info(f"Performing full best managers update...")
                self.create_table()
                fixtures = self.get_fixtures()
                self.process_fixtures(fixtures, full_update=True, shutdown_flag=shutdown_flag)
                self.update_status(max_fixture_id, full_update=True)
            elif max_fixture_id > self.last_processed_fixture_id:
                logging.info(f"New fixtures detected from fixture_id {self.last_processed_fixture_id + 1} to {max_fixture_id}.")
                fixtures = self.get_fixtures(
                    start_fixture_id=self.last_processed_fixture_id + 1,
                    end_fixture_id=max_fixture_id
                )
                if not fixtures:
                    logging.info(f"No new fixtures to update.")
                    self.update_status(max_fixture_id, full_update=False)
                    return
                self.process_fixtures(fixtures, full_update=False, shutdown_flag=shutdown_flag)
                self.update_status(max_fixture_id, full_update=False)
            else:
                logging.info(f"No new fixtures detected. Skipping best managers update.")
                return
        except Exception as e:
            logging.error(f"An error occurred in best manager update: {e}")
            logging.debug(traceback.format_exc())
            raise

    def create_table(self):
        logging.info("Ensuring dc_best_managers table exists with correct columns.")
        create_query = f"""
        CREATE TABLE IF NOT EXISTS `dc_best_managers` (
            `id` INT AUTO_INCREMENT PRIMARY KEY,
            `name` VARCHAR(255),
            `rank_old` INT,
            `rank_a` INT,
            `rank_b` INT DEFAULT NULL,
            UNIQUE KEY `uniq_name` (`name`),
            INDEX (`rank_old`),
            INDEX (`rank_a`)
        ) ENGINE=InnoDB DEFAULT CHARSET={CHARSET} COLLATE={COLLATION}
        """
        self.db.execute_query(self.db.dest_conn, create_query)

        # Get existing columns with types
        existing_columns = self.db.get_existing_columns_with_types('dc_best_managers')
        required_columns = {
            'id': 'INT AUTO_INCREMENT',
            'name': 'VARCHAR(255)',
            'rank_old': 'INT',
            'rank_a': 'INT',
            'rank_b': 'INT DEFAULT NULL'
        }

        for col, required_type in required_columns.items():
            if col not in existing_columns:
                # Add missing columns
                if col == 'id':
                    continue  # Already created by the initial CREATE
                if col == 'name':
                    alter_query = "ALTER TABLE `dc_best_managers` ADD COLUMN `name` VARCHAR(255)"
                    self.db.execute_query(self.db.dest_conn, alter_query)
                    logging.info(f"Added column `name` to table `dc_best_managers`.")
                    alter_query = "ALTER TABLE `dc_best_managers` ADD UNIQUE KEY `uniq_name` (`name`)"
                    self.db.execute_query(self.db.dest_conn, alter_query)
                    logging.info(f"Added UNIQUE KEY on `name` in table `dc_best_managers`.")
                else:
                    alter_query = f"ALTER TABLE `dc_best_managers` ADD COLUMN `{col}` {required_type}"
                    self.db.execute_query(self.db.dest_conn, alter_query)
                    logging.info(f"Added column `{col}` to table `dc_best_managers`.")
                    # Add index if needed
                    if col in ['rank_old', 'rank_a']:
                        index_query = f"ALTER TABLE `dc_best_managers` ADD INDEX (`{col}`)"
                        self.db.execute_query(self.db.dest_conn, index_query)
                        logging.info(f"Added index on `{col}` in table `dc_best_managers`.")
            else:
                # Column exists, check data type
                existing_type = existing_columns[col]
                if not self.compare_column_types(existing_type, required_type):
                    # Alter column to required type
                    alter_query = f"ALTER TABLE `dc_best_managers` MODIFY COLUMN `{col}` {required_type}"
                    self.db.execute_query(self.db.dest_conn, alter_query)
                    logging.info(f"Modified column `{col}` to type {required_type} in table `dc_best_managers`.")

        # Ensure indexes exist on rank_old and rank_a
        existing_indexes = self.db.get_existing_indexes('dc_best_managers')
        for col in ['rank_old', 'rank_a']:
            if col not in existing_indexes:
                index_query = f"ALTER TABLE `dc_best_managers` ADD INDEX (`{col}`)"
                self.db.execute_query(self.db.dest_conn, index_query)
                logging.info(f"Added index on `{col}` in table `dc_best_managers`.")

    def compare_column_types(self, existing_type, required_type):
        # Simplify types for comparison
        existing_base_type = existing_type.split('(')[0].strip().lower()
        required_base_type = required_type.split('(')[0].strip().lower()

        # For VARCHAR, compare base type and length
        if existing_base_type == 'varchar' and required_base_type == 'varchar':
            existing_length_match = re.search(r'\((\d+)\)', existing_type)
            required_length_match = re.search(r'\((\d+)\)', required_type)
            if existing_length_match and required_length_match:
                existing_length = int(existing_length_match.group(1))
                required_length = int(required_length_match.group(1))
                return existing_length >= required_length
            else:
                return True  # Unable to determine length, assume compatible
        else:
            return existing_base_type == required_base_type

    def get_fixtures(self, start_fixture_id=None, end_fixture_id=None):
        query = """
        SELECT fixture_id, home_manager, away_manager, home_goals, away_goals, home_club, away_club
        FROM fixtures
        WHERE 1=1
        """
        params = ()
        if start_fixture_id is not None and end_fixture_id is not None:
            query += " AND fixture_id BETWEEN ? AND ?"
            params = (start_fixture_id, end_fixture_id)
        elif start_fixture_id is not None:
            query += " AND fixture_id >= ?"
            params = (start_fixture_id,)
        elif end_fixture_id is not None:
            query += " AND fixture_id <= ?"
            params = (end_fixture_id,)
        # Else, fetch all fixtures

        fixtures = self.db.execute_query('sqlite', query, params)
        return fixtures

    def process_fixtures(self, fixtures, full_update, shutdown_flag):
        logging.info(f"Processing {len(fixtures)} fixtures.")

        # If we're doing a full update, start from scratch
        if full_update:
            manager_points_old = {}
            manager_points_a = {}
        else:
            # Otherwise load existing managers
            manager_points_old = {}
            manager_points_a = {}
            existing_managers = self.get_existing_managers()
            for manager in existing_managers:
                manager_name = manager['name']
                manager_points_old[manager_name] = manager['rank_old'] or 0
                manager_points_a[manager_name] = manager['rank_a'] or 0

        # Prepare the club ratings dictionary
        club_ratings = self.get_club_ratings()

        K = 5   # K-Factor for adjusting points
        D = 20  # Scaling factor for rating difference

        for fixture in tqdm(fixtures, desc="Processing fixtures"):
            if shutdown_flag.is_set():
                logging.info("Shutdown signal received. Exiting process_fixtures early.")
                break

            home_manager = fixture['home_manager']
            away_manager = fixture['away_manager']
            home_goals = fixture['home_goals']
            away_goals = fixture['away_goals']
            home_club_id = fixture['home_club']
            away_club_id = fixture['away_club']

            # Skip if missing essential data
            if None in (home_manager, away_manager, home_goals, away_goals, home_club_id, away_club_id):
                continue


            # Validate club ratings
            if home_club_id not in club_ratings:
                logging.error(f"Club rating not found for home club ID: {home_club_id}")
                continue
            if away_club_id not in club_ratings:
                logging.error(f"Club rating not found for away club ID: {away_club_id}")
                continue

            home_rating = float(club_ratings[home_club_id])
            away_rating = float(club_ratings[away_club_id])

            # Elo expected results
            rating_diff = away_rating - home_rating
            exponent = rating_diff / D
            E_home = 1 / (1 + pow(10, exponent))
            E_away = 1 - E_home

            # Actual match result
            if home_goals > away_goals:
                S_home = 1.0
                S_away = 0.0
                self.add_points(manager_points_old, home_manager, 2)
                self.add_points(manager_points_old, away_manager, 0)
            elif home_goals < away_goals:
                S_home = 0.0
                S_away = 1.0
                self.add_points(manager_points_old, home_manager, 0)
                self.add_points(manager_points_old, away_manager, 2)
            else:
                S_home = 0.5
                S_away = 0.5
                self.add_points(manager_points_old, home_manager, 1)
                self.add_points(manager_points_old, away_manager, 1)

            # Elo-based points for rank_a
            points_home = K * (S_home - E_home)
            points_away = K * (S_away - E_away)
            self.add_points(manager_points_a, home_manager, points_home)
            self.add_points(manager_points_a, away_manager, points_away)

        # After processing all fixtures, offset rank_a by +100 and round
        for name in manager_points_a:
            manager_points_a[name] += 100
            manager_points_a[name] = int(round(manager_points_a[name]))

        # Update the dc_best_managers table
        self.update_manager_points(manager_points_old, manager_points_a)

    def add_points(self, manager_points, manager_name, points):
        if not manager_name:
            return
        if manager_name not in manager_points:
            manager_points[manager_name] = 0
        manager_points[manager_name] += points

    def get_existing_managers(self):
        query = "SELECT name, rank_old, rank_a FROM dc_best_managers"
        managers = self.db.execute_query(self.db.dest_conn, query)
        return managers

    def update_manager_points(self, manager_points_old, manager_points_a):
        if not manager_points_old and not manager_points_a:
            logging.info("No manager points to update.")
            return

        insert_query = """
        INSERT INTO dc_best_managers (`name`, `rank_old`, `rank_a`)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE rank_old = VALUES(rank_old), rank_a = VALUES(rank_a)
        """
        names = sorted(set(manager_points_old.keys()) | set(manager_points_a.keys()))
        data = []
        for name in names:
            rank_old = manager_points_old.get(name, 0)
            rank_a = manager_points_a.get(name, 0)
            data.append((name, rank_old, rank_a))

        batch_size = 1000
        for i in range(0, len(data), batch_size):
            batch = data[i:i+batch_size]
            self.db.execute_many(self.db.dest_conn, insert_query, batch)
        logging.info("Updated manager points in dc_best_managers table.")

    def get_existing_columns(self, table_name):
        return self.db.get_existing_columns(table_name)

    def get_existing_indexes(self, table_name):
        query = f"SHOW INDEX FROM `{table_name}`"
        indexes_info = self.db.execute_query(self.db.dest_conn, query)
        indexes = set()
        for index in indexes_info:
            if index['Key_name'] != 'PRIMARY' and index['Non_unique'] == 1:
                indexes.add(index['Column_name'])
        return indexes

    def get_club_ratings(self):
        # Fetch avg_player_rating_top21 from dc_club_info
        query = "SELECT club_id, avg_player_rating_top21 FROM dc_club_info"
        club_info = self.db.execute_query(self.db.dest_conn, query)
        club_ratings = {}
        for club in club_info:
            club_id = club['club_id']
            avg_rating = club['avg_player_rating_top21']
            if avg_rating is None:
                logging.error(f"Average player rating is missing for club ID: {club_id}")
                continue
            rating = float(avg_rating)
            club_ratings[club_id] = rating
        return club_ratings
