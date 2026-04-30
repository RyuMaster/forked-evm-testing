# File: updaters/league_updater.py
import logging, re
from datetime import datetime, timedelta
import traceback
from .trade_updater import initialize_trade_update_status_table

from db_manager import CHARSET, COLLATION

class LeagueUpdater:
    def __init__(self, db_manager):
        self.db = db_manager
        self.component_name = 'league_updater'
        # This variable now stores the last processed message height from the messages table
        self.last_processed_fixture_id = None  
        self.last_full_update = None
        self.full_update_interval = timedelta(hours=6)  # Full update every 6 hours

        # Initialize the status table
        self.initialize_status_table()

    def initialize_status_table(self):
        """Create the status table using shared function."""
        initialize_trade_update_status_table(self.db)

    def get_update_status(self):
        query = "SELECT `last_processed_height`, `last_full_update` FROM `trade_update_status` WHERE `component` = %s"
        result = self.db.execute_query(self.db.dest_conn, query, (self.component_name,))
        if result:
            self.last_processed_fixture_id = result[0]['last_processed_height']
            self.last_full_update = result[0]['last_full_update']
            if self.last_full_update in (None, '0000-00-00 00:00:00', 'None'):
                self.last_full_update = None
        else:
            # Insert initial status
            insert_query = "INSERT INTO `trade_update_status` (`component`, `last_processed_height`, `last_full_update`) VALUES (%s, %s, %s)"
            self.db.execute_query(self.db.dest_conn, insert_query, (self.component_name, 0, None))
            self.last_processed_fixture_id = 0
            self.last_full_update = None

    def update_status(self, last_processed_fixture_id):
        update_query = """
        UPDATE `trade_update_status`
        SET `last_processed_height` = %s, `last_full_update` = %s
        WHERE `component` = %s
        """
        last_full_update_time = datetime.now()
        params = (last_processed_fixture_id, last_full_update_time, self.component_name)
        self.db.execute_query(self.db.dest_conn, update_query, params)
        self.last_processed_fixture_id = last_processed_fixture_id
        self.last_full_update = last_full_update_time

    def update_leagues(self, shutdown_flag, perform_full_update=False):
        try:
            self.get_update_status()
            now = datetime.now()

            # Query the messages table in the archival (source) database for fixture messages (type 300)
            max_message_height_result = self.db.execute_query(
                'source',
                "SELECT MAX(height) as max_message_height FROM messages WHERE type = 300"
            )
            max_message_height = (max_message_height_result[0]['max_message_height']
                                  if max_message_height_result and max_message_height_result[0]['max_message_height']
                                  else 0)

            need_update = False
            if perform_full_update:
                logging.info("Performing scheduled full league update (forced).")
                need_update = True
            elif max_message_height > self.last_processed_fixture_id:
                logging.info(f"New fixture messages detected from height {self.last_processed_fixture_id + 1} to {max_message_height}.")
                need_update = True
            elif self.last_full_update is None or now - self.last_full_update >= self.full_update_interval:
                logging.info("Full update interval reached or not set. Performing scheduled full league update.")
                need_update = True

            if need_update:
                logging.info("Performing full league update...")
                self.create_tables()
                self.process_leagues_and_table_rows(shutdown_flag)
                self.update_status(max_message_height)
            else:
                logging.info("No new fixture messages detected and full update interval not reached. Skipping league update.")
        except Exception as e:
            logging.error(f"An error occurred in league update: {e}")
            logging.debug(traceback.format_exc())
            raise

    def create_tables(self):
        # Create or update dc_leagues table
        logging.info("Ensuring dc_leagues table exists with correct columns.")
        # Get columns from sqlite leagues table
        leagues_columns_info = self.db.execute_query('sqlite', "PRAGMA table_info(leagues)")
        leagues_columns = {}
        for col_info in leagues_columns_info:
            col_name = col_info['name']
            sqlite_type = col_info['type']
            mysql_type = self.map_sqlite_type_to_mysql(sqlite_type, col_name)
            leagues_columns[col_name] = mysql_type

        # Add 'updated_at' column
        leagues_columns['updated_at'] = 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'

        # Create dc_leagues table
        self.create_table('dc_leagues', leagues_columns, unique_keys=['season_id', 'level', 'country_id'])

        # Similarly for dc_table_rows
        logging.info("Ensuring dc_table_rows table exists with correct columns.")
        table_rows_columns_info = self.db.execute_query('sqlite', "PRAGMA table_info(table_rows)")
        table_rows_columns = {}
        for col_info in table_rows_columns_info:
            col_name = col_info['name']
            sqlite_type = col_info['type']
            mysql_type = self.map_sqlite_type_to_mysql(sqlite_type, col_name)
            table_rows_columns[col_name] = mysql_type

        # Add 'updated_at' column
        table_rows_columns['updated_at'] = 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'

        # Create dc_table_rows table, with unique key on season_id, league_id, club_id
        self.create_table('dc_table_rows', table_rows_columns, unique_keys=['season_id', 'league_id', 'club_id'])

    def create_table(self, table_name, columns, unique_keys=None):
        existing_columns = self.db.get_existing_columns(table_name)

        if existing_columns is None:
            # Table does not exist, create it
            columns_sql = ', '.join(f"`{col}` {dtype}" for col, dtype in columns.items())

            unique_key_definition = ""
            if unique_keys:
                unique_key_definition = f", UNIQUE KEY `unique_{table_name}_{'_'.join(unique_keys)}` ({', '.join(f'`{col}`' for col in unique_keys)})"

            # Prepare index columns, excluding non-indexable types
            index_columns = [col for col in columns if col not in ['updated_at'] and self.is_indexable(columns[col])]
            index_sql_list = [f"INDEX (`{col}`)" for col in index_columns]
            index_sql_list.append("INDEX (`updated_at`)")
            index_sql = ', '.join(index_sql_list)

            create_query = f"""
            CREATE TABLE `{table_name}` (
                {columns_sql}
                {unique_key_definition},
                {index_sql}
            ) ENGINE=InnoDB DEFAULT CHARSET={CHARSET} COLLATE={COLLATION}
            """
            try:
                self.db.execute_query(self.db.dest_conn, create_query)
                logging.info(f"Table `{table_name}` created successfully.")
            except Exception as e:
                logging.error(f"Error creating table `{table_name}`: {e}")
                raise
        else:
            # Table exists, add any missing columns
            missing_columns = {col: dtype for col, dtype in columns.items() if col not in existing_columns}
            for col, dtype in missing_columns.items():
                try:
                    alter_query = f"ALTER TABLE `{table_name}` ADD COLUMN `{col}` {dtype}"
                    self.db.execute_query(self.db.dest_conn, alter_query)
                    logging.info(f"Added column `{col}` to table `{table_name}`.")
                except Exception as e:
                    logging.error(f"Error adding column `{col}` to table `{table_name}`: {e}")
                    raise

    def is_indexable(self, data_type):
        # Remove size specifications and other modifiers for comparison
        base_type = data_type.lower()
        base_type = re.sub(r'\(.*\)', '', base_type).strip()
        if 'text' in base_type or 'blob' in base_type or 'json' in base_type:
            return False
        return True

    def map_sqlite_type_to_mysql(self, sqlite_type, column_name=None):
        # Handle specific columns with explicit data types
        if column_name == 'level':
            return 'INT'
        elif column_name == 'name':
            return f'VARCHAR(255) COLLATE {COLLATION}'  # Adjust the size as needed
        elif column_name == 'country_id':
            return f'VARCHAR(3) COLLATE {COLLATION}'

        # General mapping based on SQLite type
        type_upper = sqlite_type.strip().upper()
        if 'INT' in type_upper:
            if column_name in ['ticket_cost', 'prize_money_pot', 'tv_money']:
                return 'BIGINT'
            else:
                return 'INT'
        elif any(t in type_upper for t in ['CHAR', 'CLOB', 'TEXT', 'VARCHAR']):
            return f'TEXT COLLATE {COLLATION}'
        elif 'BLOB' in type_upper:
            return 'LONGBLOB'
        elif any(t in type_upper for t in ['REAL', 'FLOA', 'DOUB']):
            if column_name in ['ticket_cost', 'prize_money_pot', 'tv_money']:
                return 'BIGINT'
            return 'FLOAT'
        elif 'NUMERIC' in type_upper or 'DECIMAL' in type_upper:
            if column_name in ['ticket_cost', 'prize_money_pot', 'tv_money']:
                return 'BIGINT'
            return 'DECIMAL(38,5)'
        elif 'DATE' in type_upper or 'TIME' in type_upper:
            return 'DATETIME'
        else:
            return f'TEXT COLLATE {COLLATION}'

    def process_leagues_and_table_rows(self, shutdown_flag):
        try:
            # Process leagues table
            leagues_data = self.db.execute_query('sqlite', "SELECT * FROM leagues")
            if not leagues_data:
                logging.info("No leagues data found in SQLite. Skipping leagues update.")
            else:
                # Get columns from leagues table
                leagues_columns_info = self.db.execute_query('sqlite', "PRAGMA table_info(leagues)")
                columns = [col_info['name'] for col_info in leagues_columns_info]
                columns.append('updated_at')  # Append updated_at column

                insert_query = self.prepare_insert_query('dc_leagues', columns, unique_keys=['season_id', 'level', 'country_id'])

                rows_to_update = []
                for row in leagues_data:
                    row_data = [row.get(col) for col in columns if col != 'updated_at']
                    row_data.append(None)  # 'updated_at' column value (will default)
                    rows_to_update.append(row_data)

                # Batch insert/update
                total_update_rows = len(rows_to_update)
                batch_size = 1000
                for i in range(0, total_update_rows, batch_size):
                    if shutdown_flag.is_set():
                        logging.info("Shutdown signal received. Exiting process_leagues_and_table_rows early.")
                        break
                    batch = rows_to_update[i:i+batch_size]
                    self.db.execute_many(self.db.dest_conn, insert_query, batch)
                logging.info(f"Successfully updated dc_leagues with {total_update_rows} rows.")

            # Process table_rows table
            table_rows_data = self.db.execute_query('sqlite', "SELECT * FROM table_rows")
            if not table_rows_data:
                logging.info("No table_rows data found in SQLite. Skipping table_rows update.")
            else:
                # Get columns from table_rows table
                table_rows_columns_info = self.db.execute_query('sqlite', "PRAGMA table_info(table_rows)")
                columns = [col_info['name'] for col_info in table_rows_columns_info]
                columns.append('updated_at')  # Append updated_at column

                insert_query = self.prepare_insert_query('dc_table_rows', columns, unique_keys=['season_id', 'league_id', 'club_id'])

                rows_to_update = []
                for row in table_rows_data:
                    row_data = [row.get(col) for col in columns if col != 'updated_at']
                    row_data.append(None)  # 'updated_at' column value (will default)
                    rows_to_update.append(row_data)

                # Batch insert/update
                total_update_rows = len(rows_to_update)
                batch_size = 1000
                for i in range(0, total_update_rows, batch_size):
                    if shutdown_flag.is_set():
                        logging.info("Shutdown signal received. Exiting process_leagues_and_table_rows early.")
                        break
                    batch = rows_to_update[i:i+batch_size]
                    self.db.execute_many(self.db.dest_conn, insert_query, batch)
                logging.info(f"Successfully updated dc_table_rows with {total_update_rows} rows.")
        except Exception as e:
            logging.error(f"Error processing leagues and table_rows: {e}")
            logging.debug(traceback.format_exc())
            raise

    def prepare_insert_query(self, dest_table, columns, unique_keys):
        update_columns = ', '.join(f"`{col}` = VALUES(`{col}`)" for col in columns if col not in unique_keys and col != 'updated_at')
        insert_query = f"""
        INSERT INTO `{dest_table}` ({', '.join(f'`{col}`' for col in columns)})
        VALUES ({', '.join(['%s'] * len(columns))})
        ON DUPLICATE KEY UPDATE
            {update_columns}
        """
        return insert_query
