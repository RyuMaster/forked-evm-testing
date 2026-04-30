# sqlite_updater.py

import logging
import hashlib
import time
import traceback
from tqdm import tqdm
import json
import re
import base64
from datetime import datetime, timedelta

from db_manager import CHARSET, COLLATION

class SQLiteUpdater:
    def __init__(self, db_manager, club_info_updater):
        self.db = db_manager
        self.club_info_updater = club_info_updater
        # Initialize sets for updated clubs
        self.updated_club_ids = set()
        self.collected_updated_club_ids = set()

        # Define the tables to process with their configurations
        self.tables_to_process = [
            {
                'source_table': 'users',
                'dest_table': 'dc_users',
                'component': 'dc_users',
                'key_column': 'id',
                'indexes': ['club_id', 'name'],  # Add name as regular index for lookups
                # Don't use unique_keys for name - let PRIMARY KEY (id) handle uniqueness
                # Since SQLite rowid maps 1:1 to unique names, this prevents PAD SPACE issues
                'unique_keys': None,
                # Include rowid as 'id' so we have a unique key for each user row
                'special_query': """
                    SELECT u.rowid AS id, u.*, c.club_id
                    FROM users u
                    LEFT JOIN clubs c ON u.name = c.manager_name
                """,
                'table_alias': 'u',
                'columns': {
                    # Import 'id' from source and treat as primary key (no auto-increment)
                    'id': 'INT',
                    'name': 'VARCHAR(255)',
                    'balance': 'BIGINT',
                    'last_active': 'BIGINT',
                    'club_id': 'INT',
                    'last_updated_height': 'BIGINT'
                }
            },
            {
                'source_table': 'clubs',
                'dest_table': 'dc_clubs',
                'component': 'dc_clubs',
                'key_column': 'club_id',
                'indexes': ['manager_name', 'country_id'],
                'columns': {
                    'club_id': 'INT',  # Ensure club_id is included
                    'manager_name': 'VARCHAR(254)',
                    'country_id': 'VARCHAR(3)',
                    'last_updated_height': 'BIGINT',
                    # These are calculated from transfer_counts, not in clubs table
                    'transfers_in': 'INT',
                    'transfers_out': 'INT'
                },
                # Use correlated scalar subqueries for efficient per-club lookups
                # The WHERE filter will be applied to clubs first, then subqueries execute only for those clubs
                'special_query': """
                    SELECT 
                        c.*,
                        COALESCE(
                            (SELECT SUM(transfers) 
                             FROM transfer_counts 
                             WHERE to_club = c.club_id AND from_club != 0),
                            0
                        ) AS transfers_in,
                        COALESCE(
                            (SELECT SUM(transfers) 
                             FROM transfer_counts 
                             WHERE from_club = c.club_id),
                            0
                        ) AS transfers_out
                    FROM clubs c
                """,
                'table_alias': 'c',
            },
            {
                'source_table': 'players',
                'dest_table': 'dc_players',
                'component': 'dc_players',
                'key_column': 'player_id',
                'indexes': ['agent_name', 'country_id'],
                'columns': {
                    'player_id': 'INT',
                    'club_id': 'INT',
                    'agent_name': 'VARCHAR(254)',
                    'country_id': 'VARCHAR(3)',
                    'last_updated_height': 'BIGINT'
                },
            },
            {
                'source_table': 'share_balances',
                'dest_table': 'dc_share_balances',
                'component': 'dc_share_balances',
                'key_column': 'id',
                'columns': {
                    'id': 'INT AUTO_INCREMENT',
                    'name': 'VARCHAR(255)',
                    'share_type': 'VARCHAR(50)',
                    'share_id': 'INT',
                    'num': 'BIGINT',
                },
                'indexes': [],
                'unique_keys': ['name', 'share_type', 'share_id'],
                # Composite index that lets the API satisfy
                #   SELECT name, num WHERE share_id=? AND share_type=? AND num>?
                #   ORDER BY num DESC LIMIT N
                # via a single backward index range scan (no filesort).
                # Without this the JOIN-and-COUNT endpoints in datacentre_api
                # hold MDL_SHARED on dc_share_balances long enough that the
                # 6-hourly RENAME swap can pile up a metadata-lock cascade.
                'composite_indexes': [
                    {'name': 'idx_share_lookup_num',
                     'columns': ['share_id', 'share_type', 'num']},
                ],
            },
        ]
        self.initialize_status_table()
        self.last_full_update = None
        self.full_update_interval = timedelta(hours=6)

        # Initialize per-component last_processed_heights
        self.last_processed_heights = {}

    def initialize_status_table(self):
        # Ensure the status table exists
        create_query = f"""
        CREATE TABLE IF NOT EXISTS `sqlite_update_status` (
            `component` VARCHAR(50) PRIMARY KEY,
            `last_processed_entry_id` BIGINT,
            `last_full_update` DATETIME,
            `last_processed_height` BIGINT
        ) ENGINE=InnoDB DEFAULT CHARSET={CHARSET} COLLATE={COLLATION}
        """
        self.db.execute_query(self.db.dest_conn, create_query)
        logging.info("Initialized sqlite_update_status table.")

    def get_update_status(self, component):
        query = "SELECT `last_processed_entry_id`, `last_full_update`, `last_processed_height` FROM `sqlite_update_status` WHERE `component` = %s"
        result = self.db.execute_query(self.db.dest_conn, query, (component,))
        if result:
            self.last_processed_entry_id = result[0]['last_processed_entry_id'] or 0
            # FIX: Don't overwrite with NULL - only update if there's a valid timestamp
            if result[0]['last_full_update'] is not None:
                self.last_full_update = result[0]['last_full_update']
            last_processed_height = result[0]['last_processed_height']
            self.last_processed_heights[component] = last_processed_height if last_processed_height is not None else -1
        else:
            # Insert initial status with last_processed_height = -1
            insert_query = "INSERT INTO `sqlite_update_status` (`component`, `last_processed_entry_id`, `last_full_update`, `last_processed_height`) VALUES (%s, %s, %s, %s)"
            self.db.execute_query(self.db.dest_conn, insert_query, (component, 0, None, -1))
            self.last_processed_entry_id = 0
            self.last_full_update = None
            self.last_processed_heights[component] = -1

    def update_status(self, component, last_entry_id, last_processed_height, full_update=False):
        update_query = """
        UPDATE `sqlite_update_status` 
        SET `last_processed_entry_id` = %s, `last_full_update` = %s, `last_processed_height` = %s
        WHERE `component` = %s
        """
        from datetime import datetime
        last_full_update_time = datetime.now() if full_update else self.last_full_update
        self.db.execute_query(self.db.dest_conn, update_query, (last_entry_id, last_full_update_time, last_processed_height, component))
        self.last_processed_entry_id = last_entry_id
        if full_update:
            self.last_full_update = last_full_update_time
        self.last_processed_heights[component] = last_processed_height

    def map_sqlite_type_to_mysql(self, sqlite_type, column_name=None):
        type_upper = sqlite_type.strip().upper()
        if 'INT' in type_upper:
            if column_name in ['value', 'balance', 'wages', 'num', 'last_updated_height']:
                return 'BIGINT'
            else:
                return 'INT'
        elif any(t in type_upper for t in ['CHAR', 'CLOB', 'TEXT', 'VARCHAR']):
            if column_name == 'name':
                return f'VARCHAR(255) COLLATE {COLLATION}'
            elif column_name == 'share_type':
                return f'VARCHAR(50) COLLATE {COLLATION}'
            else:
                return f'TEXT COLLATE {COLLATION}'
        elif 'BLOB' in type_upper:
            return 'LONGBLOB'
        elif any(t in type_upper for t in ['REAL', 'FLOA', 'DOUB']):
            return 'FLOAT'
        elif 'NUMERIC' in type_upper or 'DECIMAL' in type_upper:
            return 'DECIMAL(38,5)'
        elif 'DATE' in type_upper or 'TIME' in type_upper:
            return 'DATETIME'
        else:
            return f'TEXT COLLATE {COLLATION}'

    def create_table(self, table_name, columns, indexes=None, key_column=None, unique_keys=None,
                     composite_indexes=None):
        logging.info(f"Ensuring table `{table_name}` exists with correct columns.")
        existing_columns = self.db.get_existing_columns(table_name)

        # Add 'updated_at' and 'last_updated_height' columns if not present
        if 'updated_at' not in columns:
            columns['updated_at'] = 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'
        if 'last_updated_height' not in columns:
            columns['last_updated_height'] = 'BIGINT'
        # Remove 'checksum' column as we no longer use it
        if 'checksum' in columns:
            del columns['checksum']

        if existing_columns is None:
            # Table does not exist, create it
            columns_sql = ', '.join(f"`{col}` {dtype}" for col, dtype in columns.items())

            # Handle primary key
            primary_key_definition = ""
            if key_column:
                if isinstance(key_column, (tuple, list)):
                    primary_key_definition = f", PRIMARY KEY ({', '.join(f'`{col}`' for col in key_column)})"
                else:
                    primary_key_definition = f", PRIMARY KEY (`{key_column}`)"

            # Add unique keys
            unique_key_definition = ""
            if unique_keys:
                unique_key_definition = f", UNIQUE KEY `unique_{table_name}_{'_'.join(unique_keys)}` ({', '.join(f'`{col}`' for col in unique_keys)})"

            # Prepare index columns
            index_columns = [
                col for col in columns
                if col not in ['updated_at', 'last_updated_height'] and self.is_indexable(columns[col])
            ]
            index_sql_list = [f"INDEX (`{col}`)" for col in index_columns]
            if indexes:
                index_sql_list.extend([f"INDEX (`{index}`)" for index in indexes])
            if composite_indexes:
                for ci in composite_indexes:
                    cols_sql = ', '.join(f"`{c}`" for c in ci['columns'])
                    index_sql_list.append(f"INDEX `{ci['name']}` ({cols_sql})")
            index_sql_list.append("INDEX (`updated_at`)")
            index_sql_list.append("INDEX (`last_updated_height`)")
            index_sql = ', '.join(index_sql_list)

            create_query = f"""
            CREATE TABLE `{table_name}` (
                {columns_sql}
                {primary_key_definition}
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

            # Add any composite indexes that don't already exist on the table.
            # Uses ALGORITHM=INPLACE, LOCK=NONE so the build doesn't block reads/writes.
            if composite_indexes:
                existing_index_names = self.db.get_existing_index_names(table_name)
                for ci in composite_indexes:
                    if ci['name'] in existing_index_names:
                        continue
                    cols_sql = ', '.join(f"`{c}`" for c in ci['columns'])
                    alter_query = (
                        f"ALTER TABLE `{table_name}` "
                        f"ADD INDEX `{ci['name']}` ({cols_sql}), "
                        f"ALGORITHM=INPLACE, LOCK=NONE"
                    )
                    try:
                        self.db.execute_query(self.db.dest_conn, alter_query)
                        logging.info(f"Added composite index `{ci['name']}` on `{table_name}`.")
                    except Exception as e:
                        logging.error(f"Error adding composite index `{ci['name']}` on `{table_name}`: {e}")
                        raise

    @staticmethod
    def has_outer_where_clause(query):
        """
        Check if query has a WHERE clause at the outer level (not inside subqueries).
        This prevents incorrectly detecting WHERE clauses inside parenthesized subqueries.
        """
        query_upper = query.upper()
        depth = 0
        i = 0
        while i < len(query_upper):
            if query_upper[i] == '(':
                depth += 1
            elif query_upper[i] == ')':
                depth -= 1
            elif depth == 0 and query_upper[i:i+5] == 'WHERE':
                # Make sure it's a full word (not part of another word like NOWHERE)
                before_ok = i == 0 or not query_upper[i-1].isalnum()
                after_ok = i + 5 >= len(query_upper) or not query_upper[i+5].isalnum()
                if before_ok and after_ok:
                    return True
            i += 1
        return False

    def get_source_data(self, source_table, last_processed_height, special_query=None, table_alias=None):
        if last_processed_height is None:
            # Fetch all data
            if special_query:
                query = special_query
                params = ()
            else:
                query = f"SELECT * FROM {source_table}"
                params = ()
        elif last_processed_height == -1:
            # Fetch data where last_updated_height >= -1
            if special_query:
                query = special_query
                if self.has_outer_where_clause(query):
                    query += f" AND {table_alias}.last_updated_height >= ?"
                else:
                    query += f" WHERE {table_alias}.last_updated_height >= ?"
                params = (-1,)
            else:
                query = f"SELECT * FROM {source_table} WHERE last_updated_height >= ?"
                params = (-1,)
        else:
            # Fetch data where last_updated_height > last_processed_height
            if special_query:
                query = special_query
                if self.has_outer_where_clause(query):
                    query += f" AND {table_alias}.last_updated_height > ?"
                else:
                    query += f" WHERE {table_alias}.last_updated_height > ?"
                params = (last_processed_height,)
            else:
                query = f"SELECT * FROM {source_table} WHERE last_updated_height > ?"
                params = (last_processed_height,)

        source_data = self.db.execute_query('sqlite', query, params)
        return source_data

    def prepare_insert_query(self, dest_table, columns, key_column, unique_keys, target_table=None):
        # Allow override of target table (for staging tables)
        table_name = target_table or dest_table

        if unique_keys:
            primary_key_columns = unique_keys
        else:
            primary_key_columns = [key_column] if key_column else []

        # For dc_players, preserve loan columns during SQLite updates
        preserve_columns = []
        if dest_table == 'dc_players':
            preserve_columns = ['loan_offered', 'loan_offer_accepted', 'loaned_to_club']

        # Exclude updated_at from INSERT (let DEFAULT CURRENT_TIMESTAMP handle new rows)
        insert_columns = [col for col in columns if col != 'updated_at']

        update_columns = ', '.join(
            f"`{col}` = VALUES(`{col}`)"
            for col in columns
            if col not in primary_key_columns + ['updated_at'] + preserve_columns
        )

        # Explicitly update updated_at timestamp on UPDATE
        if update_columns:
            update_columns += ', `updated_at` = CURRENT_TIMESTAMP'
        else:
            update_columns = '`updated_at` = CURRENT_TIMESTAMP'

        insert_query = f"""
        INSERT INTO `{table_name}` ({', '.join(f'`{col}`' for col in insert_columns)})
        VALUES ({', '.join(['%s'] * len(insert_columns))})
        ON DUPLICATE KEY UPDATE
            {update_columns}
        """
        return insert_query

    def process_row_data(self, row, columns, key_column, unique_keys):
        row_data_dict = {}
        columns_without_updated_at = [col for col in columns if col != 'updated_at']

        for col in columns_without_updated_at:
            value = row.get(col)
            if isinstance(value, bytes):
                import base64
                encoded_value = base64.b64encode(value).decode('ascii')
                # We'll store the raw bytes in MySQL
                row_data_dict[col] = value
            elif value is None:
                row_data_dict[col] = None
            else:
                value = str(value)
                row_data_dict[col] = value

        # Don't include updated_at in row_data (excluded from INSERT, uses DEFAULT)
        # Construct 'row_data' in the order of 'columns_without_updated_at'
        row_data = [row_data_dict.get(col) for col in columns_without_updated_at]

        # Build row_key
        if unique_keys:
            row_key = tuple(row.get(col) for col in unique_keys)
        elif key_column:
            row_key = (row.get(key_column),)
        else:
            row_key = None

        return row_data, row_key

    def is_indexable(self, data_type):
        # Remove size specifications and other modifiers for comparison
        base_type = data_type.lower()
        base_type = re.sub(r'\(.*\)', '', base_type).strip()
        if 'text' in base_type or 'blob' in base_type or 'json' in base_type:
            return False
        return True

    def create_staging_table_for_share_balances(self, copy_data=False):
        """
        Create or recreate staging table for dc_share_balances with same schema.
        If copy_data=True, copies all existing data from main table.
        Used for atomic updates to prevent race conditions.
        """
        staging_table = "dc_share_balances_staging"
        main_table = "dc_share_balances"

        cursor = self.db.dest_conn.cursor()
        try:
            # Drop staging table if it exists
            drop_query = f"DROP TABLE IF EXISTS `{staging_table}`"
            cursor.execute(drop_query)

            # Create staging table with same schema as main table
            create_query = f"CREATE TABLE `{staging_table}` LIKE `{main_table}`"
            cursor.execute(create_query)

            if copy_data:
                # Copy all existing data from main table to staging
                copy_query = f"""
                INSERT INTO `{staging_table}`
                SELECT * FROM `{main_table}`
                """
                cursor.execute(copy_query)
                copied_rows = cursor.rowcount
                logging.info(f"Copied {copied_rows:,} rows from {main_table} to staging")
            else:
                logging.info(f"Created empty staging table `{staging_table}`")

            self.db.dest_conn.commit()
        except Exception as e:
            logging.error(f"Error creating staging table: {e}")
            self.db.dest_conn.rollback()
            raise
        finally:
            cursor.close()

    def atomic_swap_share_balances_tables(self):
        """
        Atomically swap staging table with main dc_share_balances table.
        This operation is atomic - API queries will see either old or new data, never partial.
        """
        staging_table = "dc_share_balances_staging"
        main_table = "dc_share_balances"

        max_retries = 3
        retry_delay = 10  # seconds between retries

        for attempt in range(1, max_retries + 1):
            cursor = self.db.dest_conn.cursor()
            try:
                # Set a lock wait timeout so the RENAME fails fast instead of
                # blocking all API queries and causing a connection pileup.
                cursor.execute("SET SESSION lock_wait_timeout = 5")

                # Atomic swap
                swap_query = f"""
                RENAME TABLE
                    `{main_table}` TO `{main_table}_old`,
                    `{staging_table}` TO `{main_table}`
                """
                cursor.execute(swap_query)
                logging.info(f"Atomically swapped staging table to {main_table}")

                # Drop old table
                drop_query = f"DROP TABLE IF EXISTS `{main_table}_old`"
                cursor.execute(drop_query)
                logging.info(f"Dropped old table")

                self.db.dest_conn.commit()
                return  # Success
            except Exception as e:
                self.db.dest_conn.rollback()
                if attempt < max_retries:
                    logging.warning(f"Table swap attempt {attempt}/{max_retries} failed (lock timeout): {e}. Retrying in {retry_delay}s...")
                    time.sleep(retry_delay)
                else:
                    logging.error(f"Table swap failed after {max_retries} attempts: {e}")
                    raise
            finally:
                cursor.close()

    def create_and_populate_tables(self, shutdown_flag):
        try:
            now = datetime.now()
            if self.last_full_update is None or now - self.last_full_update >= self.full_update_interval:
                perform_full_update = True
            else:
                perform_full_update = False

            for table_info in self.tables_to_process:
                try:
                    source_table = table_info['source_table']
                    dest_table = table_info['dest_table']
                    key_column = table_info.get('key_column', 'id')
                    indexes = table_info.get('indexes', [])
                    composite_indexes = table_info.get('composite_indexes', [])
                    special_query = table_info.get('special_query')
                    unique_keys = table_info.get('unique_keys')
                    component = table_info.get('component')

                    logging.info(f"Creating and populating `{dest_table}` table...")

                    # Always get columns from SQLite schema
                    columns_info = self.db.execute_query('sqlite', f"PRAGMA table_info({source_table})")
                    columns_from_schema = {}
                    for col_info in columns_info:
                        col_name = col_info['name']
                        sqlite_type = col_info['type']
                        mysql_type = self.map_sqlite_type_to_mysql(sqlite_type, col_name)
                        columns_from_schema[col_name] = mysql_type

                    # Get any columns specified in table_info
                    columns_override = table_info.get('columns') or {}
                    # Update columns_from_schema with columns_override
                    columns_from_schema.update(columns_override)
                    
                    # For dc_players, add loan columns that exist only in MySQL
                    if dest_table == 'dc_players':
                        columns_from_schema.update({
                            'loan_offered': 'BIGINT',
                            'loan_offer_accepted': 'BIGINT', 
                            'loaned_to_club': 'BIGINT'
                        })
                    
                    columns = columns_from_schema

                    # Add 'updated_at' column
                    columns['updated_at'] = 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'

                    # List of columns for data processing
                    columns_list = list(columns.keys())

                    self.create_table(dest_table, columns, indexes, key_column, unique_keys,
                                      composite_indexes=composite_indexes)

                    if dest_table == 'dc_share_balances':
                        # Handle dc_share_balances separately
                        self.get_update_status(component)
                        if perform_full_update:
                            logging.info("Performing full update for dc_share_balances.")
                            self.populate_share_balances_table(
                                source_table, dest_table, columns_list, shutdown_flag,
                                key_column, unique_keys, special_query, perform_full_update
                            )
                            # Get the max entry_id from share_transactions
                            max_entry_id_result = self.db.execute_query(
                                'source', "SELECT MAX(id) as max_entry_id FROM share_transactions"
                            )
                            max_entry_id = max_entry_id_result[0]['max_entry_id'] if max_entry_id_result[0]['max_entry_id'] else 0
                            self.update_status(component, max_entry_id, last_processed_height=0, full_update=True)
                        else:
                            logging.info("Performing incremental update for dc_share_balances.")
                            self.populate_share_balances_table(
                                source_table, dest_table, columns_list, shutdown_flag,
                                key_column, unique_keys, special_query, perform_full_update
                            )
                    else:
                        # For other tables, process using last_updated_height
                        self.get_update_status(component)
                        self.populate_table(
                            source_table, dest_table, columns_list, shutdown_flag,
                            key_column, unique_keys, special_query, component, perform_full_update
                        )

                        # After processing dc_clubs, collect updated club IDs
                        if dest_table == 'dc_clubs':
                            self.updated_club_ids.update(self.collected_updated_club_ids)
                            logging.info(f"Collected updated club IDs: {self.collected_updated_club_ids}")
                            self.collected_updated_club_ids.clear()

                except Exception as e:
                    logging.error(f"Error processing table `{dest_table}`: {e}")
                    logging.debug(traceback.format_exc())
                    # Continue to the next table instead of raising
                    continue

            # ### Now that all tables are updated, handle post-update tasks ###

            # 1) Update club_info for any changed clubs
            if self.updated_club_ids:
                logging.info(f"Triggering update of club_info for updated clubs: {self.updated_club_ids}")
                self.club_info_updater.update_club_info(shutdown_flag, club_ids_to_update=list(self.updated_club_ids))

            # 2) ### NEW OR CHANGED CODE BELOW ###
            #    Sync `dc_users.club_id` to reflect any changes in `dc_clubs.manager_name` for those updated clubs.
            if self.updated_club_ids:
                self.sync_users_manager_club_ids(self.updated_club_ids, shutdown_flag)
                self.updated_club_ids.clear()

            logging.info("All tables created and populated successfully.")

        except Exception as e:
            logging.error(f"An error occurred in create_and_populate_tables: {e}")
            logging.debug(traceback.format_exc())
            raise

    def populate_table(self, source_table, dest_table, columns, shutdown_flag,
                       key_column, unique_keys, special_query, component, perform_full_update):
        try:
            # Ensure columns are consistently ordered
            columns = sorted(columns)

            if perform_full_update:
                last_processed_height = None
                max_last_updated_height = -1
            else:
                last_processed_height = self.last_processed_heights.get(component, -1)
                max_last_updated_height = last_processed_height

            # ADDED DEBUG LOG:
            logging.debug(
                "Populating table %s (source: %s) with last_processed_height=%s, perform_full_update=%s",
                dest_table, source_table, last_processed_height, perform_full_update
            )

            # Get table_alias from table_info
            table_alias = None
            for table_info in self.tables_to_process:
                if table_info['component'] == component:
                    table_alias = table_info.get('table_alias')
                    break

            source_data = self.get_source_data(source_table, last_processed_height, special_query, table_alias)
            
            # ADDED DEBUG LOG:
            logging.debug("Retrieved %d rows from source table '%s' for component '%s'.", len(source_data), source_table, component)

            if not source_data:
                logging.info(f"No new updates for `{dest_table}`. Skipping update.")
                return

            insert_query = self.prepare_insert_query(dest_table, columns, key_column, unique_keys)

            rows_to_update = []
            updated_club_ids = set()

            for i, row in enumerate(source_data):
                # ADDED DEBUG LOG:
                if i % 10000 == 0:  # Log every 10k rows to avoid spamming
                    logging.debug("Processing row %d for %s: %s", i, dest_table, row)
                row_data, row_key = self.process_row_data(row, columns, key_column, unique_keys)
                # Get the last_updated_height from the row, defaulting to -1 if None
                last_updated_height = row.get('last_updated_height', -1)
                if last_updated_height > max_last_updated_height:
                    max_last_updated_height = last_updated_height

                if any(k is None for k in row_key):
                    logging.warning(f"Row missing unique key column(s) '{unique_keys}'. Skipping row: {row}")
                    continue

                rows_to_update.append(row_data)

                # If we're populating `dc_clubs`, track which clubs changed
                if dest_table == 'dc_clubs':
                    updated_club_ids.add(row[key_column])  # e.g. club_id

            total_update_rows = len(rows_to_update)

            # ADDED DEBUG LOG:
            logging.debug(
                "Finished preparing rows for %s: total rows to update=%d, max_last_updated_height=%d",
                dest_table, total_update_rows, max_last_updated_height
            )

            batch_size = 5000
            with tqdm(total=total_update_rows, desc=f"Populating {dest_table}", unit="rows") as pbar:
                for i in range(0, total_update_rows, batch_size):
                    if shutdown_flag.is_set():
                        logging.info("Shutdown signal received. Exiting populate_table early.")
                        break

                    batch = rows_to_update[i:i+batch_size]
                    self.db.execute_many(self.db.dest_conn, insert_query, batch)
                    pbar.update(len(batch))

            logging.info(f"Successfully populated `{dest_table}` with {total_update_rows} updated rows.")

            # Update the last_processed_height for the component
            self.update_status(component, last_entry_id=0, last_processed_height=max_last_updated_height, full_update=perform_full_update)

            # After updating `dc_clubs`, store the updated club_ids so we can handle them later
            if dest_table == 'dc_clubs' and updated_club_ids:
                self.collected_updated_club_ids.update(updated_club_ids)

        except Exception as e:
            logging.error(f"Error populating table `{dest_table}`: {e}")
            logging.debug(traceback.format_exc())
            raise

    def populate_share_balances_table(self, source_table, dest_table, columns, shutdown_flag,
                                      key_column, unique_keys, special_query, perform_full_update):
        if perform_full_update:
            self.update_share_balances_full(dest_table, columns, shutdown_flag, key_column, unique_keys)
            # Update last_processed_entry_id
            max_entry_id_result = self.db.execute_query('source', "SELECT MAX(id) as max_entry_id FROM share_transactions")
            max_entry_id = max_entry_id_result[0]['max_entry_id'] if max_entry_id_result[0]['max_entry_id'] else 0
            self.update_status('dc_share_balances', max_entry_id, last_processed_height=0, full_update=True)
        else:
            self.update_share_balances_incrementally(dest_table, columns, shutdown_flag, key_column, unique_keys)
            # Update last_processed_entry_id is done in update_share_balances_incrementally

    def update_share_balances_full(self, dest_table, columns, shutdown_flag, key_column, unique_keys):
        try:
            logging.info(f"Performing full update for `{dest_table}` with staging table...")
            staging_table = "dc_share_balances_staging"

            # Create empty staging table
            self.create_staging_table_for_share_balances()

            # Get all share balances from SQLite
            share_balances_data = self.db.execute_query('sqlite', "SELECT * FROM share_balances")

            if not share_balances_data:
                logging.info("No share balances found in SQLite. Creating empty table.")
                # Still swap to ensure table is consistent (empty)
                self.atomic_swap_share_balances_tables()
                return

            # Prepare insert query for STAGING table
            insert_query = self.prepare_insert_query(dest_table, columns, key_column, unique_keys, target_table=staging_table)
            rows_to_update = []

            for row in share_balances_data:
                row_data, row_key = self.process_row_data(row, columns, key_column, unique_keys)
                if any(k is None for k in row_key):
                    logging.warning(f"Row missing unique key column(s) '{unique_keys}'. Skipping row: {row}")
                    continue
                rows_to_update.append(row_data)

            # Insert into STAGING table
            total_update_rows = len(rows_to_update)
            batch_size = 5000
            with tqdm(total=total_update_rows, desc=f"Updating {staging_table}", unit="rows") as pbar:
                for i in range(0, total_update_rows, batch_size):
                    if shutdown_flag.is_set():
                        logging.info("Shutdown signal received. Exiting update_share_balances_full early.")
                        break

                    batch = rows_to_update[i:i+batch_size]
                    self.db.execute_many(self.db.dest_conn, insert_query, batch)
                    pbar.update(len(batch))

            # Atomic swap: staging becomes main table
            logging.info("Performing atomic table swap...")
            self.atomic_swap_share_balances_tables()

            logging.info(f"Successfully performed full update of `{dest_table}` with {total_update_rows} rows.")
        except Exception as e:
            logging.error(f"Error performing full update for `{dest_table}`: {e}")
            logging.debug(traceback.format_exc())
            raise

    def update_share_balances_incrementally(self, dest_table, columns, shutdown_flag, key_column, unique_keys):
        try:
            component = 'dc_share_balances'

            self.get_update_status(component)
            last_entry_id = self.last_processed_entry_id

            max_entry_id_result = self.db.execute_query('source', "SELECT MAX(id) as max_entry_id FROM share_transactions")
            max_entry_id = max_entry_id_result[0]['max_entry_id'] if max_entry_id_result[0]['max_entry_id'] else 0

            if max_entry_id == last_entry_id:
                logging.info("No new entries in share_transactions. Skipping incremental update for dc_share_balances.")
                return

            new_transactions_query = """
            SELECT id as entry_id, share_type, name, other_name
            FROM share_transactions
            WHERE id > %s
            """
            new_transactions = self.db.execute_query('source', new_transactions_query, (last_entry_id,))

            affected_names = set()
            for tx in new_transactions:
                if tx['name']:
                    affected_names.add(tx['name'])
                if tx['other_name']:
                    affected_names.add(tx['other_name'])

            if not affected_names:
                logging.info("No affected names found in new transactions. Skipping incremental update for dc_share_balances.")
                self.update_status(component, max_entry_id, last_processed_height=0)
                return

            # Get updated share balances from SQLite
            placeholders = ','.join(['?'] * len(affected_names))
            share_balances_query = f"""
            SELECT *
            FROM share_balances
            WHERE name IN ({placeholders})
            """
            share_balances_data = self.db.execute_query('sqlite', share_balances_query, tuple(affected_names))

            sqlite_keys = set((row['name'], row['share_type'], row['share_id']) for row in share_balances_data)

            # Check what exists in main table for affected names
            mysql_share_balances_query = f"""
            SELECT name, share_type, share_id
            FROM `{dest_table}`
            WHERE name IN ({', '.join(['%s'] * len(affected_names))})
            """
            mysql_share_balances = self.db.execute_query(
                self.db.dest_conn, mysql_share_balances_query, tuple(affected_names)
            )

            mysql_keys = set((row['name'], row['share_type'], row['share_id']) for row in mysql_share_balances)

            # Find obsolete entries to delete
            keys_to_delete = mysql_keys - sqlite_keys

            # Prepare insert query for main table
            insert_query = self.prepare_insert_query(dest_table, columns, key_column, unique_keys)
            rows_to_update = []

            for row in share_balances_data:
                row_data, row_key = self.process_row_data(row, columns, key_column, unique_keys)
                if any(k is None for k in row_key):
                    logging.warning(f"Row missing unique key column(s) '{unique_keys}'. Skipping row: {row}")
                    continue
                rows_to_update.append(row_data)

            total_update_rows = len(rows_to_update)
            if total_update_rows == 0 and len(keys_to_delete) == 0:
                logging.info(f"No changes detected for `{dest_table}`. Skipping incremental update.")
                self.update_status(component, max_entry_id, last_processed_height=0)
                return

            # Execute all updates in single transaction for atomicity
            # Save and disable autocommit to enable true transactions
            original_autocommit = self.db.dest_conn.autocommit
            self.db.dest_conn.autocommit = False

            cursor = self.db.dest_conn.cursor()
            try:
                # Delete obsolete entries
                if keys_to_delete:
                    placeholders = ','.join(['(%s, %s, %s)'] * len(keys_to_delete))
                    delete_query = f"""
                    DELETE FROM `{dest_table}`
                    WHERE (`name`, `share_type`, `share_id`) IN ({placeholders})
                    """
                    params = [val for key in keys_to_delete for val in key]
                    cursor.execute(delete_query, tuple(params))
                    logging.info(f"Deleted {len(keys_to_delete)} obsolete entries")

                # Upsert updated rows in batches
                if total_update_rows > 0:
                    batch_size = 5000
                    with tqdm(total=total_update_rows, desc=f"Updating {dest_table}", unit="rows") as pbar:
                        for i in range(0, total_update_rows, batch_size):
                            if shutdown_flag.is_set():
                                logging.info("Shutdown signal received. Exiting update_share_balances_incrementally early.")
                                raise Exception("Shutdown requested")

                            batch = rows_to_update[i:i+batch_size]
                            cursor.executemany(insert_query, batch)
                            pbar.update(len(batch))

                # Single commit - all deletes and upserts are atomic!
                self.db.dest_conn.commit()
                logging.info(f"Committed transaction with {len(keys_to_delete)} deletes and {total_update_rows} upserts")

            except Exception as e:
                logging.error(f"Error during transaction, rolling back: {e}")
                self.db.dest_conn.rollback()
                raise
            finally:
                cursor.close()
                # Restore original autocommit state
                self.db.dest_conn.autocommit = original_autocommit

            logging.info(f"Successfully updated `{dest_table}` with {total_update_rows} updated rows.")
            self.update_status(component, max_entry_id, last_processed_height=0)
        except Exception as e:
            logging.error(f"Error updating share balances incrementally: {e}")
            logging.debug(traceback.format_exc())
            raise

    ### NEW OR CHANGED CODE BELOW ###
    def sync_users_manager_club_ids(self, updated_club_ids, shutdown_flag):
        """
        Ensure dc_users.club_id matches the manager_name field in dc_clubs,
        only for the subset of clubs that were just updated.

        1) Assign the club_id to any user whose name = manager_name for these clubs.
        2) Remove (set to NULL) the club_id for any user that no longer manages that club.
        """
        if shutdown_flag.is_set() or not updated_club_ids:
            return

        logging.info("Synchronizing dc_users.club_id with dc_clubs.manager_name for updated clubs...")

        placeholders = ','.join(['%s'] * len(updated_club_ids))

        # 1) Set user.club_id to c.club_id if user.name = c.manager_name
        #    for clubs in updated_club_ids.
        assign_query = f"""
            UPDATE dc_users u
            JOIN dc_clubs c ON u.name = c.manager_name
            SET u.club_id = c.club_id
            WHERE c.club_id IN ({placeholders})
        """
        self.db.execute_query(self.db.dest_conn, assign_query, tuple(updated_club_ids))

        # 2) For any user whose club_id is in this updated set,
        #    if that user is no longer manager_name of that club (c is NULL),
        #    set user.club_id to NULL.
        remove_query = f"""
            UPDATE dc_users u
            LEFT JOIN dc_clubs c
                ON u.name = c.manager_name
                AND c.club_id = u.club_id
            SET u.club_id = NULL
            WHERE c.club_id IS NULL
              AND u.club_id IN ({placeholders})
        """
        self.db.execute_query(self.db.dest_conn, remove_query, tuple(updated_club_ids))

        logging.info("Successfully synced dc_users with dc_clubs for updated clubs.")
