# trade_updater.py
import logging
import traceback
import json
from datetime import datetime, timedelta
from tqdm import tqdm
import re

from db_manager import CHARSET, COLLATION

def initialize_trade_update_status_table(db_manager):
    """
    Shared function to create the trade_update_status table.
    
    This table is used by multiple updater modules to track their processing status:
    
    1. TradeUpdaterBase (and subclasses like PlayerUpdater, ClubUpdater, etc.):
       - last_processed_height: Tracks blockchain height for incremental trade updates
       - last_full_update: Timestamp for periodic full data rebuilds (every 6 hours)
    
    2. LeagueUpdater:
       - last_processed_height: Tracks fixture message heights from archival database
       - last_full_update: Timestamp for periodic full league data updates
    
    3. PlayerLoanUpdater:
       - last_processed_height: Tracks loan update IDs from player_loan_updates table
       - last_full_update: Timestamp for periodic full data rebuilds (every 6 hours)
    
    Each component uses a unique 'component' name to maintain separate status tracking.
    """
    create_query = f"""
    CREATE TABLE IF NOT EXISTS `trade_update_status` (
        `component` VARCHAR(50) PRIMARY KEY,
        `last_processed_height` BIGINT,
        `last_full_update` DATETIME
    ) ENGINE=InnoDB DEFAULT CHARSET={CHARSET} COLLATE={COLLATION}
    """
    db_manager.execute_query(db_manager.dest_conn, create_query)
    logging.info("Initialized trade_update_status table.")

class TradeUpdaterBase:
    def __init__(self, db_manager, component_name, key_column, dest_table, update_columns, share_type=None):
        self.db = db_manager
        self.component_name = component_name
        self.key_column = key_column
        self.dest_table = dest_table
        self.update_columns = update_columns
        self.share_type = share_type  # Add share_type attribute

        self.last_processed_height = None
        self.last_full_update = None
        self.full_update_interval = timedelta(hours=6)  # Full update every 6 hours

        # Flag to force full update on startup
        self.perform_full_update_on_startup = True

        # Initialize the status table
        self.initialize_status_table()

    def initialize_status_table(self):
        # Use shared function to create the status table
        initialize_trade_update_status_table(self.db)

    def get_update_status(self):
        query = "SELECT `last_processed_height`, `last_full_update` FROM `trade_update_status` WHERE `component` = %s"
        result = self.db.execute_query(self.db.dest_conn, query, (self.component_name,))
        if result:
            self.last_processed_height = result[0]['last_processed_height']
            self.last_full_update = result[0]['last_full_update']
            logging.debug(f"Retrieved last_full_update = {self.last_full_update} ({type(self.last_full_update)}) for component {self.component_name}")

            # Handle NULL and zero dates
            if not self.last_full_update or str(self.last_full_update) in ('0000-00-00 00:00:00', 'None'):
                self.last_full_update = None
                logging.info(f"First run for {self.component_name}. Performing initial full update.")
        else:
            # Insert initial status
            insert_query = "INSERT INTO `trade_update_status` (`component`, `last_processed_height`, `last_full_update`) VALUES (%s, %s, %s)"
            self.db.execute_query(self.db.dest_conn, insert_query, (self.component_name, 0, None))
            self.last_processed_height = 0
            self.last_full_update = None
            logging.info(f"Inserted initial status for {self.component_name}. Performing initial full update.")
  

    def update_status(self, last_height, full_update=False):
        if full_update:
            update_query = """
            UPDATE `trade_update_status` 
            SET `last_processed_height` = %s, `last_full_update` = %s
            WHERE `component` = %s
            """
            last_full_update_time = datetime.now()
            params = (last_height, last_full_update_time, self.component_name)
            self.last_full_update = last_full_update_time
        else:
            update_query = """
            UPDATE `trade_update_status` 
            SET `last_processed_height` = %s
            WHERE `component` = %s
            """
            params = (last_height, self.component_name)

        self.db.execute_query(self.db.dest_conn, update_query, params)
        self.last_processed_height = last_height
              
    def update_trading_data(self, shutdown_flag):
        try:
            self.get_update_status()
            now = datetime.now()
            perform_full_update = False

            # Always perform full update on startup
            if self.perform_full_update_on_startup:
                logging.info(f"Performing full trading data update for {self.component_name} on startup.")
                perform_full_update = True
                self.perform_full_update_on_startup = False  # Reset the flag after first use
            elif self.last_full_update is None:
                logging.info(f"First run for {self.component_name}. Performing initial full update.")
                perform_full_update = True
            elif now - self.last_full_update >= self.full_update_interval:
                perform_full_update = True

            # Get the maximum height from the source database
            max_height_result = self.db.execute_query('source', "SELECT MAX(height) as max_height FROM share_trade_history")
            max_height = max_height_result[0]['max_height'] if max_height_result[0]['max_height'] else 0

            if perform_full_update:
                logging.info(f"Performing full trading data update for {self.component_name}...")
                affected_entities = None  # Will process all entities
            elif max_height > self.last_processed_height:
                logging.info(f"New trades detected from height {self.last_processed_height + 1} to {max_height}.")
                affected_entities = self.get_affected_entities(self.last_processed_height, max_height)
                if not affected_entities:
                    logging.info(f"No affected {self.component_name} to update.")
                    self.update_status(max_height, full_update=False)
                    return
            else:
                logging.info(f"No new trades detected. Skipping {self.component_name} trading data update.")
                return

            # Process the trading data
            self.process_trading_data(affected_entities, perform_full_update, shutdown_flag)

            # Update last processed height and last full update time
            self.update_status(max_height, full_update=perform_full_update)

        except Exception as e:
            logging.error(f"An error occurred in {self.component_name} update_trading_data: {e}")
            logging.debug(traceback.format_exc())
            raise

    def process_trading_data(self, affected_entities, perform_full_update, shutdown_flag):
        logging.info(f"Updating trading data for `{self.dest_table}`...")

        # Fetch data from source database
        source_query, params = self.get_source_query(perform_full_update, affected_entities)
        if not source_query:
            logging.info("No source data to process.")
            return

        # Execute the source query on the source database
        source_data = self.db.execute_query('source', source_query, params)

        logging.info(f"Processing trading data for {len(source_data)} entities.")
        logging.debug(f"Entities being processed: {[row[self.key_column] for row in source_data]}")

        # Define columns and data types for the table
        columns = self.get_columns_for_table()

        # Create the table if it does not exist, or add missing columns
        self.create_table(columns)

        # Prepare for data processing
        columns_list = [self.key_column] + self.update_columns

        rows_to_update = []
        for row in source_data:
            # Process the row
            row_data = self.process_row_data(row, columns_list)
            row_key = row.get(self.key_column)
            if row_key is None:
                logging.warning(f"Row missing key column '{self.key_column}'. Skipping row: {row}")
                continue

            if perform_full_update or affected_entities is None or row_key in affected_entities:
                rows_to_update.append(row_data)
                logging.debug(f"Updating {self.dest_table} for {self.key_column}={row_key}")

        total_update_rows = len(rows_to_update)
        if total_update_rows == 0:
            logging.info(f"No changes detected for `{self.dest_table}`. Skipping update.")
            return

        logging.info(f"Updating {total_update_rows} rows in `{self.dest_table}`.")

        # Prepare insert query (update existing rows)
        insert_query = self.prepare_insert_query(columns_list)

        batch_size = 5000
        with tqdm(total=total_update_rows, desc=f"Updating trading data for {self.dest_table}", unit="rows") as pbar:
            for i in range(0, total_update_rows, batch_size):
                if shutdown_flag.is_set():
                    logging.info("Shutdown signal received. Exiting update_trading_data early.")
                    break

                batch = rows_to_update[i:i+batch_size]
                self.db.execute_many(self.db.dest_conn, insert_query, batch)
                pbar.update(len(batch))

        logging.info(f"Successfully updated trading data for `{self.dest_table}` with {total_update_rows} updated rows.")

    def process_row_data(self, row, columns_list):
        row_data = []
        for col in columns_list:
            value = row.get(col, None)
            # If columns like last_7days are JSON arrays, ensure consistent JSON
            if col in ['last_7days', 'last_7days_price'] and value is not None:
                parsed_value = json.loads(value)
                value = json.dumps(parsed_value, separators=(',', ':'))
            row_data.append(value)
        return row_data

    def prepare_insert_query(self, columns):
        update_cols_sql = ', '.join(f"`{col}`=VALUES(`{col}`)" for col in self.update_columns)
        insert_query = f"""
            INSERT INTO `{self.dest_table}` ({', '.join(f'`{col}`' for col in columns)})
            VALUES ({', '.join(['%s'] * len(columns))})
            ON DUPLICATE KEY UPDATE
                {update_cols_sql}
        """
        return insert_query

    def get_affected_entities(self, start_height, end_height):
        if self.share_type:
            new_trades_query = f"""
            SELECT DISTINCT share_id AS {self.key_column}
            FROM share_trade_history
            WHERE height > %s AND height <= %s AND share_type = %s
            """
            params = (start_height, end_height, self.share_type)
            new_trades = self.db.execute_query('source', new_trades_query, params)
            affected_entities = {trade[self.key_column] for trade in new_trades if trade[self.key_column]}
            return affected_entities
        else:
            # Must be implemented in subclass
            raise NotImplementedError("Subclasses should implement get_affected_entities")

    def get_source_query(self, perform_full_update, entity_set):
        if self.share_type:
            return self.generate_source_query(perform_full_update, entity_set)
        else:
            # Must be implemented in subclass
            raise NotImplementedError("Subclasses should implement get_source_query")

    def generate_source_query(self, perform_full_update, entity_set):
        """
        Builds a source query that sums the last 7 days' trading volume, 
        an array of daily volumes for the last 7 days (last_7days),
        an array of daily average prices (last_7days_price),
        plus the `last_price` from the most recent trade.
        """
        params = []
        key_column = self.key_column

        where_clauses = ["sth.share_type = %s"]
        params.append(self.share_type)

        if perform_full_update:
            pass
        elif entity_set:
            placeholders = ','.join(['%s'] * len(entity_set))
            where_clauses.append(f"sth.share_id IN ({placeholders})")
            params.extend(entity_set)
        else:
            # No entities to update
            return None, ()

        where_clause = 'WHERE ' + ' AND '.join(where_clauses)

        # Generate the expressions for the last 7 days
        last_7days_exprs = []
        last_7days_price_exprs = []

        # We now wrap DATE_SUB(...) with DATE(...) so MariaDB can parse properly:
        for i in range(6, -1, -1):
            date_expr = f"DATE(DATE_SUB(CURDATE(), INTERVAL {i} DAY))"
            total_volume_expr = (
                f"IFNULL(SUM(CASE WHEN DATE(FROM_UNIXTIME(b.date)) = {date_expr} "
                f"THEN sth.price * sth.num END), 0)"
            )
            avg_price_expr = (
                f"IFNULL(ROUND(SUM(CASE WHEN DATE(FROM_UNIXTIME(b.date)) = {date_expr} "
                f"THEN sth.price * sth.num END) / NULLIF(SUM(CASE WHEN DATE(FROM_UNIXTIME(b.date)) = {date_expr} "
                f"THEN sth.num END), 0), 2), 0)"
            )
            last_7days_exprs.append(total_volume_expr)
            last_7days_price_exprs.append(avg_price_expr)

        def generate_concat_expression(expr_list):
            # Use double-quoted bracket strings to avoid issues in MariaDB
            result = ['"["']
            expr_count = len(expr_list)
            for idx, expr in enumerate(expr_list):
                result.append(expr)
                if idx < expr_count - 1:
                    # Insert a literal comma between days
                    result.append('","')
            result.append('"]"')
            return "CONCAT(" + ", ".join(result) + ")"

        last_7days_str = generate_concat_expression(last_7days_exprs) + " AS last_7days"
        last_7days_price_str = generate_concat_expression(last_7days_price_exprs) + " AS last_7days_price"

        # Instead of just MAX(sth.price), we select the truly latest price from a subquery:
        source_query = f"""
            SELECT 
                sth.share_id AS {key_column},
                (
                  SELECT sth2.price
                  FROM share_trade_history sth2
                  JOIN blocks b2 ON sth2.height = b2.height
                  WHERE sth2.share_id = sth.share_id
                    AND sth2.share_type = sth.share_type
                  ORDER BY b2.date DESC, sth2.id DESC
                  LIMIT 1
                ) AS last_price,
                SUM(
                    CASE 
                        WHEN b.date >= UNIX_TIMESTAMP(DATE_SUB(NOW(), INTERVAL 1 DAY)) 
                        THEN sth.price * sth.num 
                        ELSE 0 
                    END
                ) AS volume_1_day,
                SUM(
                    CASE 
                        WHEN b.date >= UNIX_TIMESTAMP(DATE_SUB(NOW(), INTERVAL 7 DAY)) 
                        THEN sth.price * sth.num 
                        ELSE 0 
                    END
                ) AS volume_7_day,
                {last_7days_str},
                {last_7days_price_str}
            FROM share_trade_history sth
            JOIN blocks b ON sth.height = b.height
            {where_clause}
            GROUP BY sth.share_id
        """
        return source_query, tuple(params)

    def get_columns_for_table(self):
        columns = {}

        # Set data type for key_column
        columns[self.key_column] = 'INT'

        # Define data types for update_columns
        for col in self.update_columns:
            if col == 'last_price':
                columns[col] = 'BIGINT'
            elif col in ['last_7days', 'last_7days_price']:
                columns[col] = 'LONGTEXT'
            elif 'volume' in col:
                columns[col] = 'BIGINT'
            else:
                columns[col] = 'BIGINT'

        # Add 'updated_at' column
        columns['updated_at'] = 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'

        return columns

    def is_indexable(self, data_type):
        # Remove size specs and modifiers for comparison
        base_type = data_type.lower()
        base_type = re.sub(r'\(.*\)', '', base_type).strip()
        if 'text' in base_type or 'blob' in base_type or 'json' in base_type:
            return False
        return True

    def create_table(self, columns, indexes=None):
        logging.info(f"Ensuring table `{self.dest_table}` exists with correct columns.")
        existing_columns = self.db.get_existing_columns_with_types(self.dest_table)

        # Add 'updated_at' column if not present
        if 'updated_at' not in columns:
            columns['updated_at'] = 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'

        if existing_columns is None:
            # Table does not exist, create it
            columns_sql = ', '.join(f"`{col}` {dtype}" for col, dtype in columns.items())
            # Add primary key on self.key_column
            columns_sql += f", PRIMARY KEY (`{self.key_column}`)"

            # Prepare index columns
            index_columns = [
                col for col in columns
                if col not in ['updated_at', self.key_column] and self.is_indexable(columns[col])
            ]
            index_sql_list = [f"INDEX (`{col}`)" for col in index_columns]
            if indexes:
                index_sql_list.extend([f"INDEX (`{index}`)" for index in indexes])
            index_sql_list.append("INDEX (`updated_at`)")
            index_sql = ', '.join(index_sql_list)

            create_query = f"""
            CREATE TABLE IF NOT EXISTS `{self.dest_table}` (
                {columns_sql},
                {index_sql}
            ) ENGINE=InnoDB DEFAULT CHARSET={CHARSET} COLLATE={COLLATION}
            """
            try:
                self.db.execute_query(self.db.dest_conn, create_query)
                logging.info(f"Table `{self.dest_table}` created successfully.")
            except Exception as e:
                logging.error(f"Error creating table `{self.dest_table}`: {e}")
                raise
        else:
            # Table exists, check for missing columns or mismatched types
            for col, dtype in columns.items():
                existing_dtype = existing_columns.get(col)
                if existing_dtype is None:
                    # Add missing column
                    try:
                        alter_query = f"ALTER TABLE `{self.dest_table}` ADD COLUMN `{col}` {dtype}"
                        self.db.execute_query(self.db.dest_conn, alter_query)
                        logging.info(f"Added column `{col}` to table `{self.dest_table}`.")
                    except Exception as e:
                        logging.error(f"Error adding column `{col}` to table `{self.dest_table}`: {e}")
                        raise
                else:
                    # Compare data types
                    normalized_existing = self.normalize_data_type(existing_dtype)
                    normalized_expected = self.normalize_data_type(dtype)
                    if normalized_existing != normalized_expected:
                        # Modify column to match
                        try:
                            alter_query = f"ALTER TABLE `{self.dest_table}` MODIFY COLUMN `{col}` {dtype}"
                            self.db.execute_query(self.db.dest_conn, alter_query)
                            logging.info(f"Modified column `{col}` in table `{self.dest_table}` to `{dtype}`.")
                        except Exception as e:
                            logging.error(f"Error modifying column `{col}` in table `{self.dest_table}`: {e}")
                            raise

    def normalize_data_type(self, data_type):
        # Remove parentheses, 'unsigned', 'collate', etc.
        base_type = data_type.lower()
        base_type = re.sub(r'\(.*\)', '', base_type)
        base_type = re.sub(r'\bunsigned\b', '', base_type)
        base_type = re.sub(r'\bcharacter set\s+\w+', '', base_type)
        base_type = re.sub(r'\bcollate\s+\w+', '', base_type)
        base_type = re.sub(r'\bdefault\s+[^ ]+', '', base_type)
        base_type = re.sub(r'\bnot null\b', '', base_type)
        base_type = re.sub(r'\bnull\b', '', base_type)
        base_type = re.sub(r'\bon update\s+\w+', '', base_type)
        base_type = base_type.strip()

        # Normalize BOOLEAN -> TINYINT(1)
        if base_type == 'boolean':
            base_type = 'tinyint(1)'

        return base_type
