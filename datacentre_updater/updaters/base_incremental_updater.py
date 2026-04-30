# updaters/base_incremental_updater.py

import logging
import time
from db_manager import CHARSET, COLLATION

class BaseIncrementalUpdater:
    """
    Base class for updaters with incremental + periodic full recalculation pattern.
    
    Subclasses must implement:
    - create_table(table_name): Create table with proper schema
    - process_incremental(last_processed_id, shutdown_flag): Process new records, return last ID processed
    - process_full(shutdown_flag): Process all records to staging table, return True/False
    """
    
    def __init__(self, db_manager, component_name, dest_table):
        self.db = db_manager
        self.component_name = component_name
        self.dest_table = dest_table
        self.staging_table = f"{dest_table}_staging"
        self.status_table = "incremental_update_status"
        
        # Ensure status table exists
        self.ensure_status_table_exists()
    
    # ============================================================================
    # Abstract methods - subclass must implement
    # ============================================================================
    
    def create_table(self, table_name):
        """
        Create table with proper schema.
        Called for both main table and staging table.
        
        Args:
            table_name: Name of table to create
        """
        raise NotImplementedError("Subclass must implement create_table")
    
    def process_incremental(self, last_processed_id, shutdown_flag):
        """
        Process new records since last_processed_id.
        Should write updates directly to self.dest_table.
        Handles shutdown gracefully by returning last successfully processed ID.
        
        Args:
            last_processed_id: Starting point (exclusive) - process records with id > this
            shutdown_flag: threading.Event to check for shutdown requests
            
        Returns:
            int: The highest ID successfully processed (used as new checkpoint)
        """
        raise NotImplementedError("Subclass must implement process_incremental")
    
    def process_full(self, shutdown_flag):
        """
        Process all records from scratch.
        Should write to self.staging_table.
        Handles shutdown gracefully by returning None.
        
        Args:
            shutdown_flag: threading.Event to check for shutdown requests
            
        Returns:
            int or None: The highest ID processed if completed successfully,
                         None if interrupted by shutdown
        """
        raise NotImplementedError("Subclass must implement process_full")
    
    # ============================================================================
    # Template methods - orchestration (framework code)
    # ============================================================================
    
    def update_incremental(self, shutdown_flag):
        """
        Incremental update with atomic transaction including checkpoint.
        Wraps process_incremental in transaction with checkpoint update.
        """
        # Ensure main table exists
        self.ensure_main_table_exists()
        
        # Get last checkpoint
        last_processed_id = self.get_last_processed_id()
        
        logging.info(f"{self.component_name}: Starting incremental update from ID {last_processed_id}")
        
        # Start atomic transaction
        original_autocommit = self.db.dest_conn.autocommit
        self.db.dest_conn.autocommit = False
        cursor = self.db.dest_conn.cursor()
        
        try:
            # Call subclass to process and write data
            new_last_id = self.process_incremental(last_processed_id, shutdown_flag)
            
            # Update checkpoint in same transaction
            cursor.execute(f"""
                UPDATE `{self.status_table}` 
                SET last_processed_id = %s, last_update_time = CURRENT_TIMESTAMP
                WHERE component = %s
            """, (new_last_id, self.component_name))
            
            # Commit everything atomically
            self.db.dest_conn.commit()
            logging.info(f"{self.component_name}: Incremental update completed, checkpoint updated to {new_last_id}")
            
        except Exception as e:
            logging.error(f"{self.component_name}: Error in incremental update, rolling back: {e}")
            self.db.dest_conn.rollback()
            raise
        finally:
            cursor.close()
            self.db.dest_conn.autocommit = original_autocommit
    
    def update_full(self, shutdown_flag):
        """
        Full update with staging table and atomic swap.
        Creates staging, calls process_full, then swaps or cleans up.
        """
        logging.info(f"{self.component_name}: Starting full update with staging table")
        
        # Ensure main table exists
        self.ensure_main_table_exists()
        
        # Create staging table
        self.create_staging_table()
        
        try:
            # Call subclass to process and write to staging
            new_last_id = self.process_full(shutdown_flag)
            
            if new_last_id is not None:
                # Atomic swap
                self.atomic_swap_tables()
                
                # Update checkpoint after successful swap
                self.update_checkpoint_after_full(new_last_id)
                
                logging.info(f"{self.component_name}: Full update completed successfully")
            else:
                logging.info(f"{self.component_name}: Full update interrupted by shutdown")
        
        finally:
            # Clean up: drop staging/old table
            # After successful swap: drops {dest_table}_old (old main table)
            # After interruption: drops {staging_table} (incomplete staging)
            self.cleanup_after_full_update()
    
    # ============================================================================
    # Helper methods - status table management
    # ============================================================================
    
    def ensure_status_table_exists(self):
        """Create the status tracking table if it doesn't exist"""
        create_query = f"""
        CREATE TABLE IF NOT EXISTS `{self.status_table}` (
            `component` VARCHAR(100) PRIMARY KEY,
            `last_processed_id` BIGINT NOT NULL,
            `last_full_update` TIMESTAMP NOT NULL,
            `last_update_time` TIMESTAMP NOT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
        self.db.execute_query(self.db.dest_conn, create_query)
        
        # Initialize row for this component if it doesn't exist
        init_query = f"""
        INSERT IGNORE INTO `{self.status_table}` 
            (component, last_processed_id, last_full_update, last_update_time)
        VALUES (%s, 0, '1970-01-01 00:00:01', '1970-01-01 00:00:01')
        """
        self.db.execute_query(self.db.dest_conn, init_query, (self.component_name,))
    
    def get_last_processed_id(self):
        """Get the last processed ID from status table"""
        query = f"""
        SELECT last_processed_id
        FROM `{self.status_table}`
        WHERE component = %s
        """
        result = self.db.execute_query(self.db.dest_conn, query, (self.component_name,))
        if result and len(result) > 0:
            return result[0]['last_processed_id']
        return 0
    
    def update_checkpoint_after_full(self, last_processed_id):
        """Update checkpoint and full update timestamp after successful full recalculation"""
        cursor = self.db.dest_conn.cursor()
        try:
            cursor.execute(f"""
                UPDATE `{self.status_table}`
                SET last_processed_id = %s,
                    last_full_update = CURRENT_TIMESTAMP,
                    last_update_time = CURRENT_TIMESTAMP
                WHERE component = %s
            """, (last_processed_id, self.component_name))
            self.db.dest_conn.commit()
        finally:
            cursor.close()
    
    # ============================================================================
    # Helper methods - table management
    # ============================================================================
    
    def ensure_main_table_exists(self):
        """Ensure main table exists by calling subclass create_table"""
        cursor = self.db.dest_conn.cursor()
        try:
            cursor.execute(f"SHOW TABLES LIKE '{self.dest_table}'")
            if not cursor.fetchone():
                logging.info(f"{self.component_name}: Creating main table {self.dest_table}")
                self.create_table(self.dest_table)
        finally:
            cursor.close()
    
    def create_staging_table(self):
        """Create staging table with same schema as main"""
        cursor = self.db.dest_conn.cursor()
        try:
            # Drop if exists
            cursor.execute(f"DROP TABLE IF EXISTS `{self.staging_table}`")
            self.db.dest_conn.commit()
            
            # Create with same schema
            logging.info(f"{self.component_name}: Creating staging table {self.staging_table}")
            self.create_table(self.staging_table)
        finally:
            cursor.close()
    
    def atomic_swap_tables(self):
        """
        Atomically swap staging and main tables using RENAME TABLE.
        After this: main table has new data, old main table is renamed to {dest_table}_old
        """
        max_retries = 3
        retry_delay = 10  # seconds between retries

        for attempt in range(1, max_retries + 1):
            cursor = self.db.dest_conn.cursor()
            try:
                # Set a lock wait timeout so the RENAME fails fast instead of
                # blocking all API queries and causing a connection pileup.
                cursor.execute("SET SESSION lock_wait_timeout = 5")
                cursor.execute(f"""
                    RENAME TABLE
                        `{self.dest_table}` TO `{self.dest_table}_old`,
                        `{self.staging_table}` TO `{self.dest_table}`
                """)
                self.db.dest_conn.commit()
                logging.info(f"{self.component_name}: Atomically swapped {self.staging_table} to {self.dest_table}")
                return  # Success
            except Exception as e:
                self.db.dest_conn.rollback()
                if attempt < max_retries:
                    logging.warning(
                        f"{self.component_name}: Table swap attempt {attempt}/{max_retries} failed (lock timeout): {e}. "
                        f"Retrying in {retry_delay}s..."
                    )
                    time.sleep(retry_delay)
                else:
                    logging.error(f"{self.component_name}: Table swap failed after {max_retries} attempts: {e}")
                    raise
            finally:
                cursor.close()
    
    def cleanup_after_full_update(self):
        """
        Clean up after full update.
        Drops either the incomplete staging table (if interrupted)
        or the old main table (if swap succeeded).
        """
        cursor = self.db.dest_conn.cursor()
        try:
            # Try to drop both - one will exist depending on whether swap happened
            cursor.execute(f"DROP TABLE IF EXISTS `{self.staging_table}`")
            cursor.execute(f"DROP TABLE IF EXISTS `{self.dest_table}_old`")
            self.db.dest_conn.commit()
        finally:
            cursor.close()
