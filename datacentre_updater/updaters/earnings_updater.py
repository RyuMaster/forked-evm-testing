# earnings_updater.py

import logging
import time
from db_manager import CHARSET, COLLATION
from .base_incremental_updater import BaseIncrementalUpdater

class EarningsUpdater(BaseIncrementalUpdater):
    def __init__(self, db_manager):
        super().__init__(db_manager, 'earnings_updater', 'dc_earnings')
        
        # Cache for block height thresholds
        self.height_cache = {
            'height_7d': None,
            'height_30d': None,
            'cache_time': 0
        }
        self.height_cache_ttl = 300  # 5 minutes
    
    # ============================================================================
    # Implementation of abstract methods
    # ============================================================================
    
    def create_table(self, table_name):
        """Create the dc_earnings table with proper schema"""
        create_query = f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` (
            `name` VARCHAR(255) CHARACTER SET {CHARSET} COLLATE {COLLATION} NOT NULL,
            `share_type` VARCHAR(50) NOT NULL,
            `share_id` BIGINT NOT NULL,
            `earnings_7d` BIGINT DEFAULT 0,
            `earnings_30d` BIGINT DEFAULT 0,
            `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (`name`, `share_type`, `share_id`),
            INDEX `idx_share_lookup` (`share_type`, `share_id`),
            INDEX `idx_updated_at` (`updated_at`)
        ) ENGINE=InnoDB DEFAULT CHARSET={CHARSET} COLLATE={COLLATION}
        """
        self.db.execute_query(self.db.dest_conn, create_query)
        logging.info(f"Created table `{table_name}`")
    
    def process_incremental(self, last_processed_id, shutdown_flag):
        """
        Process new dividend entries since last_processed_id.
        Writes updates directly to self.dest_table.
        Returns the highest ID successfully processed.
        
        Note: shutdown_flag is ignored - incremental updates are fast and run to completion.
        """
        start_time = time.time()
        
        # Get height thresholds (cached)
        height_7d, height_30d = self.get_height_thresholds()
        
        if height_30d == 0:
            logging.warning("Could not get height thresholds, skipping incremental update")
            return last_processed_id
        
        # On first run (last_processed_id=0), start from the first id within 30d window
        # This avoids loading months of old historical data
        if last_processed_id == 0:
            min_id_query = """
            SELECT MIN(id) as min_id
            FROM user_balance_sheets
            WHERE height >= %s
                AND type LIKE 'dividend%%'
            """
            min_id_result = self.db.execute_query('source', min_id_query, (height_30d,))
            if min_id_result and min_id_result[0]['min_id']:
                last_processed_id = min_id_result[0]['min_id'] - 1  # Start from just before this
                logging.info(f"First run: Starting from id {last_processed_id + 1} (earliest entry in 30d window)")
        
        # Get ALL new dividends within the 30d height window
        query = """
        SELECT
            id,
            name,
            other_type,
            other_id,
            height,
            amount
        FROM user_balance_sheets
        WHERE id > %s
            AND height >= %s
            AND type LIKE 'dividend%%'
            AND other_type IS NOT NULL
            AND other_id > 0
        ORDER BY id ASC
        """
        
        new_dividends = self.db.execute_query('source', query, (last_processed_id, height_30d))
        
        if not new_dividends:
            # No new dividends
            return last_processed_id
        
        new_count = len(new_dividends)
        new_max_id = max(row['id'] for row in new_dividends)
        
        logging.info(f"Found {new_count:,} new dividend entries (id {last_processed_id + 1} to {new_max_id}), processing...")
        
        # Get affected users
        affected_users = set()
        for row in new_dividends:
            affected_users.add(row['name'])
        
        total_affected = len(affected_users)
        logging.info(f"Affected users: {total_affected:,}")
        
        # Process users in batches to avoid huge IN clauses
        user_batch_size = 1000
        affected_users_list = list(affected_users)
        all_earnings_combined = []
        
        for batch_start in range(0, total_affected, user_batch_size):
            batch_end = min(batch_start + user_batch_size, total_affected)
            user_batch = affected_users_list[batch_start:batch_end]
            
            logging.info(f"Processing user batch {batch_start + 1}-{batch_end} of {total_affected}...")
            
            # For this batch of users, get ALL their dividends from last 30 days
            user_placeholders = ','.join(['%s'] * len(user_batch))
            all_dividends_query = f"""
            SELECT
                name,
                other_type,
                other_id,
                height,
                amount
            FROM user_balance_sheets
            WHERE name IN ({user_placeholders})
                AND type LIKE 'dividend%%'
                AND height >= %s
                AND other_type IS NOT NULL
                AND other_id > 0
            ORDER BY name, other_type, other_id
            """
            
            params = list(user_batch) + [height_30d]
            all_user_dividends = self.db.execute_query('source', all_dividends_query, params)
            
            if not all_user_dividends:
                continue
            
            # Process in memory: Group by (name, share_type, share_id)
            earnings_dict = {}
            
            for row in all_user_dividends:
                name = row['name']
                share_type = row['other_type']
                share_id = row['other_id']
                height = row['height']
                amount = row['amount']
                
                key = (name, share_type, share_id)
                
                if key not in earnings_dict:
                    earnings_dict[key] = {'e7': 0, 'e30': 0}
                
                # Add to 30d always
                earnings_dict[key]['e30'] += amount
                
                # Add to 7d if within window
                if height >= height_7d:
                    earnings_dict[key]['e7'] += amount
            
            # Convert to list for batch upsert
            for (name, share_type, share_id), earnings in earnings_dict.items():
                all_earnings_combined.append((
                    name,
                    share_type,
                    share_id,
                    earnings['e7'],
                    earnings['e30']
                ))
        
        # Write all earnings to main table (base class wraps this in transaction with checkpoint)
        total_rows_upserted = self.upsert_earnings_batch(all_earnings_combined) if all_earnings_combined else 0
        
        elapsed = time.time() - start_time
        
        logging.info(
            f"Incremental earnings update: Processed {total_affected:,} users, "
            f"upserted {total_rows_upserted:,} rows in {elapsed:.2f}s"
        )
        
        return new_max_id
    
    def process_full(self, shutdown_flag):
        """
        Process all records from scratch.
        Writes to self.staging_table.
        Returns the highest processed ID if completed, None if interrupted.
        """
        start_time = time.time()
        logging.info("Starting full earnings recalculation...")
        
        # Get max ID upfront for checkpoint (total max, not just dividends)
        max_id_result = self.db.execute_query('source', "SELECT MAX(id) as max_id FROM user_balance_sheets")
        current_max_id = max_id_result[0]['max_id'] if max_id_result and max_id_result[0]['max_id'] else 0
        
        # Get height thresholds (will refresh cache)
        height_7d, height_30d = self.get_height_thresholds()
        
        if height_30d == 0:
            logging.error("Could not get height thresholds, aborting full recalculation")
            return None
        
        # Get ALL dividend data in ONE query
        query = """
        SELECT
            name,
            other_type,
            other_id,
            height,
            amount
        FROM user_balance_sheets
        WHERE type LIKE 'dividend%%'
            AND height >= %s
            AND other_type IS NOT NULL
            AND other_id > 0
        ORDER BY name, other_type, other_id
        """
        
        logging.info("Loading all dividend data into memory...")
        all_dividends = self.db.execute_query('source', query, (height_30d,))
        
        if not all_dividends:
            logging.info("No dividends found in last 30 days")
            return current_max_id
        
        total_rows = len(all_dividends)
        logging.info(f"Loaded {total_rows:,} dividend rows, processing in memory...")
        
        # Extract unique users who have dividends in last 30 days
        unique_users = set()
        for row in all_dividends:
            unique_users.add(row['name'])
        
        unique_user_count = len(unique_users)
        logging.info(f"Found {unique_user_count:,} unique users with recent dividends")
        
        # Process in memory: Group by (name, share_type, share_id)
        earnings_dict = {}  # key: (name, share_type, share_id), value: {'e7': x, 'e30': y}
        
        for idx, row in enumerate(all_dividends):
            if shutdown_flag.is_set():
                logging.info("Full recalculation interrupted by shutdown signal")
                return None
            
            name = row['name']
            share_type = row['other_type']
            share_id = row['other_id']
            height = row['height']
            amount = row['amount']
            
            key = (name, share_type, share_id)
            
            if key not in earnings_dict:
                earnings_dict[key] = {'e7': 0, 'e30': 0}
            
            # Add to 30d always
            earnings_dict[key]['e30'] += amount
            
            # Add to 7d if within window
            if height >= height_7d:
                earnings_dict[key]['e7'] += amount
            
            # Progress logging every 10k rows
            if (idx + 1) % 10000 == 0:
                logging.info(f"Processed {idx + 1:,}/{total_rows:,} dividend rows in memory...")
        
        if shutdown_flag.is_set():
            return None
        
        # Convert to list for batch insert
        all_earnings = []
        for (name, share_type, share_id), earnings in earnings_dict.items():
            all_earnings.append((
                name,
                share_type,
                share_id,
                earnings['e7'],
                earnings['e30']
            ))
        
        logging.info(f"Calculated earnings for {unique_user_count:,} users, {len(all_earnings):,} share combinations")
        
        # Insert into STAGING table
        logging.info("Writing fresh earnings to staging table...")
        total_rows_upserted = self.upsert_earnings_batch(all_earnings, table_name=self.staging_table)
        
        elapsed = time.time() - start_time
        logging.info(
            f"Full earnings recalculation completed: Processed {unique_user_count:,} users, "
            f"inserted {total_rows_upserted:,} rows in {elapsed:.2f}s"
        )
        
        return current_max_id
    
    # ============================================================================
    # Business logic helper methods
    # ============================================================================
    
    def get_height_thresholds(self):
        """
        Get block heights for 7 days and 30 days ago.
        Results are cached for 5 minutes to avoid repeated queries.
        Returns (height_7d, height_30d)
        """
        current_time = time.time()
        
        # Check if cache is still valid
        if (self.height_cache['height_7d'] is not None and
            self.height_cache['height_30d'] is not None and
            current_time - self.height_cache['cache_time'] < self.height_cache_ttl):
            return (self.height_cache['height_7d'], self.height_cache['height_30d'])
        
        # Cache expired or empty, fetch fresh values
        try:
            # Get height for 30 days ago
            query_30d = """
            SELECT height FROM blocks
            WHERE date >= UNIX_TIMESTAMP() - (30 * 86400)
            ORDER BY height ASC LIMIT 1
            """
            result_30d = self.db.execute_query('source', query_30d)
            height_30d = result_30d[0]['height'] if result_30d else 0
            
            # Get height for 7 days ago
            query_7d = """
            SELECT height FROM blocks
            WHERE date >= UNIX_TIMESTAMP() - (7 * 86400)
            ORDER BY height ASC LIMIT 1
            """
            result_7d = self.db.execute_query('source', query_7d)
            height_7d = result_7d[0]['height'] if result_7d else 0
            
            # Update cache
            self.height_cache['height_7d'] = height_7d
            self.height_cache['height_30d'] = height_30d
            self.height_cache['cache_time'] = current_time
            
            logging.info(f"Updated height thresholds: 7d={height_7d}, 30d={height_30d}")
            return (height_7d, height_30d)
        
        except Exception as e:
            logging.error(f"Error fetching height thresholds: {e}")
            # Return cached values if available, otherwise 0
            if self.height_cache['height_7d'] is not None:
                logging.warning("Using cached height thresholds due to error")
                return (self.height_cache['height_7d'], self.height_cache['height_30d'])
            else:
                logging.error("No cached height thresholds available, using 0")
                return (0, 0)
    
    def upsert_earnings_batch(self, earnings_data, table_name=None):
        """
        Batch UPSERT earnings into specified table (defaults to main table).
        earnings_data: list of (name, share_type, share_id, earnings_7d, earnings_30d) tuples
        table_name: target table name (defaults to self.dest_table)
        Returns number of rows upserted.
        """
        if not earnings_data:
            return 0
        
        target_table = table_name or self.dest_table
        
        upsert_query = f"""
        INSERT INTO `{target_table}`
            (name, share_type, share_id, earnings_7d, earnings_30d)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            earnings_7d = VALUES(earnings_7d),
            earnings_30d = VALUES(earnings_30d),
            updated_at = CURRENT_TIMESTAMP
        """
        
        batch_size = 1000
        total_upserted = 0
        
        for i in range(0, len(earnings_data), batch_size):
            batch = earnings_data[i:i + batch_size]
            self.db.execute_many(self.db.dest_conn, upsert_query, batch)
            total_upserted += len(batch)
        
        return total_upserted
