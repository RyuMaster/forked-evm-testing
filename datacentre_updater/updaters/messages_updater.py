# messages_updater.py

import logging
import time
from db_manager import CHARSET, COLLATION
from .base_incremental_updater import BaseIncrementalUpdater

class TransferMessagesUpdater(BaseIncrementalUpdater):
    def __init__(self, db_manager):
        super().__init__(db_manager, 'transfer_messages_updater', 'dc_transfer_counts')

    # ============================================================================
    # Implementation of abstract methods
    # ============================================================================

    def create_table(self, table_name):
        """Create the dc_transfer_counts table with proper schema"""
        create_query = f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` (
            `season_id` BIGINT NOT NULL,
            `from_club` BIGINT NOT NULL,
            `to_club` BIGINT NOT NULL,
            `transfers` BIGINT NOT NULL DEFAULT 0,
            PRIMARY KEY (`season_id`, `from_club`, `to_club`),
            INDEX `idx_from_club` (`from_club`),
            INDEX `idx_to_club` (`to_club`)
        ) ENGINE=InnoDB DEFAULT CHARSET={CHARSET} COLLATE={COLLATION}
        """
        self.db.execute_query(self.db.dest_conn, create_query)
        logging.info(f"Created table `{table_name}`")

    def process_incremental(self, last_processed_id, shutdown_flag):
        """
        Process new transfer messages since last_processed_id.
        Writes updates directly to self.dest_table.
        Returns the highest ID successfully processed.
        """
        start_time = time.time()
        batch_size = 10000

        current_last_id = last_processed_id
        total_messages = 0
        total_upserted = 0

        while True:
            query = """
            SELECT
                id,
                season_id,
                club_2 AS from_club,
                club_1 AS to_club
            FROM messages
            WHERE id > %s
                AND type = 9
            ORDER BY id ASC
            LIMIT %s
            """

            batch = self.db.execute_query('source', query, (current_last_id, batch_size))

            if not batch:
                break

            # Aggregate this batch
            transfer_counts = {}
            for row in batch:
                key = (row['season_id'], row['from_club'], row['to_club'])
                transfer_counts[key] = transfer_counts.get(key, 0) + 1

            # Upsert this batch's aggregated counts
            upsert_data = [
                (season_id, from_club, to_club, count)
                for (season_id, from_club, to_club), count in transfer_counts.items()
            ]
            total_upserted += self.upsert_transfer_counts(upsert_data, self.dest_table)

            current_last_id = batch[-1]['id']
            total_messages += len(batch)

            if len(batch) < batch_size:
                break

            if shutdown_flag.is_set():
                logging.info(f"Incremental update interrupted by shutdown signal at id {current_last_id}")
                break

        if total_messages > 0:
            elapsed = time.time() - start_time
            logging.info(
                f"Incremental transfer messages update: Processed {total_messages:,} messages, "
                f"upserted {total_upserted:,} rows in {elapsed:.2f}s"
            )

        return current_last_id

    def process_full(self, shutdown_flag):
        """
        Process all transfer messages from scratch.
        Writes to self.staging_table.
        Returns the highest processed ID if completed, None if interrupted.
        """
        start_time = time.time()
        logging.info("Starting full transfer messages recalculation...")

        # Get max ID upfront for checkpoint
        max_id_result = self.db.execute_query('source', "SELECT MAX(id) as max_id FROM messages")
        current_max_id = max_id_result[0]['max_id'] if max_id_result and max_id_result[0]['max_id'] else 0

        batch_size = 10000
        total_inserted = 0

        # Get all transfer messages aggregated by season/club pair
        query = """
        SELECT
            season_id,
            club_2 AS from_club,
            club_1 AS to_club,
            COUNT(*) AS transfers
        FROM messages
        WHERE type = 9
        GROUP BY season_id, club_2, club_1
        """

        self.db.source_conn = self.db.check_and_reconnect(self.db.source_conn)
        cursor = self.db.source_conn.cursor(dictionary=True)
        try:
            cursor.execute(query)
            batch = []
            for row in cursor:
                batch.append((row['season_id'], row['from_club'], row['to_club'], row['transfers']))
                if len(batch) >= batch_size:
                    self.upsert_transfer_counts(batch, self.staging_table)
                    total_inserted += len(batch)
                    batch = []
                    if shutdown_flag.is_set():
                        logging.info("Full recalculation interrupted by shutdown signal")
                        return None
            if batch:
                self.upsert_transfer_counts(batch, self.staging_table)
                total_inserted += len(batch)
        finally:
            cursor.close()

        if total_inserted == 0:
            logging.info("No transfer messages found")
        else:
            logging.info(f"Inserted {total_inserted:,} unique season/club pair combinations")

        elapsed = time.time() - start_time
        logging.info(f"Full transfer messages recalculation completed in {elapsed:.2f}s")

        return current_max_id

    # ============================================================================
    # Helper methods
    # ============================================================================

    def upsert_transfer_counts(self, data, table_name):
        """UPSERT transfer counts, adding to existing counts."""
        if not data:
            return 0

        query = f"""
        INSERT INTO `{table_name}`
            (season_id, from_club, to_club, transfers)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            transfers = transfers + VALUES(transfers)
        """
        self.db.execute_many(self.db.dest_conn, query, data)
        return len(data)
