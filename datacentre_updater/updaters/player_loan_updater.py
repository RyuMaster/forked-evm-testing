# player_loan_updater.py

import logging
import traceback
from datetime import datetime, timedelta
from .trade_updater import initialize_trade_update_status_table

class PlayerLoanUpdater:
    def __init__(self, db_manager):
        self.db = db_manager
        self.component_name = 'player_loans'
        self.last_processed_id = None
        self.last_full_update = None
        self.full_update_interval = timedelta(hours=6)  # Full update every 6 hours
        
        # Flag to force full update on startup
        self.perform_full_update_on_startup = True
        
        # Initialize the status table
        self.initialize_status_table()

    def initialize_status_table(self):
        """Create the status table using shared function."""
        initialize_trade_update_status_table(self.db)
    
    def get_update_status(self):
        """Get the last processed ID and full update timestamp for this component."""
        query = "SELECT `last_processed_height`, `last_full_update` FROM `trade_update_status` WHERE `component` = %s"
        result = self.db.execute_query(self.db.dest_conn, query, (self.component_name,))
        if result:
            self.last_processed_id = result[0]['last_processed_height']
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
            self.last_processed_id = 0
            self.last_full_update = None
            logging.info(f"Inserted initial status for {self.component_name}. Performing initial full update.")
    
    def update_status(self, last_id, full_update=False):
        """Update the last processed ID and optionally the full update timestamp for this component."""
        if full_update:
            update_query = """
            UPDATE `trade_update_status` 
            SET `last_processed_height` = %s, `last_full_update` = %s
            WHERE `component` = %s
            """
            last_full_update_time = datetime.now()
            params = (last_id, last_full_update_time, self.component_name)
            self.last_full_update = last_full_update_time
        else:
            update_query = """
            UPDATE `trade_update_status` 
            SET `last_processed_height` = %s
            WHERE `component` = %s
            """
            params = (last_id, self.component_name)

        self.db.execute_query(self.db.dest_conn, update_query, params)
        self.last_processed_id = last_id

    def update_player_loans(self, shutdown_flag):
        """
        Update player loan status based on player_loan_updates from archival database.
        Supports both incremental and full update modes.
        """
        try:
            self.get_update_status()
            now = datetime.now()
            perform_full_update = False

            # Always perform full update on startup
            if self.perform_full_update_on_startup:
                logging.info(f"Performing full player loan update for {self.component_name} on startup.")
                perform_full_update = True
                self.perform_full_update_on_startup = False  # Reset the flag after first use
            elif self.last_full_update is None:
                logging.info(f"First run for {self.component_name}. Performing initial full update.")
                perform_full_update = True
            elif now - self.last_full_update >= self.full_update_interval:
                logging.info(f"Full update interval exceeded. Performing full player loan update for {self.component_name}.")
                perform_full_update = True

            if perform_full_update:
                self.perform_full_loan_update(shutdown_flag)
            else:
                self.perform_incremental_loan_update(shutdown_flag)
            
        except Exception as e:
            logging.error(f"Error updating player loans: {e}")
            logging.debug(traceback.format_exc())
            raise
    
    def perform_full_loan_update(self, shutdown_flag):
        """
        Perform a full update: reset all loan columns to NULL, reset last processed ID, 
        then run incremental update to process all rows.
        """
        logging.info("Starting full player loan update...")
        
        # Step 1: Reset all loan columns to NULL for all players
        reset_query = """
        UPDATE dc_players 
        SET loan_offered = NULL, loan_offer_accepted = NULL, loaned_to_club = NULL
        """
        self.db.execute_query(self.db.dest_conn, reset_query)
        logging.info("Reset all player loan columns to NULL.")
        
        # Step 2: Reset last processed ID to 0
        original_last_processed_id = self.last_processed_id
        self.last_processed_id = 0
        
        # Step 3: Run incremental update (which will process all rows from the beginning)
        try:
            self.perform_incremental_loan_update(shutdown_flag, is_full_update=True)
            
            # Update status with full update flag
            self.update_status(self.last_processed_id, full_update=True)
            logging.info(f"Successfully completed full player loan update. Processed up to ID {self.last_processed_id}.")
            
        except Exception as e:
            # Restore original last processed ID on error
            self.last_processed_id = original_last_processed_id
            logging.error(f"Error during full player loan update: {e}")
            raise
    
    def perform_incremental_loan_update(self, shutdown_flag, is_full_update=False):
        """
        Perform incremental update: process new loan updates since last processed ID.
        Only processes updates for the current season (largest season_id).
        """
        # Get the current season (largest season_id in the table)
        current_season_query = """
        SELECT MAX(season_id) as current_season 
        FROM player_loan_updates
        """
        
        current_season_result = self.db.execute_query('source', current_season_query)
        if not current_season_result or current_season_result[0]['current_season'] is None:
            logging.debug("No seasons found in player_loan_updates table.")
            return
        
        current_season = current_season_result[0]['current_season']
        logging.debug(f"Current season determined as: {current_season}")
        
        # Query new loan updates from archival database for current season only
        query = """
        SELECT id, player_id, club_id, season_id, accepting_club_id, fee, action
        FROM player_loan_updates 
        WHERE id > %s AND season_id = %s
        ORDER BY id ASC
        """
        
        new_updates = self.db.execute_query('source', query, (self.last_processed_id, current_season))
        
        if not new_updates:
            if not is_full_update:
                logging.debug(f"No new player loan updates found for current season {current_season}.")
            return
        
        update_type = "full" if is_full_update else "incremental"
        logging.info(f"Processing {len(new_updates)} player loan updates for season {current_season} ({update_type}).")
        
        # Process each update
        for update in new_updates:
            if shutdown_flag.is_set():
                logging.info("Shutdown signal received. Exiting player loan updater early.")
                break
            
            self.process_loan_update(update)
            self.last_processed_id = update['id']
        
        # Update the last processed ID (only for incremental updates, full updates handle this separately)
        if not is_full_update:
            self.update_status(self.last_processed_id)
            logging.info(f"Successfully processed incremental player loan updates up to ID {self.last_processed_id} for season {current_season}.")

    def process_loan_update(self, update):
        """
        Process a single loan update and update the dc_players table accordingly.
        Based on the player_loan_updates schema:
        - No action, no accepting_club_id: Player listed for loan
        - No action, has accepting_club_id: Club accepted offer (pending agent approval) 
        - action='declined': Agent declined
        - action='cancelled': Manager unlisted player
        - action='finalised': Loan finalized
        """
        player_id = update['player_id']
        club_id = update['club_id'] 
        season_id = update['season_id']
        accepting_club_id = update['accepting_club_id']
        action = update['action']
        
        try:
            if action is None and accepting_club_id is None:
                # Player listed for loan - set loan_offered to season ID, clear others
                query = """
                UPDATE dc_players 
                SET loan_offered = %s, loan_offer_accepted = NULL, loaned_to_club = NULL
                WHERE player_id = %s
                """
                self.db.execute_query(self.db.dest_conn, query, (season_id, player_id))
                
            elif action is None and accepting_club_id is not None:
                # Club accepted offer (pending agent approval)
                query = """
                UPDATE dc_players 
                SET loan_offered = %s, loan_offer_accepted = %s, loaned_to_club = NULL
                WHERE player_id = %s
                """
                self.db.execute_query(self.db.dest_conn, query, (season_id, accepting_club_id, player_id))
                
            elif action == 'declined':
                # Agent declined - keep loan_offered but clear accepted club
                query = """
                UPDATE dc_players 
                SET loan_offered = %s, loan_offer_accepted = NULL, loaned_to_club = NULL
                WHERE player_id = %s
                """
                self.db.execute_query(self.db.dest_conn, query, (season_id, player_id))
                
            elif action == 'cancelled':
                # Manager unlisted player - clear all loan columns
                query = """
                UPDATE dc_players 
                SET loan_offered = NULL, loan_offer_accepted = NULL, loaned_to_club = NULL
                WHERE player_id = %s
                """
                self.db.execute_query(self.db.dest_conn, query, (player_id,))
                
            elif action == 'finalised':
                # Loan finalized - set loan_offered to season, loaned_to_club to accepting club
                query = """
                UPDATE dc_players 
                SET loan_offered = %s, loan_offer_accepted = %s, loaned_to_club = %s
                WHERE player_id = %s
                """
                self.db.execute_query(self.db.dest_conn, query, (season_id, accepting_club_id, accepting_club_id, player_id))
                
            else:
                logging.warning(f"Unknown loan action: {action} for player {player_id} in update {update['id']}")
                
        except Exception as e:
            logging.error(f"Error processing loan update {update['id']} for player {player_id}: {e}")
            raise