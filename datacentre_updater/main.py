# File: main.py
import http.server
import logging
import signal
import threading
import time
import traceback

from db_manager import DBManager
from config import USER_ENRICHMENT_PROVIDER_FACTORY, DUMP_OUTPUT_FOLDER
from updaters.sqlite_updater import SQLiteUpdater
from updaters.club_updater import ClubUpdater
from updaters.player_updater import PlayerUpdater
from updaters.user_updater import UserUpdater
from updaters.club_info_updater import ClubInfoUpdater
from updaters.best_manager_updater import BestManagerUpdater
from updaters.league_updater import LeagueUpdater
from updaters.datapack_updater import DataPackUpdater
# >>> NEW: Import PriceUpdater <<<
from updaters.price_updater import PriceUpdater
# >>> NEW: Import ManagerActivityUpdater <<<
from updaters.manager_activity_updater import ManagerActivityUpdater
from updaters.player_loan_updater import PlayerLoanUpdater
from updaters.earnings_updater import EarningsUpdater
from updaters.messages_updater import TransferMessagesUpdater
from updaters.datadump import DataDumpUpdater
from updaters.player_earnings_updater import PlayerEarningsUpdater

def start_health_server(healthy_flag, port=8000):
    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == '/healthz':
                if healthy_flag.is_set():
                    self.send_response(200)
                    self.end_headers()
                    self.wfile.write(b'ok')
                else:
                    self.send_response(503)
                    self.end_headers()
                    self.wfile.write(b'starting')
            else:
                self.send_response(404)
                self.end_headers()

        def log_message(self, format, *args):
            pass  # suppress request logging

    server = http.server.HTTPServer(('', port), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

def get_next_4am_timestamp():
    """Returns unix timestamp of next 4am UTC"""
    from datetime import datetime, timedelta
    now = datetime.utcnow()
    next_4am = now.replace(hour=4, minute=0, second=0, microsecond=0)
    if now.hour >= 4:
        next_4am += timedelta(days=1)
    return int(next_4am.timestamp())

def main():
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    logger.info("Starting the script...")

    db_manager = DBManager()
    logger.info("DBManager initialized.")
    
    # === Initialize updaters ===
    best_manager_updater = BestManagerUpdater(db_manager)
    logger.info("BestManagerUpdater initialized.")

    club_info_updater = ClubInfoUpdater(db_manager)
    logger.info("ClubInfoUpdater initialized.")

    sqlite_updater = SQLiteUpdater(db_manager, club_info_updater)
    logger.info("SQLiteUpdater initialized.")

    club_updater = ClubUpdater(db_manager)
    logger.info("ClubUpdater initialized.")

    player_updater = PlayerUpdater(db_manager)
    logger.info("PlayerUpdater initialized.")

    user_updater = UserUpdater(db_manager)
    logger.info("UserUpdater initialized.")

    league_updater = LeagueUpdater(db_manager)
    logger.info("LeagueUpdater initialized.")

    # NEW: DataPackUpdater to handle names from JSON
    datapack_updater = DataPackUpdater(db_manager)
    logger.info("DataPackUpdater initialized.")

    # >>> NEW: PriceUpdater <<<
    price_updater = PriceUpdater(db_manager)
    logger.info("PriceUpdater initialized.")

    # >>> NEW: ManagerActivityUpdater with UserEnrichmentProvider <<<
    user_enrichment_provider = USER_ENRICHMENT_PROVIDER_FACTORY(db_manager)
    manager_activity_updater = ManagerActivityUpdater(db_manager, user_enrichment_provider)
    logger.info("ManagerActivityUpdater initialized.")

    player_loan_updater = PlayerLoanUpdater(db_manager)
    logger.info("PlayerLoanUpdater initialized.")

    # NEW: EarningsUpdater
    earnings_updater = EarningsUpdater(db_manager)
    logger.info("EarningsUpdater initialized.")

    # NEW: TransferMessagesUpdater
    transfer_messages_updater = TransferMessagesUpdater(db_manager)
    logger.info("TransferMessagesUpdater initialized.")

    # DataDumpUpdater (optional - only if DUMP_OUTPUT_FOLDER is configured)
    if DUMP_OUTPUT_FOLDER:
        data_dump_updater = DataDumpUpdater(db_manager, DUMP_OUTPUT_FOLDER)
        logger.info(f"DataDumpUpdater initialized (output: {DUMP_OUTPUT_FOLDER}).")
    else:
        data_dump_updater = None
        logger.warning("DUMP_OUTPUT_FOLDER not configured - DataDumpUpdater disabled.")

    player_earnings_updater = PlayerEarningsUpdater(db_manager)
    logger.info("PlayerEarningsUpdater initialized.")

    healthy_flag = threading.Event()
    start_health_server(healthy_flag)
    logger.info("Health server started on port 8000.")

    shutdown_flag = threading.Event()

    def signal_handler(sig, frame):
        logger.info(f"Received signal {sig}. Initiating graceful shutdown...")
        shutdown_flag.set()

    # Register signal handlers
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    # Initialize last run times
    current_time = time.time()
    last_sqlite_update_time = current_time
    last_trading_update_time = current_time
    sqlite_update_interval = 10  # seconds
    trading_update_interval = 10  # seconds
    last_club_info_update_time = current_time
    club_info_update_interval = 86400  # 24 hours
    last_best_manager_update_time = current_time
    best_manager_update_interval = 10  # seconds
    last_league_update_time = current_time
    league_update_interval = 60  # 60 seconds
    last_full_league_update_time = current_time
    full_league_update_interval = 6 * 3600  # 6 hours
    last_price_update_time = current_time
    price_update_interval = 600  # 600 seconds
    # NEW: Manager activity update interval (once a day)
    last_manager_activity_update_time = current_time
    manager_activity_update_interval = 86400  # 24 hours
    last_player_loan_update_time = current_time
    player_loan_update_interval = 15  # 15 seconds
    # NEW: Earnings update intervals
    # Check for new earnings at :05 and :35 past each hour (matches are at :00 and :30)
    last_earnings_incremental_update_time = 0  # Force first check
    next_earnings_full_update_time = get_next_4am_timestamp()
    # NEW: Transfer messages update intervals
    last_transfer_messages_incremental_time = current_time
    transfer_messages_incremental_interval = 10  # 10 seconds
    last_transfer_messages_full_time = current_time
    transfer_messages_full_interval = 6 * 3600  # 6 hours
    last_data_dump_time = 0  # Force first run immediately
    data_dump_interval = DataDumpUpdater.UPDATE_INTERVAL

    # === Perform initial data setup before entering the main loop ===
    logger.info("Performing initial data setup...")

    # 1) Download & populate new name tables from datapack (only on initial run, or skip on error)
    try:
        datapack_updater.update_datapack()
    except Exception as e:
        logger.error(f"Could not update datapack: {e}")
        logger.debug(traceback.format_exc())

    # 2) Run sqlite_updater to create and populate tables from SQLite
    try:
        sqlite_updater.create_and_populate_tables(shutdown_flag)
        logger.info("SQLite tables created and populated successfully.")
    except Exception as e:
        logger.error(f"An error occurred while updating SQLite tables: {e}")
        logger.debug(traceback.format_exc())

    # 3) Run club_updater to ensure dc_clubs_trading table is created
    try:
        club_updater.update_trading_data(shutdown_flag)
        logger.info("Club data updated successfully.")
    except Exception as e:
        logger.error(f"An error occurred while updating club data: {e}")
        logger.debug(traceback.format_exc())

    # 4) Run player_updater to ensure dc_players_trading table is updated
    try:
        player_updater.update_trading_data(shutdown_flag)
        logger.info("Player data updated successfully.")
    except Exception as e:
        logger.error(f"An error occurred while updating player data: {e}")
        logger.debug(traceback.format_exc())

    # 5) Run league_updater to create and populate dc_leagues and dc_table_rows
    try:
        league_updater.update_leagues(shutdown_flag, perform_full_update=True)
        logger.info("Leagues and table rows updated successfully.")
    except Exception as e:
        logger.error(f"An error occurred while updating leagues and table rows: {e}")
        logger.debug(traceback.format_exc())
    last_league_update_time = current_time
    last_full_league_update_time = current_time

    # NEW: Run price updater immediately on startup so that price_history and svc_trades tables are created/populated.
    try:
        price_updater.update_prices()
        logger.info("Price updater ran successfully on startup.")
    except Exception as e:
        logger.error(f"An error occurred while updating prices on startup: {e}")
        logger.debug(traceback.format_exc())

    # NEW: Run manager activity updater once on startup
    try:
        manager_activity_updater.update_manager_activity()
        logger.info("Manager activity updated successfully.")
    except Exception as e:
        logger.error(f"An error occurred while updating manager activity: {e}")
        logger.debug(traceback.format_exc())

    # NEW: Initialize earnings updater on startup
    # Table and status table are created automatically by base class
    # Run initial catch-up to populate data if needed
    try:
        logger.info("Running initial earnings catch-up...")
        earnings_updater.update_incremental(shutdown_flag)
        logger.info("Initial earnings catch-up complete!")
    except Exception as e:
        logger.error(f"An error occurred during initial earnings update: {e}")
        logger.debug(traceback.format_exc())

    # NEW: Initialize transfer messages updater on startup
    try:
        logger.info("Running initial transfer messages catch-up...")
        transfer_messages_updater.update_incremental(shutdown_flag)
        logger.info("Initial transfer messages catch-up complete!")
    except Exception as e:
        logger.error(f"An error occurred during initial transfer messages update: {e}")
        logger.debug(traceback.format_exc())

    healthy_flag.set()
    logger.info("Initial data setup complete. Marking service as healthy.")

    # === Main loop ===
    try:
        while not shutdown_flag.is_set():
            current_time = time.time()

            # Run SQLite update at the specified interval
            if current_time - last_sqlite_update_time >= sqlite_update_interval:
                logger.info("Starting create_and_populate_tables (SQLiteUpdater)...")
                try:
                    sqlite_updater.create_and_populate_tables(shutdown_flag)
                    logger.info("SQLite tables created and populated successfully.")
                except Exception as e:
                    logger.error(f"An error occurred while updating SQLite tables: {e}")
                    logger.debug(traceback.format_exc())
                last_sqlite_update_time = current_time

            # Run trading data update at the specified interval
            if current_time - last_trading_update_time >= trading_update_interval:
                logger.info("Starting update_trading_data (Updaters)...")
                try:
                    club_updater.update_trading_data(shutdown_flag)
                    player_updater.update_trading_data(shutdown_flag)
                    user_updater.update_trading_data(shutdown_flag)
                    logger.info("Trading data updated successfully.")
                except Exception as e:
                    logger.error(f"An error occurred while updating trading data: {e}")
                    logger.debug(traceback.format_exc())
                last_trading_update_time = current_time

            # Run Club Info update at the specified interval
            if current_time - last_club_info_update_time >= club_info_update_interval:
                logger.info("Starting update_club_info (ClubInfoUpdater)...")
                try:
                    club_info_updater.update_club_info(shutdown_flag)
                    logger.info("Club info updated successfully.")
                except Exception as e:
                    logger.error(f"An error occurred while updating club info: {e}")
                    logger.debug(traceback.format_exc())
                last_club_info_update_time = current_time

            # Run BestManagerUpdater at the specified interval
            if current_time - last_best_manager_update_time >= best_manager_update_interval:
                logger.info("Starting update_best_managers (BestManagerUpdater)...")
                try:
                    best_manager_updater.update_best_managers(shutdown_flag)
                    logger.info("Best managers updated successfully.")
                except Exception as e:
                    logger.error(f"An error occurred while updating best managers: {e}")
                    logger.debug(traceback.format_exc())
                last_best_manager_update_time = current_time

            # Run LeagueUpdater at the specified interval
            if current_time - last_league_update_time >= league_update_interval:
                try:
                    if current_time - last_full_league_update_time >= full_league_update_interval:
                        perform_full_update = True
                        last_full_league_update_time = current_time
                    else:
                        perform_full_update = False

                    league_updater.update_leagues(shutdown_flag, perform_full_update=perform_full_update)
                    logger.info("Leagues and table rows updated successfully.")
                except Exception as e:
                    logger.error(f"An error occurred while updating leagues and table rows: {e}")
                    logger.debug(traceback.format_exc())
                last_league_update_time = current_time

            # Run PriceUpdater at the specified interval
            if current_time - last_price_update_time >= price_update_interval:
                logger.info("Starting price update (PriceUpdater)...")
                try:
                    price_updater.update_prices()
                except Exception as e:
                    logger.error(f"An error occurred while updating prices: {e}")
                    logger.debug(traceback.format_exc())
                last_price_update_time = current_time

            # Run ManagerActivityUpdater at the specified interval (daily)
            if current_time - last_manager_activity_update_time >= manager_activity_update_interval:
                logger.info("Starting manager activity update (ManagerActivityUpdater)...")
                try:
                    manager_activity_updater.update_manager_activity()
                    logger.info("Manager activity updated successfully.")
                except Exception as e:
                    logger.error(f"An error occurred while updating manager activity: {e}")
                    logger.debug(traceback.format_exc())
                last_manager_activity_update_time = current_time

            # Run PlayerLoanUpdater at the specified interval
            if current_time - last_player_loan_update_time >= player_loan_update_interval:
                logger.info("Starting player loan update (PlayerLoanUpdater)...")
                try:
                    player_loan_updater.update_player_loans(shutdown_flag)
                    logger.info("Player loans updated successfully.")
                except Exception as e:
                    logger.error(f"An error occurred while updating player loans: {e}")
                    logger.debug(traceback.format_exc())
                last_player_loan_update_time = current_time

            # NEW: Run EarningsUpdater (incremental) at :05 and :35 past each hour
            # Matches play at :00 and :30, earnings appear shortly after
            from datetime import datetime
            now_utc = datetime.utcnow()
            current_minute = now_utc.minute

            # Check if we're at :05 or :35 and haven't run in the last minute
            if (current_minute == 5 or current_minute == 35):
                if current_time - last_earnings_incremental_update_time >= 60:  # At least 1 minute since last check
                    try:
                        earnings_updater.update_incremental(shutdown_flag)
                        last_earnings_incremental_update_time = current_time
                    except Exception as e:
                        logger.error(f"An error occurred while updating earnings incrementally: {e}")
                        logger.debug(traceback.format_exc())

            # NEW: Run EarningsUpdater (full recalculation) daily at 4am
            if current_time >= next_earnings_full_update_time:
                logger.info("Starting full earnings recalculation (EarningsUpdater)...")
                try:
                    earnings_updater.update_full(shutdown_flag)
                    logger.info("Full earnings recalculation completed.")
                    next_earnings_full_update_time = get_next_4am_timestamp()
                except Exception as e:
                    logger.error(f"An error occurred during full earnings recalculation: {e}")
                    logger.debug(traceback.format_exc())

            # NEW: Run TransferMessagesUpdater (incremental) at specified interval
            if current_time - last_transfer_messages_incremental_time >= transfer_messages_incremental_interval:
                try:
                    transfer_messages_updater.update_incremental(shutdown_flag)
                except Exception as e:
                    logger.error(f"An error occurred while updating transfer messages incrementally: {e}")
                    logger.debug(traceback.format_exc())
                last_transfer_messages_incremental_time = current_time

            # Run DataDumpUpdater and PlayerEarningsUpdater at the specified interval
            if current_time - last_data_dump_time >= data_dump_interval:
                if data_dump_updater is not None:
                    try:
                        data_dump_updater.update()
                    except Exception as e:
                        logger.error(f"An error occurred while running data dump: {e}")
                        logger.debug(traceback.format_exc())
                
                try:
                    player_earnings_updater.update()
                except Exception as e:
                    logger.error(f"An error occurred while running player earnings update: {e}")
                    logger.debug(traceback.format_exc())
                
                last_data_dump_time = current_time

            # NEW: Run TransferMessagesUpdater (full recalculation) at specified interval
            if current_time - last_transfer_messages_full_time >= transfer_messages_full_interval:
                logger.info("Starting full transfer messages recalculation (TransferMessagesUpdater)...")
                try:
                    transfer_messages_updater.update_full(shutdown_flag)
                    logger.info("Full transfer messages recalculation completed.")
                except Exception as e:
                    logger.error(f"An error occurred during full transfer messages recalculation: {e}")
                    logger.debug(traceback.format_exc())
                last_transfer_messages_full_time = current_time

            # Sleep to prevent high CPU usage
            time.sleep(1)

    except Exception as e:
        logger.error(f"An error occurred in the main loop: {e}")
        logger.debug(traceback.format_exc())
    finally:
        logger.info("Closing database connections...")
        db_manager.close()
        logger.info("Database connections closed.")

    logger.info("Script finished.")

if __name__ == "__main__":
    main()
