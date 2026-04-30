# updaters/game_week_updater.py

import logging
import traceback
from datetime import datetime, timedelta
import time

from db_manager import CHARSET, COLLATION

class GameWeekUpdater:
    def __init__(self, db_manager):
        self.db = db_manager
        self.component_name = 'game_week_updater'
        self.full_update_interval = timedelta(hours=6)
        self.last_full_update = None
        self.initialize_status_table()

    def initialize_status_table(self):
        # Ensure the status table exists
        create_query = f"""
        CREATE TABLE IF NOT EXISTS `game_week_update_status` (
            `component` VARCHAR(50) PRIMARY KEY,
            `last_full_update` DATETIME
        ) ENGINE=InnoDB DEFAULT CHARSET={CHARSET} COLLATE={COLLATION}
        """
        self.db.execute_query(self.db.dest_conn, create_query)
        logging.info("Initialized game_week_update_status table.")

    def get_update_status(self):
        query = "SELECT `last_full_update` FROM `game_week_update_status` WHERE `component` = %s"
        result = self.db.execute_query(self.db.dest_conn, query, (self.component_name,))
        if result:
            self.last_full_update = result[0]['last_full_update']
            logging.debug(f"Retrieved last_full_update = {self.last_full_update} ({type(self.last_full_update)}) for component {self.component_name}")

            # Handle NULL and zero dates
            if not self.last_full_update or str(self.last_full_update) in ('0000-00-00 00:00:00', 'None'):
                self.last_full_update = None
                logging.info(f"First run for {self.component_name}.")
        else:
            # Insert initial status
            insert_query = "INSERT INTO `game_week_update_status` (`component`, `last_full_update`) VALUES (%s, %s)"
            self.db.execute_query(self.db.dest_conn, insert_query, (self.component_name, None))
            self.last_full_update = None
            logging.info(f"Inserted initial status for {self.component_name}.")
    
    def update_status(self):
        update_query = """
        UPDATE `game_week_update_status` 
        SET `last_full_update` = %s
        WHERE `component` = %s
        """
        last_full_update_time = datetime.now()
        params = (last_full_update_time, self.component_name)
        self.db.execute_query(self.db.dest_conn, update_query, params)
        self.last_full_update = last_full_update_time

    def update_game_weeks(self, shutdown_flag):
        try:
            self.get_update_status()
            self.process_game_weeks(shutdown_flag)
            self.update_status()
        except Exception as e:
            logging.error(f"An error occurred in GameWeekUpdater: {e}")
            logging.debug(traceback.format_exc())
            raise

    def process_game_weeks(self, shutdown_flag):
        try:
            logging.info("Updating game weeks...")
            # Get seasons from SQLite
            seasons = self.db.execute_query('sqlite', "SELECT season_id, start, end FROM seasons")
            if not seasons:
                logging.warning("No seasons found in SQLite database.")
                return

            # Ensure dc_game_weeks table exists
            self.create_game_weeks_table()

            for season in seasons:
                if shutdown_flag.is_set():
                    logging.info("Shutdown signal received. Exiting process_game_weeks early.")
                    break

                season_id = season['season_id']
                start = season['start']
                end = season['end']

                logging.info(f"Processing game weeks for season {season_id}")

                # Delete existing game weeks for this season
                delete_query = "DELETE FROM dc_game_weeks WHERE season_id = %s"
                self.db.execute_query(self.db.dest_conn, delete_query, (season_id,))
                logging.info(f"Deleted existing game weeks for season {season_id}")

                game_weeks = self.compute_game_weeks(start, end)

                # Prepare data for bulk insert
                insert_data = []
                for gameweek_number, (week_start, week_end) in enumerate(game_weeks, start=1):
                    if shutdown_flag.is_set():
                        logging.info("Shutdown signal received. Exiting process_game_weeks early.")
                        break

                    insert_data.append((season_id, gameweek_number, week_start, week_end))

                # Bulk insert new game weeks
                if insert_data:
                    insert_query = """
                    INSERT INTO dc_game_weeks (season_id, gameweek, start, end)
                    VALUES (%s, %s, %s, %s)
                    """
                    self.db.execute_many(self.db.dest_conn, insert_query, insert_data)
                    logging.info(f"Inserted {len(insert_data)} game weeks for season {season_id}")
                else:
                    logging.info(f"No game weeks generated for season {season_id}")

            logging.info("Game weeks updated successfully.")
        except Exception as e:
            logging.error(f"Error in process_game_weeks: {e}")
            logging.debug(traceback.format_exc())
            raise

    def create_game_weeks_table(self):
        try:
            create_query = f"""
            CREATE TABLE IF NOT EXISTS dc_game_weeks (
                id INT AUTO_INCREMENT PRIMARY KEY,
                season_id INT,
                gameweek INT,
                start INT,
                end INT,
                UNIQUE KEY `unique_season_gameweek` (`season_id`, `gameweek`),
                INDEX `idx_start` (`start`),
                INDEX `idx_end` (`end`)
            ) ENGINE=InnoDB DEFAULT CHARSET={CHARSET} COLLATE={COLLATION}
            """
            self.db.execute_query(self.db.dest_conn, create_query)
            logging.info("Ensured dc_game_weeks table exists.")
        except Exception as e:
            logging.error(f"Error creating dc_game_weeks table: {e}")
            logging.debug(traceback.format_exc())
            raise

    def compute_game_weeks(self, start, end):
        start_datetime = datetime.utcfromtimestamp(start)

        if not end or end <= start:
            # If end time is not set or before/equal to start time, assume season is ongoing
            end_datetime = datetime.utcnow()
        else:
            end_datetime = datetime.utcfromtimestamp(end)

        if start_datetime >= end_datetime:
            return []  # No game weeks to generate

        res = []

        # Get the next Saturday from the start date
        day_of_week = start_datetime.weekday()  # Monday=0, Sunday=6
        days_to_add = (5 - day_of_week) % 7  # Saturday is 5
        current_datetime = start_datetime + timedelta(days=days_to_add)

        week_start = int(start_datetime.timestamp())

        sat = True if current_datetime.weekday() == 5 else False

        while True:
            if sat:
                week_end_datetime = current_datetime + timedelta(days=3)  # Next Tuesday
            else:
                week_end_datetime = current_datetime + timedelta(days=2)  # Next Thursday
            sat = not sat

            week_end_timestamp = int(week_end_datetime.timestamp())

            if week_end_timestamp > int(end_datetime.timestamp()):
                week_end_timestamp = int(end_datetime.timestamp())
                res.append((week_start, week_end_timestamp))
                break

            res.append((week_start, week_end_timestamp))

            # Prepare for next iteration
            week_start = week_end_timestamp + 1
            current_datetime = week_end_datetime + timedelta(days=1)

        return res



