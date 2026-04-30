# File: updaters/datapack_updater.py

import logging
import requests
import json
import traceback

from config import DATAPACK_URL
from db_manager import CHARSET, COLLATION

class DataPackUpdater:
    def __init__(self, db_manager):
        self.db = db_manager
        self.logger = logging.getLogger(__name__)

    def update_datapack(self):
        """
        Downloads and processes the JSON data pack to create/populate
        dc_player_names, dc_club_names, dc_league_names, dc_cup_names, dc_venue_names.
        If there's a URL or JSON issue, we raise an exception so the caller can log the error and continue.
        """
        if not DATAPACK_URL:
            raise ValueError("DATAPACK_URL is not set in environment variables.")

        self.logger.info(f"Attempting to fetch datapack from {DATAPACK_URL}...")

        try:
            response = requests.get(DATAPACK_URL, timeout=15)
            response.raise_for_status()
            data = response.json()
        except Exception as e:
            self.logger.error(f"Failed to download/parse datapack JSON: {e}")
            raise

        # The JSON structure is expected to contain 'PackData' with sub-sections
        pack = data.get("PackData", {})
        if not pack:
            raise ValueError("Invalid datapack: missing 'PackData' key")

        # Create & populate each new table
        self.create_player_names_table()
        self.populate_player_names_table(pack.get("PlayerData", {}))

        self.create_club_names_table()
        self.populate_club_names_table(pack.get("ClubData", {}))

        self.create_league_names_table()
        self.populate_league_names_table(pack.get("LeagueData", {}))

        self.create_cup_names_table()
        self.populate_cup_names_table(pack.get("CupData", {}))

        self.create_venue_names_table()
        self.populate_venue_names_table(pack.get("StadiumData", {}))

        self.logger.info("Datapack tables populated successfully.")

    # -------------------------------------------------------------------------
    # Player names
    # -------------------------------------------------------------------------
    def create_player_names_table(self):
        """
        dc_player_names: columns -> player_id (INT PRIMARY KEY), first_name (VARCHAR), last_name (VARCHAR)
        """
        table_name = "dc_player_names"
        create_query = f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` (
            `player_id` INT NOT NULL,
            `first_name` VARCHAR(255),
            `last_name` VARCHAR(255),
            PRIMARY KEY (`player_id`)
        ) ENGINE=InnoDB DEFAULT CHARSET={CHARSET} COLLATE={COLLATION}
        """
        self.db.execute_query(self.db.dest_conn, create_query)
        self.logger.info(f"Ensured table {table_name} exists.")

    def populate_player_names_table(self, player_data):
        """
        Example structure from JSON:
        "PlayerData": {
          "P": [
            { "id": "257971", "f": "Hilton", "s": "Orlando Moreira" },
            ...
          ]
        }

        - If first name (f) is None, use player_id as first_name.
        - If last name (s) is None, leave as NULL.
        """
        table_name = "dc_player_names"
        rows = player_data.get("P", [])
        if not rows:
            self.logger.info("No player data found in datapack. Skipping.")
            return

        self.logger.info(f"Populating {table_name} with {len(rows)} rows...")

        insert_query = f"""
        INSERT INTO `{table_name}` (player_id, first_name, last_name)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE
            `first_name`=VALUES(`first_name`),
            `last_name`=VALUES(`last_name`)
        """

        params_list = []
        for item in rows:
            # item example: { "id": "257971", "f": "Hilton", "s": "Orlando Moreira" }
            try:
                pid = int(item.get("id", 0))
            except ValueError:
                # skip if "id" is not an integer
                self.logger.warning(f"Skipping invalid player id: {item.get('id')}")
                continue

            raw_first_name = item.get("f")  # might be None
            if raw_first_name is None:
                # fallback: use the player_id as a string
                first_name = str(pid)
            else:
                # ensure it's a string, truncated to 255
                first_name = str(raw_first_name)[:255]

            raw_last_name = item.get("s")  # might be None
            if raw_last_name is None:
                last_name = None  # keep as NULL
            else:
                # ensure it's a string, truncated to 255
                last_name = str(raw_last_name)[:255]

            params_list.append((pid, first_name, last_name))

        if params_list:
            self.db.execute_many(self.db.dest_conn, insert_query, params_list)
            self.logger.info(f"Inserted/updated {len(params_list)} player rows in {table_name}.")

    # -------------------------------------------------------------------------
    # Club names
    # -------------------------------------------------------------------------
    def create_club_names_table(self):
        """
        dc_club_names: columns -> club_id (INT PRIMARY KEY), club_name (VARCHAR), rgb (VARCHAR)
        """
        table_name = "dc_club_names"
        create_query = f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` (
            `club_id` INT NOT NULL,
            `club_name` VARCHAR(255),
            `rgb` VARCHAR(50),
            PRIMARY KEY (`club_id`)
        ) ENGINE=InnoDB DEFAULT CHARSET={CHARSET} COLLATE={COLLATION}
        """
        self.db.execute_query(self.db.dest_conn, create_query)
        self.logger.info(f"Ensured table {table_name} exists.")

    def populate_club_names_table(self, club_data):
        """
        "ClubData": {
          "C": [
            { "id": "33", "n": "Manchester United", "rgb": "217,2,13" },
            ...
          ]
        }

        - If club name is None, use club_id as the fallback name.
        """
        table_name = "dc_club_names"
        rows = club_data.get("C", [])
        if not rows:
            self.logger.info("No club data found in datapack. Skipping.")
            return

        self.logger.info(f"Populating {table_name} with {len(rows)} rows...")

        insert_query = f"""
        INSERT INTO `{table_name}` (club_id, club_name, rgb)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE
            `club_name`=VALUES(`club_name`),
            `rgb`=VALUES(`rgb`)
        """

        params_list = []
        for item in rows:
            try:
                cid = int(item.get("id", 0))
            except ValueError:
                self.logger.warning(f"Skipping invalid club id: {item.get('id')}")
                continue

            raw_name = item.get("n")  # might be None
            if raw_name is None:
                club_name = str(cid)
            else:
                club_name = str(raw_name)[:255]

            raw_rgb = item.get("rgb")
            if raw_rgb is None:
                rgb_val = ""
            else:
                rgb_val = str(raw_rgb)[:50]

            params_list.append((cid, club_name, rgb_val))

        if params_list:
            self.db.execute_many(self.db.dest_conn, insert_query, params_list)
            self.logger.info(f"Inserted/updated {len(params_list)} club rows in {table_name}.")

    # -------------------------------------------------------------------------
    # League names
    # -------------------------------------------------------------------------
    def create_league_names_table(self):
        """
        dc_league_names: columns -> league_name (VARCHAR(255)), country_id (VARCHAR(3)), division (INT)
        Unique key on (country_id, division).
        """
        table_name = "dc_league_names"
        create_query = f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` (
            `league_name` VARCHAR(255),
            `country_id` VARCHAR(3),
            `division` INT,
            UNIQUE KEY (`country_id`, `division`)
        ) ENGINE=InnoDB DEFAULT CHARSET={CHARSET} COLLATE={COLLATION}
        """
        self.db.execute_query(self.db.dest_conn, create_query)
        self.logger.info(f"Ensured table {table_name} exists.")

    def populate_league_names_table(self, league_data):
        """
        "LeagueData": {
          "L": [
            { "id": "1", "n": "Kategoria Superiore", "c": "ALB", "d": "1", "i": "ALB1.png" },
            ...
          ]
        }
        - If league_name is None, we fallback to, say, "<country_id>-<division>" or just the 'id' from JSON.
        """
        table_name = "dc_league_names"
        rows = league_data.get("L", [])
        if not rows:
            self.logger.info("No league data found in datapack. Skipping.")
            return

        self.logger.info(f"Populating {table_name} with {len(rows)} rows...")

        insert_query = f"""
        INSERT INTO `{table_name}` (league_name, country_id, division)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE
            `league_name`=VALUES(`league_name`)
        """

        params_list = []
        for item in rows:
            # fallback for division, country, etc.
            country_id = str(item.get("c", ""))[:3].upper()
            division_str = item.get("d", "0")
            try:
                division = int(division_str)
            except ValueError:
                division = 0

            raw_league_name = item.get("n")  # might be None
            if raw_league_name is None:
                # fallback to item["id"] if present, or "Unknown"
                raw_id = item.get("id", "Unknown")
                league_name = f"{raw_id}"
            else:
                league_name = str(raw_league_name)[:255]

            params_list.append((league_name, country_id, division))

        if params_list:
            self.db.execute_many(self.db.dest_conn, insert_query, params_list)
            self.logger.info(f"Inserted/updated {len(params_list)} league rows in {table_name}.")

    # -------------------------------------------------------------------------
    # Cup names
    # -------------------------------------------------------------------------
    def create_cup_names_table(self):
        """
        dc_cup_names: columns -> country_id (VARCHAR(10) PRIMARY KEY), cup_name (VARCHAR(255))
        We store "id" as country_id, "n" as cup_name.
        """
        table_name = "dc_cup_names"
        create_query = f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` (
            `country_id` VARCHAR(10) NOT NULL,
            `cup_name` VARCHAR(255),
            PRIMARY KEY (`country_id`)
        ) ENGINE=InnoDB DEFAULT CHARSET={CHARSET} COLLATE={COLLATION}
        """
        self.db.execute_query(self.db.dest_conn, create_query)
        self.logger.info(f"Ensured table {table_name} exists.")

    def populate_cup_names_table(self, cup_data):
        """
        "CupData": {
          "C": [
            { "id": "AFR", "n": "CAF Champions League", "i": "AFR.png" },
            ...
          ]
        }
        - If cup_name is None, fallback to the ID
        """
        table_name = "dc_cup_names"
        rows = cup_data.get("C", [])
        if not rows:
            self.logger.info("No cup data found in datapack. Skipping.")
            return

        self.logger.info(f"Populating {table_name} with {len(rows)} rows...")

        insert_query = f"""
        INSERT INTO `{table_name}` (country_id, cup_name)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE
            `cup_name`=VALUES(`cup_name`)
        """

        params_list = []
        for item in rows:
            cid = str(item.get("id", ""))[:10]
            if not cid:
                self.logger.warning("Skipping cup with no id.")
                continue

            raw_cup_name = item.get("n")  # might be None
            if raw_cup_name is None:
                cup_name = cid
            else:
                cup_name = str(raw_cup_name)[:255]

            params_list.append((cid, cup_name))

        if params_list:
            self.db.execute_many(self.db.dest_conn, insert_query, params_list)
            self.logger.info(f"Inserted/updated {len(params_list)} cup rows in {table_name}.")

    # -------------------------------------------------------------------------
    # Venue names
    # -------------------------------------------------------------------------
    def create_venue_names_table(self):
        """
        dc_venue_names: columns -> venue_id (INT PRIMARY KEY), venue_name (VARCHAR(255))
        """
        table_name = "dc_venue_names"
        create_query = f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` (
            `venue_id` INT NOT NULL,
            `venue_name` VARCHAR(255),
            PRIMARY KEY (`venue_id`)
        ) ENGINE=InnoDB DEFAULT CHARSET={CHARSET} COLLATE={COLLATION}
        """
        self.db.execute_query(self.db.dest_conn, create_query)
        self.logger.info(f"Ensured table {table_name} exists.")

    def populate_venue_names_table(self, stadium_data):
        """
        "StadiumData": {
          "S": [
            { "id": "556", "n": "Old Trafford" },
            ...
          ]
        }
        - If venue_name is None, fallback to the venue_id
        """
        table_name = "dc_venue_names"
        rows = stadium_data.get("S", [])
        if not rows:
            self.logger.info("No stadium data found in datapack. Skipping.")
            return

        self.logger.info(f"Populating {table_name} with {len(rows)} rows...")

        insert_query = f"""
        INSERT INTO `{table_name}` (venue_id, venue_name)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE
            `venue_name`=VALUES(`venue_name`)
        """

        params_list = []
        for item in rows:
            try:
                vid = int(item.get("id", 0))
            except ValueError:
                self.logger.warning(f"Skipping invalid venue id: {item.get('id')}")
                continue

            raw_venue_name = item.get("n")  # might be None
            if raw_venue_name is None:
                venue_name = str(vid)
            else:
                venue_name = str(raw_venue_name)[:255]

            params_list.append((vid, venue_name))

        if params_list:
            self.db.execute_many(self.db.dest_conn, insert_query, params_list)
            self.logger.info(f"Inserted/updated {len(params_list)} venue rows in {table_name}.")
