import logging
import time
import mysql.connector
import sqlite3
import re
from contextlib import contextmanager
from config import SOURCE_DB_CONFIG, DEST_DB_CONFIG, SQLITE_PATH, BIGQUERY_DB_CONFIG

# Configuration for charset and collation used throughout the
# app for database tables.
CHARSET = 'utf8mb4'
COLLATION = 'utf8mb4_0900_bin'

class DBManager:
    def __init__(self):
        self.dest_conn = None
        self.source_conn = None
        self.bigquery_conn = None  # NEW: BigQuery local connection
        self.connect()

    def connect_dest_db(self):
        self.dest_conn = mysql.connector.connect(
            charset=CHARSET,
            collation=COLLATION,
            autocommit=True,
            use_unicode=True,
            **DEST_DB_CONFIG,
            sql_mode="NO_ZERO_DATE,NO_ZERO_IN_DATE"
        )
        logging.info(f"Connected to destination database at {DEST_DB_CONFIG['host']}")

    def connect_source_db(self):
        self.source_conn = mysql.connector.connect(
            charset=CHARSET,
            collation=COLLATION,
            autocommit=True,
            use_unicode=True,
            **SOURCE_DB_CONFIG
        )
        logging.info(f"Connected to source database at {SOURCE_DB_CONFIG['host']}")
        # Set the isolation level to READ COMMITTED
        self.source_conn.cursor().execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")

    # NEW: Connect to the BigQuery local database (if configured)
    def connect_bigquery_db(self):
        # Only connect if all required config values are present
        if all(BIGQUERY_DB_CONFIG.get(key) for key in ['host', 'user', 'password', 'database']):
            self.bigquery_conn = mysql.connector.connect(
                charset=CHARSET,
                collation=COLLATION,
                autocommit=True,
                use_unicode=True,
                **BIGQUERY_DB_CONFIG,
                sql_mode="NO_ZERO_DATE,NO_ZERO_IN_DATE"
            )
            logging.info(f"Connected to bigquery_local database at {BIGQUERY_DB_CONFIG['host']}")
        else:
            self.bigquery_conn = None
            logging.info("BigQuery database not configured, skipping connection.")

    def connect(self):
        self.connect_dest_db()
        self.connect_source_db()
        self.connect_bigquery_db()   # NEW: Connect to BigQuery local (if configured)

    def reconnect(self):
        self.close()
        self.connect()

    @contextmanager
    def sqlite_connection(self):
        # Open SQLite in read-only mode and start a read transaction
        conn = sqlite3.connect(f"file:{SQLITE_PATH}?mode=ro", uri=True)
        conn.isolation_level = 'DEFERRED'
        try:
            conn.execute('BEGIN')
            yield conn
        finally:
            conn.rollback()
            conn.close()

    @contextmanager
    def mysql_connection(self, connection):
        try:
            yield connection
        except mysql.connector.Error as err:
            logging.error(f"MySQL error: {err}")
            raise

    def check_and_reconnect(self, connection):
        """Check if connection is alive and reconnect if needed"""
        try:
            if connection == self.dest_conn:
                if not self.dest_conn.is_connected():
                    logging.info("Destination database connection lost. Reconnecting...")
                    self.connect_dest_db()
                    return self.dest_conn
            elif connection == self.source_conn:
                if not self.source_conn.is_connected():
                    logging.info("Source database connection lost. Reconnecting...")
                    self.connect_source_db()
                    return self.source_conn
            elif connection == self.bigquery_conn:
                if self.bigquery_conn and not self.bigquery_conn.is_connected():
                    logging.info("BigQuery database connection lost. Reconnecting...")
                    self.connect_bigquery_db()
                    return self.bigquery_conn
        except:
            # If checking connection status fails, try to reconnect anyway
            if connection == self.dest_conn:
                self.connect_dest_db()
                return self.dest_conn
            elif connection == self.source_conn:
                self.connect_source_db()
                return self.source_conn
            elif connection == self.bigquery_conn:
                self.connect_bigquery_db()
                return self.bigquery_conn
        return connection

    def execute_query(self, connection, query, params=None):
        if isinstance(connection, str):
            connection_str = connection.lower()
            if connection_str == 'sqlite':
                with self.sqlite_connection() as sqlite_conn:
                    sqlite_conn.row_factory = sqlite3.Row  # Access columns by name
                    cursor = sqlite_conn.cursor()
                    cursor.execute(query, params or ())
                    result = cursor.fetchall()
                    columns = result[0].keys() if result else []
                    return [dict(zip(columns, row)) for row in result]
            elif connection_str == 'source':
                # Use the source MySQL connection
                self.source_conn = self.check_and_reconnect(self.source_conn)
                with self.mysql_connection(self.source_conn) as conn:
                    cursor = conn.cursor(dictionary=True)
                    try:
                        cursor.execute(query, params or ())
                        result = cursor.fetchall()
                        return result
                    except mysql.connector.Error as err:
                        logging.error(f"Error executing query: {err}")
                        raise
                    finally:
                        cursor.close()
            elif connection_str == 'bigquery':
                # Use the BigQuery local connection
                if self.bigquery_conn is None:
                    raise ValueError("BigQuery database is not configured")
                self.bigquery_conn = self.check_and_reconnect(self.bigquery_conn)
                with self.mysql_connection(self.bigquery_conn) as conn:
                    cursor = conn.cursor(dictionary=True)
                    try:
                        cursor.execute(query, params or ())
                        result = cursor.fetchall()
                        return result
                    except mysql.connector.Error as err:
                        logging.error(f"Error executing query on BigQuery local: {err}")
                        raise
                    finally:
                        cursor.close()
            else:
                raise ValueError(f"Unknown connection string: {connection}")
        else:
            # Check and reconnect if needed for direct connection objects
            connection = self.check_and_reconnect(connection)
            with self.mysql_connection(connection) as conn:
                cursor = conn.cursor(dictionary=True)
                try:
                    cursor.execute(query, params or ())
                    result = cursor.fetchall()
                    return result
                except mysql.connector.Error as err:
                    logging.error(f"Error executing query: {err}")
                    raise
                finally:
                    cursor.close()

    def get_existing_columns(self, table_name):
        # Check and reconnect if needed
        self.dest_conn = self.check_and_reconnect(self.dest_conn)
        cursor = self.dest_conn.cursor()
        try:
            cursor.execute(f"SHOW COLUMNS FROM `{table_name}`")
            columns = {row[0] for row in cursor.fetchall()}
            return columns
        except mysql.connector.Error as err:
            if err.errno == 1146:  # Table doesn't exist
                return None
            else:
                logging.error(f"Error getting columns for `{table_name}`: {err}")
                raise
        finally:
            cursor.close()

    def execute_many(self, connection, query, params, max_retries=3):
        attempt = 0
        while attempt < max_retries:
            # Check and reconnect if needed before executing
            connection = self.check_and_reconnect(connection)
            cursor = connection.cursor()
            try:
                cursor.executemany(query, params)
                connection.commit()
                return
            except mysql.connector.Error as err:
                if err.errno in (2055, 2013, 2006):  # Lost connection, MySQL server has gone away
                    logging.warning(f"Connection error (errno {err.errno}). Reconnecting...")
                    self.reconnect()
                    # Update the connection reference after reconnect
                    if connection == self.dest_conn:
                        connection = self.dest_conn
                    elif connection == self.source_conn:
                        connection = self.source_conn
                    elif connection == self.bigquery_conn:
                        connection = self.bigquery_conn
                    attempt += 1
                    time.sleep(1)
                else:
                    logging.error(f"Error executing batch query: {err}")
                    connection.rollback()
                    raise
            finally:
                cursor.close()
        logging.critical("Max retries reached for execute_many.")
        raise Exception("execute_many failed after retries.")

    def get_existing_columns_with_types(self, table_name):
        # Check and reconnect if needed
        self.dest_conn = self.check_and_reconnect(self.dest_conn)
        cursor = self.dest_conn.cursor()
        try:
            cursor.execute(f"SHOW COLUMNS FROM `{table_name}`")
            columns = {}
            for row in cursor.fetchall():
                col_name = row[0]
                col_type = row[1]
                columns[col_name] = col_type
            return columns
        except mysql.connector.Error as err:
            if err.errno == 1146:  # Table doesn't exist
                return None
            else:
                logging.error(f"Error getting columns for `{table_name}`: {err}")
                raise
        finally:
            cursor.close()

    def get_existing_indexes(self, table_name):
        cursor = self.dest_conn.cursor(dictionary=True)
        try:
            cursor.execute(f"SHOW INDEX FROM `{table_name}`")
            indexes_info = cursor.fetchall()
            indexes = set()
            for index in indexes_info:
                if index['Key_name'] != 'PRIMARY':
                    indexes.add(index['Column_name'])
            return indexes
        except mysql.connector.Error as err:
            logging.error(f"Error getting indexes for `{table_name}`: {err}")
            raise
        finally:
            cursor.close()

    def get_existing_index_names(self, table_name):
        """Return the set of index names (Key_name) on a table, excluding PRIMARY.
        Used to detect missing composite indexes that need adding via ALTER TABLE."""
        cursor = self.dest_conn.cursor(dictionary=True)
        try:
            cursor.execute(f"SHOW INDEX FROM `{table_name}`")
            return {row['Key_name'] for row in cursor.fetchall() if row['Key_name'] != 'PRIMARY'}
        except mysql.connector.Error as err:
            logging.error(f"Error getting index names for `{table_name}`: {err}")
            raise
        finally:
            cursor.close()

    def mysql_version_supports_json(self):
        # Get the MySQL server version
        if not self.dest_conn.is_connected():
            self.dest_conn.reconnect()
        cursor = self.dest_conn.cursor()
        cursor.execute("SELECT VERSION()")
        version_str = cursor.fetchone()[0]
        cursor.close()
        # Extract the major and minor version numbers
        version_parts = version_str.split('-')[0].split('.')
        major = int(version_parts[0])
        minor = int(version_parts[1]) if len(version_parts) > 1 else 0
        # MySQL supports JSON types from version 5.7 onwards
        # MariaDB supports JSON from version 10.2 onwards (as an alias for LONGTEXT)
        if 'MariaDB' in version_str:
            maria_major = major
            maria_minor = minor
            if maria_major > 10 or (maria_major == 10 and maria_minor >= 2):
                return True
        else:
            if major > 5 or (major == 5 and minor >= 7):
                return True
        return False

    def close(self):
        if self.dest_conn:
            self.dest_conn.close()
        if self.source_conn:
            self.source_conn.close()
        if self.bigquery_conn:
            self.bigquery_conn.close()
