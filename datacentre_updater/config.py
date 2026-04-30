import os
from dotenv import load_dotenv

dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path)

SOURCE_DB_CONFIG = {
   'host': os.environ['SOURCE_DB_HOST'],
   'user': os.environ['SOURCE_DB_USER'],
   'password': os.environ['SOURCE_DB_PASSWORD'],
   'database': os.environ['SOURCE_DB_NAME'],
}

DEST_DB_CONFIG = {
   'host': os.environ['DEST_DB_HOST'],
   'user': os.environ['DEST_DB_USER'],
   'password': os.environ['DEST_DB_PASSWORD'],
   'database': os.environ['DEST_DB_NAME'],
}

# BigQuery Local Database Configuration (OPTIONAL)
BIGQUERY_DB_CONFIG = {
    'host': os.environ.get('BIGQUERY_DB_HOST', ''),
    'user': os.environ.get('BIGQUERY_DB_USER', ''),
    'password': os.environ.get('BIGQUERY_DB_PASSWORD', ''),
    'database': os.environ.get('BIGQUERY_DB_NAME', ''),
}

SQLITE_PATH = os.environ['SQLITE_PATH']

# NEW: Add the datapack URL
DATAPACK_URL = os.environ.get('DATAPACK_URL', '')

# Output folder for data dumps (share orders, leagues, etc.).
# Leave unset to disable the data dump updater.
DUMP_OUTPUT_FOLDER = os.environ.get('DUMP_OUTPUT_FOLDER', '')

# Graph API settings
SVC_POLYGON_SUBGRAPH_URL = os.environ.get('SVC_POLYGON_SUBGRAPH_URL', '')
POLYGON_STATS_SUBGRAPH_URL = os.environ.get('POLYGON_STATS_SUBGRAPH_URL', '')
SV_SUBGRAPH_URL = os.environ.get('SV_SUBGRAPH_URL', '')
GAME_ID = os.environ.get('GAME_ID', 'sv')

import logging
from user_enrichment import UserEnrichmentProvider, BigQueryUserEnrichmentProvider, DummyUserEnrichmentProvider

def _create_user_enrichment_provider(db_manager=None) -> UserEnrichmentProvider:
    """Create the appropriate user enrichment provider based on configuration."""
    logger = logging.getLogger(__name__)

    # Check if BIGQUERY_DB_CONFIG is properly configured
    if all(BIGQUERY_DB_CONFIG.get(key) for key in ['host', 'user', 'password', 'database']):
        if db_manager is None:
            raise ValueError("db_manager is required for BigQueryUserEnrichmentProvider")
        logger.info("Initializing BigQueryUserEnrichmentProvider")
        return BigQueryUserEnrichmentProvider(db_manager)
    else:
        logger.warning("BIGQUERY_DB not configured. Using DummyUserEnrichmentProvider (sequential number mapping)")
        return DummyUserEnrichmentProvider()

# User enrichment provider factory - will be called with db_manager when needed
USER_ENRICHMENT_PROVIDER_FACTORY = _create_user_enrichment_provider
