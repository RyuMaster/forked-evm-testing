import logging
from abc import ABC, abstractmethod
from typing import Dict


class UserEnrichmentProvider(ABC):
    """Abstract interface for providing user enrichment data."""
    
    @abstractmethod
    def get_valid_user_mappings(self, usernames: list) -> Dict[str, int]:
        """
        Get valid user mappings for the given usernames.
        
        Args:
            usernames: List of usernames to get mappings for
            
        Returns:
            Dictionary mapping username to group_id for valid users
        """
        pass


class BigQueryUserEnrichmentProvider(UserEnrichmentProvider):
    """Provider that queries the BigQuery local database for user enrichment data."""
    
    def __init__(self, db_manager):
        """
        Initialize the provider with a database manager.
        
        Args:
            db_manager: Database manager instance with access to BigQuery connection
        """
        self.db_manager = db_manager
        self.logger = logging.getLogger(__name__)
    
    def get_valid_user_mappings(self, usernames: list) -> Dict[str, int]:
        """
        Query the BigQuery local database for valid user mappings.
        
        Only includes users with valid enrichment data (non-NULL 1st_move).
        Filters results to only return mappings for the provided usernames.
        
        Args:
            usernames: List of usernames to get mappings for
            
        Returns:
            Dictionary mapping username to group_id for valid users
        """
        if not usernames:
            return {}
        
        query = """
        SELECT um.username, um.group_id
        FROM user_mappings um
        INNER JOIN user_enrichment ue ON um.group_id = ue.group_id
        WHERE ue.`1st_move` IS NOT NULL
        """
        
        results = self.db_manager.execute_query('bigquery', query)
        
        # Filter to only include the usernames we're interested in
        username_set = set(usernames)
        mappings = {}
        for row in results:
            username = row['username']
            group_id = row['group_id']
            if username and username in username_set:
                mappings[username] = group_id
        
        self.logger.info(f"Loaded {len(mappings)} valid user mappings from BigQuery local (filtered from {len(results)} total)")
        return mappings


class DummyUserEnrichmentProvider(UserEnrichmentProvider):
    """Provider that maps usernames to sequential numbers for testing/fallback."""
    
    def __init__(self):
        """Initialize the dummy provider."""
        self.logger = logging.getLogger(__name__)
    
    def get_valid_user_mappings(self, usernames: list) -> Dict[str, int]:
        """
        Map each username to a sequential integer starting from 1.
        
        Args:
            usernames: List of usernames to map
            
        Returns:
            Dictionary mapping each username to a sequential integer
        """
        mappings = {username: idx + 1 for idx, username in enumerate(sorted(usernames)) if username}
        self.logger.info(f"Generated {len(mappings)} dummy user mappings")
        return mappings
