# modules/services/earnings_service.py
"""
Centralized earnings service with Redis caching
This service can be used throughout the codebase for consistent and fast earnings queries
"""

import time
import json
import hashlib
import logging
from typing import Dict, List, Optional, Tuple
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

# Cache TTL in seconds
EARNINGS_CACHE_TTL = 300  # 5 minutes


class EarningsService:
    """
    Service for fetching and caching user earnings data

    Usage:
        earnings_service = EarningsService(redis_client)
        earnings = await earnings_service.get_earnings(
            archival_session,
            "username",
            "club",
            [1316, 1384, ...]
        )
    """

    def __init__(self, redis_client):
        self.redis = redis_client

    def _get_cache_key(self, name: str, share_type: str, share_ids: List[int]) -> str:
        """Generate a deterministic cache key"""
        # Use first 16 chars of hash for better uniqueness with large lists
        share_ids_hash = hashlib.md5(
            f"{sorted(share_ids)}".encode()
        ).hexdigest()[:16]
        return f"earnings:{name}:{share_type}:{share_ids_hash}"

    async def get_earnings(
        self,
        archival_session: AsyncSession,
        name: str,
        share_type: str,
        share_ids: List[int],
        use_cache: bool = True
    ) -> Dict[str, Dict[str, int]]:
        """
        Get earnings for a user's shares (with caching)

        Args:
            archival_session: Database session for archival database
            name: Username
            share_type: 'club' or 'player'
            share_ids: List of share IDs to get earnings for
            use_cache: Whether to use Redis cache (default: True)

        Returns:
            Dictionary mapping "type_id" to {'e7': amount, 'e30': amount}
        """
        if not share_ids:
            return {}

        # Try cache first if enabled
        if use_cache and self.redis:
            cached = await self._get_from_cache(name, share_type, share_ids)
            if cached is not None:
                return cached

        # Fetch from database
        earnings = await self._fetch_from_db(
            archival_session, name, share_type, share_ids
        )

        # Cache the results if enabled
        if use_cache and self.redis:
            await self._save_to_cache(name, share_type, share_ids, earnings)

        return earnings

    async def get_earnings_batch(
        self,
        archival_session: AsyncSession,
        user_shares: List[Tuple[str, str, List[int]]],
        use_cache: bool = True
    ) -> Dict[str, Dict[str, Dict[str, int]]]:
        """
        Get earnings for multiple users/share types in batch

        Args:
            archival_session: Database session
            user_shares: List of (username, share_type, share_ids) tuples
            use_cache: Whether to use Redis cache

        Returns:
            Nested dictionary: {username: {share_key: {'e7': amount, 'e30': amount}}}
        """
        results = {}

        for name, share_type, share_ids in user_shares:
            if name not in results:
                results[name] = {}

            earnings = await self.get_earnings(
                archival_session, name, share_type, share_ids, use_cache
            )
            results[name].update(earnings)

        return results

    async def invalidate_cache(self, name: str, share_type: Optional[str] = None):
        """
        Invalidate cached earnings for a user

        Args:
            name: Username
            share_type: Optional share type to invalidate (if None, invalidates all)
        """
        if not self.redis:
            return

        try:
            if share_type:
                # Invalidate specific share type
                pattern = f"earnings:{name}:{share_type}:*"
            else:
                # Invalidate all earnings for user
                pattern = f"earnings:{name}:*"

            # Get all matching keys
            keys = []
            async for key in self.redis.scan_iter(match=pattern):
                keys.append(key)

            # Delete keys if found
            if keys:
                await self.redis.delete(*keys)
                logger.info(f"Invalidated {len(keys)} earnings cache entries for {name}")

        except Exception as e:
            logger.error(f"Failed to invalidate cache: {e}")

    async def _get_from_cache(
        self,
        name: str,
        share_type: str,
        share_ids: List[int]
    ) -> Optional[Dict[str, Dict[str, int]]]:
        """Get cached earnings data"""
        try:
            cache_key = self._get_cache_key(name, share_type, share_ids)
            cached_data = await self.redis.get(cache_key)

            if cached_data:
                logger.debug(f"Cache HIT: {cache_key}")
                return json.loads(cached_data)

            logger.debug(f"Cache MISS: {cache_key}")
            return None

        except Exception as e:
            logger.error(f"Redis read error: {e}")
            return None

    async def _save_to_cache(
        self,
        name: str,
        share_type: str,
        share_ids: List[int],
        earnings_data: Dict[str, Dict[str, int]]
    ):
        """Save earnings data to cache"""
        try:
            cache_key = self._get_cache_key(name, share_type, share_ids)

            await self.redis.setex(
                cache_key,
                EARNINGS_CACHE_TTL,
                json.dumps(earnings_data)
            )

            logger.debug(f"Cached: {cache_key} (TTL: {EARNINGS_CACHE_TTL}s)")

        except Exception as e:
            logger.error(f"Redis write error: {e}")

    async def _fetch_from_db(
        self,
        archival_session: AsyncSession,
        name: str,
        share_type: str,
        share_ids: List[int]
    ) -> Dict[str, Dict[str, int]]:
        """
        Fetch earnings from database using optimized query
        """
        start_time = time.time()

        now = int(time.time())
        seven_days_ago = now - (7 * 86400)
        thirty_days_ago = now - (30 * 86400)
        name_len = len(name)

        # Optimized query with STRAIGHT_JOIN
        query = text("""
            SELECT
                ubs.other_id,
                SUM(IF(b.date >= :seven_days_ago, ubs.amount, 0)) as e7,
                SUM(ubs.amount) as e30
            FROM user_balance_sheets ubs STRAIGHT_JOIN blocks b
                ON ubs.height = b.height AND b.date >= :thirty_days_ago
            WHERE ubs.name = :name
                AND CHAR_LENGTH(ubs.name) = :name_len
                AND ubs.other_type = :share_type
                AND ubs.other_id IN :share_ids
                AND ubs.type LIKE 'dividend%'
            GROUP BY ubs.other_id
        """)

        params = {
            'name': name,
            'name_len': name_len,
            'share_type': share_type,
            'share_ids': tuple(share_ids),
            'seven_days_ago': seven_days_ago,
            'thirty_days_ago': thirty_days_ago
        }

        result = await archival_session.execute(query, params)
        rows = result.fetchall()

        # Build result dictionary
        earnings_data = {}

        for row in rows:
            key = f"{share_type}_{row.other_id}"
            earnings_data[key] = {
                'e7': int(row.e7 or 0),
                'e30': int(row.e30 or 0)
            }

        # Add zero earnings for shares with no dividends
        for share_id in share_ids:
            key = f"{share_type}_{share_id}"
            if key not in earnings_data:
                earnings_data[key] = {'e7': 0, 'e30': 0}

        elapsed = time.time() - start_time
        logger.info(f"Fetched earnings for {len(share_ids)} {share_type}s in {elapsed:.3f}s")

        return earnings_data

    async def get_single_share_earnings(
        self,
        archival_session: AsyncSession,
        name: str,
        share_type: str,
        share_id: int,
        use_cache: bool = True
    ) -> Dict[str, int]:
        """
        Convenience method for getting earnings for a single share

        Returns:
            {'e7': amount, 'e30': amount}
        """
        earnings = await self.get_earnings(
            archival_session, name, share_type, [share_id], use_cache
        )

        key = f"{share_type}_{share_id}"
        return earnings.get(key, {'e7': 0, 'e30': 0})

    async def get_total_earnings(
        self,
        archival_session: AsyncSession,
        name: str,
        use_cache: bool = True
    ) -> Dict[str, int]:
        """
        Get total earnings across all shares for a user

        Returns:
            {'e7': total_amount, 'e30': total_amount}
        """
        # This would need to query all shares first
        # For now, this is a placeholder for the pattern

        query = text("""
            SELECT
                SUM(IF(b.date >= UNIX_TIMESTAMP(NOW() - INTERVAL 7 DAY), ubs.amount, 0)) as e7,
                SUM(IF(b.date >= UNIX_TIMESTAMP(NOW() - INTERVAL 30 DAY), ubs.amount, 0)) as e30
            FROM user_balance_sheets ubs
            JOIN blocks b ON ubs.height = b.height
            WHERE ubs.name = :name
                AND ubs.type LIKE 'dividend%'
                AND b.date >= UNIX_TIMESTAMP(NOW() - INTERVAL 30 DAY)
        """)

        result = await archival_session.execute(query, {'name': name})
        row = result.first()

        if row:
            return {
                'e7': int(row.e7 or 0),
                'e30': int(row.e30 or 0)
            }

        return {'e7': 0, 'e30': 0}


# Singleton instance (optional pattern)
_earnings_service = None

def get_earnings_service(redis_client) -> EarningsService:
    """
    Get or create the earnings service singleton

    Usage:
        from modules.services.earnings_service import get_earnings_service
        from modules.base import get_redis_client

        redis = get_redis_client()
        earnings_service = get_earnings_service(redis)
    """
    global _earnings_service
    if _earnings_service is None:
        _earnings_service = EarningsService(redis_client)
    return _earnings_service