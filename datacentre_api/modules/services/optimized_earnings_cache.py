# modules/services/optimized_earnings_cache.py
"""
Optimized earnings cache that works with the single batch query
"""

import time
import json
import hashlib
import logging
from typing import Dict, List, Optional
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

# Cache TTL in seconds
EARNINGS_CACHE_TTL = 300  # 5 minutes


async def get_cached_or_fetch_earnings(
    archival_session: AsyncSession,
    name: str,
    rows: List,
    redis_client=None
) -> Dict[str, Dict[str, int]]:
    """
    Get earnings with caching support using the optimized single batch query.
    If Redis is available, cache the entire result.
    """

    # Prepare data for query
    now = int(time.time())
    seven_days_ago = now - (7 * 86400)
    thirty_days_ago = now - (30 * 86400)
    name_len = len(name)

    # Create cache key based on user and share IDs
    cache_key = None
    if redis_client:
        share_ids_str = "_".join(sorted([f"{r.share_type}:{r.share_id}" for r in rows]))
        cache_key = f"earnings_optimized:{name}:{hashlib.md5(share_ids_str.encode()).hexdigest()[:16]}"

        # Try to get from cache
        try:
            cached_data = await redis_client.get(cache_key)
            if cached_data:
                logger.info(f"Cache HIT for optimized earnings query")
                return json.loads(cached_data)
        except Exception as e:
            logger.debug(f"Cache read error: {e}")

    # Not in cache, fetch from database
    logger.info(f"Fetching earnings for {len(rows)} shares with optimized batch query")

    # Group share IDs by type for more efficient IN clauses
    club_ids = []
    player_ids = []
    for row in rows:
        if row.share_type == 'club':
            club_ids.append(str(row.share_id))
        else:
            player_ids.append(str(row.share_id))

    # Build optimized WHERE conditions using IN clauses
    conditions = []
    if club_ids:
        conditions.append(f"(ubs.other_type = 'club' AND ubs.other_id IN ({','.join(club_ids)}))")
    if player_ids:
        conditions.append(f"(ubs.other_type = 'player' AND ubs.other_id IN ({','.join(player_ids)}))")

    share_earnings = {}

    if conditions:
        where_clause = " OR ".join(conditions)

        # Optimized single batch query for ALL earnings
        earnings_batch_query = text(f"""
            SELECT STRAIGHT_JOIN
                ubs.other_type,
                ubs.other_id,
                SUM(IF(b.date >= :seven_days_ago, ubs.amount, 0)) as e7,
                SUM(ubs.amount) as e30
            FROM user_balance_sheets ubs
            INNER JOIN blocks b ON ubs.height = b.height
            WHERE ubs.name = :name
                AND CHAR_LENGTH(ubs.name) = :name_len
                AND ubs.type LIKE 'dividend%'
                AND b.date >= :thirty_days_ago
                AND ({where_clause})
            GROUP BY ubs.other_type, ubs.other_id
        """)

        params = {
            'name': name,
            'name_len': name_len,
            'seven_days_ago': seven_days_ago,
            'thirty_days_ago': thirty_days_ago
        }

        start_time = time.time()
        earnings_result = await archival_session.execute(earnings_batch_query, params)
        earnings_rows = earnings_result.fetchall()
        query_time = time.time() - start_time

        for earnings_row in earnings_rows:
            key = f"{earnings_row.other_type}_{earnings_row.other_id}"
            share_earnings[key] = {
                'e7': int(earnings_row.e7 or 0),
                'e30': int(earnings_row.e30 or 0)
            }

        # Add zero earnings for shares with no dividends
        for row in rows:
            key = f"{row.share_type}_{row.share_id}"
            if key not in share_earnings:
                share_earnings[key] = {'e7': 0, 'e30': 0}

        logger.info(f"Fetched earnings for {len(rows)} shares in {query_time:.3f}s")

    # Cache the result if Redis is available
    if redis_client and cache_key:
        try:
            await redis_client.setex(
                cache_key,
                EARNINGS_CACHE_TTL,
                json.dumps(share_earnings)
            )
            logger.info(f"Cached optimized earnings result (TTL: {EARNINGS_CACHE_TTL}s)")
        except Exception as e:
            logger.debug(f"Cache write error: {e}")

    return share_earnings