"""
Service for getting SVC to USDC price with caching and weighted average calculation
Always serves cached data, updates in background every 20 minutes
"""

import logging
import json
import time
from datetime import datetime, timedelta
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional
from fastapi import BackgroundTasks

logger = logging.getLogger(__name__)

CACHE_KEY = "svc_price_weighted"
CACHE_TTL = 24 * 3600  # Keep cache for 24 hours
UPDATE_INTERVAL = 20 * 60  # Update every 20 minutes

async def get_previous_daily_average(mysql_session: AsyncSession, before_ts: datetime) -> Optional[float]:
    """
    Find the latest SVC trade before before_ts, determine which UTC calendar day
    that trade was on, and return the volume-weighted average price for that day.
    Returns None if no trades exist before before_ts.
    """
    from ..base import SVCTrades
    try:
        last_trade_query = (
            select(func.date(SVCTrades.trade_ts).label('trade_date'))
            .where(SVCTrades.trade_ts < before_ts)
            .order_by(SVCTrades.trade_ts.desc())
            .limit(1)
        )
        result = await mysql_session.execute(last_trade_query)
        row = result.fetchone()
        if not row:
            return None

        trade_date = row.trade_date
        vwap_query = (
            select(
                func.sum(SVCTrades.volume_usdc).label('total_usdc'),
                func.sum(SVCTrades.volume_svc).label('total_svc'),
            )
            .where(func.date(SVCTrades.trade_ts) == trade_date)
        )
        result = await mysql_session.execute(vwap_query)
        row = result.fetchone()
        if row and row.total_usdc and row.total_svc and row.total_svc > 0:
            return float(row.total_usdc / row.total_svc)
    except Exception as e:
        logger.error(f"Error in get_previous_daily_average: {e}")

    return None

async def _calculate_weighted_price(mysql_session: AsyncSession) -> Optional[float]:
    """
    Calculate 24-hour weighted average price from svc_trades table.
    If no trades exist in the last 24 hours, falls back to the VWAP of the
    last calendar day that had trades.
    """
    from ..base import SVCTrades
    try:
        twenty_four_hours_ago = datetime.now() - timedelta(hours=24)

        weighted_avg_query = (
            select(
                func.sum(SVCTrades.volume_usdc).label('total_usdc'),
                func.sum(SVCTrades.volume_svc).label('total_svc'),
            )
            .where(SVCTrades.trade_ts >= twenty_four_hours_ago)
        )
        result = await mysql_session.execute(weighted_avg_query)
        row = result.fetchone()

        if row and row.total_usdc and row.total_svc and row.total_svc > 0:
            svc_price = row.total_usdc / row.total_svc
            logger.info(f"Calculated 24-hour weighted average SVC price: ${svc_price:.6f}")
            return float(svc_price)
    except Exception as e:
        logger.error(f"Error calculating weighted average price: {e}")
        return None

    # No trades in the last 24 hours — fall back to the last day that had trades
    logger.info("No trades in last 24 hours, falling back to last active trading day")
    return await get_previous_daily_average(mysql_session, datetime.now())

async def _update_price_cache(redis_client, mysql_session: AsyncSession):
    """
    Background task to update the SVC price cache.
    """
    logger.info("Starting background update of SVC price cache")

    new_price = await _calculate_weighted_price(mysql_session)

    if new_price and redis_client:
        try:
            cache_data = {
                "price": float(new_price),
                "last_updated": time.time()
            }
            await redis_client.setex(
                CACHE_KEY,
                CACHE_TTL,
                json.dumps(cache_data)
            )
            logger.info(f"Updated SVC price cache: ${new_price:.6f}")
        except Exception as e:
            logger.error(f"Failed to update cache: {e}")

async def get_svc_price(
    redis_client=None,
    mysql_session: Optional[AsyncSession] = None,
    background_tasks: Optional[BackgroundTasks] = None
) -> float:
    """
    Get the current SVC to USDC price.
    ALWAYS returns cached data immediately. Updates in background if needed.

    Args:
        redis_client: Redis client for cache access
        mysql_session: Database session (only needed if cache is missing/stale)
        background_tasks: FastAPI background tasks for async updates

    Returns:
        SVC to USDC price as float (from cache)
    """

    if not redis_client:
        # No Redis available - calculate price directly if we have a session
        logger.warning("No Redis client available, calculating price directly")
        if mysql_session:
            price = await _calculate_weighted_price(mysql_session)
            if price:
                return price
        # Last resort fallback
        return 0.011  # Approximate recent price as emergency fallback

    try:
        # Always try to get from cache first
        cached_value = await redis_client.get(CACHE_KEY)

        if cached_value:
            cache_data = json.loads(cached_value)
            price = cache_data.get("price", 0.0)
            last_updated = cache_data.get("last_updated", 0)

            # Check if update needed (older than 20 minutes)
            if (time.time() - last_updated) > UPDATE_INTERVAL:
                # Schedule background update if we have the necessary components
                if mysql_session and background_tasks:
                    logger.info("SVC price cache is stale, scheduling background update")
                    background_tasks.add_task(_update_price_cache, redis_client, mysql_session)

            logger.debug(f"Serving SVC price from cache: ${price:.6f}")
            return float(price)
        else:
            # No cache exists - need to populate it
            logger.warning("No SVC price cache found")

            if mysql_session:
                # Try to populate cache immediately if we have a session
                await _update_price_cache(redis_client, mysql_session)

                # Try to get the newly cached value
                cached_value = await redis_client.get(CACHE_KEY)
                if cached_value:
                    cache_data = json.loads(cached_value)
                    price = cache_data.get("price", 0.0)
                    return float(price)

    except Exception as e:
        logger.error(f"Error getting SVC price from cache: {e}")

    # Emergency fallback - return approximate recent price
    logger.warning("Using emergency fallback SVC price")
    return 0.011
