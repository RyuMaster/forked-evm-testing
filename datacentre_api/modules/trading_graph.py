from enum import Enum
from sqlalchemy import (
    select,
    func,
    text,
)
from typing import List, Optional
from datetime import datetime, timedelta
import asyncio
import json
import time
import logging

from pydantic import BaseModel
from fastapi import APIRouter, Depends, Query, HTTPException, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession

from .base import get_archival_session, get_mysql_session, Blocks, get_redis_client
from .share_history import ShareTradeHistory
from .user_balance_sheet import UserBalanceSheets  # Import for payouts data

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class TradingGraphDataPoint(BaseModel):
    timestamp: int
    datetime: datetime
    volume: int  # total traded value
    weighted_average_price: float

# >>> NEW: Define an enum for valid time ranges <<<
class TimeRangeEnum(str, Enum):
    oneday = "1d"
    sevend = "7d"
    thirtyd = "30d"
    ninetyd = "90d"
    sixm = "6m"
    oneyear = "1y"

trading_graph_router = APIRouter()

@trading_graph_router.get(
    "/trading_graph",
    response_model=List[TradingGraphDataPoint],
    summary="Retrieve aggregated trading data",
    description="Generates time-series data (volume and weighted average price) for a club or player's trading activity, grouped by specified time intervals (e.g., 1d, 7d, 30d, 90d, 6m, 1y)."
)
async def get_trading_graph(
    club_id: Optional[int] = Query(None, description="Club ID"),
    player_id: Optional[int] = Query(None, description="Player ID"),
    # >>> REPLACE time_range STR with an enum <<<
    time_range: TimeRangeEnum = Query(
        ...,
        description="Time range: '1d', '7d', '30d', '90d', '6m', '1y'",
    ),
    session: AsyncSession = Depends(get_archival_session),
):
    # Validate that exactly one of club_id or player_id is provided
    if (club_id is None) == (player_id is None):
        raise HTTPException(
            status_code=400,
            detail="Please provide exactly one of 'club_id' or 'player_id'.",
        )

    # Determine share_type and share_id
    if club_id is not None:
        share_type = 'club'
        share_id = club_id
    else:
        share_type = 'player'
        share_id = player_id

    # Determine date range and time-bucket format based on enum
    now = datetime.utcnow()
    end_time = now
    if time_range == TimeRangeEnum.oneday:
        start_time = now - timedelta(days=1)
        time_bucket_format = '%Y-%m-%d %H:00:00'
    elif time_range == TimeRangeEnum.sevend:
        start_time = now - timedelta(days=7)
        time_bucket_format = '%Y-%m-%d 00:00:00'
    elif time_range == TimeRangeEnum.thirtyd:
        start_time = now - timedelta(days=30)
        time_bucket_format = '%Y-%m-%d 00:00:00'
    elif time_range == TimeRangeEnum.ninetyd:
        start_time = now - timedelta(days=90)
        time_bucket_format = '%Y-%m-%d 00:00:00'
    elif time_range == TimeRangeEnum.sixm:
        start_time = now - timedelta(days=180)  # Approx. 6 months
        time_bucket_format = '%Y-%m-01 00:00:00'
    elif time_range == TimeRangeEnum.oneyear:
        start_time = now - timedelta(days=365)
        time_bucket_format = '%Y-%m-01 00:00:00'
    else:
        # Shouldn't happen due to the enum
        raise HTTPException(status_code=400, detail=f"Invalid time_range: {time_range}")

    # Convert start_time and end_time to Unix timestamps
    start_unix = int(start_time.timestamp())
    end_unix = int(end_time.timestamp())

    # Alias tables
    trade_alias = ShareTradeHistory.__table__.alias("trade_alias")
    blocks_alias = Blocks.__table__.alias("blocks_alias")

    # Join clause
    join_clause = trade_alias.join(
        blocks_alias,
        trade_alias.c.height == blocks_alias.c.height
    )

    # Build the aggregation query
    aggregation_query = select(
        func.UNIX_TIMESTAMP(
            func.DATE_FORMAT(
                func.FROM_UNIXTIME(blocks_alias.c.date),
                time_bucket_format
            )
        ).label('time_bucket'),
        func.sum(trade_alias.c.price * trade_alias.c.num).label('total_volume'),
        (
            func.sum(trade_alias.c.price * trade_alias.c.num) /
            func.nullif(func.sum(trade_alias.c.num), 0)
        ).label('weighted_avg_price')
    ).select_from(join_clause)

    # Apply filters
    filters = [
        (trade_alias.c.share_type == share_type),
        (trade_alias.c.share_id == share_id),
        (blocks_alias.c.date >= start_unix),
        (blocks_alias.c.date <= end_unix),
    ]
    for condition in filters:
        aggregation_query = aggregation_query.where(condition)

    # Group by the time_bucket expression
    aggregation_query = aggregation_query.group_by('time_bucket')

    # Order by time_bucket ascending
    aggregation_query = aggregation_query.order_by('time_bucket')

    # Execute the query
    result = await session.execute(aggregation_query)
    rows = result.fetchall()

    # Prepare the response data
    data_points = []
    for row in rows:
        timestamp = int(row.time_bucket)
        total_volume = int(row.total_volume or 0)
        weighted_avg_price = float(row.weighted_avg_price or 0.0)
        data_point = TradingGraphDataPoint(
            timestamp=timestamp,
            datetime=datetime.utcfromtimestamp(timestamp),
            volume=total_volume,
            weighted_average_price=weighted_avg_price
        )
        data_points.append(data_point)

    return data_points


class PayoutsDataPoint(BaseModel):
    date: str
    timestamp: int
    total_payouts: int  # Total payouts in SVC smallest units
    club_payouts: int   # Payouts from clubs in smallest units
    player_payouts: int # Payouts from players in smallest units


class MonthlyDataPoint(BaseModel):
    month: str  # Format: YYYY-MM
    timestamp: int  # First day of month
    total_payouts: int  # Total payouts including dev (positive = into circulation)
    cash_injection: int  # SVC burnt from players to clubs (negative = out of circulation)
    player_game_shares: int  # SVC minted (negative = out of circulation)
    circulation: int  # Total SVC in circulation at end of month


class CombinedPayoutsResponse(BaseModel):
    daily: List[PayoutsDataPoint]
    monthly: List[MonthlyDataPoint]


async def _fetch_and_cache_monthly_data(r, archival_session: AsyncSession):
    """
    Fetch monthly aggregated data and cache it.
    Optimized to find month boundaries efficiently then aggregate.
    """
    current_date = datetime.utcnow()
    current_month_str = current_date.strftime("%Y-%m")
    
    # Always return cache if it exists - freshness is checked at endpoint level
    cache_key_all = "monthly_data_all_v5"  # v5 for cumulative circulation fix
    cached_all = await r.get(cache_key_all)
    
    if cached_all:
        cached_data = json.loads(cached_all)
        return cached_data["data"]
    
    logger.info("Fetching monthly data with optimized boundary approach...")
    
    # Step 1: Get date range (instant with index)
    range_query = text("SELECT MIN(date) as min_date, MAX(date) as max_date FROM blocks")
    result = await archival_session.execute(range_query)
    date_range = result.one()
    
    min_date = datetime.utcfromtimestamp(date_range.min_date)
    max_date = datetime.utcfromtimestamp(date_range.max_date)
    
    # Step 2: Find first block of each month (fast with index on date)
    current = datetime(min_date.year, min_date.month, 1)
    month_boundaries = []
    month_timestamps = {}
    
    while current <= max_date:
        month_start_unix = int(current.timestamp())
        month_str = current.strftime("%Y-%m")
        
        # Get first block height of this month
        query = text("""
            SELECT height 
            FROM blocks 
            WHERE date >= :month_start 
            ORDER BY date 
            LIMIT 1
        """)
        
        result = await archival_session.execute(query, {"month_start": month_start_unix})
        height = result.scalar()
        
        if height:
            month_boundaries.append({
                "month": month_str,
                "start_height": height
            })
            month_timestamps[month_str] = month_start_unix
        
        # Move to next month
        if current.month == 12:
            current = datetime(current.year + 1, 1, 1)
        else:
            current = datetime(current.year, current.month + 1, 1)
    
    # Add end boundary
    month_boundaries.append({
        "month": "END",
        "start_height": 999999999
    })
    
    # Step 3: Build CASE statement using height boundaries
    month_cases = []
    for i in range(len(month_boundaries) - 1):
        current_month = month_boundaries[i]
        next_month = month_boundaries[i + 1]
        month_cases.append(
            f"WHEN height >= {current_month['start_height']} "
            f"AND height < {next_month['start_height']} "
            f"THEN '{current_month['month']}'"
        )
    
    month_case_sql = "CASE " + " ".join(month_cases) + " END"
    
    # Single optimized query for all months - no JOINs
    monthly_query = text(f"""
        SELECT 
            {month_case_sql} as month,
            SUM(CASE WHEN type = 'club donation' THEN amount ELSE 0 END) as cash_injection,
            SUM(CASE WHEN type = 'mint' THEN amount ELSE 0 END) as player_game_shares,
            SUM(CASE 
                WHEN type IN ('dev') OR 
                     (other_type IN ('club', 'player') AND type NOT IN ('mint', 'club donation', 'burn'))
                THEN amount ELSE 0 END) as total_payouts
        FROM user_balance_sheets
        GROUP BY month
        HAVING month IS NOT NULL
        ORDER BY month
    """)
    
    result = await archival_session.execute(monthly_query)
    rows = result.fetchall()
    
    # Build monthly data - use cumulative calculation for circulation
    monthly_data = []
    cumulative_circulation = 0
    
    for row in rows:
        # Calculate monthly net change in circulation
        monthly_net = (row.total_payouts or 0) + (row.cash_injection or 0) + (row.player_game_shares or 0)
        # Add to cumulative total
        cumulative_circulation += monthly_net
        
        month_data = {
            "month": row.month,
            "timestamp": month_timestamps.get(row.month, 0),
            "total_payouts": int(row.total_payouts or 0),
            "cash_injection": int(row.cash_injection or 0),
            "player_game_shares": int(row.player_game_shares or 0),
            "circulation": int(cumulative_circulation)  # Cumulative circulation at end of month
        }
        monthly_data.append(month_data)
    
    # Cache the complete result
    cache_data = {
        "data": monthly_data,
        "timestamp": int(time.time())
    }
    await r.set(cache_key_all, json.dumps(cache_data), ex=86400)  # 24 hour cache
    
    logger.info(f"Cached monthly data for {len(monthly_data)} months")
    return monthly_data


async def _fetch_and_cache_payouts_data(r, session: AsyncSession, cache_key: str):
    """
    Fetch payouts data and cache it in Redis.
    Returns cached data if available, otherwise fetches fresh.
    """
    # Check cache first
    cached = await r.get(cache_key)
    if cached:
        data_json = json.loads(cached)
        return data_json["data"]
    
    # Get the current date from blocks
    max_date_query = select(func.max(Blocks.date))
    max_date_result = await session.execute(max_date_query)
    max_date_unix = max_date_result.scalar()
    
    if not max_date_unix:
        raise HTTPException(status_code=404, detail="No block data available")
    
    current_date = datetime.utcfromtimestamp(max_date_unix)
    thirty_days_ago = current_date - timedelta(days=30)
    thirty_days_ago_unix = int(thirty_days_ago.timestamp())
    
    # Query for daily payouts grouped by date and other_type
    daily_payouts_query = text("""
        SELECT 
            DATE(FROM_UNIXTIME(b.date)) as payout_date,
            MIN(b.date) as unix_timestamp,
            ubs.other_type,
            SUM(ubs.amount) as daily_amount
        FROM user_balance_sheets ubs
        JOIN blocks b ON ubs.height = b.height
        WHERE b.date >= :start_unix
            AND ubs.amount > 0
            AND ubs.other_type IN ('club', 'player')
        GROUP BY DATE(FROM_UNIXTIME(b.date)), ubs.other_type
        ORDER BY payout_date
    """)
    
    result = await session.execute(
        daily_payouts_query,
        {"start_unix": thirty_days_ago_unix}
    )
    
    rows = result.fetchall()
    
    # Organize data by date
    payouts_by_date = {}
    for row in rows:
        date_str = row.payout_date.strftime("%Y-%m-%d")
        if date_str not in payouts_by_date:
            payouts_by_date[date_str] = {
                "timestamp": int(row.unix_timestamp),
                "club": 0,
                "player": 0
            }
        payouts_by_date[date_str][row.other_type] = int(row.daily_amount or 0)  # Keep as smallest units
    
    # Build the response for each of the last 30 days
    data_points = []
    for i in range(29, -1, -1):  # 30 days including today
        target_date = current_date - timedelta(days=i)
        date_str = target_date.strftime("%Y-%m-%d")
        
        if date_str in payouts_by_date:
            data = payouts_by_date[date_str]
            data_point = {
                "date": date_str,
                "timestamp": data["timestamp"],
                "total_payouts": data["club"] + data["player"],
                "club_payouts": data["club"],
                "player_payouts": data["player"]
            }
        else:
            # No data for this day
            data_point = {
                "date": date_str,
                "timestamp": int(target_date.replace(hour=0, minute=0, second=0).timestamp()),
                "total_payouts": 0,
                "club_payouts": 0,
                "player_payouts": 0
            }
        
        data_points.append(data_point)
    
    # Cache the result
    cache_data = {
        "data": data_points,
        "last_updated": int(time.time())
    }
    await r.set(cache_key, json.dumps(cache_data), ex=86400)  # 24 hour expiry
    logger.info("Payouts data cached successfully")
    
    return data_points


@trading_graph_router.get(
    "/trading_graph/all",
    response_model=CombinedPayoutsResponse,
    summary="Retrieve daily and monthly payouts data",
    description="Shows daily payouts (last 30 days) and monthly aggregated data including payouts, cash injections, player game shares, and circulation. Cached with intelligent TTL."
)
async def get_payouts_graph(
    background_tasks: BackgroundTasks,
    archival_session: AsyncSession = Depends(get_archival_session),
    r = Depends(get_redis_client),
):
    daily_cache_key = "payouts_data_v1"
    combined_cache_key = "combined_payouts_v5"  # v5 for cumulative circulation fix
    
    # Try to get combined cache first
    cached_combined = await r.get(combined_cache_key)
    if cached_combined:
        data_json = json.loads(cached_combined)
        last_updated = data_json.get("last_updated", 0)
        
        # If it's fresh (< 600 seconds / 10 minutes), return it immediately
        if (time.time() - last_updated) < 600:
            logger.info("Serving fresh combined payouts data from cache")
            return CombinedPayoutsResponse(**data_json["data"])
        else:
            # Always serve cached data and refresh in background
            logger.info("Cache >10min old; serving cached data and refreshing in background")
            background_tasks.add_task(
                _refresh_combined_cache, r, archival_session,
                daily_cache_key, combined_cache_key
            )
            return CombinedPayoutsResponse(**data_json["data"])
    
    # No cache, fetch everything
    logger.info("No combined cache found; fetching fresh data")
    
    # Get daily data
    daily_data = await _fetch_and_cache_payouts_data(r, archival_session, daily_cache_key)
    
    # Get monthly data  
    monthly_data = await _fetch_and_cache_monthly_data(r, archival_session)
    
    # Combine and cache
    combined_data = {
        "daily": daily_data,
        "monthly": monthly_data
    }
    
    cache_data = {
        "data": combined_data,
        "last_updated": int(time.time())
    }
    await r.set(combined_cache_key, json.dumps(cache_data), ex=86400)  # 24 hour expiry
    
    return CombinedPayoutsResponse(**combined_data)


async def _refresh_combined_cache(
    r, archival_session: AsyncSession,
    daily_cache_key: str, combined_cache_key: str
):
    """Background task to refresh the combined cache."""
    try:
        # Get daily data
        daily_data = await _fetch_and_cache_payouts_data(r, archival_session, daily_cache_key)
        
        # Get monthly data
        monthly_data = await _fetch_and_cache_monthly_data(r, archival_session)
        
        # Combine and cache
        combined_data = {
            "daily": daily_data,
            "monthly": monthly_data
        }
        
        cache_data = {
            "data": combined_data,
            "last_updated": int(time.time())
        }
        await r.set(combined_cache_key, json.dumps(cache_data), ex=86400)
        logger.info("Combined payouts cache refreshed successfully")
    except Exception as e:
        logger.error(f"Error refreshing combined cache: {e}")
