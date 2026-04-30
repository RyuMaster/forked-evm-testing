# rich_list.py

import json
import asyncio
import logging
from sqlalchemy import Column, Integer, BigInteger, String, DateTime, select, func, desc, case
from typing import List, Optional, Dict, Any
from pydantic import BaseModel
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime

from .base import (
    Base,
    get_mysql_session,
    get_userconfig_session,
    get_redis_client,
    PaginatedResponse,
    PerPageOptions,
    DEFAULT_PROFILE_PIC_URL,
    mysql_session_maker,
    userconfig_session_maker,
)

logger = logging.getLogger(__name__)

# Cache TTL: 1 hour (rich list doesn't change frequently)
RICH_LIST_CACHE_TTL = 3600
RICH_LIST_CACHE_KEY = "rich_list:full_data"
RICH_LIST_BUILDING_KEY = "rich_list:building"  # Lock to prevent concurrent builds

from modules.utils.profile import get_profiles_for_users
from enum import Enum
from .share_balances import DCShareBalances
from .clubs import DCClubsTrading
from .players import DCPlayersTrading
from .users import DCUsers

# Pydantic model for the API response
class RichListResponse(BaseModel):
    name: str
    balance: int
    club_asset_value: int
    player_asset_value: int
    total_networth: int
    rank: int
    profile_pic: Optional[str] = None
    last_active_unix: Optional[int] = None
    last_active: Optional[datetime] = None

    class Config:
        from_attributes = True  # Updated from orm_mode = True

# Create an APIRouter instance for rich_list
rich_list_router = APIRouter()

class RichListSortBy(str, Enum):
    balance = "balance"
    total_networth = "total_networth"
    last_active_unix = "last_active_unix"


async def build_rich_list_cache(
    session: AsyncSession,
    userconfig_session: AsyncSession,
    redis
) -> List[Dict[str, Any]]:
    """
    Build the full rich list data and cache it.
    This is the slow operation that computes all networth values.
    """
    # Subquery to calculate club asset values
    club_assets = (
        select(
            DCShareBalances.name,
            func.sum(DCShareBalances.num * func.coalesce(DCClubsTrading.last_price, 0)).label('club_asset_value')
        )
        .join(
            DCClubsTrading,
            (DCShareBalances.share_id == DCClubsTrading.club_id) &
            (DCShareBalances.share_type == 'club')
        )
        .group_by(DCShareBalances.name)
        .subquery()
    )

    # Subquery to calculate player asset values
    player_assets = (
        select(
            DCShareBalances.name,
            func.sum(DCShareBalances.num * func.coalesce(DCPlayersTrading.last_price, 0)).label('player_asset_value')
        )
        .join(
            DCPlayersTrading,
            (DCShareBalances.share_id == DCPlayersTrading.player_id) &
            (DCShareBalances.share_type == 'player')
        )
        .group_by(DCShareBalances.name)
        .subquery()
    )

    # Main query to calculate total_networth
    main_query = (
        select(
            DCUsers.name,
            DCUsers.balance,
            DCUsers.last_active.label('last_active_unix'),
            func.coalesce(club_assets.c.club_asset_value, 0).label('club_asset_value'),
            func.coalesce(player_assets.c.player_asset_value, 0).label('player_asset_value'),
            (
                DCUsers.balance +
                func.coalesce(club_assets.c.club_asset_value, 0) +
                func.coalesce(player_assets.c.player_asset_value, 0)
            ).label('total_networth')
        )
        .outerjoin(club_assets, DCUsers.name == club_assets.c.name)
        .outerjoin(player_assets, DCUsers.name == player_assets.c.name)
        .where(DCUsers.name != "Reserved")
        .order_by(
            (
                DCUsers.balance +
                func.coalesce(club_assets.c.club_asset_value, 0) +
                func.coalesce(player_assets.c.player_asset_value, 0)
            ).desc()
        )
    )

    # Execute query
    result = await session.execute(main_query)
    rows = result.fetchall()

    # Batch fetch profile pics for all users
    names_needed = [row.name for row in rows if row.name]
    name_to_pic = await get_profiles_for_users(names_needed, userconfig_session)

    # Build full data list with ranks (sorted by total_networth desc)
    full_data = []
    current_rank = 0
    last_networth = None
    for i, row in enumerate(rows):
        # Dense rank: same networth = same rank
        if row.total_networth != last_networth:
            current_rank = i + 1
            last_networth = row.total_networth

        full_data.append({
            "name": row.name,
            "balance": int(row.balance) if row.balance else 0,
            "club_asset_value": int(row.club_asset_value) if row.club_asset_value else 0,
            "player_asset_value": int(row.player_asset_value) if row.player_asset_value else 0,
            "total_networth": int(row.total_networth) if row.total_networth else 0,
            "rank": current_rank,
            "profile_pic": name_to_pic.get(row.name, DEFAULT_PROFILE_PIC_URL),
            "last_active_unix": int(row.last_active_unix) if row.last_active_unix else None,
        })

    # Cache the full data
    await redis.set(RICH_LIST_CACHE_KEY, json.dumps(full_data), ex=RICH_LIST_CACHE_TTL)
    # Clear building lock
    await redis.delete(RICH_LIST_BUILDING_KEY)

    logger.info(f"Rich list cache built with {len(full_data)} users")
    return full_data


async def get_full_rich_list_data(
    session: AsyncSession,
    userconfig_session: AsyncSession,
    redis
) -> Optional[List[Dict[str, Any]]]:
    """
    Get full rich list data from cache, or trigger background build if not available.
    Returns None if cache is empty and build is in progress.
    """
    # Check cache first
    cached_data = await redis.get(RICH_LIST_CACHE_KEY)
    if cached_data:
        return json.loads(cached_data)

    # Atomic lock: SET NX returns True only if key didn't exist
    # This prevents multiple workers from building simultaneously
    acquired = await redis.set(RICH_LIST_BUILDING_KEY, "1", ex=300, nx=True)
    if not acquired:
        return None  # Cache miss but build in progress by another worker

    # Build in foreground for first request (background would return empty)
    return await build_rich_list_cache(session, userconfig_session, redis)


async def refresh_rich_list_cache_background():
    """
    Background task to refresh the rich list cache.
    Called on startup and can be triggered manually.
    """
    redis = get_redis_client()

    # Atomic lock: SET NX returns True only if key didn't exist
    # This prevents race condition when multiple workers start simultaneously
    acquired = await redis.set(RICH_LIST_BUILDING_KEY, "1", ex=300, nx=True)
    if not acquired:
        logger.info("Rich list cache build already in progress, skipping")
        return

    try:
        async with mysql_session_maker() as session:
            async with userconfig_session_maker() as userconfig_session:
                await build_rich_list_cache(session, userconfig_session, redis)
    except Exception as e:
        logger.error(f"Error building rich list cache: {e}")
        await redis.delete(RICH_LIST_BUILDING_KEY)


@rich_list_router.get(
    "/rich_list",
    response_model=PaginatedResponse[RichListResponse],
    summary="Retrieve the richest users",
    description="Generates a ranked list of users by net worth (balance + owned club/player assets). Includes user's last active timestamp. Supports pagination, filtering by name, and sorting."
)
async def get_rich_list(
    page: int = Query(1, ge=1, description="Page number, starting from 1"),
    per_page: PerPageOptions = Query(
        PerPageOptions.twenty,
        description="Number of records per page (options: 5, 10, 20, 50, 100)",
    ),
    name: Optional[str] = Query(None, description="Filter by specific user name (case sensitive)"),
    sort_by: RichListSortBy = Query(
        RichListSortBy.total_networth,
        description="Field to sort by ('balance', 'total_networth', or 'last_active_unix')"
    ),
    sort_order: Optional[str] = Query(
        "desc", description="Sort order: 'asc' or 'desc'", regex="^(asc|desc)$"
    ),
    session: AsyncSession = Depends(get_mysql_session),
    userconfig_session: AsyncSession = Depends(get_userconfig_session),
):
    per_page_val = per_page.value
    redis = get_redis_client()

    # Get full cached data (or compute and cache it)
    full_data = await get_full_rich_list_data(session, userconfig_session, redis)

    # If cache is being built, return a message
    if full_data is None:
        raise HTTPException(
            status_code=503,
            detail="Rich list is being computed. Please try again in a few seconds."
        )

    # Filter by name if provided
    if name:
        name_len = len(name)
        filtered_data = [d for d in full_data if d["name"] == name and len(d["name"]) == name_len]
    else:
        filtered_data = full_data

    # Sort if different from default (total_networth desc)
    chosen_sort = sort_by.value
    reverse = (sort_order == "desc")

    if chosen_sort != "total_networth" or sort_order != "desc":
        # Re-sort the data
        filtered_data = sorted(
            filtered_data,
            key=lambda x: (x[chosen_sort] is None, x[chosen_sort] or 0),
            reverse=reverse
        )

    # Calculate pagination
    total = len(filtered_data)
    total_pages = (total + per_page_val - 1) // per_page_val if total else 0

    # Apply pagination
    start_idx = (page - 1) * per_page_val
    end_idx = start_idx + per_page_val
    page_data = filtered_data[start_idx:end_idx]

    # Fetch fresh last_active data for users on this page
    page_names = [d["name"] for d in page_data]
    if page_names:
        fresh_activity_query = select(DCUsers.name, DCUsers.last_active).where(DCUsers.name.in_(page_names))
        fresh_result = await session.execute(fresh_activity_query)
        fresh_activity = {row.name: row.last_active for row in fresh_result.fetchall()}
    else:
        fresh_activity = {}

    # Build response items
    items = []
    for d in page_data:
        # Use fresh last_active data
        last_active_unix = fresh_activity.get(d["name"], d["last_active_unix"])
        last_active = datetime.utcfromtimestamp(last_active_unix) if last_active_unix else None

        items.append(
            RichListResponse(
                name=d["name"],
                balance=d["balance"],
                club_asset_value=d["club_asset_value"],
                player_asset_value=d["player_asset_value"],
                total_networth=d["total_networth"],
                rank=d["rank"],
                profile_pic=d["profile_pic"],
                last_active_unix=last_active_unix,
                last_active=last_active,
            )
        )

    return PaginatedResponse(
        page=page,
        per_page=per_page_val,
        total=total,
        total_pages=total_pages,
        items=items,
    )
