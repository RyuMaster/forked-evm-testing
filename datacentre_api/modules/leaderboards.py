from fastapi import APIRouter, HTTPException, Depends, Query
from pydantic import BaseModel
from typing import Optional, List, Callable, Awaitable
import httpx
import json
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, case
import time
import asyncio

from modules.base import get_redis_client, get_userconfig_session, get_mysql_session, DCUsers, DEFAULT_PROFILE_PIC_URL, DCEarnings, GRAPH_SUBGRAPH_SV_URL
from modules.utils.profile import get_profiles_for_users
from modules.services.svc_price_service import get_svc_price

# Create a new router with a prefix (optional)
leaderboards_router = APIRouter(prefix="/leaderboards")

# Constants for the GraphQL endpoint and pagination
GRAPHQL_URL = GRAPH_SUBGRAPH_SV_URL
PAGE_SIZE = 1000
CACHE_TTL = 300  # 5 minutes
EARNINGS_CACHE_TTL = 1800  # 30 minutes for earnings data

# Response models
class LeaderboardEntry(BaseModel):
    account: str
    totalBonusShares: int
    profile_pic: Optional[str] = None
    last_active_unix: Optional[int] = None
    last_active: Optional[datetime] = None

class LeaderboardResponse(BaseModel):
    entries: list[LeaderboardEntry]

class TopEarnerEntry(BaseModel):
    name: str
    total_earnings: int
    club_earnings: int
    player_earnings: int
    total_earnings_usdc: float
    profile_pic: str
    last_active_unix: Optional[int] = None
    last_active: Optional[datetime] = None

class TopEarnersResponse(BaseModel):
    days: int
    svc2usdc: float
    entries: List[TopEarnerEntry]

async def fetch_leaderboard_entries(query: str) -> dict:
    """
    Fetches all entries from the GraphQL endpoint using pagination,
    aggregates bonusShares per account, and returns a dictionary mapping account -> totalBonusShares.
    """
    aggregated = {}
    skip = 0

    while True:
        variables = {"first": PAGE_SIZE, "skip": skip}
        async with httpx.AsyncClient() as client:
            response = await client.post(GRAPHQL_URL, json={"query": query, "variables": variables})
        if response.status_code != 200:
            raise HTTPException(status_code=500, detail=f"GraphQL query failed with status {response.status_code}")
        data = response.json()
        if "errors" in data:
            raise HTTPException(status_code=500, detail=f"GraphQL errors: {data['errors']}")
        items = data.get("data", {}).get("referrerBonuses", [])
        if not items:
            break

        for item in items:
            referrer = item.get("referrer")
            if not referrer or not referrer.get("account"):
                continue
            account = referrer.get("account")
            bonusShares = item.get("bonusShares")
            try:
                bonusShares = int(bonusShares)
            except (ValueError, TypeError):
                bonusShares = 0
            aggregated[account] = aggregated.get(account, 0) + bonusShares

        if len(items) < PAGE_SIZE:
            break
        skip += PAGE_SIZE

    return aggregated
   
async def _enrich_referral_entries(
    entries: dict[str, int],
    userconfig_session: AsyncSession,
    mysql_session: AsyncSession,
) -> List[LeaderboardEntry]:
    """
    Given aggregated entries mapping account -> totalBonusShares,
    sort, take top 20, and enrich each with profile_pic and last_active.
    """
    # Sort by bonusShares descending and take top 20
    sorted_entries = sorted(entries.items(), key=lambda x: x[1], reverse=True)[:20]
    names = [acct for acct, _ in sorted_entries]
    # Batch fetch profile pics
    name_to_pic = await get_profiles_for_users(names, userconfig_session)
    # Fetch last_active per user by primary key lookup
    name_to_last_active: dict[str, Optional[int]] = {}
    for acct in names:
        user = await mysql_session.get(DCUsers, acct)
        name_to_last_active[acct] = user.last_active if user and user.last_active is not None else None
    # Build entries
    items: List[LeaderboardEntry] = []
    for acct, total in sorted_entries:
        pic_url = name_to_pic.get(acct, DEFAULT_PROFILE_PIC_URL)
        last_active_unix = name_to_last_active.get(acct)
        last_active = datetime.utcfromtimestamp(last_active_unix) if last_active_unix else None
        items.append(
            LeaderboardEntry(
                account=acct,
                totalBonusShares=total,
                profile_pic=pic_url,
                last_active_unix=last_active_unix,
                last_active=last_active,
            )
        )
    return items

@leaderboards_router.get(
    "/referrals/time-range",
    response_model=LeaderboardResponse,
    summary="Get top 50 referrers within a specific time range",
    description=(
        "Fetches referral bonus data for the referrals cup (September 8, 2025 00:00 UTC to "
        "October 5, 2025 23:59 UTC), aggregates bonusShares per referrer, and returns the top 50 referrers."
    )
)
async def get_referrals_leaderboard_time_range(
    userconfig_session: AsyncSession = Depends(get_userconfig_session),
    mysql_session: AsyncSession = Depends(get_mysql_session),
):
    redis = get_redis_client()
    cache_key = "leaderboard:referrals:time_range"
    cached_data = redis and await redis.get(cache_key)
    if cached_data:
        # Return cached response
        return LeaderboardResponse.parse_raw(cached_data)

    query = """
    query getReferrerBonuses($first: Int!, $skip: Int!) {
      referrerBonuses(
        first: $first,
        skip: $skip,
        where: {
          timestamp_gte: 1757289600,
          timestamp_lt: 1759708800
        }
      ) {
        referrer {
          account
        }
        bonusShares
      }
    }
    """
    entries = await fetch_leaderboard_entries(query)
    # Sort and take top 50
    sorted_entries = sorted(entries.items(), key=lambda x: x[1], reverse=True)[:50]
    names = [acct for acct, _ in sorted_entries]
    # Batch fetch profile pics
    name_to_pic = await get_profiles_for_users(names, userconfig_session)
    # Batch fetch last_active_unix from DCUsers per user
    name_to_last_active: dict[str, Optional[int]] = {}
    for acct in names:
        user = await mysql_session.get(DCUsers, acct)
        name_to_last_active[acct] = user.last_active if user and user.last_active is not None else None
    items: List[LeaderboardEntry] = []
    for acct, total in sorted_entries:
        pic_url = name_to_pic.get(acct, DEFAULT_PROFILE_PIC_URL)
        last_active_unix = name_to_last_active.get(acct)
        last_active = datetime.utcfromtimestamp(last_active_unix) if last_active_unix else None
        items.append(
            LeaderboardEntry(
                account=acct,
                totalBonusShares=total,
                profile_pic=pic_url,
                last_active_unix=last_active_unix,
                last_active=last_active,
            )
        )
    response_obj = LeaderboardResponse(entries=items)
    # Cache the result in Redis for 5 minutes
    if redis:
        await redis.set(cache_key, response_obj.json(), ex=CACHE_TTL)
    return response_obj

@leaderboards_router.get(
    "/referrals/all",
    response_model=LeaderboardResponse,
    summary="Get top 20 referrers for all time",
    description=(
        "Fetches all referral bonus data (without any time range filter), aggregates bonusShares per referrer, "
        "and returns the top 20 referrers."
    )
)
async def get_referrals_leaderboard_all(
    userconfig_session: AsyncSession = Depends(get_userconfig_session),
    mysql_session: AsyncSession = Depends(get_mysql_session),
):
    redis = get_redis_client()
    cache_key = "leaderboard:referrals:all"
    cached_data = redis and await redis.get(cache_key)
    if cached_data:
        return LeaderboardResponse.parse_raw(cached_data)

    query = """
    query getReferrerBonuses($first: Int!, $skip: Int!) {
      referrerBonuses(
        first: $first,
        skip: $skip
      ) {
        referrer {
          account
        }
        bonusShares
      }
    }
    """
    entries = await fetch_leaderboard_entries(query)
    # Sort and take top 20
    sorted_entries = sorted(entries.items(), key=lambda x: x[1], reverse=True)[:20]
    names = [acct for acct, _ in sorted_entries]
    # Batch fetch profile pics
    name_to_pic = await get_profiles_for_users(names, userconfig_session)
    # Batch fetch last_active_unix from DCUsers per user
    name_to_last_active: dict[str, Optional[int]] = {}
    for acct in names:
        user = await mysql_session.get(DCUsers, acct)
        name_to_last_active[acct] = user.last_active if user and user.last_active is not None else None
    items: List[LeaderboardEntry] = []
    for acct, total in sorted_entries:
        pic_url = name_to_pic.get(acct, DEFAULT_PROFILE_PIC_URL)
        last_active_unix = name_to_last_active.get(acct)
        last_active = datetime.utcfromtimestamp(last_active_unix) if last_active_unix else None
        items.append(
            LeaderboardEntry(
                account=acct,
                totalBonusShares=total,
                profile_pic=pic_url,
                last_active_unix=last_active_unix,
                last_active=last_active,
            )
        )
    response_obj = LeaderboardResponse(entries=items)
    # Cache the result in Redis for 5 minutes
    if redis:
        await redis.set(cache_key, response_obj.json(), ex=CACHE_TTL)
    return response_obj

@leaderboards_router.get(
    "/earnings",
    response_model=TopEarnersResponse,
    summary="Get top 10 earners leaderboard",
    description="""
    Returns the top 10 users by total earnings (club + player dividends)
    for 7 or 30 days. Includes profile images and last active timestamps.
    """
)
async def get_top_earners(
    days: int = Query(30, description="Number of days to look back (7 or 30 only)"),
    mysql_session: AsyncSession = Depends(get_mysql_session),
    userconfig_session: AsyncSession = Depends(get_userconfig_session),
):
    # Validate days parameter
    if days not in [7, 30]:
        raise HTTPException(status_code=400, detail="Days must be either 7 or 30")

    redis = get_redis_client()
    cache_key = f"leaderboard:earnings:{days}d"

    # Try to get cached earnings data (excludes last_active for live updates)
    cached_data = redis and await redis.get(cache_key)

    if cached_data:
        # Parse cached earnings data
        cached = json.loads(cached_data)
        rows_data = cached["rows"]
        rate = cached["rate"]
        profiles = cached["profiles"]
        usernames = [row["name"] for row in rows_data]
    else:
        # OPTIMIZED: Use pre-aggregated dc_earnings table (very fast!)
        earnings_field = DCEarnings.earnings_7d if days == 7 else DCEarnings.earnings_30d

        earnings_query = (
            select(
                DCEarnings.name,
                func.sum(earnings_field).label("total_earnings"),
                func.sum(
                    case((DCEarnings.share_type == "club", earnings_field), else_=0)
                ).label("club_earnings"),
                func.sum(
                    case((DCEarnings.share_type == "player", earnings_field), else_=0)
                ).label("player_earnings")
            )
            .group_by(DCEarnings.name)
            .order_by(func.sum(earnings_field).desc())
            .limit(10)
        )

        # Execute against main MySQL database (dc_earnings is in datacentre db)
        result = await mysql_session.execute(earnings_query)
        rows = result.fetchall()

        if not rows:
            return TopEarnersResponse(
                days=days,
                svc2usdc=0.0,
                entries=[]
            )

        # Get the current SVC to USDC rate
        rate = await get_svc_price(redis_client=redis, mysql_session=mysql_session)

        # Get profile pictures for all users
        usernames = [row.name for row in rows]
        profiles = await get_profiles_for_users(usernames, userconfig_session)

        # Prepare data for caching (without last_active)
        rows_data = [
            {
                "name": row.name,
                "total_earnings": int(row.total_earnings or 0),
                "club_earnings": int(row.club_earnings or 0),
                "player_earnings": int(row.player_earnings or 0)
            }
            for row in rows
        ]

        # Cache earnings data, rate, and profiles for 30 minutes
        cache_payload = {
            "rows": rows_data,
            "rate": rate,
            "profiles": profiles
        }
        if redis:
            await redis.set(cache_key, json.dumps(cache_payload), ex=EARNINGS_CACHE_TTL)

    # ALWAYS fetch fresh last_active data (live updates)
    users_query = select(DCUsers.name, DCUsers.last_active).where(DCUsers.name.in_(usernames))
    users_result = await mysql_session.execute(users_query)

    # Build mapping, handling both bytes and string names
    name_to_last_active = {}
    for row in users_result:
        # Handle both varchar (returns string) and varbinary (returns bytes)
        if isinstance(row.name, bytes):
            name_str = row.name.decode('utf-8')
        else:
            name_str = row.name
        name_to_last_active[name_str] = row.last_active

    # If we got no results, try with encoded names (for varbinary columns)
    if not name_to_last_active:
        encoded_names = [name.encode('utf-8') if isinstance(name, str) else name for name in usernames]
        users_query = select(DCUsers.name, DCUsers.last_active).where(DCUsers.name.in_(encoded_names))
        users_result = await mysql_session.execute(users_query)

        for row in users_result:
            name_str = row.name.decode('utf-8') if isinstance(row.name, bytes) else row.name
            name_to_last_active[name_str] = row.last_active

    # Build the response combining cached earnings with live last_active
    entries = []
    for row_data in rows_data:
        total_earnings = row_data["total_earnings"]
        club_earnings = row_data["club_earnings"]
        player_earnings = row_data["player_earnings"]
        name = row_data["name"]
        last_active_unix = name_to_last_active.get(name)

        entries.append(TopEarnerEntry(
            name=name,
            total_earnings=total_earnings,
            club_earnings=club_earnings,
            player_earnings=player_earnings,
            total_earnings_usdc=round(total_earnings / 10000 * rate, 2),
            profile_pic=profiles.get(name, DEFAULT_PROFILE_PIC_URL),
            last_active_unix=last_active_unix,
            last_active=datetime.utcfromtimestamp(last_active_unix) if last_active_unix else None
        ))

    return TopEarnersResponse(
        days=days,
        svc2usdc=rate,
        entries=entries
    )

