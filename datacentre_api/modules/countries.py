# modules/countries.py

from sqlalchemy import (
    select,
    func,
    or_,
)
from sqlalchemy.orm import aliased
from typing import List, Optional
from pydantic import BaseModel
from fastapi import APIRouter, Depends, Query, HTTPException, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession
import json
import time
from enum import Enum
# Instead of importing 'redis', we import only 'get_redis_client'.
# Also import get_mysql_session from base so we can get a DB session.
from .base import (
    get_mysql_session,
    get_redis_client,  # <--- the new recommended approach
    DCClubInfo,
    DCPlayers,
    DCClubs,
    DCUsers,
    DCLeagues,
)
# Import DCPlayersTrading from players.py
from .players import DCPlayersTrading

# Import DCClubsTrading from clubs.py
from .clubs import DCClubsTrading

# CountryResponse Pydantic model
class CountryResponse(BaseModel):
    country_id: str
    total_clubs: int = 0
    total_players: int = 0
    total_volume_1_day: int = 0
    total_volume_7_day: int = 0
    last_7days: Optional[List[int]] = None
    market_cap: int = 0
    club_market_cap: int = 0
    player_market_cap: int = 0
    total_available_jobs: int = 0
    total_leagues: int = 0  # Add total_leagues field

    class Config:
        from_attributes = True

class CountriesSortBy(str, Enum):
    country_id = "country_id"
    total_clubs = "total_clubs"
    total_players = "total_players"
    total_volume_1_day = "total_volume_1_day"
    total_volume_7_day = "total_volume_7_day"
    market_cap = "market_cap"
    club_market_cap = "club_market_cap"
    player_market_cap = "player_market_cap"
    total_available_jobs = "total_available_jobs"
    total_leagues = "total_leagues"

# Create an APIRouter instance for countries
countries_router = APIRouter()

async def _load_countries_data(session: AsyncSession) -> List[CountryResponse]:
    """
    Core function that queries the DB for all countries data,
    returns a list of CountryResponse. No caching here—just DB logic.
    """
    # Alias models
    dc_club_info_alias = aliased(DCClubInfo)
    dc_clubs_trading_alias = aliased(DCClubsTrading)
    dc_players_alias = aliased(DCPlayers)
    dc_players_trading_alias = aliased(DCPlayersTrading)
    dc_clubs_alias = aliased(DCClubs)
    dc_users_alias = aliased(DCUsers)
    dc_leagues_alias = aliased(DCLeagues)

    # Get the latest season_id where comp_type = 0
    latest_season_query = select(func.max(DCLeagues.season_id)).where(DCLeagues.comp_type == 0)
    latest_season_result = await session.execute(latest_season_query)
    latest_season_id = latest_season_result.scalar_one()

    # Subquery for total_clubs per country
    total_clubs_subquery = (
        select(
            dc_clubs_alias.country_id.label('country_id'),
            func.count(dc_clubs_alias.club_id).label('total_clubs')
        )
        .select_from(dc_clubs_alias)
        .group_by(dc_clubs_alias.country_id)
        .subquery('total_clubs_subquery')
    )

    # Subquery for total_players per country
    total_players_subquery = (
        select(
            dc_clubs_alias.country_id.label('country_id'),
            func.count(dc_players_alias.player_id).label('total_players')
        )
        .select_from(dc_players_alias)
        .join(dc_clubs_alias, dc_players_alias.club_id == dc_clubs_alias.club_id)
        .group_by(dc_clubs_alias.country_id)
        .subquery('total_players_subquery')
    )

    # Subquery for total_available_jobs per country
    available_subquery = (
        select(
            dc_clubs_alias.country_id.label('country_id'),
            func.sum(dc_club_info_alias.available).label('total_available_jobs'),
        )
        .select_from(dc_club_info_alias)
        .join(dc_clubs_alias, dc_club_info_alias.club_id == dc_clubs_alias.club_id)
        .group_by(dc_clubs_alias.country_id)
        .subquery('available_subquery')
    )

    # Subquery for clubs trading data per country
    clubs_trading_subquery = (
        select(
            dc_clubs_alias.country_id.label('country_id'),
            func.sum(dc_clubs_trading_alias.volume_1_day).label('volume_clubs_1_day'),
            func.sum(dc_clubs_trading_alias.volume_7_day).label('volume_clubs_7_day'),
            func.sum(dc_clubs_trading_alias.last_price * 1_000_000).label('club_market_cap'),
        )
        .select_from(dc_clubs_trading_alias)
        .join(dc_clubs_alias, dc_clubs_trading_alias.club_id == dc_clubs_alias.club_id)
        .group_by(dc_clubs_alias.country_id)
        .subquery('clubs_trading_subquery')
    )

    # Subquery for players trading data per country
    players_trading_subquery = (
        select(
            dc_clubs_alias.country_id.label('country_id'),
            func.sum(dc_players_trading_alias.volume_1_day).label('volume_players_1_day'),
            func.sum(dc_players_trading_alias.volume_7_day).label('volume_players_7_day'),
            func.sum(dc_players_trading_alias.last_price * 1_000_000).label('player_market_cap'),
        )
        .select_from(dc_players_trading_alias)
        .join(dc_players_alias, dc_players_trading_alias.player_id == dc_players_alias.player_id)
        .join(dc_clubs_alias, dc_players_alias.club_id == dc_clubs_alias.club_id)
        .group_by(dc_clubs_alias.country_id)
        .subquery('players_trading_subquery')
    )

    # Subquery for total_leagues per country (filter by latest season)
    total_leagues_subquery = (
        select(
            dc_leagues_alias.country_id.label('country_id'),
            func.count(dc_leagues_alias.league_id).label('total_leagues')
        )
        .select_from(dc_leagues_alias)
        .where(
            dc_leagues_alias.comp_type == 0,  # Only league competitions
            dc_leagues_alias.season_id == latest_season_id  # Latest season
        )
        .group_by(dc_leagues_alias.country_id)
        .subquery('total_leagues_subquery')
    )

    # Computed columns
    total_volume_1_day = (
        func.coalesce(clubs_trading_subquery.c.volume_clubs_1_day, 0) +
        func.coalesce(players_trading_subquery.c.volume_players_1_day, 0)
    ).label('total_volume_1_day')

    total_volume_7_day = (
        func.coalesce(clubs_trading_subquery.c.volume_clubs_7_day, 0) +
        func.coalesce(players_trading_subquery.c.volume_players_7_day, 0)
    ).label('total_volume_7_day')

    market_cap = (
        func.coalesce(clubs_trading_subquery.c.club_market_cap, 0) +
        func.coalesce(players_trading_subquery.c.player_market_cap, 0)
    ).label('market_cap')

    # Subquery to get all country_ids from dc_clubs table
    country_ids_subquery = (
        select(dc_clubs_alias.country_id.label('country_id'))
        .distinct()
        .subquery('country_ids_subquery')
    )

    # Build main query
    main_query = (
        select(
            country_ids_subquery.c.country_id,
            func.coalesce(total_clubs_subquery.c.total_clubs, 0).label('total_clubs'),
            func.coalesce(total_players_subquery.c.total_players, 0).label('total_players'),
            func.coalesce(available_subquery.c.total_available_jobs, 0).label('total_available_jobs'),
            func.coalesce(clubs_trading_subquery.c.volume_clubs_1_day, 0).label('volume_clubs_1_day'),
            func.coalesce(clubs_trading_subquery.c.volume_clubs_7_day, 0).label('volume_clubs_7_day'),
            func.coalesce(clubs_trading_subquery.c.club_market_cap, 0).label('club_market_cap'),
            func.coalesce(players_trading_subquery.c.volume_players_1_day, 0).label('volume_players_1_day'),
            func.coalesce(players_trading_subquery.c.volume_players_7_day, 0).label('volume_players_7_day'),
            func.coalesce(players_trading_subquery.c.player_market_cap, 0).label('player_market_cap'),
            total_volume_1_day,
            total_volume_7_day,
            market_cap,
            func.coalesce(total_leagues_subquery.c.total_leagues, 0).label('total_leagues'),
        )
        .select_from(country_ids_subquery)
        .outerjoin(total_clubs_subquery, country_ids_subquery.c.country_id == total_clubs_subquery.c.country_id)
        .outerjoin(total_players_subquery, country_ids_subquery.c.country_id == total_players_subquery.c.country_id)
        .outerjoin(available_subquery, country_ids_subquery.c.country_id == available_subquery.c.country_id)
        .outerjoin(clubs_trading_subquery, country_ids_subquery.c.country_id == clubs_trading_subquery.c.country_id)
        .outerjoin(players_trading_subquery, country_ids_subquery.c.country_id == players_trading_subquery.c.country_id)
        .outerjoin(total_leagues_subquery, country_ids_subquery.c.country_id == total_leagues_subquery.c.country_id)
    )

    result = await session.execute(main_query)
    rows = result.fetchall()

    items: List[CountryResponse] = []
    for row in rows:
        row_dict = dict(row._mapping)
        country_id = row_dict.get('country_id')

        # For last_7days, gather from clubs + players
        clubs_last7days_query = (
            select(dc_clubs_trading_alias.last_7days)
            .select_from(dc_clubs_alias)
            .join(dc_clubs_trading_alias, dc_clubs_alias.club_id == dc_clubs_trading_alias.club_id)
            .where(dc_clubs_alias.country_id == country_id)
        )
        clubs_last7days_result = await session.execute(clubs_last7days_query)
        clubs_last7days_rows = clubs_last7days_result.fetchall()

        players_last7days_query = (
            select(dc_players_trading_alias.last_7days)
            .select_from(dc_players_alias)
            .join(dc_players_trading_alias, dc_players_alias.player_id == dc_players_trading_alias.player_id)
            .join(dc_clubs_alias, dc_players_alias.club_id == dc_clubs_alias.club_id)
            .where(dc_clubs_alias.country_id == country_id)
        )
        players_last7days_result = await session.execute(players_last7days_query)
        players_last7days_rows = players_last7days_result.fetchall()

        last_7days_array = [0] * 7

        for club_row in clubs_last7days_rows:
            last_7days_str = club_row.last_7days
            if last_7days_str:
                try:
                    last_7days_list = json.loads(last_7days_str)
                    last_7days_list = [int(value or 0) for value in last_7days_list]
                    last_7days_array = [sum(x) for x in zip(last_7days_array, last_7days_list)]
                except json.JSONDecodeError:
                    continue

        for player_row in players_last7days_rows:
            last_7days_str = player_row.last_7days
            if last_7days_str:
                try:
                    last_7days_list = json.loads(last_7days_str)
                    last_7days_list = [int(value or 0) for value in last_7days_list]
                    last_7days_array = [sum(x) for x in zip(last_7days_array, last_7days_list)]
                except json.JSONDecodeError:
                    continue

        country_response = CountryResponse(
            country_id=country_id,
            total_clubs=int(row_dict.get('total_clubs') or 0),
            total_players=int(row_dict.get('total_players') or 0),
            total_volume_1_day=int(row_dict.get('total_volume_1_day') or 0),
            total_volume_7_day=int(row_dict.get('total_volume_7_day') or 0),
            last_7days=last_7days_array,
            market_cap=int(row_dict.get('market_cap') or 0),
            club_market_cap=int(row_dict.get('club_market_cap') or 0),
            player_market_cap=int(row_dict.get('player_market_cap') or 0),
            total_available_jobs=int(row_dict.get('total_available_jobs') or 0),
            total_leagues=int(row_dict.get('total_leagues') or 0),
        )
        items.append(country_response)

    return items

async def _fetch_and_cache_countries(
    r,  # The already-initialized Redis client
    session,  # We accept this to match the background task signature, but won't use it
    cache_key: str,
) -> List[CountryResponse]:
    """
    Re-fetches data from the DB, builds the list of CountryResponse,
    and updates Redis with a fresh timestamp. Returns the new data.
    Only called when Redis is available (r is not None).
    """
    from .base import mysql_session_maker  # Import the sessionmaker here
    async with mysql_session_maker() as local_session:
        data = await _load_countries_data(local_session)
        payload = {
            "data": [item.dict() for item in data],
            "last_updated": time.time(),
        }
        # Store as JSON in Redis - r should not be None when this function is called
        if r is not None:
            try:
                await r.set(cache_key, json.dumps(payload))
            except Exception:
                pass  # Ignore Redis errors
        return data



@countries_router.get(
    "/countries",
    response_model=List[CountryResponse],
    summary="Retrieve data for countries",
    description="Fetch an aggregated list of countries, each with metrics such as total clubs, total players, trading volumes, and market caps. A background task refreshes the data every five minutes. Returns a list of CountryResponse objects."
)
async def get_countries(
    sort_by: Optional[CountriesSortBy] = Query(
        None,
        description="Field to sort by",
    ),
    sort_order: Optional[str] = Query(
        "desc",
        description="Sort order: 'asc' or 'desc'",
        regex="^(asc|desc)$",
    ),
    country_id: Optional[str] = Query(None, description="Filter by country ID"),
    background_tasks: BackgroundTasks = BackgroundTasks(),
    session: AsyncSession = Depends(get_mysql_session),
    r = Depends(get_redis_client),
):
    cache_key = "countries_data_v1"
    all_data = None
    
    # Try to use cache if Redis is available
    if r is not None:
        try:
            cached_value = await r.get(cache_key)
            if cached_value:
                data_json = json.loads(cached_value)
                last_updated = data_json.get("last_updated", 0)
                if (time.time() - last_updated) < 300:
                    all_data = [CountryResponse(**x) for x in data_json["data"]]
                else:
                    background_tasks.add_task(_fetch_and_cache_countries, r, session, cache_key)
                    all_data = [CountryResponse(**x) for x in data_json["data"]]
            else:
                all_data = await _fetch_and_cache_countries(r, session, cache_key)
        except Exception:
            # Redis is configured but has intermittent issues - return empty data
            # to avoid expensive DB queries during Redis outages
            all_data = []

    # Fall back to direct database query if Redis not available (r is None)
    if all_data is None:
        all_data = await _load_countries_data(session)

    if country_id:
        all_data = [c for c in all_data if c.country_id == country_id]

    sortable_map = {
        "country_id": lambda x: x.country_id,
        "total_clubs": lambda x: x.total_clubs,
        "total_players": lambda x: x.total_players,
        "total_volume_1_day": lambda x: x.total_volume_1_day,
        "total_volume_7_day": lambda x: x.total_volume_7_day,
        "market_cap": lambda x: x.market_cap,
        "club_market_cap": lambda x: x.club_market_cap,
        "player_market_cap": lambda x: x.player_market_cap,
        "total_available_jobs": lambda x: x.total_available_jobs,
        "total_leagues": lambda x: x.total_leagues,
    }

    chosen_sort = sort_by.value if sort_by else "total_volume_7_day"  # fallback default

    if chosen_sort not in sortable_map:
        raise HTTPException(status_code=400, detail=f"Invalid sort_by field: {chosen_sort}")

    reverse_sort = (sort_order == "desc")
    all_data = sorted(all_data, key=sortable_map[chosen_sort], reverse=reverse_sort)

    return all_data