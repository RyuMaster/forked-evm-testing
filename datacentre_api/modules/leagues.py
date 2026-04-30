# modules/leagues.py
from sqlalchemy import (
    Column,
    Integer,
    BigInteger,
    String,
    select,
    func,
    Text,
    case,
    and_,
    or_,
)
from typing import List, Optional
from pydantic import BaseModel
from fastapi import APIRouter, Depends, Query, HTTPException, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased
from datetime import datetime, timedelta
import json
import time
from enum import Enum

# >>> NEW: Import get_redis_client and also get_userconfig_session and DEFAULT_PROFILE_PIC_URL for manager profiles <<<
from .base import (
    Base,
    get_mysql_session,
    get_redis_client,  # for Redis caching
    get_userconfig_session,  # for fetching profile pics
    PaginatedResponse,
    PerPageOptions,
    DCClubInfo,
    DCPlayers,
    DCClubs,
    DCLeagues,
    DCTableRows,
    DCUsers,
    DEFAULT_PROFILE_PIC_URL,
)

# Import DCPlayersTrading from players.py
from .players import DCPlayersTrading

# Import DCClubsTrading from clubs.py
from .clubs import DCClubsTrading

# Import our profile helper
from modules.utils.profile import get_profiles_for_users

# -------------------------------------------------------------------------
# Pydantic models
# -------------------------------------------------------------------------

class LeagueTableRowResponse(BaseModel):
    club_id: int
    league_id: int
    manager_name: Optional[str] = None 
    # >>> NEW FIELDS <<<
    manager_profile_pic: Optional[str] = None
    manager_last_active_unix: Optional[int] = None
    country_id: str
    division: int
    club_ix: int
    played: int
    won: int
    drawn: int
    lost: int
    goals_for: int
    goals_against: int
    pts: int
    form: str
    old_position: int
    new_position: int
    season_id: int
    # >>> NEW FIELDS <<<
    stadium_size: Optional[int] = None
    fanbase: Optional[int] = None
    balance: Optional[int] = None
    avg_player_rating: Optional[int] = None
    top_3_players: Optional[List[int]] = None

    class Config:
        from_attributes = True

class LeagueResponse(BaseModel):
    league_id: int
    country_id: Optional[str]
    division: Optional[int]
    ticket_cost: Optional[int]
    tv_money: Optional[int]
    prize_money_pot: Optional[int]
    ave_attendance: Optional[int]
    num_teams: Optional[int]
    round: Optional[int]
    num_rounds: Optional[int]
    comp_type: Optional[int]

    total_volume_1_day: int = 0
    total_volume_7_day: int = 0
    last_7days: Optional[List[int]] = None
    volume_clubs_1_day: int = 0
    volume_players_1_day: int = 0
    volume_clubs_7_day: int = 0
    volume_players_7_day: int = 0
    market_cap: int = 0
    club_market_cap: int = 0
    player_market_cap: int = 0
    total_wages: int = 0
    avg_wages: int = 0
    total_player_value: int = 0
    total_clubs: int = 0
    total_players: int = 0
    total_available_jobs: int = 0

    class Config:
        from_attributes = True

# Create an APIRouter instance for leagues
leagues_router = APIRouter()

# -------------------------------------------------------------------------
# NEW: Helper functions to load/cache all leagues data in one go
# -------------------------------------------------------------------------

async def _load_leagues_data(session: AsyncSession, season_id: Optional[int] = None) -> List[LeagueResponse]:
    """
    Fetches *all* leagues data from the DB (no pagination, no sorting).
    Builds a list of LeagueResponse objects, including last_7days info.
    This is analogous to _load_countries_data in countries.py.
    """
    # Get the season_id to use - either provided or latest
    if season_id is None:
        # Get the latest season_id where comp_type = 0
        latest_season_query = select(func.max(DCLeagues.season_id)).where(DCLeagues.comp_type == 0)
        latest_season_result = await session.execute(latest_season_query)
        season_id = latest_season_result.scalar_one()

    # Alias models
    dc_leagues_alias = aliased(DCLeagues)
    dc_club_info_alias = aliased(DCClubInfo)
    dc_clubs_trading_alias = aliased(DCClubsTrading)
    dc_players_alias = aliased(DCPlayers)
    dc_players_trading_alias = aliased(DCPlayersTrading)
    dc_club_info_for_players_alias = aliased(DCClubInfo)
    dc_clubs_alias = aliased(DCClubs)
    # Note: DCUsers not needed here
    dc_users_alias = aliased(DCUsers)

    # Subquery for clubs data per league
    clubs_subquery = (
        select(
            dc_club_info_alias.league_id.label('league_id'),
            func.sum(dc_clubs_trading_alias.volume_1_day).label('volume_clubs_1_day'),
            func.sum(dc_clubs_trading_alias.volume_7_day).label('volume_clubs_7_day'),
            func.sum(dc_clubs_trading_alias.last_price * 1_000_000).label('club_market_cap'),
        )
        .select_from(dc_club_info_alias)
        .join(dc_clubs_trading_alias, dc_club_info_alias.club_id == dc_clubs_trading_alias.club_id)
        .group_by(dc_club_info_alias.league_id)
        .subquery('clubs_subquery')
    )

    # Subquery for players data per league
    players_subquery = (
        select(
            dc_club_info_for_players_alias.league_id.label('league_id'),
            func.sum(dc_players_trading_alias.volume_1_day).label('volume_players_1_day'),
            func.sum(dc_players_trading_alias.volume_7_day).label('volume_players_7_day'),
            func.sum(dc_players_trading_alias.last_price * 1_000_000).label('player_market_cap'),
        )
        .select_from(dc_players_alias)
        .join(dc_players_trading_alias, dc_players_alias.player_id == dc_players_trading_alias.player_id)
        .join(dc_club_info_for_players_alias, dc_players_alias.club_id == dc_club_info_for_players_alias.club_id)
        .group_by(dc_club_info_for_players_alias.league_id)
        .subquery('players_subquery')
    )

    # Subquery for wages and total_player_value per league
    wages_subquery = (
        select(
            dc_club_info_alias.league_id.label('league_id'),
            func.sum(dc_players_alias.wages).label('total_wages'),
            func.sum(dc_players_alias.value).label('total_player_value'),
            func.count(func.distinct(dc_club_info_alias.club_id)).label('total_clubs'),
        )
        .select_from(dc_club_info_alias)
        .join(dc_players_alias, dc_club_info_alias.club_id == dc_players_alias.club_id)
        .group_by(dc_club_info_alias.league_id)
        .subquery('wages_subquery')
    )

    # Subquery for total players per league
    players_count_subquery = (
        select(
            dc_club_info_for_players_alias.league_id.label('league_id'),
            func.count(dc_players_alias.player_id).label('total_players'),
        )
        .select_from(dc_players_alias)
        .join(dc_club_info_for_players_alias, dc_players_alias.club_id == dc_club_info_for_players_alias.club_id)
        .group_by(dc_club_info_for_players_alias.league_id)
        .subquery('players_count_subquery')
    )

    # Subquery for total_available_jobs per league
    available_subquery = (
        select(
            dc_club_info_alias.league_id.label('league_id'),
            func.sum(dc_club_info_alias.available).label('total_available_jobs'),
        )
        .select_from(dc_club_info_alias)
        .group_by(dc_club_info_alias.league_id)
        .subquery('available_subquery')
    )

    # Computed columns
    total_volume_1_day = (
        func.coalesce(clubs_subquery.c.volume_clubs_1_day, 0) +
        func.coalesce(players_subquery.c.volume_players_1_day, 0)
    ).label('total_volume_1_day')

    total_volume_7_day = (
        func.coalesce(clubs_subquery.c.volume_clubs_7_day, 0) +
        func.coalesce(players_subquery.c.volume_players_7_day, 0)
    ).label('total_volume_7_day')

    market_cap = (
        func.coalesce(clubs_subquery.c.club_market_cap, 0) +
        func.coalesce(players_subquery.c.player_market_cap, 0)
    ).label('market_cap')

    avg_wages = case(
        (players_count_subquery.c.total_players != 0,
         wages_subquery.c.total_wages / players_count_subquery.c.total_players),
        else_=0
    ).label('avg_wages')

    # Build main query: fetch ALL leagues for the latest_season_id
    main_query = (
        select(
            dc_leagues_alias.league_id,
            dc_leagues_alias.country_id,
            dc_leagues_alias.level.label('division'),
            dc_leagues_alias.ticket_cost,
            dc_leagues_alias.tv_money,
            dc_leagues_alias.prize_money_pot,
            dc_leagues_alias.ave_attendance,
            dc_leagues_alias.num_teams,
            dc_leagues_alias.round,
            dc_leagues_alias.num_rounds,
            dc_leagues_alias.comp_type,
            func.coalesce(clubs_subquery.c.volume_clubs_1_day, 0).label('volume_clubs_1_day'),
            func.coalesce(clubs_subquery.c.volume_clubs_7_day, 0).label('volume_clubs_7_day'),
            func.coalesce(clubs_subquery.c.club_market_cap, 0).label('club_market_cap'),
            func.coalesce(players_subquery.c.volume_players_1_day, 0).label('volume_players_1_day'),
            func.coalesce(players_subquery.c.volume_players_7_day, 0).label('volume_players_7_day'),
            func.coalesce(players_subquery.c.player_market_cap, 0).label('player_market_cap'),
            total_volume_1_day,
            total_volume_7_day,
            market_cap,
            func.coalesce(wages_subquery.c.total_wages, 0).label('total_wages'),
            avg_wages,
            func.coalesce(wages_subquery.c.total_player_value, 0).label('total_player_value'),
            func.coalesce(wages_subquery.c.total_clubs, 0).label('total_clubs'),
            func.coalesce(players_count_subquery.c.total_players, 0).label('total_players'),
            func.coalesce(available_subquery.c.total_available_jobs, 0).label('total_available_jobs'),
        )
        .select_from(dc_leagues_alias)
        .outerjoin(clubs_subquery, dc_leagues_alias.league_id == clubs_subquery.c.league_id)
        .outerjoin(players_subquery, dc_leagues_alias.league_id == players_subquery.c.league_id)
        .outerjoin(wages_subquery, dc_leagues_alias.league_id == wages_subquery.c.league_id)
        .outerjoin(players_count_subquery, dc_leagues_alias.league_id == players_count_subquery.c.league_id)
        .outerjoin(available_subquery, dc_leagues_alias.league_id == available_subquery.c.league_id)
        .where(
            dc_leagues_alias.season_id == season_id,
            dc_leagues_alias.comp_type == 0,
        )
    )

    result = await session.execute(main_query)
    rows = result.fetchall()

    items: List[LeagueResponse] = []
    for row in rows:
        row_dict = dict(row._mapping)
        league_id = row_dict.get('league_id')

        # Next: gather last_7days from clubs + players
        # clubs_last7days
        clubs_last7days_query = (
            select(
                dc_club_info_alias.league_id,
                dc_clubs_trading_alias.last_7days,
            )
            .select_from(dc_club_info_alias)
            .join(dc_clubs_trading_alias, dc_club_info_alias.club_id == dc_clubs_trading_alias.club_id)
            .where(dc_club_info_alias.league_id == league_id)
        )
        clubs_last7days_result = await session.execute(clubs_last7days_query)
        clubs_last7days_rows = clubs_last7days_result.fetchall()

        # players_last7days
        players_last7days_query = (
            select(
                dc_club_info_for_players_alias.league_id,
                dc_players_trading_alias.last_7days,
            )
            .select_from(dc_players_alias)
            .join(dc_players_trading_alias, dc_players_alias.player_id == dc_players_trading_alias.player_id)
            .join(dc_club_info_for_players_alias, dc_players_alias.club_id == dc_club_info_for_players_alias.club_id)
            .where(dc_club_info_for_players_alias.league_id == league_id)
        )
        players_last7days_result = await session.execute(players_last7days_query)
        players_last7days_rows = players_last7days_result.fetchall()

        # Sum up last_7days data
        last_7days_array = [0] * 7

        for club_row in clubs_last7days_rows:
            last_7days_str = club_row.last_7days
            if last_7days_str:
                try:
                    arr = json.loads(last_7days_str)
                    arr = [int(x or 0) for x in arr]
                    last_7days_array = [sum(x) for x in zip(last_7days_array, arr)]
                except json.JSONDecodeError:
                    continue

        for player_row in players_last7days_rows:
            last_7days_str = player_row.last_7days
            if last_7days_str:
                try:
                    arr = json.loads(last_7days_str)
                    arr = [int(x or 0) for x in arr]
                    last_7days_array = [sum(x) for x in zip(last_7days_array, arr)]
                except json.JSONDecodeError:
                    continue

        items.append(LeagueResponse(
            league_id=league_id,
            country_id=row_dict.get('country_id'),
            division=row_dict.get('division'),
            ticket_cost=row_dict.get('ticket_cost'),
            tv_money=row_dict.get('tv_money'),
            prize_money_pot=row_dict.get('prize_money_pot'),
            ave_attendance=row_dict.get('ave_attendance'),
            num_teams=row_dict.get('num_teams'),
            round=row_dict.get('round'),
            num_rounds=row_dict.get('num_rounds'),
            comp_type=row_dict.get('comp_type'),
            total_volume_1_day=int(row_dict.get('total_volume_1_day') or 0),
            total_volume_7_day=int(row_dict.get('total_volume_7_day') or 0),
            last_7days=last_7days_array,
            volume_clubs_1_day=int(row_dict.get('volume_clubs_1_day') or 0),
            volume_players_1_day=int(row_dict.get('volume_players_1_day') or 0),
            volume_clubs_7_day=int(row_dict.get('volume_clubs_7_day') or 0),
            volume_players_7_day=int(row_dict.get('volume_players_7_day') or 0),
            market_cap=int(row_dict.get('market_cap') or 0),
            club_market_cap=int(row_dict.get('club_market_cap') or 0),
            player_market_cap=int(row_dict.get('player_market_cap') or 0),
            total_wages=int(row_dict.get('total_wages') or 0),
            avg_wages=int(row_dict.get('avg_wages') or 0),
            total_player_value=int(row_dict.get('total_player_value') or 0),
            total_clubs=int(row_dict.get('total_clubs') or 0),
            total_players=int(row_dict.get('total_players') or 0),
            total_available_jobs=int(row_dict.get('total_available_jobs') or 0),
        ))

    return items

async def _fetch_and_cache_leagues(
    r,
    session,  # We accept this to match the background task signature, but won't use it
    cache_key: str,
    season_id: Optional[int] = None,
) -> List[LeagueResponse]:
    """
    Re-fetches data from the DB, builds the list of LeagueResponse,
    and updates Redis with a fresh timestamp. Returns the new data.
    Only called when Redis is available (r is not None).
    """
    from .base import mysql_session_maker  # Import the sessionmaker here
    async with mysql_session_maker() as local_session:
        data = await _load_leagues_data(local_session, season_id)
        payload = {
            "data": [item.dict() for item in data],
            "last_updated": time.time(),
        }
        # Store as JSON - r should not be None when this function is called
        if r is not None:
            try:
                await r.set(cache_key, json.dumps(payload))
            except Exception:
                pass  # Ignore Redis errors
        return data

# -------------------------------------------------------------------------
# /leagues -- now uses the same caching approach as /countries
# -------------------------------------------------------------------------

class LeaguesSortBy(str, Enum):
    league_id = "league_id"
    country_id = "country_id"
    division = "division"
    ticket_cost = "ticket_cost"
    tv_money = "tv_money"
    prize_money_pot = "prize_money_pot"
    ave_attendance = "ave_attendance"
    num_teams = "num_teams"
    round = "round"
    num_rounds = "num_rounds"
    comp_type = "comp_type"
    total_volume_1_day = "total_volume_1_day"
    total_volume_7_day = "total_volume_7_day"
    volume_clubs_1_day = "volume_clubs_1_day"
    volume_players_1_day = "volume_players_1_day"
    volume_clubs_7_day = "volume_clubs_7_day"
    volume_players_7_day = "volume_players_7_day"
    market_cap = "market_cap"
    club_market_cap = "club_market_cap"
    player_market_cap = "player_market_cap"
    total_wages = "total_wages"
    avg_wages = "avg_wages"
    total_player_value = "total_player_value"
    total_clubs = "total_clubs"
    total_players = "total_players"
    total_available_jobs = "total_available_jobs"

@leagues_router.get(
    "/leagues",
    response_model=PaginatedResponse[LeagueResponse],
    summary="Retrieve data for leagues (with 5-min cache)",
    description="Fetch a paginated list of leagues along with trading volumes, market cap, and other aggregate statistics. Uses an in-memory cache to serve data quickly and refreshes every five minutes. Returns a PaginatedResponse of LeagueResponse objects."
)
async def get_leagues(
    page: int = Query(1, ge=1, description="Page number, starting from 1"),
    per_page: PerPageOptions = Query(
        PerPageOptions.twenty,
        description="Number of records per page (options: 5, 10, 20, 50)",
    ),
    sort_by: Optional[LeaguesSortBy] = Query(
        None,
        description="Field to sort by",
    ),
    sort_order: Optional[str] = Query(
        "asc",
        description="Sort order: 'asc' or 'desc'",
        regex="^(asc|desc)$",
    ),
    country_id: Optional[str] = Query(None, description="Filter by country ID (supports comma-separated list)"),
    division: Optional[int] = Query(None, description="Filter by division (league level)"),
    league_id: Optional[int] = Query(None, description="Filter by league ID"),
    season_id: Optional[int] = Query(None, description="Filter by season ID. If not specified, uses the latest season"),
    background_tasks: BackgroundTasks = BackgroundTasks(),
    session: AsyncSession = Depends(get_mysql_session),
    r = Depends(get_redis_client),
):
    # Include season_id in cache key to cache different seasons separately
    # If season_id is None, we'll use "latest" in the cache key
    cache_key = f"leagues_data_v1_season_{season_id if season_id else 'latest'}"
    all_data = None
    
    # Try to use cache if Redis is available
    if r is not None:
        try:
            cached_value = await r.get(cache_key)
            if cached_value:
                data_json = json.loads(cached_value)
                last_updated = data_json.get("last_updated", 0)
                if (time.time() - last_updated) < 300:
                    all_data = [LeagueResponse(**x) for x in data_json["data"]]
                else:
                    background_tasks.add_task(_fetch_and_cache_leagues, r, session, cache_key, season_id)
                    all_data = [LeagueResponse(**x) for x in data_json["data"]]
            else:
                all_data = await _fetch_and_cache_leagues(r, session, cache_key, season_id)
        except Exception:
            # Redis is configured but has intermittent issues - return empty data
            # to avoid expensive DB queries during Redis outages
            all_data = []

    # Fall back to direct database query if Redis not available (r is None)
    if all_data is None:
        all_data = await _load_leagues_data(session)

    if country_id is not None:
        # Support comma-separated list of country IDs
        country_list = [c.strip().upper() for c in country_id.split(',')]
        all_data = [x for x in all_data if x.country_id in country_list]

    if division is not None:
        all_data = [x for x in all_data if x.division == division]

    if league_id is not None:
        all_data = [x for x in all_data if x.league_id == league_id]

    sortable_map = {
        "league_id": lambda x: x.league_id,
        "country_id": lambda x: x.country_id or "",
        "division": lambda x: x.division or 0,
        "ticket_cost": lambda x: x.ticket_cost or 0,
        "tv_money": lambda x: x.tv_money or 0,
        "prize_money_pot": lambda x: x.prize_money_pot or 0,
        "ave_attendance": lambda x: x.ave_attendance or 0,
        "num_teams": lambda x: x.num_teams or 0,
        "round": lambda x: x.round or 0,
        "num_rounds": lambda x: x.num_rounds or 0,
        "comp_type": lambda x: x.comp_type or 0,
        "total_volume_1_day": lambda x: x.total_volume_1_day,
        "total_volume_7_day": lambda x: x.total_volume_7_day,
        "volume_clubs_1_day": lambda x: x.volume_clubs_1_day,
        "volume_players_1_day": lambda x: x.volume_players_1_day,
        "volume_clubs_7_day": lambda x: x.volume_clubs_7_day,
        "volume_players_7_day": lambda x: x.volume_players_7_day,
        "market_cap": lambda x: x.market_cap,
        "club_market_cap": lambda x: x.club_market_cap,
        "player_market_cap": lambda x: x.player_market_cap,
        "total_wages": lambda x: x.total_wages,
        "avg_wages": lambda x: x.avg_wages,
        "total_player_value": lambda x: x.total_player_value,
        "total_clubs": lambda x: x.total_clubs,
        "total_players": lambda x: x.total_players,
        "total_available_jobs": lambda x: x.total_available_jobs,
    }

    chosen_sort = sort_by.value if sort_by else None
    if chosen_sort and chosen_sort not in sortable_map:
        raise HTTPException(status_code=400, detail=f"Invalid sort_by field: {chosen_sort}")

    if chosen_sort:
        reverse_sort = (sort_order == "desc")
        all_data = sorted(all_data, key=sortable_map[chosen_sort], reverse=reverse_sort)

    total = len(all_data)
    total_pages = (total + per_page - 1) // per_page if total else 0
    start_ix = (page - 1) * per_page
    end_ix = start_ix + per_page
    items_page = all_data[start_ix:end_ix]

    return PaginatedResponse(
        page=page,
        per_page=per_page,
        total=total,
        total_pages=total_pages,
        items=items_page,
    )

# -------------------------------------------------------------------------
# The /league_tables endpoint has been updated to include additional fields:
#   - stadium_size (from dc_clubs.stadium_size_current)
#   - fanbase (from dc_clubs.fans_current)
#   - balance (from dc_clubs.balance)
#   - avg_player_rating (from dc_club_info.avg_player_rating_top21)
#   - top_3_players (from dc_players, top 3 sorted by rating)
#   - manager_profile_pic (new: fetched from userconfig via get_profiles_for_users)
#   - manager_last_active_unix (new: from DCUsers.last_active)
# and is now Redis cached with expiry at 1:01 or 1:31 UTC.
# -------------------------------------------------------------------------

@leagues_router.get(
    "/league_tables",
    response_model=List[LeagueTableRowResponse],
    summary="Get league table standings",
    description="Retrieve the current league table for a specified league with live manager data and cached static data."
)
async def get_league_tables(
    league_id: Optional[int] = Query(None, description="League ID"),
    country_id: Optional[str] = Query(None, description="Country ID"),
    division: Optional[int] = Query(None, description="Division (level)"),
    season_id: Optional[int] = Query(None, description="Season ID"),
    session: AsyncSession = Depends(get_mysql_session),
    r = Depends(get_redis_client),
    userconfig_session: AsyncSession = Depends(get_userconfig_session),
    background_tasks: BackgroundTasks = BackgroundTasks(),
):
    # Continental cup country IDs
    continental_cups = ["AME", "ASI", "AFR", "EUR"]
    is_continental_cup = country_id in continental_cups

    # --- Determine league_id, country_id, division, season_id as before ---
    if league_id is None:
        if country_id is None:
            raise HTTPException(status_code=400, detail="You must provide either league_id or country_id")
        if division is None and not is_continental_cup:
            raise HTTPException(status_code=400, detail="You must provide either league_id or country_id and division")

        if season_id is None:
            if is_continental_cup:
                # For continental cups, get latest season_id for the country
                season_query = select(func.max(DCLeagues.season_id)).where(
                    DCLeagues.country_id == country_id
                )
            else:
                # Regular leagues require specific division
                season_query = select(func.max(DCLeagues.season_id)).where(
                    DCLeagues.country_id == country_id,
                    DCLeagues.level == division,
                    DCLeagues.comp_type == 0
                )
            season_result = await session.execute(season_query)
            season_id = season_result.scalar_one_or_none()
            if season_id is None:
                raise HTTPException(status_code=404, detail="No season found for the specified country_id and division")

        if is_continental_cup:
            # For continental cups, we'll get all leagues for this country_id and season_id
            # across all levels (groups)
            league_query = select(DCLeagues.league_id, DCLeagues.level).where(
                DCLeagues.country_id == country_id,
                DCLeagues.season_id == season_id
            )
            league_result = await session.execute(league_query)
            league_levels = league_result.all()
            if not league_levels:
                raise HTTPException(status_code=404, detail="No leagues found for the specified continental cup")

            # We'll process multiple leagues, so initialize items list for all results
            all_items = []

            # Process each league (group) one by one
            for league_info in league_levels:
                league_id = league_info.league_id
                group_division = league_info.level

                # Get cached data for this specific league
                cache_key = f"league_table_rows_{league_id}_{season_id}"
                cached_value = None
                if r is not None:
                    try:
                        cached_value = await r.get(cache_key)
                    except Exception:
                        cached_value = None
                if not cached_value:
                    if r is None:
                        # Redis not configured - fallback to generate data directly
                        league_items = await generate_league_table_fallback(
                            league_id, season_id, country_id, group_division, session, userconfig_session
                        )
                        all_items.extend(league_items)
                        continue
                    else:
                        # Redis configured but data missing/timeout - skip to avoid expensive queries
                        continue

                # Process this league's data
                league_items = await process_league_table_data(
                    cached_value, country_id, group_division, season_id,
                    session, userconfig_session
                )
                all_items.extend(league_items)

            if not all_items:
                raise HTTPException(status_code=404, detail="No table data found for the continental cup")

            return all_items
        else:
            # Regular league - get single league_id as before
            league_query = select(DCLeagues.league_id).where(
                DCLeagues.country_id == country_id,
                DCLeagues.level == division,
                DCLeagues.comp_type == 0,
                DCLeagues.season_id == season_id
            )
            league_result = await session.execute(league_query)
            league_id = league_result.scalar_one_or_none()
            if league_id is None:
                raise HTTPException(status_code=404, detail="No league found for the specified parameters")
    else:
        # When league_id is provided directly
        league_info_query = select(
            DCLeagues.country_id,
            DCLeagues.level.label('division'),
            DCLeagues.season_id
        ).where(DCLeagues.league_id == league_id)
        league_info_result = await session.execute(league_info_query)
        league_info = league_info_result.first()
        if not league_info:
            raise HTTPException(status_code=404, detail="No league found for the specified league_id")
        country_id = league_info.country_id
        division = league_info.division
        if season_id is None:
            season_id = league_info.season_id

    # For single league case (either directly provided or regular league)
    cache_key = f"league_table_rows_{league_id}_{season_id}"
    cached_value = None
    if r is not None:
        try:
            cached_value = await r.get(cache_key)
        except Exception:
            cached_value = None
    if not cached_value:
        if r is None:
            # Redis not configured - fallback to generate data directly from database
            return await generate_league_table_fallback(
                league_id, season_id, country_id, division, session, userconfig_session
            )
        else:
            # Redis configured but data missing/timeout - return empty to prevent API disruption
            return []

    return await process_league_table_data(
        cached_value, country_id, division, season_id,
        session, userconfig_session
    )

# Shared function to build league table cache data
async def build_league_table_cache_data(
    session: AsyncSession, 
    league_filter: Optional[tuple] = None
) -> dict:
    """
    Build league table cache data for all leagues or filtered by (league_id, season_id).
    Returns dict mapping (league_id, season_id) -> cached_table_data
    """
    # 1. Fetch table rows (all or filtered)
    # Select columns explicitly (not the ORM model) so rows are NOT tracked
    # in the session identity map. The old code used select(DCTableRows) +
    # .scalars().all() which loaded full ORM objects -- under the periodic
    # 60s background refresh across 4 gunicorn workers, the GC delay on
    # SQLAlchemy's reference cycles caused steady memory growth.
    cols = DCTableRows.__table__.columns
    if league_filter:
        league_id, season_id = league_filter
        table_rows_query = (
            select(*cols)
            .where(
                DCTableRows.league_id == league_id,
                DCTableRows.season_id == season_id
            )
            .order_by(DCTableRows.new_position.asc())
        )
    else:
        table_rows_query = select(*cols).order_by(DCTableRows.new_position.asc())

    result = await session.execute(table_rows_query)
    all_table_rows = result.fetchall()

    # 2. Group table rows by (league_id, season_id)
    leagues_table_data = {}
    for row in all_table_rows:
        league_key = (row.league_id, row.season_id)
        if league_key not in leagues_table_data:
            leagues_table_data[league_key] = []
        row_dict = {
            k: v.isoformat() if isinstance(v, datetime) else v
            for k, v in row._mapping.items()
        }
        leagues_table_data[league_key].append(row_dict)

    if not leagues_table_data:
        return {}

    # 3. Collect all club_ids needed for extra queries
    all_club_ids = {row["club_id"] for rows in leagues_table_data.values() for row in rows}

    # 4. Fetch extra club data for all club_ids in one go
    dc_clubs_alias = aliased(DCClubs)
    dc_club_info_alias = aliased(DCClubInfo)
    extra_query = (
        select(
            dc_clubs_alias.club_id,
            dc_clubs_alias.stadium_size_current,
            dc_clubs_alias.fans_current,
            dc_clubs_alias.balance,
            dc_club_info_alias.avg_player_rating_top21
        )
        .select_from(dc_clubs_alias)
        .outerjoin(dc_club_info_alias, dc_clubs_alias.club_id == dc_club_info_alias.club_id)
        .where(dc_clubs_alias.club_id.in_(list(all_club_ids)))
    )
    result_extra = await session.execute(extra_query)
    extra_rows = result_extra.all()
    extra_mapping = {row.club_id: row for row in extra_rows}

    # 5. Fetch top 3 players for all clubs in one query
    from sqlalchemy.sql import over
    subq = (
        select(
            DCPlayers.club_id,
            DCPlayers.player_id,
            func.row_number().over(partition_by=DCPlayers.club_id, order_by=DCPlayers.rating.desc()).label("rn")
        )
        .where(DCPlayers.club_id.in_(list(all_club_ids)))
    ).subquery()
    top_players_query = select(subq.c.club_id, subq.c.player_id).where(subq.c.rn <= 3)
    result_top = await session.execute(top_players_query)
    top_players_rows = result_top.fetchall()
    top_players_dict = {}
    for row_top in top_players_rows:
        club_id = row_top[0]
        player_id = row_top[1]
        top_players_dict.setdefault(club_id, []).append(player_id)

    # 6. Build enhanced cache data for each league
    result_leagues = {}
    for league_key, table_rows_list in leagues_table_data.items():
        cached_table_data = []
        for row in table_rows_list:
            club_id = row["club_id"]
            extra = extra_mapping.get(club_id, None)
            avg_rating = None
            if extra and extra.avg_player_rating_top21 is not None:
                try:
                    avg_rating = int(float(extra.avg_player_rating_top21))
                except (TypeError, ValueError):
                    avg_rating = None
            cached_row = {
                **row,  # include all base row data
                "stadium_size": extra.stadium_size_current if extra else None,
                "fanbase": extra.fans_current if extra else None,
                "balance": extra.balance if extra else None,
                "avg_player_rating": avg_rating,
                "top_3_players": top_players_dict.get(club_id, []),
            }
            cached_table_data.append(cached_row)
        result_leagues[league_key] = cached_table_data
    
    return result_leagues

# Helper function to generate league table data when Redis is unavailable
async def generate_league_table_fallback(
    league_id: int, season_id: int, country_id: str, division: int,
    session: AsyncSession, userconfig_session: AsyncSession
):
    """Generate league table data directly from database when Redis cache is unavailable."""
    # Use the shared function to build cache data for this specific league
    league_cache_data = await build_league_table_cache_data(session, (league_id, season_id))
    
    if not league_cache_data or (league_id, season_id) not in league_cache_data:
        return []  # Return empty list if no data found
    
    cached_table_data = league_cache_data[(league_id, season_id)]
    
    # Use the existing process function to handle manager data and final formatting
    fake_cached_value = json.dumps({"cached_table_data": cached_table_data})
    return await process_league_table_data(
        fake_cached_value, country_id, division, season_id,
        session, userconfig_session
    )

# Helper function to process league table data from cache
async def process_league_table_data(
    cached_value, country_id, division, season_id,
    session: AsyncSession, userconfig_session: AsyncSession
):
    cache_payload = json.loads(cached_value)
    cached_rows = cache_payload.get("cached_table_data", [])

    if not cached_rows:
        return []  # Return empty list if no data found

    # Extract all club IDs for manager data query
    club_ids = [row["club_id"] for row in cached_rows]

    # Fetch ONLY manager-related data in real-time
    dc_clubs_alias = aliased(DCClubs)
    dc_users_alias = aliased(DCUsers)
    manager_query = (
        select(
            dc_clubs_alias.club_id,
            dc_clubs_alias.manager_name,
            dc_users_alias.last_active.label("manager_last_active")
        )
        .select_from(dc_clubs_alias)
        .outerjoin(dc_users_alias, dc_clubs_alias.manager_name == dc_users_alias.name)
        .where(dc_clubs_alias.club_id.in_(club_ids))
    )
    result_managers = await session.execute(manager_query)
    manager_rows = result_managers.all()
    manager_mapping = {row.club_id: row for row in manager_rows}

    # Batch fetch manager profile pics for existing managers
    manager_names_needed = {manager_mapping.get(club_id).manager_name
                         for club_id in club_ids
                         if club_id in manager_mapping and manager_mapping.get(club_id).manager_name}
    name_to_pic = await get_profiles_for_users(list(manager_names_needed), userconfig_session)

    # Build the final response by combining cached data with real-time manager data
    items = []
    for row in cached_rows:
        club_id = row["club_id"]
        manager_info = manager_mapping.get(club_id, None)
        manager_name = manager_info.manager_name if manager_info else None
        manager_profile_pic = DEFAULT_PROFILE_PIC_URL
        if manager_name:
            manager_profile_pic = name_to_pic.get(manager_name, DEFAULT_PROFILE_PIC_URL)

        response_item = LeagueTableRowResponse(
            club_id=row["club_id"],
            league_id=row["league_id"],
            manager_name=manager_name,
            manager_profile_pic=manager_profile_pic,
            manager_last_active_unix=manager_info.manager_last_active if manager_info else None,
            country_id=country_id,
            division=division,
            club_ix=row["club_ix"],
            played=row["played"],
            won=row["won"],
            drawn=row["drawn"],
            lost=row["lost"],
            goals_for=row["goals_for"],
            goals_against=row["goals_against"],
            pts=row["pts"],
            form=row["form"],
            old_position=row["old_position"],
            new_position=row["new_position"],
            season_id=row["season_id"],
            stadium_size=row["stadium_size"],
            fanbase=row["fanbase"],
            balance=row["balance"],
            avg_player_rating=row["avg_player_rating"],
            top_3_players=row["top_3_players"],
        )
        items.append(response_item)

    return items


# NEW FUNCTION: Batch update cached league table data for all leagues
async def update_all_league_table_caches(r, session: AsyncSession):
    """
    Fetch all league table rows across all leagues in one go,
    join in the extra club and top players data, group by (league_id, season_id)
    and update Redis cache for each league.
    """
    # Return early if Redis is not available - no point doing expensive computations
    if r is None:
        return
    
    # Use the shared function to build cache data for all leagues
    leagues_cache_data = await build_league_table_cache_data(session)
    
    # Store each league's cache data in Redis
    for league_key, cached_table_data in leagues_cache_data.items():
        league_id, season_id = league_key
        payload = {
            "cached_table_data": cached_table_data,
            "last_updated": time.time()
        }
        cache_key = f"league_table_rows_{league_id}_{season_id}"
        # r may be None when Redis is not configured — the early return above
        # handles that case, but we keep this check as defensive programming
        if r is not None:
            try:
                await r.set(cache_key, json.dumps(payload), ex=120)  # 120 seconds TTL for overlap
            except Exception:
                pass  # Ignore Redis errors during cache updates
