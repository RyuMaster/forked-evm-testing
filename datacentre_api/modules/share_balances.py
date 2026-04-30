# modules/share_balances.py

from sqlalchemy import Column, Integer, BigInteger, String, DateTime, select, func, ForeignKey, or_, and_, case, Text, text
from typing import List, Optional, Any, Dict
from enum import Enum
from pydantic import BaseModel
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased
from datetime import datetime
from modules.utils.profile import get_profiles_for_users
from .base import (
    Base,
    get_mysql_session,
    get_userconfig_session,  # <--- For userconfig session
    get_archival_session,  # For proposals
    PaginatedResponse,
    PerPageOptions,
    DCPlayers,
    DCClubs,
    DCShareBalances,
    DCClubInfo,
    DCUsers,
    DCTableRows,  # For league positions
    DCLeagues,  # For current season
    DEFAULT_PROFILE_PIC_URL,  # <--- For default pic logic
    DCEarnings,  # For aggregated earnings data
)
from .proposals import ProposalUpdates  # For active proposals
# Import DCPlayers and DCClubs from your existing modules
from .players import DCPlayers, DCPlayersTrading
from .clubs import DCClubs, DCClubsTrading
import time
import logging
import json

logger = logging.getLogger(__name__)

# Position mapping for converting bitfield to position names
POSITION_MAPPING = {
    "GK": 1,
    "LB": 2,
    "CB": 4,
    "RB": 8,
    "DML": 16,
    "DMC": 32,
    "DMR": 64,
    "LM": 128,
    "CM": 256,
    "RM": 512,
    "AML": 1024,
    "AMC": 2048,
    "AMR": 4096,
    "FL": 8192,
    "FC": 16384,
    "FR": 32768
}

def get_position_names(multi_position_value):
    """Convert multi_position bitfield to list of position names"""
    if not multi_position_value:
        return []
    positions = []
    for pos_name, pos_value in POSITION_MAPPING.items():
        if multi_position_value & pos_value:
            positions.append(pos_name)
    return positions

def calculate_time_remaining(end_timestamp: int) -> str:
    """Calculate time remaining for a proposal."""
    now = int(time.time())
    if end_timestamp <= now:
        return "Ended"

    remaining = end_timestamp - now
    days = remaining // 86400
    hours = (remaining % 86400) // 3600
    minutes = (remaining % 3600) // 60

    if days > 0:
        return f"{days}d {hours}h"
    elif hours > 0:
        return f"{hours}h {minutes}m"
    else:
        return f"{minutes}m"

def determine_proposal_stage(proposal_type: str, start_timestamp: int, end_timestamp: int, has_votes: bool) -> str:
    """Determine the stage of a proposal."""
    now = int(time.time())
    if end_timestamp <= now:
        return "completed"

    # Unlock proposals are always in voting stage (yes/no vote)
    if proposal_type == "unlock":
        return "voting"

    # For agent/manager proposals
    if start_timestamp and now >= start_timestamp:
        return "voting"
    elif start_timestamp and now < start_timestamp:
        return "proposal"

    # Fallback: if we have votes, it's voting; otherwise proposal
    return "voting" if has_votes else "proposal"

def apply_manager_agent_filters(
    query,
    has_manager,
    has_agent,
    manager_last_active_unix_min,
    manager_last_active_unix_max,
    agent_last_active_unix_min,
    agent_last_active_unix_max,
    clubs_alias,
    DCPlayers,
    manager_user_alias,
    agent_user_alias,
    DCShareBalances
):
    """Apply manager and agent filters to a query to avoid code duplication"""
    
    # Apply has_manager filter (clubs only)
    if has_manager is not None:
        if has_manager:
            query = query.where(
                or_(
                    DCShareBalances.share_type == 'player',
                    and_(
                        DCShareBalances.share_type == 'club',
                        clubs_alias.manager_name.isnot(None)
                    )
                )
            )
        else:
            query = query.where(
                or_(
                    DCShareBalances.share_type == 'player',
                    and_(
                        DCShareBalances.share_type == 'club',
                        clubs_alias.manager_name.is_(None)
                    )
                )
            )

    # Apply has_agent filter (players only)
    if has_agent is not None:
        if has_agent:
            query = query.where(
                or_(
                    DCShareBalances.share_type == 'club',
                    and_(
                        DCShareBalances.share_type == 'player',
                        DCPlayers.agent_name.isnot(None)
                    )
                )
            )
        else:
            query = query.where(
                or_(
                    DCShareBalances.share_type == 'club',
                    and_(
                        DCShareBalances.share_type == 'player',
                        DCPlayers.agent_name.is_(None)
                    )
                )
            )

    # Apply manager_last_active_unix filters (clubs only)
    if manager_last_active_unix_min is not None:
        query = query.where(
            or_(
                DCShareBalances.share_type == 'player',
                and_(
                    DCShareBalances.share_type == 'club',
                    manager_user_alias.last_active >= manager_last_active_unix_min
                )
            )
        )

    if manager_last_active_unix_max is not None:
        query = query.where(
            or_(
                DCShareBalances.share_type == 'player',
                and_(
                    DCShareBalances.share_type == 'club',
                    or_(
                        manager_user_alias.last_active <= manager_last_active_unix_max,
                        and_(
                            manager_user_alias.last_active.is_(None),
                            clubs_alias.manager_name.isnot(None)
                        )
                    )
                )
            )
        )

    # Apply agent_last_active_unix filters (players only)
    if agent_last_active_unix_min is not None:
        query = query.where(
            or_(
                DCShareBalances.share_type == 'club',
                and_(
                    DCShareBalances.share_type == 'player',
                    agent_user_alias.last_active >= agent_last_active_unix_min
                )
            )
        )

    if agent_last_active_unix_max is not None:
        query = query.where(
            or_(
                DCShareBalances.share_type == 'club',
                and_(
                    DCShareBalances.share_type == 'player',
                    or_(
                        agent_user_alias.last_active <= agent_last_active_unix_max,
                        and_(
                            agent_user_alias.last_active.is_(None),
                            DCPlayers.agent_name.isnot(None)
                        )
                    )
                )
            )
        )
    
    return query

class ShareBalanceResponse(BaseModel):
    name: str
    share_type: str
    share_id: int
    num: int
    last_price: int = 0  # Trading price of the share
    country_id: Optional[str] = None
    league_id: Optional[int] = None
    division: Optional[int] = None
    club_id: Optional[int] = None
    profile_pic: Optional[str] = None
    last_active_unix: Optional[int] = None
    last_active: Optional[datetime] = None
    agent_name: Optional[str] = None  # For player shares
    agent_profile_pic: Optional[str] = None
    agent_last_active_unix: Optional[int] = None  # Agent's last active timestamp
    manager_name: Optional[str] = None  # For club shares
    manager_profile_pic: Optional[str] = None
    manager_last_active_unix: Optional[int] = None  # Manager's last active timestamp

class DetailedShareBalanceResponse(ShareBalanceResponse):
    # Additional fields for detailed response
    # Club-specific fields
    club_balance: Optional[int] = None  # For club shares
    old_position: Optional[int] = None  # League position
    new_position: Optional[int] = None  # Current league position
    league_form: Optional[str] = None  # Form from dc_table_rows (league matches only)
    stadium_size_start: Optional[int] = None  # Starting stadium size
    stadium_size_current: Optional[int] = None  # Current stadium size
    fans_start: Optional[int] = None  # Starting fan base
    fans_current: Optional[int] = None  # Current fan base
    manager_locked: Optional[int] = None  # Manager lock status (0 or 1)
    # Player-specific fields
    dob: Optional[int] = None  # Date of birth unix timestamp
    player_value: Optional[int] = None  # Player's value
    wages: Optional[int] = None  # Player's wages
    position: Optional[int] = None  # Main position (bitfield)
    position_name: Optional[str] = None  # Main position human-readable name
    multi_position: Optional[int] = None  # Additional positions (bitfield)
    positions: Optional[List[str]] = None  # All positions human-readable
    fitness: Optional[int] = None  # Fitness level
    morale: Optional[int] = None  # Morale level
    rating: Optional[int] = None  # Standard rating
    rating_gk: Optional[int] = None  # Goalkeeper rating
    rating_tackling: Optional[int] = None
    rating_passing: Optional[int] = None
    rating_shooting: Optional[int] = None
    injured: Optional[int] = None  # Unix timestamp when player recovers (None if not injured)
    contract: Optional[int] = None  # Contract length left
    form: Optional[str] = None  # Form (from dc_clubs for clubs, dc_players for players)
    banned: Optional[int] = None  # Ban status
    nationality: Optional[str] = None  # Player's nationality (country_id)
    desired_contract: Optional[int] = None  # Desired contract length
    allow_transfer: Optional[int] = None  # Whether player allows transfer
    allow_renew: Optional[int] = None  # Whether player allows contract renewal
    loan_offered: Optional[int] = None  # Season ID if loan offered
    loan_offer_accepted: Optional[int] = None  # Whether loan offer was accepted
    loaned_to_club: Optional[int] = None  # Club ID if player is on loan
    # Shared fields
    earnings_7d: Optional[int] = None  # Earnings from this share in last 7 days
    earnings_30d: Optional[int] = None  # Earnings from this share in last 30 days
    # Proposal fields
    has_active_proposal: Optional[bool] = None  # Whether there's an active proposal
    proposal_id: Optional[int] = None  # Proposal ID if active
    proposal_type: Optional[str] = None  # 'agent', 'manager', or 'unlock'
    proposal_stage: Optional[str] = None  # 'proposal', 'voting', or 'completed'
    proposal_end_time_unix: Optional[int] = None  # Unix timestamp when proposal ends
    proposal_time_remaining: Optional[str] = None  # Human readable time remaining
    proposal_total_votes: Optional[int] = None  # Total votes cast
    proposal_leading_candidate: Optional[str] = None  # Name of leading candidate (if voting stage)
    proposal_leading_votes: Optional[int] = None  # Votes for leading candidate

    class Config:
        from_attributes = True

class UserClubShareResponse(BaseModel):
    name: str
    club_id: int
    num_shares: int
    profile_pic: Optional[str] = None
    last_active_unix: Optional[int] = None
    last_active: Optional[datetime] = None

    class Config:
        from_attributes = True

# Create an APIRouter instance for share balances
share_balances_router = APIRouter()

class ShareBalancesSortBy(str, Enum):
    name = "name"
    share_type = "share_type"
    share_id = "share_id"
    num = "num"
    last_active_unix = "last_active_unix"

@share_balances_router.get(
    "/share_balance_by_user_club",
    summary="Get share balance for a specific user and club",
    description="Returns the number of shares (influence) a user owns in a specific club",
    response_model=UserClubShareResponse
)
async def get_share_balance_by_user_club(
    name: str = Query(..., description="Username to look up"),
    club_id: int = Query(..., description="Club ID to check for share ownership"),
    session: AsyncSession = Depends(get_mysql_session),
    userconfig_session: AsyncSession = Depends(get_userconfig_session),
):
    # Create a query to look up shares for this specific user and club
    query = select(
        DCShareBalances.name,
        DCShareBalances.num.label("num_shares"),
        DCShareBalances.share_id.label("club_id"),
        DCUsers.last_active.label("last_active_unix"),
    ).select_from(DCShareBalances).where(
        DCShareBalances.name == name,
        DCShareBalances.share_type == 'club',
        DCShareBalances.share_id == club_id
    ).outerjoin(
        DCUsers,
        DCShareBalances.name == DCUsers.name
    )
    
    result = await session.execute(query)
    row = result.first()
    
    if not row:
        # If no shares found, return 0 shares but still with user info
        # First check if user exists
        user_query = select(
            DCUsers.name,
            DCUsers.last_active.label("last_active_unix")
        ).where(DCUsers.name == name)
        user_result = await session.execute(user_query)
        user_row = user_result.first()
        
        if not user_row:
            raise HTTPException(status_code=404, detail=f"User {name} not found")
            
        # Get profile pic for the user
        name_to_pic = await get_profiles_for_users([name], userconfig_session)
        profile_pic = name_to_pic.get(name, DEFAULT_PROFILE_PIC_URL)
        
        # Convert last_active_unix to datetime
        last_active_unix = user_row.last_active_unix
        last_active = datetime.utcfromtimestamp(last_active_unix) if last_active_unix else None
        
        return UserClubShareResponse(
            name=name,
            club_id=club_id,
            num_shares=0,
            profile_pic=profile_pic,
            last_active_unix=last_active_unix,
            last_active=last_active
        )
    
    # Get profile pic for the user
    name_to_pic = await get_profiles_for_users([row.name], userconfig_session)
    profile_pic = name_to_pic.get(row.name, DEFAULT_PROFILE_PIC_URL)
    
    # Convert last_active_unix to datetime
    last_active_unix = row.last_active_unix
    last_active = datetime.utcfromtimestamp(last_active_unix) if last_active_unix else None
    
    return UserClubShareResponse(
        name=row.name,
        club_id=row.club_id,
        num_shares=row.num_shares,
        profile_pic=profile_pic,
        last_active_unix=last_active_unix,
        last_active=last_active
    )

@share_balances_router.get(
    "/share_balances",
    summary="Retrieve influence balances",
    description="Returns influence ownership data for clubs or players. Can filter by: name only, club_id only, player_id only, club_id+name (user's influence in specific club), or player_id+name (user's influence in specific player). Includes user's last active timestamp. Can also aggregate data by country or league."
)
async def get_share_balances(
    page: int = Query(1, ge=1, description="Page number, starting from 1"),
    per_page: PerPageOptions = Query(
        PerPageOptions.twenty,
        description="Number of records per page (options: 5, 10, 20, 50)",
    ),
    # Change sort_by to our new enum; default can be None or 'num'
    sort_by: Optional[ShareBalancesSortBy] = Query(
        None,
        description="Field to sort by (options: 'name', 'share_type', 'share_id', 'num', 'last_active_unix')",
    ),
    sort_order: Optional[str] = Query(
        "asc",
        description="Sort order: 'asc' or 'desc'",
        regex="^(asc|desc)$",
    ),
    club_id: Optional[int] = Query(None, description="Filter by club ID"),
    player_id: Optional[int] = Query(None, description="Filter by player ID"),
    name: Optional[str] = Query(None, description="Filter by name"),
    share_type: Optional[str] = Query(None, description="Filter by share type: 'club' or 'player'", regex="^(club|player)$"),
    countries: Optional[bool] = Query(False, description="Set to True to get data aggregated per country"),
    leagues: Optional[bool] = Query(False, description="Set to True to get data aggregated per league"),
    country_id: Optional[str] = Query(None, description="Filter by country ID (used with name)"),
    league_id: Optional[int] = Query(None, description="Filter by league ID (used with name)"),
    has_manager: Optional[bool] = Query(None, description="Filter clubs by manager presence (True: has manager, False: no manager)"),
    has_agent: Optional[bool] = Query(None, description="Filter players by agent presence (True: has agent, False: no agent)"),
    manager_last_active_unix_min: Optional[int] = Query(None, description="Minimum unix timestamp for manager's last active time (clubs only)"),
    manager_last_active_unix_max: Optional[int] = Query(None, description="Maximum unix timestamp for manager's last active time (clubs only, includes NULL if manager exists)"),
    agent_last_active_unix_min: Optional[int] = Query(None, description="Minimum unix timestamp for agent's last active time (players only)"),
    agent_last_active_unix_max: Optional[int] = Query(None, description="Maximum unix timestamp for agent's last active time (players only, includes NULL if agent exists)"),
    session: AsyncSession = Depends(get_mysql_session),
    userconfig_session: AsyncSession = Depends(get_userconfig_session),
):
    if countries and leagues:
        raise HTTPException(
            status_code=400,
            detail="Please set only one of 'countries' or 'leagues' to True."
        )
    if countries or leagues:
        if name is None:
            raise HTTPException(
                status_code=400,
                detail="When 'countries' or 'leagues' is specified, 'name' must be provided."
            )
    elif country_id is not None or league_id is not None:
        if name is None:
            raise HTTPException(
                status_code=400,
                detail="When 'country_id' or 'league_id' is specified, 'name' must be provided."
            )
    else:
        # Allow either single parameter or specific combinations
        has_club = club_id is not None
        has_player = player_id is not None
        has_name = name is not None

        valid_combinations = [
            has_name and not has_club and not has_player,  # name only
            has_club and not has_player and not has_name,  # club_id only
            has_player and not has_club and not has_name,  # player_id only
            has_club and has_name and not has_player,      # club_id + name
            has_player and has_name and not has_club,       # player_id + name
        ]

        if not any(valid_combinations):
            raise HTTPException(
                status_code=400,
                detail="Please provide one of: 'name', 'club_id', 'player_id', 'club_id+name', or 'player_id+name'."
            )

    if countries or leagues:
        # Build the query
        club_info_alias = aliased(DCClubInfo)
        player_club_info_alias = aliased(DCClubInfo)

        # Determine the grouping field
        if countries:
            group_id_label = 'country_id'
            group_field = case(
                (DCShareBalances.share_type == 'club', club_info_alias.country_id),
                (DCShareBalances.share_type == 'player', player_club_info_alias.country_id),
                else_=None
            ).label(group_id_label)
        elif leagues:
            group_id_label = 'league_id'
            group_field = case(
                (DCShareBalances.share_type == 'club', club_info_alias.league_id),
                (DCShareBalances.share_type == 'player', player_club_info_alias.league_id),
                else_=None
            ).label(group_id_label)

        no_of_clubs = func.count(
            func.distinct(
                case(
                    (DCShareBalances.share_type == 'club', DCShareBalances.share_id),
                    else_=None
                )
            )
        ).label('no_of_clubs')

        no_of_players = func.count(
            func.distinct(
                case(
                    (DCShareBalances.share_type == 'player', DCShareBalances.share_id),
                    else_=None
                )
            )
        ).label('no_of_players')

        no_of_club_inf = func.sum(
            case(
                (DCShareBalances.share_type == 'club', DCShareBalances.num),
                else_=0
            )
        ).label('no_of_club_inf')

        no_of_players_inf = func.sum(
            case(
                (DCShareBalances.share_type == 'player', DCShareBalances.num),
                else_=0
            )
        ).label('no_of_players_inf')

        select_query = select(
            group_field,
            no_of_clubs,
            no_of_players,
            no_of_club_inf,
            no_of_players_inf
        ).select_from(DCShareBalances)

        # Filter by exact user name (including trailing spaces)
        name_len = len(name)
        select_query = select_query.where(DCShareBalances.name == name)
        select_query = select_query.where(func.char_length(DCShareBalances.name) == name_len)
        
        # Add share_type filter if provided for aggregated queries
        if share_type is not None:
            select_query = select_query.where(DCShareBalances.share_type == share_type)

        # Apply joins
        select_query = select_query.outerjoin(
            DCPlayers,
            and_(
                DCShareBalances.share_type == 'player',
                DCPlayers.player_id == DCShareBalances.share_id
            )
        ).outerjoin(
            club_info_alias,
            and_(
                DCShareBalances.share_type == 'club',
                club_info_alias.club_id == DCShareBalances.share_id
            )
        ).outerjoin(
            player_club_info_alias,
            and_(
                DCShareBalances.share_type == 'player',
                DCPlayers.club_id == player_club_info_alias.club_id
            )
        )

        select_query = select_query.group_by(group_field)

        result = await session.execute(select_query)
        rows = result.fetchall()

        # For aggregated queries, no direct user-based profile pic is relevant.
        # So we simply return the aggregated data here.
        items = []
        for row in rows:
            item = {
                group_id_label: getattr(row, group_id_label),
                'no_of_clubs': row.no_of_clubs,
                'no_of_players': row.no_of_players,
                'no_of_club_inf': row.no_of_club_inf,
                'no_of_players_inf': row.no_of_players_inf
            }
            items.append(item)

        return items
    else:
        conditions = []

        if country_id is not None or league_id is not None:
            if name is None:
                raise HTTPException(
                    status_code=400,
                    detail="When 'country_id' or 'league_id' is specified, 'name' must be provided."
                )
            else:
                name_len = len(name)
                conditions.append(DCShareBalances.name == name)
                conditions.append(func.char_length(DCShareBalances.name) == name_len)
        else:
            # Handle single parameters and combinations
            if club_id is not None:
                conditions.append(DCShareBalances.share_type == 'club')
                conditions.append(DCShareBalances.share_id == club_id)
            if player_id is not None:
                conditions.append(DCShareBalances.share_type == 'player')
                conditions.append(DCShareBalances.share_id == player_id)
            if name is not None:
                name_len = len(name)
                conditions.append(DCShareBalances.name == name)
                conditions.append(func.char_length(DCShareBalances.name) == name_len)

        # Add share_type filter if provided
        if share_type is not None:
            conditions.append(DCShareBalances.share_type == share_type)

        # Build the total_query
        total_query = select(func.count()).select_from(DCShareBalances)

        club_info_alias = aliased(DCClubInfo)
        player_club_info_alias = aliased(DCClubInfo)
        user_alias = aliased(DCUsers)
        agent_user_alias = aliased(DCUsers)
        manager_user_alias = aliased(DCUsers)
        clubs_alias = aliased(DCClubs)

        total_query = total_query.outerjoin(
            DCPlayers,
            and_(
                DCShareBalances.share_type == 'player',
                DCPlayers.player_id == DCShareBalances.share_id
            )
        ).outerjoin(
            clubs_alias,
            and_(
                DCShareBalances.share_type == 'club',
                clubs_alias.club_id == DCShareBalances.share_id
            )
        ).outerjoin(
            club_info_alias,
            and_(
                DCShareBalances.share_type == 'club',
                club_info_alias.club_id == DCShareBalances.share_id
            )
        ).outerjoin(
            player_club_info_alias,
            and_(
                DCShareBalances.share_type == 'player',
                DCPlayers.club_id == player_club_info_alias.club_id
            )
        ).outerjoin(
            user_alias,
            DCShareBalances.name == user_alias.name
        ).outerjoin(
            agent_user_alias,
            and_(
                DCShareBalances.share_type == 'player',
                DCPlayers.agent_name == agent_user_alias.name
            )
        ).outerjoin(
            manager_user_alias,
            and_(
                DCShareBalances.share_type == 'club',
                clubs_alias.manager_name == manager_user_alias.name
            )
        )

        for condition in conditions:
            total_query = total_query.where(condition)

        if country_id is not None:
            country_condition = or_(
                and_(
                    DCShareBalances.share_type == 'club',
                    club_info_alias.country_id == country_id
                ),
                and_(
                    DCShareBalances.share_type == 'player',
                    player_club_info_alias.country_id == country_id
                )
            )
            total_query = total_query.where(country_condition)

        if league_id is not None:
            league_condition = or_(
                and_(
                    DCShareBalances.share_type == 'club',
                    club_info_alias.league_id == league_id
                ),
                and_(
                    DCShareBalances.share_type == 'player',
                    player_club_info_alias.league_id == league_id
                )
            )
            total_query = total_query.where(league_condition)

        # Apply manager and agent filters using helper function
        total_query = apply_manager_agent_filters(
            total_query,
            has_manager,
            has_agent,
            manager_last_active_unix_min,
            manager_last_active_unix_max,
            agent_last_active_unix_min,
            agent_last_active_unix_max,
            clubs_alias,
            DCPlayers,
            manager_user_alias,
            agent_user_alias,
            DCShareBalances
        )

        total_result = await session.execute(total_query)
        total = total_result.scalar_one()
        total_pages = (total + per_page.value - 1) // per_page.value if total else 0

        # Add aliases for clubs table to get manager_name
        clubs_alias = aliased(DCClubs)
        # Add aliases for agent and manager users
        agent_user_alias = aliased(DCUsers)
        manager_user_alias = aliased(DCUsers)
        # Add aliases for trading tables
        players_trading_alias = aliased(DCPlayersTrading)
        clubs_trading_alias = aliased(DCClubsTrading)
        
        select_query = select(
            DCShareBalances.name,
            DCShareBalances.share_type,
            DCShareBalances.share_id,
            DCShareBalances.num,
            DCPlayers.club_id.label("player_club_id"),
            DCPlayers.agent_name.label("agent_name"),  # Add agent_name for players
            clubs_alias.manager_name.label("manager_name"),  # Add manager_name for clubs
            club_info_alias.country_id.label("club_country_id"),
            club_info_alias.league_id.label("club_league_id"),
            club_info_alias.division.label("club_division"),
            player_club_info_alias.country_id.label("player_country_id"),
            player_club_info_alias.league_id.label("player_league_id"),
            player_club_info_alias.division.label("player_division"),
            user_alias.last_active.label("last_active_unix"),
            agent_user_alias.last_active.label("agent_last_active_unix"),  # Agent's last active
            manager_user_alias.last_active.label("manager_last_active_unix"),  # Manager's last active
            func.coalesce(players_trading_alias.last_price, 0).label("player_last_price"),  # Player's last price
            func.coalesce(clubs_trading_alias.last_price, 0).label("club_last_price"),  # Club's last price
        ).select_from(DCShareBalances)

        select_query = select_query.outerjoin(
            DCPlayers,
            and_(
                DCShareBalances.share_type == 'player',
                DCPlayers.player_id == DCShareBalances.share_id
            )
        ).outerjoin(
            clubs_alias,
            and_(
                DCShareBalances.share_type == 'club',
                clubs_alias.club_id == DCShareBalances.share_id
            )
        ).outerjoin(
            club_info_alias,
            and_(
                DCShareBalances.share_type == 'club',
                club_info_alias.club_id == DCShareBalances.share_id
            )
        ).outerjoin(
            player_club_info_alias,
            and_(
                DCShareBalances.share_type == 'player',
                DCPlayers.club_id == player_club_info_alias.club_id
            )
        ).outerjoin(
            user_alias,
            DCShareBalances.name == user_alias.name
        ).outerjoin(
            agent_user_alias,
            and_(
                DCShareBalances.share_type == 'player',
                DCPlayers.agent_name == agent_user_alias.name
            )
        ).outerjoin(
            manager_user_alias,
            and_(
                DCShareBalances.share_type == 'club',
                clubs_alias.manager_name == manager_user_alias.name
            )
        ).outerjoin(
            players_trading_alias,
            and_(
                DCShareBalances.share_type == 'player',
                players_trading_alias.player_id == DCShareBalances.share_id
            )
        ).outerjoin(
            clubs_trading_alias,
            and_(
                DCShareBalances.share_type == 'club',
                clubs_trading_alias.club_id == DCShareBalances.share_id
            )
        )

        for condition in conditions:
            select_query = select_query.where(condition)

        if country_id is not None:
            country_condition = or_(
                and_(
                    DCShareBalances.share_type == 'club',
                    club_info_alias.country_id == country_id
                ),
                and_(
                    DCShareBalances.share_type == 'player',
                    player_club_info_alias.country_id == country_id
                )
            )
            select_query = select_query.where(country_condition)

        if league_id is not None:
            league_condition = or_(
                and_(
                    DCShareBalances.share_type == 'club',
                    club_info_alias.league_id == league_id
                ),
                and_(
                    DCShareBalances.share_type == 'player',
                    player_club_info_alias.league_id == league_id
                )
            )
            select_query = select_query.where(league_condition)

        # Apply manager and agent filters using helper function
        select_query = apply_manager_agent_filters(
            select_query,
            has_manager,
            has_agent,
            manager_last_active_unix_min,
            manager_last_active_unix_max,
            agent_last_active_unix_min,
            agent_last_active_unix_max,
            clubs_alias,
            DCPlayers,
            manager_user_alias,
            agent_user_alias,
            DCShareBalances
        )

        sortable_fields = {
            "name": DCShareBalances.name,
            "share_type": DCShareBalances.share_type,
            "share_id": DCShareBalances.share_id,
            "num": DCShareBalances.num,
            "last_active_unix": user_alias.last_active,
        }

        sort_by_fields = []
        chosen_sort = sort_by.value if sort_by else None
        if chosen_sort:
            if chosen_sort in sortable_fields:
                sort_column = sortable_fields[chosen_sort]
                if sort_order == "desc":
                    sort_column = sort_column.desc()
                else:
                    sort_column = sort_column.asc()
                sort_by_fields.append(sort_column)
            else:
                raise HTTPException(status_code=400, detail=f"Invalid sort_by field: {chosen_sort}")
        else:
            # Default sorting by 'num' descending
            sort_by_fields.append(DCShareBalances.num.desc())

        select_query = select_query.order_by(*sort_by_fields)

        offset = (page - 1) * per_page.value
        select_query = select_query.offset(offset).limit(per_page.value)

        result = await session.execute(select_query)
        rows = result.fetchall()

        # ----------------------------------------------------------
        # 1) BATCH GATHER 'name' FOR PROFILE PICS
        # ----------------------------------------------------------
        names_needed = set()
        agent_manager_names = set()
        for row in rows:
            if row.name:
                names_needed.add(row.name)
            # Collect agent/manager names for profile pics
            if row.share_type == 'player' and row.agent_name:
                agent_manager_names.add(row.agent_name)
            elif row.share_type == 'club' and row.manager_name:
                agent_manager_names.add(row.manager_name)

        name_to_pic = await get_profiles_for_users(list(names_needed), userconfig_session)
        agent_manager_to_pic = await get_profiles_for_users(list(agent_manager_names), userconfig_session) if agent_manager_names else {}

        items = []
        for row in rows:
            if row.share_type == 'club':
                country_id_value = row.club_country_id
                league_id_value = row.club_league_id
                division = row.club_division
                club_id_value = row.share_id  # For clubs, share_id is the club_id
            elif row.share_type == 'player':
                country_id_value = row.player_country_id
                league_id_value = row.player_league_id
                division = row.player_division
                club_id_value = row.player_club_id  # For players, club_id is from DCPlayers
            else:
                country_id_value = None
                league_id_value = None
                division = None
                club_id_value = None

            # Determine the correct pic
            profile_pic = name_to_pic.get(row.name, DEFAULT_PROFILE_PIC_URL)

            # Convert last_active_unix to datetime
            last_active_unix = row.last_active_unix
            last_active = datetime.utcfromtimestamp(last_active_unix) if last_active_unix else None

            # Get agent/manager info based on share type
            agent_name = None
            agent_profile_pic = None
            agent_last_active_unix = None
            manager_name = None
            manager_profile_pic = None
            manager_last_active_unix = None
            
            # Get last_price based on share type
            if row.share_type == 'player':
                last_price = int(row.player_last_price) if hasattr(row, 'player_last_price') else 0
                if row.agent_name:
                    agent_name = row.agent_name
                    agent_profile_pic = agent_manager_to_pic.get(row.agent_name, DEFAULT_PROFILE_PIC_URL)
                    agent_last_active_unix = row.agent_last_active_unix if hasattr(row, 'agent_last_active_unix') else None
            elif row.share_type == 'club':
                last_price = int(row.club_last_price) if hasattr(row, 'club_last_price') else 0
                if row.manager_name:
                    manager_name = row.manager_name
                    manager_profile_pic = agent_manager_to_pic.get(row.manager_name, DEFAULT_PROFILE_PIC_URL)
                    manager_last_active_unix = row.manager_last_active_unix if hasattr(row, 'manager_last_active_unix') else None
            else:
                last_price = 0
            
            item = ShareBalanceResponse(
                name=row.name,
                share_type=row.share_type,
                share_id=row.share_id,
                num=row.num,
                last_price=last_price,
                country_id=country_id_value,
                league_id=league_id_value,
                division=division,
                club_id=club_id_value,
                profile_pic=profile_pic,  # <--- attach the retrieved pic
                last_active_unix=last_active_unix,
                last_active=last_active,
                agent_name=agent_name,
                agent_profile_pic=agent_profile_pic,
                agent_last_active_unix=agent_last_active_unix,
                manager_name=manager_name,
                manager_profile_pic=manager_profile_pic,
                manager_last_active_unix=manager_last_active_unix,
            )
            items.append(item)

        return PaginatedResponse(
            page=page,
            per_page=per_page.value,
            total=total,
            total_pages=total_pages,
            items=items,
        )

@share_balances_router.get(
    "/share_balances/detailed",
    response_model=PaginatedResponse[DetailedShareBalanceResponse],
    summary="Retrieve detailed influence balances with position and earnings",
    description="Returns detailed share ownership data including league positions, club balance, and earnings for the specified user"
)
async def get_detailed_share_balances(
    page: int = Query(1, ge=1, description="Page number, starting from 1"),
    per_page: int = Query(
        20,
        description="Number of records per page (options: 5, 10, 20, 50, 5000)",
        ge=5,
        le=5000,
    ),
    name: str = Query(..., description="Username to get detailed balances for"),
    share_type: Optional[str] = Query(None, description="Filter by share type: 'club' or 'player'", regex="^(club|player)$"),
    has_manager: Optional[bool] = Query(None, description="Filter clubs by manager presence (True: has manager, False: no manager)"),
    has_agent: Optional[bool] = Query(None, description="Filter players by agent presence (True: has agent, False: no agent)"),
    manager_last_active_unix_min: Optional[int] = Query(None, description="Minimum unix timestamp for manager's last active time (clubs only)"),
    manager_last_active_unix_max: Optional[int] = Query(None, description="Maximum unix timestamp for manager's last active time (clubs only, includes NULL if manager exists)"),
    agent_last_active_unix_min: Optional[int] = Query(None, description="Minimum unix timestamp for agent's last active time (players only)"),
    agent_last_active_unix_max: Optional[int] = Query(None, description="Maximum unix timestamp for agent's last active time (players only, includes NULL if agent exists)"),
    session: AsyncSession = Depends(get_mysql_session),
    archival_session: AsyncSession = Depends(get_archival_session),
    userconfig_session: AsyncSession = Depends(get_userconfig_session),
):
    """Get detailed share balances including league positions and earnings"""
    
    # Build base query for share balances
    name_len = len(name)
    conditions = [
        DCShareBalances.name == name,
        func.char_length(DCShareBalances.name) == name_len
    ]
    
    if share_type:
        conditions.append(DCShareBalances.share_type == share_type)
    
    # Count total items
    total_query = select(func.count()).select_from(DCShareBalances)
    
    # Need to add the joins for the filters to work
    clubs_alias = aliased(DCClubs)
    user_alias = aliased(DCUsers)
    agent_user_alias = aliased(DCUsers)
    manager_user_alias = aliased(DCUsers)
    
    total_query = total_query.outerjoin(
        DCPlayers,
        and_(
            DCShareBalances.share_type == 'player',
            DCPlayers.player_id == DCShareBalances.share_id
        )
    ).outerjoin(
        clubs_alias,
        and_(
            DCShareBalances.share_type == 'club',
            clubs_alias.club_id == DCShareBalances.share_id
        )
    ).outerjoin(
        agent_user_alias,
        and_(
            DCShareBalances.share_type == 'player',
            DCPlayers.agent_name == agent_user_alias.name
        )
    ).outerjoin(
        manager_user_alias,
        and_(
            DCShareBalances.share_type == 'club',
            clubs_alias.manager_name == manager_user_alias.name
        )
    )
    
    for condition in conditions:
        total_query = total_query.where(condition)
    
    # Apply manager and agent filters using helper function
    total_query = apply_manager_agent_filters(
        total_query,
        has_manager,
        has_agent,
        manager_last_active_unix_min,
        manager_last_active_unix_max,
        agent_last_active_unix_min,
        agent_last_active_unix_max,
        clubs_alias,
        DCPlayers,
        manager_user_alias,
        agent_user_alias,
        DCShareBalances
    )
    
    total_result = await session.execute(total_query)
    total = total_result.scalar_one()
    total_pages = (total + per_page - 1) // per_page if total else 0
    
    # Main query with all joins
    clubs_alias = aliased(DCClubs)
    player_clubs_alias = aliased(DCClubs)  # For getting player's club info
    club_info_alias = aliased(DCClubInfo)
    player_club_info_alias = aliased(DCClubInfo)
    user_alias = aliased(DCUsers)
    agent_user_alias = aliased(DCUsers)
    manager_user_alias = aliased(DCUsers)
    player_club_manager_user_alias = aliased(DCUsers)  # For player's club manager
    players_trading_alias = aliased(DCPlayersTrading)
    clubs_trading_alias = aliased(DCClubsTrading)
    
    select_query = select(
        DCShareBalances.name,
        DCShareBalances.share_type,
        DCShareBalances.share_id,
        DCShareBalances.num,
        # Player fields
        DCPlayers.club_id.label("player_club_id"),
        DCPlayers.agent_name.label("agent_name"),
        DCPlayers.dob,
        DCPlayers.value.label("player_value"),
        DCPlayers.wages,
        DCPlayers.position,
        DCPlayers.multi_position,
        DCPlayers.fitness,
        DCPlayers.morale,
        DCPlayers.rating,
        DCPlayers.rating_gk,
        DCPlayers.rating_tackling,
        DCPlayers.rating_passing,
        DCPlayers.rating_shooting,
        DCPlayers.injured,
        DCPlayers.contract,
        DCPlayers.form.label("player_form"),
        DCPlayers.banned,
        DCPlayers.country_id.label("player_nationality"),
        DCPlayers.desired_contract,
        DCPlayers.allow_transfer,
        DCPlayers.allow_renew,
        DCPlayers.loan_offered,
        DCPlayers.loan_offer_accepted,
        DCPlayers.loaned_to_club,
        # Club fields
        clubs_alias.manager_name.label("club_manager_name"),
        clubs_alias.balance.label("club_balance"),
        clubs_alias.form.label("club_form"),
        clubs_alias.manager_locked.label("club_manager_locked"),
        clubs_alias.stadium_size_start,
        clubs_alias.stadium_size_current,
        clubs_alias.fans_start,
        clubs_alias.fans_current,
        # Player's club manager fields
        player_clubs_alias.manager_name.label("player_club_manager_name"),
        club_info_alias.country_id.label("club_country_id"),
        club_info_alias.league_id.label("club_league_id"),
        club_info_alias.division.label("club_division"),
        player_club_info_alias.country_id.label("player_country_id"),
        player_club_info_alias.league_id.label("player_league_id"),
        player_club_info_alias.division.label("player_division"),
        # User fields
        user_alias.last_active.label("last_active_unix"),
        agent_user_alias.last_active.label("agent_last_active_unix"),
        manager_user_alias.last_active.label("club_manager_last_active_unix"),
        player_club_manager_user_alias.last_active.label("player_club_manager_last_active_unix"),
        # Trading fields
        func.coalesce(players_trading_alias.last_price, 0).label("player_last_price"),
        func.coalesce(clubs_trading_alias.last_price, 0).label("club_last_price"),
    ).select_from(DCShareBalances)
    
    # Add all the joins
    select_query = select_query.outerjoin(
        DCPlayers,
        and_(
            DCShareBalances.share_type == 'player',
            DCPlayers.player_id == DCShareBalances.share_id
        )
    ).outerjoin(
        clubs_alias,
        and_(
            DCShareBalances.share_type == 'club',
            clubs_alias.club_id == DCShareBalances.share_id
        )
    ).outerjoin(
        club_info_alias,
        and_(
            DCShareBalances.share_type == 'club',
            club_info_alias.club_id == DCShareBalances.share_id
        )
    ).outerjoin(
        player_club_info_alias,
        and_(
            DCShareBalances.share_type == 'player',
            DCPlayers.club_id == player_club_info_alias.club_id
        )
    ).outerjoin(
        user_alias,
        DCShareBalances.name == user_alias.name
    ).outerjoin(
        agent_user_alias,
        and_(
            DCShareBalances.share_type == 'player',
            DCPlayers.agent_name == agent_user_alias.name
        )
    ).outerjoin(
        player_clubs_alias,
        and_(
            DCShareBalances.share_type == 'player',
            DCPlayers.club_id == player_clubs_alias.club_id
        )
    ).outerjoin(
        manager_user_alias,
        and_(
            DCShareBalances.share_type == 'club',
            clubs_alias.manager_name == manager_user_alias.name
        )
    ).outerjoin(
        player_club_manager_user_alias,
        and_(
            DCShareBalances.share_type == 'player',
            player_clubs_alias.manager_name == player_club_manager_user_alias.name
        )
    ).outerjoin(
        players_trading_alias,
        and_(
            DCShareBalances.share_type == 'player',
            players_trading_alias.player_id == DCShareBalances.share_id
        )
    ).outerjoin(
        clubs_trading_alias,
        and_(
            DCShareBalances.share_type == 'club',
            clubs_trading_alias.club_id == DCShareBalances.share_id
        )
    )
    
    # Apply conditions and sorting
    for condition in conditions:
        select_query = select_query.where(condition)
    
    # Apply manager and agent filters using helper function
    select_query = apply_manager_agent_filters(
        select_query,
        has_manager,
        has_agent,
        manager_last_active_unix_min,
        manager_last_active_unix_max,
        agent_last_active_unix_min,
        agent_last_active_unix_max,
        clubs_alias,
        DCPlayers,
        manager_user_alias,
        agent_user_alias,
        DCShareBalances
    )
    
    select_query = select_query.order_by(DCShareBalances.num.desc())
    
    # Apply pagination
    offset = (page - 1) * per_page
    select_query = select_query.offset(offset).limit(per_page)
    
    result = await session.execute(select_query)
    rows = result.fetchall()
    
    # Get current season (max season_id)
    season_query = select(func.max(DCLeagues.season_id))
    season_result = await session.execute(season_query)
    current_season = season_result.scalar() or 1
    
    # Collect club IDs for position lookup
    club_ids_for_positions = []
    for row in rows:
        if row.share_type == 'club' and row.club_league_id:
            club_ids_for_positions.append((row.share_id, row.club_league_id))
    
    # OPTIMIZED: Batch fetch ALL league positions in ONE query
    league_positions = {}
    if club_ids_for_positions:
        # Build OR conditions to filter by both club_id AND league_id
        position_conditions = [
            (DCTableRows.club_id == club_id) &
            (DCTableRows.league_id == league_id) &
            (DCTableRows.season_id == current_season)
            for club_id, league_id in club_ids_for_positions
        ]

        # Single batched query for ALL positions with correct league filtering
        position_query = select(
            DCTableRows.club_id,
            DCTableRows.old_position,
            DCTableRows.new_position,
            DCTableRows.form
        ).where(or_(*position_conditions))

        position_result = await session.execute(position_query)
        positions = position_result.fetchall()

        # Build lookup dictionary
        for pos in positions:
            league_positions[pos.club_id] = {
                'old_position': pos.old_position,
                'new_position': pos.new_position,
                'form': pos.form
            }

        logger.info(f"Fetched {len(positions)} league positions in single batched query")
    
    # Fetch earnings from aggregated dc_earnings table (much faster!)
    now = int(time.time())
    share_earnings = {}

    if rows:
        # Build list of (share_type, share_id) tuples for batch query
        share_keys = [(row.share_type, row.share_id) for row in rows]

        # Single batch query to fetch all earnings from dc_earnings table
        earnings_query = select(
            DCEarnings.share_type,
            DCEarnings.share_id,
            DCEarnings.earnings_7d,
            DCEarnings.earnings_30d
        ).where(
            and_(
                DCEarnings.name == name,
                func.char_length(DCEarnings.name) == name_len,
                or_(*[
                    and_(
                        DCEarnings.share_type == share_type,
                        DCEarnings.share_id == share_id
                    )
                    for share_type, share_id in share_keys
                ])
            )
        )

        earnings_result = await session.execute(earnings_query)
        earnings_rows = earnings_result.fetchall()

        # Build lookup dictionary
        for earnings_row in earnings_rows:
            key = f"{earnings_row.share_type}_{earnings_row.share_id}"
            share_earnings[key] = {
                'e7': int(earnings_row.earnings_7d or 0),
                'e30': int(earnings_row.earnings_30d or 0)
            }

        # Add zero earnings for shares with no data in dc_earnings
        for row in rows:
            key = f"{row.share_type}_{row.share_id}"
            if key not in share_earnings:
                share_earnings[key] = {'e7': 0, 'e30': 0}

        logger.info(f"Fetched earnings for {len(rows)} shares from dc_earnings table")

    # Batch fetch active proposals for all shares (OPTIMIZED)
    active_proposals = {}
    if rows:
        current_time = int(time.time())

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
            conditions.append(f"(share_type = 'club' AND share_id IN ({','.join(club_ids)}))")
        if player_ids:
            conditions.append(f"(share_type = 'player' AND share_id IN ({','.join(player_ids)}))")

        if conditions:
            where_clause = " OR ".join(conditions)

            # Use raw SQL for better performance
            proposals_query = text(f"""
                SELECT
                    proposal_id,
                    share_type,
                    share_id,
                    type,
                    start_time,
                    end_time,
                    proposal_data,
                    option_id,
                    option_data,
                    votes,
                    winner
                FROM proposal_updates
                WHERE end_time > :current_time
                    AND type IS NOT NULL
                    AND ({where_clause})
                ORDER BY share_type, share_id, proposal_id
            """)

            try:
                proposals_result = await archival_session.execute(proposals_query, {'current_time': current_time})
                proposal_records = proposals_result.fetchall()

                # Process proposal records
                proposals_by_share = {}
                for record in proposal_records:
                    share_key = f"{record.share_type}_{record.share_id}"

                    if share_key not in proposals_by_share:
                        proposals_by_share[share_key] = {
                            'proposal_id': record.proposal_id,
                            'type': record.type,
                            'start_time': record.start_time,
                            'end_time': record.end_time,
                            'proposal_data': json.loads(record.proposal_data) if record.proposal_data else {},
                            'votes_by_option': {},
                            'total_votes': 0,
                            'leading_candidate': None,
                            'leading_votes': 0
                        }

                    # Accumulate votes by option
                    if record.option_id is not None and record.votes is not None:
                        option_votes = record.votes
                        proposals_by_share[share_key]['votes_by_option'][record.option_id] = option_votes
                        proposals_by_share[share_key]['total_votes'] += option_votes

                        # Track leading candidate (only if there are actual votes)
                        if option_votes > 0 and option_votes > proposals_by_share[share_key]['leading_votes']:
                            proposals_by_share[share_key]['leading_votes'] = option_votes
                            # Extract candidate name from option_data
                            if record.option_data:
                                try:
                                    option_data = json.loads(record.option_data)
                                    if record.type == 'agent' and 'agent' in option_data:
                                        proposals_by_share[share_key]['leading_candidate'] = option_data['agent']
                                    elif record.type == 'manager' and 'manager' in option_data:
                                        proposals_by_share[share_key]['leading_candidate'] = option_data['manager']
                                except:
                                    pass

                # Build active_proposals dictionary
                for share_key, proposal_data in proposals_by_share.items():
                    has_votes = proposal_data['total_votes'] > 0
                    stage = determine_proposal_stage(
                        proposal_data['type'],
                        proposal_data['start_time'],
                        proposal_data['end_time'],
                        has_votes
                    )

                    active_proposals[share_key] = {
                        'proposal_id': proposal_data['proposal_id'],
                        'type': proposal_data['type'],
                        'stage': stage,
                        'end_time': proposal_data['end_time'],
                        'time_remaining': calculate_time_remaining(proposal_data['end_time']),
                        'total_votes': proposal_data['total_votes'],
                        'leading_candidate': proposal_data['leading_candidate'] if stage == 'voting' else None,
                        'leading_votes': proposal_data['leading_votes'] if stage == 'voting' else None
                    }

                logger.info(f"Fetched active proposals for {len(active_proposals)} shares")
            except Exception as e:
                logger.error(f"Failed to fetch proposals: {e}")
                # Continue without proposals rather than failing the entire request

    # Get profile pictures
    names_needed = set()
    agent_manager_names = set()
    for row in rows:
        if row.name:
            names_needed.add(row.name)
        if row.share_type == 'player':
            if row.agent_name:
                agent_manager_names.add(row.agent_name)
            # Also add player's club manager if exists
            if hasattr(row, 'player_club_manager_name') and row.player_club_manager_name:
                agent_manager_names.add(row.player_club_manager_name)
        elif row.share_type == 'club':
            if hasattr(row, 'club_manager_name') and row.club_manager_name:
                agent_manager_names.add(row.club_manager_name)
    
    name_to_pic = await get_profiles_for_users(list(names_needed), userconfig_session)
    agent_manager_to_pic = await get_profiles_for_users(list(agent_manager_names), userconfig_session) if agent_manager_names else {}
    
    # Build response items
    items = []
    for row in rows:
        # Determine share-specific fields
        if row.share_type == 'club':
            country_id_value = row.club_country_id
            league_id_value = row.club_league_id
            division = row.club_division
            club_id_value = row.share_id
            last_price = int(row.club_last_price) if hasattr(row, 'club_last_price') else 0
            club_balance = row.club_balance
            club_form = row.club_form if hasattr(row, 'club_form') else None
            manager_locked_value = row.club_manager_locked if hasattr(row, 'club_manager_locked') else None
            stadium_size_start = row.stadium_size_start if hasattr(row, 'stadium_size_start') else None
            stadium_size_current = row.stadium_size_current if hasattr(row, 'stadium_size_current') else None
            fans_start = row.fans_start if hasattr(row, 'fans_start') else None
            fans_current = row.fans_current if hasattr(row, 'fans_current') else None
        elif row.share_type == 'player':
            country_id_value = row.player_country_id
            league_id_value = row.player_league_id
            division = row.player_division
            club_id_value = row.player_club_id
            last_price = int(row.player_last_price) if hasattr(row, 'player_last_price') else 0
            club_balance = None
            club_form = None
            manager_locked_value = None
            stadium_size_start = None
            stadium_size_current = None
            fans_start = None
            fans_current = None
        else:
            country_id_value = None
            league_id_value = None
            division = None
            club_id_value = None
            last_price = 0
            club_balance = None
            club_form = None
            manager_locked_value = None
            stadium_size_start = None
            stadium_size_current = None
            fans_start = None
            fans_current = None
        
        # Get league position data
        position_data = league_positions.get(row.share_id, {}) if row.share_type == 'club' else {}
        
        # Get earnings data
        earnings_key = f"{row.share_type}_{row.share_id}"
        earnings = share_earnings.get(earnings_key, {})
        
        # Get profile pictures
        profile_pic = name_to_pic.get(row.name, DEFAULT_PROFILE_PIC_URL)
        
        # Convert timestamps
        last_active_unix = row.last_active_unix
        last_active = datetime.utcfromtimestamp(last_active_unix) if last_active_unix else None
        
        # Get agent/manager info
        agent_name = None
        agent_profile_pic = None
        agent_last_active_unix = None
        manager_name = None
        manager_profile_pic = None
        manager_last_active_unix = None

        if row.share_type == 'player':
            # Agent info for players
            if row.agent_name:
                agent_name = row.agent_name
                agent_profile_pic = agent_manager_to_pic.get(row.agent_name, DEFAULT_PROFILE_PIC_URL)
                agent_last_active_unix = row.agent_last_active_unix if hasattr(row, 'agent_last_active_unix') else None
            # Manager info from player's club
            if hasattr(row, 'player_club_manager_name') and row.player_club_manager_name:
                manager_name = row.player_club_manager_name
                manager_profile_pic = agent_manager_to_pic.get(row.player_club_manager_name, DEFAULT_PROFILE_PIC_URL)
                manager_last_active_unix = row.player_club_manager_last_active_unix if hasattr(row, 'player_club_manager_last_active_unix') else None
        elif row.share_type == 'club':
            # Manager info for clubs
            if hasattr(row, 'club_manager_name') and row.club_manager_name:
                manager_name = row.club_manager_name
                manager_profile_pic = agent_manager_to_pic.get(row.club_manager_name, DEFAULT_PROFILE_PIC_URL)
                manager_last_active_unix = row.club_manager_last_active_unix if hasattr(row, 'club_manager_last_active_unix') else None
        
        # Build player-specific fields
        player_fields = {}
        if row.share_type == 'player':
            # Check injury status - if injured timestamp is in the future, player is injured
            injured_timestamp = row.injured if hasattr(row, 'injured') else 0
            if injured_timestamp and injured_timestamp > now:
                injury_recovery = injured_timestamp
            else:
                injury_recovery = None
            
            # Get position names
            multi_position_value = row.multi_position if hasattr(row, 'multi_position') else None
            positions_list = get_position_names(multi_position_value) if multi_position_value else []
            
            # Get main position name (first position in the list)
            position_value = row.position if hasattr(row, 'position') else None
            main_position_name = get_position_names(position_value)[0] if position_value and get_position_names(position_value) else None
                
            player_fields = {
                'dob': row.dob if hasattr(row, 'dob') else None,
                'player_value': row.player_value if hasattr(row, 'player_value') else None,
                'wages': row.wages if hasattr(row, 'wages') else None,
                'position': position_value,
                'position_name': main_position_name,
                'multi_position': multi_position_value,
                'positions': positions_list,
                'fitness': row.fitness if hasattr(row, 'fitness') else None,
                'morale': row.morale if hasattr(row, 'morale') else None,
                'rating': row.rating if hasattr(row, 'rating') else None,
                'rating_gk': row.rating_gk if hasattr(row, 'rating_gk') else None,
                'rating_tackling': row.rating_tackling if hasattr(row, 'rating_tackling') else None,
                'rating_passing': row.rating_passing if hasattr(row, 'rating_passing') else None,
                'rating_shooting': row.rating_shooting if hasattr(row, 'rating_shooting') else None,
                'injured': injury_recovery,  # Unix timestamp when player will recover, or None if not injured
                'contract': row.contract if hasattr(row, 'contract') else None,
                'banned': row.banned if hasattr(row, 'banned') else None,
                'nationality': row.player_nationality if hasattr(row, 'player_nationality') else None,
                'desired_contract': row.desired_contract if hasattr(row, 'desired_contract') else None,
                'allow_transfer': row.allow_transfer if hasattr(row, 'allow_transfer') else None,
                'allow_renew': row.allow_renew if hasattr(row, 'allow_renew') else None,
                'loan_offered': row.loan_offered if hasattr(row, 'loan_offered') else None,
                'loan_offer_accepted': row.loan_offer_accepted if hasattr(row, 'loan_offer_accepted') else None,
                'loaned_to_club': row.loaned_to_club if hasattr(row, 'loaned_to_club') else None,
            }
            player_form_value = row.player_form if hasattr(row, 'player_form') else None
        else:
            player_form_value = None

        # Determine which form value to use
        if row.share_type == 'club':
            form_value = club_form
        else:
            form_value = player_form_value

        # Get proposal data for this share
        share_key = f"{row.share_type}_{row.share_id}"
        proposal_info = active_proposals.get(share_key, {})

        # Build proposal fields
        if proposal_info:
            has_active_proposal = True
            proposal_id = proposal_info['proposal_id']
            proposal_type = proposal_info['type']
            proposal_stage = proposal_info['stage']
            proposal_end_time_unix = proposal_info['end_time']
            proposal_time_remaining = proposal_info['time_remaining']
            proposal_total_votes = proposal_info['total_votes']
            proposal_leading_candidate = proposal_info.get('leading_candidate')
            proposal_leading_votes = proposal_info.get('leading_votes')
        else:
            has_active_proposal = False
            proposal_id = None
            proposal_type = None
            proposal_stage = None
            proposal_end_time_unix = None
            proposal_time_remaining = None
            proposal_total_votes = None
            proposal_leading_candidate = None
            proposal_leading_votes = None

        item = DetailedShareBalanceResponse(
            name=row.name,
            share_type=row.share_type,
            share_id=row.share_id,
            num=row.num,
            last_price=last_price,
            country_id=country_id_value,
            league_id=league_id_value,
            division=division,
            club_id=club_id_value,
            profile_pic=profile_pic,
            last_active_unix=last_active_unix,
            last_active=last_active,
            agent_name=agent_name,
            agent_profile_pic=agent_profile_pic,
            agent_last_active_unix=agent_last_active_unix,
            manager_name=manager_name,
            manager_profile_pic=manager_profile_pic,
            manager_last_active_unix=manager_last_active_unix,
            # Club-specific detailed fields
            club_balance=club_balance,
            manager_locked=manager_locked_value,
            old_position=position_data.get('old_position'),
            new_position=position_data.get('new_position'),
            form=form_value,
            league_form=position_data.get('form'),
            stadium_size_start=stadium_size_start,
            stadium_size_current=stadium_size_current,
            fans_start=fans_start,
            fans_current=fans_current,
            # Earnings
            earnings_7d=earnings.get('e7', 0),
            earnings_30d=earnings.get('e30', 0),
            # Player-specific fields
            **player_fields,
            # Proposal fields
            has_active_proposal=has_active_proposal,
            proposal_id=proposal_id,
            proposal_type=proposal_type,
            proposal_stage=proposal_stage,
            proposal_end_time_unix=proposal_end_time_unix,
            proposal_time_remaining=proposal_time_remaining,
            proposal_total_votes=proposal_total_votes,
            proposal_leading_candidate=proposal_leading_candidate,
            proposal_leading_votes=proposal_leading_votes
        )
        items.append(item)
    
    return PaginatedResponse(
        page=page,
        per_page=per_page,
        total=total,
        total_pages=total_pages,
        items=items,
    )
