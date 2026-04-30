# modules/players.py
from sqlalchemy import (
    Column,
    Integer,
    BigInteger,
    String,
    DateTime,
    select,
    func,
    Text,
    or_
)
import logging
from typing import List, Optional
from pydantic import BaseModel
from fastapi import APIRouter, Depends, Query, HTTPException, Request
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from enum import Enum
from .base import (
    Base,
    get_mysql_session,
    get_archival_session,
    get_playerhistory_sqlite_session,
    calculate_age,
    PaginatedResponse,
    PerPageOptions,
    parse_json_field,
    fetch_paginated_data,
    DCPlayers,
    DCShareBalances,
    PlayerLoanUpdates,
    PlayerUpdates,
    Blocks,
    PlayerHistory,
    InjuryHistory,
    Messages,
)

from modules.base import get_userconfig_session, DEFAULT_PROFILE_PIC_URL
from modules.utils.profile import get_profiles_for_users

MESSAGE_TYPE_TRANSFER = 9
MESSAGE_TYPE_LEFT_TO_FREE_BENCH = 82

# Define positions mapping
positions_mapping = {
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
    "FR": 32768,
}

class DCPlayersTrading(Base):
    __tablename__ = "dc_players_trading"
    player_id = Column(Integer, primary_key=True)
    last_price = Column(BigInteger)
    volume_1_day = Column(BigInteger)
    volume_7_day = Column(BigInteger)
    last_7days = Column(Text)
    last_7days_price = Column(Text)
    updated_at = Column(DateTime)

# Updated enum for sorting fields
class PlayersSortBy(str, Enum):
    player_id = "player_id"
    last_price = "last_price"
    volume_1_day = "volume_1_day"
    volume_7_day = "volume_7_day"
    wages = "wages"
    rating = "rating"
    rating_gk = "rating_gk"
    rating_tackling = "rating_tackling"
    rating_passing = "rating_passing"
    rating_shooting = "rating_shooting"
    rating_stamina = "rating_stamina"
    rating_aggression = "rating_aggression"
    country_id = "country_id"
    dob = "dob"
    value = "value"
    club_id = "club_id"
    agent_name = "agent_name"
    position = "position"

# Pydantic models for responses
class PlayerResponse(BaseModel):
    player_id: int
    last_price: int = 0
    volume_1_day: int = 0
    volume_7_day: int = 0
    last_7days: Optional[List[int]] = None
    last_7days_price: Optional[List[int]] = None
    wages: Optional[int]
    multi_position: Optional[int]
    positions: Optional[List[str]] = []
    rating: Optional[int]
    country_id: Optional[str]
    dob: Optional[int]
    value: Optional[int]
    club_id: Optional[int]
    agent_name: Optional[str]
    last_transfer: Optional[int]
    age: Optional[int]
    profile_pic: Optional[str] = None
    agent_last_active_unix: Optional[int] = None

class PlayerLoanResponse(BaseModel):
    season_id: int
    from_club_id: int
    to_club_id: int
    loan_start_date: int

class PlayerRatingResponse(BaseModel):
    rating: int
    rating_gk: int
    rating_tackling: int
    rating_passing: int
    rating_shooting: int
    rating_aggression: int
    rating_stamina: int
    date_updated: int

class PlayerInjuryResponse(BaseModel):
    injury_id: int
    start_date: int
    end_date: int
    season_id: int


class PlayerTransferResponse(BaseModel):
    date: int
    club_id_from: int
    club_id_to: Optional[int]
    amount: int


class TransferHistoryItem(BaseModel):
    player_id: int
    date: int
    club_id_from: int
    club_id_to: Optional[int]
    amount: int


class LoanHistoryItem(BaseModel):
    player_id: int
    loan_start_date: int
    from_club_id: int
    to_club_id: int
    season_id: int


class PlayerHistoryResponse(BaseModel):
    player_id: int
    loans: List[PlayerLoanResponse]
    ratings: List[PlayerRatingResponse]
    injuries: List[PlayerInjuryResponse]
    transfers: List[PlayerTransferResponse]

class PlayerDetailedResponse(BaseModel):
    player_id: int
    fitness: Optional[int]
    retired: Optional[int]
    morale: Optional[int]
    injured: Optional[int]
    injury_id: Optional[int]
    wages: Optional[int]
    contract: Optional[int]
    form: Optional[str]
    position: Optional[int]
    position_main: Optional[str] = None
    multi_position: Optional[int]
    positions: Optional[List[str]] = []
    rating: Optional[int]
    rating_gk: Optional[int]
    rating_tackling: Optional[int]
    rating_passing: Optional[int]
    rating_shooting: Optional[int]
    rating_stamina: Optional[int]
    rating_aggression: Optional[int]
    banned: Optional[int]
    cup_tied: Optional[int]
    yellow_cards: Optional[int]
    red_cards: Optional[int]
    dob: Optional[int]
    side: Optional[str]
    value: Optional[int]
    country_id: Optional[str]
    club_id: Optional[int]
    agent_name: Optional[str]
    last_transfer: Optional[int]
    desired_contract: Optional[int]
    allow_transfer: Optional[int]
    allow_renew: Optional[int]
    # Loan columns
    loan_offered: Optional[int]
    loan_offer_accepted: Optional[int]
    loaned_to_club: Optional[int]
    age: Optional[int]
    # Trading data
    last_price: int = 0
    volume_1_day: int = 0
    volume_7_day: int = 0
    last_7days: Optional[List[int]] = None
    last_7days_price: Optional[List[int]] = None
    profile_pic: Optional[str] = None
    agent_last_active_unix: Optional[int] = None

players_router = APIRouter()

# -----------------------------------------------------------------------------
# Helper: Calculate DOB range from age filters
# -----------------------------------------------------------------------------
def calculate_dob_range(age_min: Optional[int], age_max: Optional[int]):
    """
    Calculate the Unix timestamp range for date of birth (dob)
    based on age_min and age_max.

    Returns a tuple (dob_min_timestamp, dob_max_timestamp)

    dob_min_timestamp: players born on or after this timestamp (i.e., younger players)
    dob_max_timestamp: players born on or before this timestamp (i.e., older players)
    """
    now = datetime.utcnow()
    dob_min_timestamp = None
    dob_max_timestamp = None

    if age_min is not None:
        # Players must be at least age_min years old => born on or before now - age_min years
        dob_max_date = now - relativedelta(years=age_min)
        # End of that day
        dob_max_date = dob_max_date.replace(hour=23, minute=59, second=59, microsecond=999999)
        dob_max_timestamp = int(dob_max_date.timestamp())

    if age_max is not None:
        # Players must be at most age_max years old => born on or after now - age_max years
        dob_min_date = now - relativedelta(years=age_max)
        # Start of that day
        dob_min_date = dob_min_date.replace(hour=0, minute=0, second=0, microsecond=0)
        dob_min_timestamp = int(dob_min_date.timestamp())

    return dob_min_timestamp, dob_max_timestamp

# -----------------------------------------------------------------------------
# Query Parameter Classes for Players Endpoints
# -----------------------------------------------------------------------------
class BasicPlayerQueryParams:
    """
    Query parameters for the basic /players endpoint.
    Only includes filters relevant to the basic player data.
    """
    def __init__(
        self,
        request: Request,
        page: int = Query(1, ge=1, description="Page number, starting from 1"),
        per_page: PerPageOptions = Query(
            PerPageOptions.twenty,
            description="Number of records per page (options: 5, 10, 20, 50)"
        ),
        sort_by: Optional[PlayersSortBy] = Query(None, description="Field to sort by"),
        sort_order: Optional[str] = Query(
            "asc", description="Sort order: 'asc' or 'desc'", regex="^(asc|desc)$"
        ),
        age_min: Optional[int] = Query(None, ge=0, description="Minimum age for players"),
        age_max: Optional[int] = Query(None, ge=0, description="Maximum age for players"),
        age: Optional[int] = Query(None, ge=0, description="Exact age for players"),
        player_id: Optional[List[int]] = Query(None, description="Filter by specific player ID(s)"),
        wages_min: Optional[int] = Query(None, description="Minimum wages"),
        wages_max: Optional[int] = Query(None, description="Maximum wages"),
        rating_min: Optional[int] = Query(None, description="Minimum rating"),
        rating_max: Optional[int] = Query(None, description="Maximum rating"),
        value_min: Optional[int] = Query(None, description="Minimum value"),
        value_max: Optional[int] = Query(None, description="Maximum value"),
        country_id: Optional[str] = Query(None, description="Country ID(s) to filter players (comma-separated for multiple values)"),
        owned: Optional[str] = Query(None, description="Filter players owned by the given name"),
        positions: Optional[str] = Query(None, description="Comma-separated list of positions to filter players"),
        club_id: Optional[int] = Query(None, description="Filter by specific club ID"),
        allow_transfer: Optional[int] = Query(None, description="Filter by allow_transfer value"),
        allow_renew: Optional[int] = Query(None, description="Filter by allow_renew value"),
    ):
        self.page = page
        self.per_page = per_page.value if isinstance(per_page, PerPageOptions) else per_page
        self.sort_by = sort_by
        self.sort_order = sort_order
        self.age_min = age_min
        self.age_max = age_max
        self.age = age
        self.player_id = player_id
        self.wages_min = wages_min
        self.wages_max = wages_max
        self.rating_min = rating_min
        self.rating_max = rating_max
        self.value_min = value_min
        self.value_max = value_max
        self.country_id = country_id
        self.owned = owned
        self.positions = positions
        self.club_id = club_id
        self.allow_transfer = allow_transfer
        self.allow_renew = allow_renew

        if self.age is not None and (self.age_min is not None or self.age_max is not None):
            raise HTTPException(
                status_code=400,
                detail="Cannot specify both 'age' and 'age_min'/'age_max'. Please choose one."
            )
        if self.age is not None:
            self.age_min = self.age_max = self.age

        all_query_params = set(request.query_params.keys())
        accepted_params = {
            "page", "per_page", "sort_by", "sort_order", "age_min", "age_max", "age",
            "player_id", "wages_min", "wages_max", "rating_min", "rating_max",
            "value_min", "value_max", "country_id", "owned", "positions", "club_id",
            "allow_transfer", "allow_renew"
        }
        unexpected_params = all_query_params - accepted_params
        if unexpected_params:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid query parameters: {', '.join(unexpected_params)}"
            )
        self.field_min_max = {
            "wages": (wages_min, wages_max, None),
            "rating": (rating_min, rating_max, None),
            "value": (value_min, value_max, None)
        }

class DetailedPlayerQueryParams:
    """
    Query parameters for the /players/detailed endpoint.
    Includes all basic filters plus extended filters for detailed player data.
    """
    def __init__(
        self,
        request: Request,
        page: int = Query(1, ge=1, description="Page number, starting from 1"),
        per_page: PerPageOptions = Query(
            PerPageOptions.twenty,
            description="Number of records per page (options: 5, 10, 20, 50)"
        ),
        sort_by: Optional[PlayersSortBy] = Query(None, description="Field to sort by"),
        sort_order: Optional[str] = Query(
            "asc", description="Sort order: 'asc' or 'desc'", regex="^(asc|desc)$"
        ),
        age_min: Optional[int] = Query(None, ge=0, description="Minimum age for players"),
        age_max: Optional[int] = Query(None, ge=0, description="Maximum age for players"),
        age: Optional[int] = Query(None, ge=0, description="Exact age for players"),
        player_id: Optional[List[int]] = Query(None, description="Filter by specific player ID(s)"),
        wages_min: Optional[int] = Query(None, description="Minimum wages"),
        wages_max: Optional[int] = Query(None, description="Maximum wages"),
        contract_min: Optional[int] = Query(None, description="Minimum contract"),
        contract_max: Optional[int] = Query(None, description="Maximum contract"),
        rating_min: Optional[int] = Query(None, description="Minimum rating"),
        rating_max: Optional[int] = Query(None, description="Maximum rating"),
        rating_gk_min: Optional[int] = Query(None, description="Minimum rating_gk"),
        rating_gk_max: Optional[int] = Query(None, description="Maximum rating_gk"),
        rating_tackling_min: Optional[int] = Query(None, description="Minimum rating_tackling"),
        rating_tackling_max: Optional[int] = Query(None, description="Maximum rating_tackling"),
        rating_passing_min: Optional[int] = Query(None, description="Minimum rating_passing"),
        rating_passing_max: Optional[int] = Query(None, description="Maximum rating_passing"),
        rating_shooting_min: Optional[int] = Query(None, description="Minimum rating_shooting"),
        rating_shooting_max: Optional[int] = Query(None, description="Maximum rating_shooting"),
        rating_aggression_min: Optional[int] = Query(None, description="Minimum rating_aggression"),
        rating_aggression_max: Optional[int] = Query(None, description="Maximum rating_aggression"),
        rating_stamina_min: Optional[int] = Query(None, description="Minimum rating_stamina"),
        rating_stamina_max: Optional[int] = Query(None, description="Maximum rating_stamina"),
        value_min: Optional[int] = Query(None, description="Minimum value"),
        value_max: Optional[int] = Query(None, description="Maximum value"),
        desired_contract_min: Optional[int] = Query(None, description="Minimum desired_contract"),
        desired_contract_max: Optional[int] = Query(None, description="Maximum desired_contract"),
        country_id: Optional[str] = Query(None, description="Country ID(s) to filter players (comma-separated for multiple values)"),
        owned: Optional[str] = Query(None, description="Filter players owned by the given name"),
        positions: Optional[str] = Query(None, description="Comma-separated list of positions to filter players"),
        club_id: Optional[int] = Query(None, description="Filter by specific club ID"),
        include_loaned: bool = Query(False, description="Include players on loan to the club when filtering by club_id"),
        allow_transfer: Optional[int] = Query(None, description="Filter by allow_transfer value"),
        allow_renew: Optional[int] = Query(None, description="Filter by allow_renew value"),
        fitness: Optional[int] = Query(None, description="Filter by fitness"),
        retired: Optional[int] = Query(None, description="Filter by retired"),
        morale: Optional[int] = Query(None, description="Filter by morale"),
        injured: Optional[int] = Query(None, description="Filter by injured"),
        injury_id: Optional[int] = Query(None, description="Filter by injury_id"),
        position: Optional[int] = Query(None, description="Filter by position"),
        multi_position: Optional[int] = Query(None, description="Filter by multi_position"),
        ability_gk: Optional[int] = Query(None, description="Filter by ability_gk"),
        ability_tackling: Optional[int] = Query(None, description="Filter by ability_tackling"),
        ability_passing: Optional[int] = Query(None, description="Filter by ability_passing"),
        ability_shooting: Optional[int] = Query(None, description="Filter by ability_shooting"),
        banned: Optional[int] = Query(None, description="Filter by banned"),
        cup_tied: Optional[int] = Query(None, description="Filter by cup_tied"),
        yellow_cards: Optional[int] = Query(None, description="Filter by yellow_cards"),
        red_cards: Optional[int] = Query(None, description="Filter by red_cards"),
        agent_name: Optional[str] = Query(None, description="Filter by agent name, or 'NULL'/'NOT_NULL'"),
        loan_offered: Optional[str] = Query(None, description="Filter by loan_offered season ID, or 'NULL'/'NOT_NULL'"),
        loan_offer_accepted: Optional[str] = Query(None, description="Filter by loan_offer_accepted, or 'NULL'/'NOT_NULL'"),
        loaned_to_club: Optional[str] = Query(None, description="Filter by loaned_to_club, or 'NULL'/'NOT_NULL'"),
    ):
        self.page = page
        self.per_page = per_page.value if isinstance(per_page, PerPageOptions) else per_page
        self.sort_by = sort_by
        self.sort_order = sort_order
        self.age_min = age_min
        self.age_max = age_max
        self.age = age
        self.player_id = player_id
        self.wages_min = wages_min
        self.wages_max = wages_max
        self.contract_min = contract_min
        self.contract_max = contract_max
        self.rating_min = rating_min
        self.rating_max = rating_max
        self.rating_gk_min = rating_gk_min
        self.rating_gk_max = rating_gk_max
        self.rating_tackling_min = rating_tackling_min
        self.rating_tackling_max = rating_tackling_max
        self.rating_passing_min = rating_passing_min
        self.rating_passing_max = rating_passing_max
        self.rating_shooting_min = rating_shooting_min
        self.rating_shooting_max = rating_shooting_max
        self.rating_aggression_min = rating_aggression_min
        self.rating_aggression_max = rating_aggression_max
        self.rating_stamina_min = rating_stamina_min
        self.rating_stamina_max = rating_stamina_max
        self.value_min = value_min
        self.value_max = value_max
        self.desired_contract_min = desired_contract_min
        self.desired_contract_max = desired_contract_max
        self.country_id = country_id
        self.owned = owned
        self.positions = positions
        self.club_id = club_id
        self.include_loaned = include_loaned
        self.allow_transfer = allow_transfer
        self.allow_renew = allow_renew
        self.fitness = fitness
        self.retired = retired
        self.morale = morale
        self.injured = injured
        self.injury_id = injury_id
        self.position = position
        self.multi_position = multi_position
        self.ability_gk = ability_gk
        self.ability_tackling = ability_tackling
        self.ability_passing = ability_passing
        self.ability_shooting = ability_shooting
        self.banned = banned
        self.cup_tied = cup_tied
        self.yellow_cards = yellow_cards
        self.red_cards = red_cards
        self.agent_name = agent_name
        self.loan_offered = loan_offered
        self.loan_offer_accepted = loan_offer_accepted
        self.loaned_to_club = loaned_to_club

        if self.age is not None and (self.age_min is not None or self.age_max is not None):
            raise HTTPException(
                status_code=400,
                detail="Cannot specify both 'age' and 'age_min'/'age_max'. Please choose one."
            )
        if self.age is not None:
            self.age_min = self.age_max = self.age

        all_query_params = set(request.query_params.keys())
        accepted_params = {
            "page", "per_page", "sort_by", "sort_order", "age_min", "age_max", "age",
            "player_id", "wages_min", "wages_max", "contract_min", "contract_max",
            "rating_min", "rating_max", "rating_gk_min", "rating_gk_max", "rating_tackling_min",
            "rating_tackling_max", "rating_passing_min", "rating_passing_max", "rating_shooting_min",
            "rating_shooting_max", "rating_aggression_min", "rating_aggression_max",
            "rating_stamina_min", "rating_stamina_max", "value_min", "value_max",
            "desired_contract_min", "desired_contract_max", "country_id", "owned", "positions",
            "club_id", "include_loaned", "allow_transfer", "allow_renew", "fitness", "retired", "morale", "injured",
            "injury_id", "position", "multi_position", "ability_gk", "ability_tackling",
            "ability_passing", "ability_shooting", "banned", "cup_tied", "yellow_cards", "red_cards",
            "agent_name", "loan_offered", "loan_offer_accepted", "loaned_to_club"
        }
        unexpected_params = all_query_params - accepted_params
        if unexpected_params:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid query parameters: {', '.join(unexpected_params)}"
            )
        self.field_min_max = {}
        detailed_fields = [
            "fitness", "retired", "morale", "injured", "injury_id", "wages", "contract",
            "position", "multi_position", "rating", "rating_gk", "rating_tackling",
            "rating_passing", "rating_shooting", "rating_aggression", "rating_stamina",
            "ability_gk", "ability_tackling", "ability_passing", "ability_shooting",
            "banned", "cup_tied", "yellow_cards", "red_cards", "value",
            "desired_contract", "allow_transfer", "allow_renew"
        ]
        for field in detailed_fields:
            min_val = locals().get(f"{field}_min")
            max_val = locals().get(f"{field}_max")
            exact_val = locals().get(field)
            self.field_min_max[field] = (min_val, max_val, exact_val)

# -----------------------------------------------------------------------------
# Helper function to apply special filters (NULL, NOT_NULL, or exact value)
# -----------------------------------------------------------------------------
def apply_nullable_filter(select_query, total_query, column, value):
    """
    Applies filtering to a query for a given column based on a value that can be
    an exact match, "NULL", or "NOT_NULL". It also handles type conversion for
    integer columns.
    """
    if value == "NULL":
        return select_query.where(column.is_(None)), total_query.where(column.is_(None))
    if value == "NOT_NULL":
        return select_query.where(column.isnot(None)), total_query.where(column.isnot(None))

    final_value = value
    if isinstance(column.type, (Integer, BigInteger)):
        try:
            final_value = int(value)
        except (ValueError, TypeError):
            raise HTTPException(
                status_code=400,
                detail=f"Invalid value '{value}' for field '{column.name}'. Must be an integer, 'NULL', or 'NOT_NULL'."
            )

    return select_query.where(column == final_value), total_query.where(column == final_value)

# -----------------------------------------------------------------------------
# Helper function to filter and fetch players
# -----------------------------------------------------------------------------
async def get_filtered_players(
    session: AsyncSession,
    userconfig_session: AsyncSession,
    params,
    detailed: bool = False
):
    per_page = params.per_page
    page = params.page
    sort_order = params.sort_order
    chosen_sort = params.sort_by.value if params.sort_by else None

    # Define table aliases
    players_alias = DCPlayers.__table__.alias("players_alias")
    trading_alias = DCPlayersTrading.__table__.alias("trading_alias")
    share_balances_alias = DCShareBalances.__table__.alias("share_balances_alias")
    from .base import DCUsers
    dc_users_alias = DCUsers.__table__.alias("dc_users_alias")

    extra_filters = []

    if params.owned:
        # When filtering by 'owned', use the share_balances table as base
        base_query = share_balances_alias
        extra_filters.append(share_balances_alias.c.name == params.owned)
        # Ensure exact match including trailing spaces
        extra_filters.append(func.char_length(share_balances_alias.c.name) == len(params.owned))
        extra_filters.append(share_balances_alias.c.share_type == "player")
        extra_filters.append(share_balances_alias.c.num > 0)
        join_clause = base_query.outerjoin(
            players_alias,
            share_balances_alias.c.share_id == players_alias.c.player_id
        ).outerjoin(
            trading_alias,
            share_balances_alias.c.share_id == trading_alias.c.player_id
        ).outerjoin(
            dc_users_alias,
            players_alias.c.agent_name == dc_users_alias.c.name
        )
        select_columns = [
            share_balances_alias.c.share_id.label("player_id"),
            trading_alias.c.last_price,
            trading_alias.c.volume_1_day,
            trading_alias.c.volume_7_day,
            trading_alias.c.last_7days,
            trading_alias.c.last_7days_price,
        ]
        exclude_fields = ["checksum", "updated_at"]
        if detailed:
            select_columns.extend([
                players_alias.c[field.name] for field in DCPlayers.__table__.columns
                if field.name not in exclude_fields
            ])
            select_columns.append(dc_users_alias.c.last_active.label("agent_last_active_unix"))
            sortable_fields = {
                **{
                    column.name: players_alias.c[column.name]
                    for column in DCPlayers.__table__.columns
                    if column.name not in exclude_fields
                },
                "player_id": share_balances_alias.c.share_id,
                "last_price": trading_alias.c.last_price,
                "volume_1_day": trading_alias.c.volume_1_day,
                "volume_7_day": trading_alias.c.volume_7_day,
            }
        else:
            select_columns.extend([
                players_alias.c.wages,
                players_alias.c.multi_position,
                players_alias.c.rating,
                players_alias.c.country_id,
                players_alias.c.dob,
                players_alias.c.value,
                players_alias.c.club_id,
                players_alias.c.agent_name,
                players_alias.c.last_transfer,
                dc_users_alias.c.last_active.label("agent_last_active_unix"),
            ])
            sortable_fields = {
                "player_id": share_balances_alias.c.share_id,
                "last_price": func.coalesce(trading_alias.c.last_price, 0),
                "volume_1_day": func.coalesce(trading_alias.c.volume_1_day, 0),
                "volume_7_day": func.coalesce(trading_alias.c.volume_7_day, 0),
                "wages": players_alias.c.wages,
                "multi_position": players_alias.c.multi_position,
                "rating": players_alias.c.rating,
                "country_id": players_alias.c.country_id,
                "dob": players_alias.c.dob,
                "value": players_alias.c.value,
                "club_id": players_alias.c.club_id,
                "agent_name": players_alias.c.agent_name,
                "position": players_alias.c.position,
            }
    else:
        if detailed:
            base_query = players_alias
            join_clause = base_query.outerjoin(
                trading_alias,
                players_alias.c.player_id == trading_alias.c.player_id
            ).outerjoin(
                dc_users_alias,
                players_alias.c.agent_name == dc_users_alias.c.name
            )
            exclude_fields = ["checksum", "updated_at"]
            select_columns = [
                players_alias.c[field.name] for field in DCPlayers.__table__.columns
                if field.name not in exclude_fields
            ] + [
                trading_alias.c.last_price,
                trading_alias.c.volume_1_day,
                trading_alias.c.volume_7_day,
                trading_alias.c.last_7days,
                trading_alias.c.last_7days_price,
                dc_users_alias.c.last_active.label("agent_last_active_unix"),
            ]
            sortable_fields = {
                **{
                    column.name: players_alias.c[column.name]
                    for column in DCPlayers.__table__.columns
                    if column.name not in exclude_fields
                },
                "player_id": players_alias.c.player_id,
                "last_price": trading_alias.c.last_price,
                "volume_1_day": trading_alias.c.volume_1_day,
                "volume_7_day": trading_alias.c.volume_7_day,
            }
        else:
            base_query = trading_alias
            join_clause = base_query.outerjoin(
                players_alias,
                trading_alias.c.player_id == players_alias.c.player_id
            ).outerjoin(
                dc_users_alias,
                players_alias.c.agent_name == dc_users_alias.c.name
            )
            select_columns = [
                trading_alias.c.player_id,
                trading_alias.c.last_price,
                trading_alias.c.volume_1_day,
                trading_alias.c.volume_7_day,
                trading_alias.c.last_7days,
                trading_alias.c.last_7days_price,
                players_alias.c.wages,
                players_alias.c.multi_position,
                players_alias.c.rating,
                players_alias.c.country_id,
                players_alias.c.dob,
                players_alias.c.value,
                players_alias.c.club_id,
                players_alias.c.agent_name,
                players_alias.c.last_transfer,
                dc_users_alias.c.last_active.label("agent_last_active_unix"),
            ]
            sortable_fields = {
                "player_id": trading_alias.c.player_id,
                "last_price": trading_alias.c.last_price,
                "volume_1_day": trading_alias.c.volume_1_day,
                "volume_7_day": trading_alias.c.volume_7_day,
                "wages": players_alias.c.wages,
                "multi_position": players_alias.c.multi_position,
                "rating": players_alias.c.rating,
                "country_id": players_alias.c.country_id,
                "dob": players_alias.c.dob,
                "value": players_alias.c.value,
                "club_id": players_alias.c.club_id,
                "agent_name": players_alias.c.agent_name,
                "position": players_alias.c.position,
            }

    select_query = select(*select_columns).select_from(join_clause)
    # Make sure to count distinct player IDs to avoid duplicates from joins
    total_query = select(func.count(func.distinct(players_alias.c.player_id))).select_from(join_clause)

    # Apply extra filters
    for condition in extra_filters:
        select_query = select_query.where(condition)
        total_query = total_query.where(condition)

    # Apply age filters if provided
    if params.age_min is not None or params.age_max is not None:
        select_query = select_query.where(players_alias.c.dob.isnot(None))
        select_query = select_query.where(players_alias.c.dob > 0)
        total_query = total_query.where(players_alias.c.dob.isnot(None))
        total_query = total_query.where(players_alias.c.dob > 0)
        dob_min_timestamp, dob_max_timestamp = calculate_dob_range(params.age_min, params.age_max)
        if dob_min_timestamp is not None:
            select_query = select_query.where(players_alias.c.dob >= dob_min_timestamp)
            total_query = total_query.where(players_alias.c.dob >= dob_min_timestamp)
        if dob_max_timestamp is not None:
            select_query = select_query.where(players_alias.c.dob <= dob_max_timestamp)
            total_query = total_query.where(players_alias.c.dob <= dob_max_timestamp)

    # Apply other min, max, and exact filters dynamically
    for field_name, (min_value, max_value, exact_value) in params.field_min_max.items():
        if hasattr(players_alias.c, field_name):
            column = players_alias.c[field_name]
            if exact_value is not None:
                select_query = select_query.where(column == exact_value)
                total_query = total_query.where(column == exact_value)
            else:
                if min_value is not None:
                    select_query = select_query.where(column >= min_value)
                    total_query = total_query.where(column >= min_value)
                if max_value is not None:
                    select_query = select_query.where(column <= max_value)
                    total_query = total_query.where(column <= max_value)
        else:
            if (min_value is not None or max_value is not None or exact_value is not None):
                raise HTTPException(status_code=400, detail=f"Invalid filter field: {field_name}")

    if params.country_id is not None:
        country_list = [country.strip().upper() for country in params.country_id.split(',')]
        if len(country_list) == 1:
            select_query = select_query.where(players_alias.c.country_id == country_list[0])
            total_query = total_query.where(players_alias.c.country_id == country_list[0])
        else:
            select_query = select_query.where(players_alias.c.country_id.in_(country_list))
            total_query = total_query.where(players_alias.c.country_id.in_(country_list))

    if params.club_id is not None:
        # Check if we should include loaned players
        include_loaned = getattr(params, 'include_loaned', False)
        
        if include_loaned:
            # Include players at the club OR on loan to the club
            club_filter = or_(
                players_alias.c.club_id == params.club_id,
                players_alias.c.loaned_to_club == params.club_id
            )
            select_query = select_query.where(club_filter)
            total_query = total_query.where(club_filter)
        else:
            # Just filter by club_id
            select_query = select_query.where(players_alias.c.club_id == params.club_id)
            total_query = total_query.where(players_alias.c.club_id == params.club_id)

    if params.player_id is not None:
        if params.owned:
            select_query = select_query.where(share_balances_alias.c.share_id.in_(params.player_id))
            total_query = total_query.where(share_balances_alias.c.share_id.in_(params.player_id))
        else:
            select_query = select_query.where(players_alias.c.player_id.in_(params.player_id))
            total_query = total_query.where(players_alias.c.player_id.in_(params.player_id))

    if params.positions:
        positions_list = [pos.strip().upper() for pos in params.positions.split(',')]
        position_values = []
        for pos in positions_list:
            if pos in positions_mapping:
                position_values.append(positions_mapping[pos])
            else:
                raise HTTPException(status_code=400, detail=f"Invalid position: {pos}")
        position_conditions = [(players_alias.c.multi_position.op('&')(value) != 0) for value in position_values]
        multi_position_filter = or_(*position_conditions)
        select_query = select_query.where(multi_position_filter)
        total_query = total_query.where(multi_position_filter)

    if hasattr(params, 'loan_offered') and params.loan_offered is not None:
        select_query, total_query = apply_nullable_filter(
            select_query, total_query, players_alias.c.loan_offered, params.loan_offered
        )

    if hasattr(params, 'loan_offer_accepted') and params.loan_offer_accepted is not None:
        select_query, total_query = apply_nullable_filter(
            select_query, total_query, players_alias.c.loan_offer_accepted, params.loan_offer_accepted
        )

    if hasattr(params, 'loaned_to_club') and params.loaned_to_club is not None:
        select_query, total_query = apply_nullable_filter(
            select_query, total_query, players_alias.c.loaned_to_club, params.loaned_to_club
        )

    if hasattr(params, 'agent_name') and params.agent_name is not None:
        select_query, total_query = apply_nullable_filter(
            select_query, total_query, players_alias.c.agent_name, params.agent_name
        )

    try:
        total, total_pages, rows = await fetch_paginated_data(
            session,
            select_query,
            total_query,
            sortable_fields,
            chosen_sort,
            sort_order,
            page,
            per_page,
            extra_filters=[],
        )
    except KeyError:
        raise HTTPException(status_code=400, detail=f"Invalid sort_by field: {chosen_sort}")

    rows_data = [dict(row._mapping) for row in rows]
    agent_names_needed = {row_dict.get("agent_name") for row_dict in rows_data if row_dict.get("agent_name")}
    name_to_pic = {}
    if agent_names_needed:
        name_to_pic = await get_profiles_for_users(list(agent_names_needed), userconfig_session)

    items = []
    for row_dict in rows_data:
        dob_value = row_dict.get("dob")
        if dob_value is not None and dob_value > 0:
            age = calculate_age(dob_value)
        else:
            age = None
            dob_value = None

        last_7days_price = parse_json_field(row_dict.get("last_7days_price"))
        last_7days = parse_json_field(row_dict.get("last_7days"))

        multi_position_value = row_dict.get("multi_position")
        positions_list = []
        if multi_position_value is not None:
            for pos_name, pos_value in positions_mapping.items():
                if multi_position_value & pos_value:
                    positions_list.append(pos_name)

        agent_name = row_dict.get("agent_name")
        agent_pic = name_to_pic.get(agent_name, DEFAULT_PROFILE_PIC_URL) if agent_name else None

        if detailed:
            # Get main position name from position field
            position_value = row_dict.get("position")
            position_main = None
            if position_value is not None:
                for pos_name, pos_value in positions_mapping.items():
                    if position_value & pos_value:
                        position_main = pos_name
                        break
            
            player_response_data = {
                key: row_dict.get(key)
                for key in PlayerDetailedResponse.__fields__.keys()
                if key not in ["age", "dob", "last_7days", "last_7days_price", "positions", "profile_pic", "position_main"]
            }
            player_response_data["dob"] = dob_value
            player_response_data["age"] = age
            player_response_data["last_7days"] = last_7days
            player_response_data["last_7days_price"] = last_7days_price
            player_response_data["positions"] = positions_list
            player_response_data["position_main"] = position_main
            player_response_data["profile_pic"] = agent_pic
            player_response_data["last_price"] = int(row_dict.get("last_price") or 0)
            player_response_data["volume_1_day"] = int(row_dict.get("volume_1_day") or 0)
            player_response_data["volume_7_day"] = int(row_dict.get("volume_7_day") or 0)
            player_response = PlayerDetailedResponse(**player_response_data)
        else:
            player_response = PlayerResponse(
                player_id=row_dict.get("player_id"),
                last_price=int(row_dict.get("last_price") or 0),
                volume_1_day=int(row_dict.get("volume_1_day") or 0),
                volume_7_day=int(row_dict.get("volume_7_day") or 0),
                last_7days=last_7days,
                last_7days_price=last_7days_price,
                wages=row_dict.get("wages"),
                multi_position=row_dict.get("multi_position"),
                positions=positions_list,
                rating=row_dict.get("rating"),
                country_id=row_dict.get("country_id"),
                dob=dob_value,
                age=age,
                value=row_dict.get("value"),
                club_id=row_dict.get("club_id"),
                agent_name=agent_name,
                last_transfer=row_dict.get("last_transfer"),
                profile_pic=agent_pic,
                agent_last_active_unix=row_dict.get("agent_last_active_unix"),
            )
        items.append(player_response)

    return PaginatedResponse(
        page=page,
        per_page=per_page,
        total=total,
        total_pages=total_pages,
        items=items,
    )

# -----------------------------------------------------------------------------
# Routes for /players and /players/detailed endpoints
# -----------------------------------------------------------------------------
@players_router.get(
    "/players",
    response_model=PaginatedResponse[PlayerResponse],
    summary="Retrieve list of soccer players",
    description="Returns a paginated list of players, including trading data such as last price, volume, and other filterable attributes like club, position, and age."
)
async def get_players(
    params: BasicPlayerQueryParams = Depends(),
    session: AsyncSession = Depends(get_mysql_session),
    userconfig_session: AsyncSession = Depends(get_userconfig_session),
):
    return await get_filtered_players(session, userconfig_session, params, detailed=False)

@players_router.get(
    "/players/detailed",
    response_model=PaginatedResponse[PlayerDetailedResponse],
    summary="Retrieve detailed soccer player data",
    description="Provides a paginated list of players with additional fields such as fitness, injuries, contract details, rating breakdowns, and more comprehensive stats."
)
async def get_players_detailed(
    params: DetailedPlayerQueryParams = Depends(),
    session: AsyncSession = Depends(get_mysql_session),
    userconfig_session: AsyncSession = Depends(get_userconfig_session),
):
    return await get_filtered_players(session, userconfig_session, params, detailed=True)

async def _fetch_transfer_history(
    archival_session: AsyncSession,
    player_id: Optional[int] = None,
    season_id: Optional[int] = None,
    club_filter: Optional[int] = None
) -> List[TransferHistoryItem]:
    # Build where clauses
    where_clauses = []
    where_clauses.append(Messages.type.in_([MESSAGE_TYPE_TRANSFER, MESSAGE_TYPE_LEFT_TO_FREE_BENCH]))
    if player_id is not None:
        where_clauses.append(Messages.data_1 == player_id)
    if season_id is not None:
        where_clauses.append(Messages.season_id == season_id)
    if club_filter is not None:
        where_clauses.append(or_(Messages.club_1 == club_filter, Messages.club_2 == club_filter))

    query = (
        select(
            Blocks.date.label("date"),
            Messages.type,
            Messages.club_1,
            Messages.club_2,
            Messages.data_2.label("amount"),
            Messages.data_1.label("player_id"),
        )
        .select_from(
            Messages.__table__.join(Blocks.__table__, Messages.height == Blocks.height)
        )
        .where(*where_clauses)
        .order_by(Messages.id)
    )
    result = await archival_session.execute(query)
    rows = result.fetchall()
    transfers: List[TransferHistoryItem] = []
    for row in rows:
        if row.type == MESSAGE_TYPE_TRANSFER:
            club_from = row.club_2
            club_to = row.club_1
        else:
            club_from = row.club_1
            club_to = None
        transfers.append(TransferHistoryItem(
            player_id=row.player_id,
            date=row.date,
            club_id_from=club_from,
            club_id_to=club_to,
            amount=row.amount,
        ))
    return transfers

async def _fetch_loan_history(
    archival_session: AsyncSession,
    player_id: Optional[int] = None,
    season_id: Optional[int] = None,
    club_filter: Optional[int] = None
) -> List[LoanHistoryItem]:
    # Build where clauses
    where_clauses = []
    where_clauses.append(PlayerLoanUpdates.action == 'finalised')
    if player_id is not None:
        where_clauses.append(PlayerLoanUpdates.player_id == player_id)
    if season_id is not None:
        where_clauses.append(PlayerLoanUpdates.season_id == season_id)
    if club_filter is not None:
        where_clauses.append(or_(PlayerLoanUpdates.club_id == club_filter, PlayerLoanUpdates.accepting_club_id == club_filter))

    query = (
        select(
            PlayerLoanUpdates.player_id,
            Blocks.date.label("loan_start_date"),
            PlayerLoanUpdates.club_id.label("from_club_id"),
            PlayerLoanUpdates.accepting_club_id.label("to_club_id"),
            PlayerLoanUpdates.season_id,
        )
        .select_from(
            PlayerLoanUpdates.__table__.join(Blocks.__table__, PlayerLoanUpdates.height == Blocks.height)
        )
        .where(*where_clauses)
        .order_by(PlayerLoanUpdates.id)
    )
    result = await archival_session.execute(query)
    rows = result.fetchall()
    loans: List[LoanHistoryItem] = []
    for row in rows:
        # Skip rows with null accepting_club_id
        if row.to_club_id is None:
            continue
        loans.append(LoanHistoryItem(
            player_id=row.player_id,
            loan_start_date=row.loan_start_date,
            from_club_id=row.from_club_id,
            to_club_id=row.to_club_id,
            season_id=row.season_id,
        ))
    return loans

@players_router.get(
    "/player/history/{player_id}",
    response_model=PlayerHistoryResponse,
    summary="Retrieve player history",
    description="Returns the history of a soccer player, including information about past loans, historical ratings, and injury history."
)
async def get_player_history(
    player_id: int,
    archival_session: AsyncSession = Depends(get_archival_session),
    sqlite_session: AsyncSession = Depends(get_playerhistory_sqlite_session),
):
    # Validate that player_id is a non-negative integer
    if player_id < 0:
        raise HTTPException(status_code=400, detail="Player ID must be a non-negative integer")
    
    # Fetch loan history using the helper function
    loan_items = await _fetch_loan_history(
        archival_session, player_id=player_id
    )
    
    loans = [
        PlayerLoanResponse(
            season_id=item.season_id,
            from_club_id=item.from_club_id,
            to_club_id=item.to_club_id,
            loan_start_date=item.loan_start_date,
        )
        for item in loan_items
    ]
    
    # Query for historical ratings from the SQLite database using injected session
    rating_query = (
        select(
            PlayerHistory.rating,
            PlayerHistory.rating_gk,
            PlayerHistory.rating_tackling,
            PlayerHistory.rating_passing,
            PlayerHistory.rating_shooting,
            PlayerHistory.rating_aggression,
            PlayerHistory.rating_stamina,
            PlayerHistory.date_updated
        )
        .where(PlayerHistory.player_id == player_id)
        .order_by(PlayerHistory.player_history_id)
    )
    
    rating_result = await sqlite_session.execute(rating_query)
    sqlite_rating_rows = rating_result.fetchall()
    
    # Query for rating updates from the archival database
    archival_rating_query = (
        select(
            PlayerUpdates.rating,
            PlayerUpdates.rating_gk,
            PlayerUpdates.rating_tackling,
            PlayerUpdates.rating_passing,
            PlayerUpdates.rating_shooting,
            PlayerUpdates.rating_aggression,
            PlayerUpdates.rating_stamina,
            Blocks.date
        )
        .select_from(
            PlayerUpdates.__table__.join(Blocks.__table__, PlayerUpdates.height == Blocks.height)
        )
        .where(
            PlayerUpdates.player_id == player_id,
            PlayerUpdates.rating.is_not(None)
        )
        .order_by(PlayerUpdates.id)
    )
    
    archival_result = await archival_session.execute(archival_rating_query)
    archival_rating_rows = archival_result.fetchall()
    
    # Combine ratings from both sources using list comprehensions
    ratings = [
        PlayerRatingResponse(
            rating=row.rating,
            rating_gk=row.rating_gk,
            rating_tackling=row.rating_tackling,
            rating_passing=row.rating_passing,
            rating_shooting=row.rating_shooting,
            rating_aggression=row.rating_aggression,
            rating_stamina=row.rating_stamina,
            date_updated=row.date_updated
        )
        for row in sqlite_rating_rows
    ] + [
        PlayerRatingResponse(
            rating=row.rating,
            rating_gk=row.rating_gk,
            rating_tackling=row.rating_tackling,
            rating_passing=row.rating_passing,
            rating_shooting=row.rating_shooting,
            rating_aggression=row.rating_aggression,
            rating_stamina=row.rating_stamina,
            date_updated=row.date
        )
        for row in archival_rating_rows
    ]
    
    # Query for injury history from the archival database
    injury_query = (
        select(
            InjuryHistory.injury_id,
            InjuryHistory.start_date,
            InjuryHistory.end_date,
            InjuryHistory.season_id
        )
        .where(InjuryHistory.player_id == player_id)
        .order_by(InjuryHistory.id)
    )
    
    injury_result = await archival_session.execute(injury_query)
    injury_rows = injury_result.fetchall()
    
    # Create injury history response objects
    injuries = [
        PlayerInjuryResponse(
            injury_id=row.injury_id,
            start_date=row.start_date,
            end_date=row.end_date,
            season_id=row.season_id
        )
        for row in injury_rows
    ]

    # Fetch transfer history for this player
    transfer_items = await _fetch_transfer_history(
        archival_session, player_id=player_id
    )
    transfers = [
        PlayerTransferResponse(
            date=item.date,
            club_id_from=item.club_id_from,
            club_id_to=item.club_id_to,
            amount=item.amount,
        )
        for item in transfer_items
    ]
    
    return PlayerHistoryResponse(
        player_id=player_id,
        loans=loans,
        ratings=ratings,
        injuries=injuries,
        transfers=transfers,
    )


@players_router.get(
    "/player/transfer_history",
    response_model=List[TransferHistoryItem],
    summary="Retrieve transfer history",
    description="Returns transfer history filtered by season_id, club_id, and/or player_id. If no season_id is specified, uses the latest season. At least one of club_id or player_id must be provided if season_id is not specified."
)
async def get_transfer_history(
    season_id: Optional[int] = Query(None, description="Filter by season ID. If not specified, uses the latest season"),
    club_id: Optional[int] = Query(None, description="Filter by club ID (matches either from or to club)"),
    player_id: Optional[int] = Query(None, description="Filter by player ID"),
    archival_session: AsyncSession = Depends(get_archival_session),
):
    # If no season_id is provided, get the latest season
    if season_id is None:
        # If no filters are provided at all, require at least one
        if club_id is None and player_id is None:
            raise HTTPException(
                status_code=400,
                detail="At least one of 'season_id', 'club_id', or 'player_id' must be provided"
            )

        # Get the latest season_id from Messages table
        latest_season_query = select(func.max(Messages.season_id))
        latest_season_result = await archival_session.execute(latest_season_query)
        season_id = latest_season_result.scalar()

        # If no season found in Messages, raise error
        if season_id is None:
            raise HTTPException(
                status_code=404,
                detail="No seasons found in transfer history"
            )

    return await _fetch_transfer_history(
        archival_session,
        season_id=season_id,
        club_filter=club_id,
        player_id=player_id
    )


@players_router.get(
    "/players/loan_history",
    response_model=List[LoanHistoryItem],
    summary="Retrieve loan history",
    description="Returns loan history filtered by season_id and/or club_id. At least one of season_id or club_id must be provided."
)
async def get_loan_history(
    season_id: Optional[int] = Query(None, description="Filter by season ID. If not specified, returns all seasons"),
    club_id: Optional[int] = Query(None, description="Filter by club ID (matches either from or to club)"),
    archival_session: AsyncSession = Depends(get_archival_session),
):
    # Require at least one filter
    if season_id is None and club_id is None:
        raise HTTPException(
            status_code=400,
            detail="At least one of 'season_id' or 'club_id' must be provided"
        )

    return await _fetch_loan_history(
        archival_session,
        season_id=season_id,
        club_filter=club_id,
        player_id=None
    )


class MoraleChangeResponse(BaseModel):
    player_id: int
    new_morale: int
    time: datetime
    unix_time: int


@players_router.get(
    "/players/morale_history/{player_id}",
    response_model=List[MoraleChangeResponse],
    summary="Get morale change history for a player",
    description="Returns the latest 10 morale changes for a specific player"
)
async def get_player_morale_history(
    player_id: int,
    limit: int = Query(10, ge=1, le=100, description="Number of records to return"),
    archival_session: AsyncSession = Depends(get_archival_session),
):
    """Get morale change history for a player from messages table (type=51)"""

    m = Messages.__table__.alias("m")
    b = Blocks.__table__.alias("b")

    query = (
        select(
            m.c.data_1.label("player_id"),
            m.c.data_2.label("new_morale"),
            b.c.date.label("unix_time")
        )
        .select_from(m.join(b, m.c.height == b.c.height))
        .where(
            m.c.type == 51,  # Morale change type
            m.c.data_1 == player_id
        )
        .order_by(m.c.height.desc())
        .limit(limit)
    )

    result = await archival_session.execute(query)
    rows = result.fetchall()

    return [
        MoraleChangeResponse(
            player_id=row.player_id,
            new_morale=row.new_morale,
            time=datetime.utcfromtimestamp(row.unix_time),
            unix_time=row.unix_time
        )
        for row in rows
    ]


