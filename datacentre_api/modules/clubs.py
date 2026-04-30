# modules/clubs.py
import base64
from enum import Enum
from typing import List, Optional, Union
from datetime import datetime
from fastapi import APIRouter, Depends, Query, HTTPException, Path
from sqlalchemy import (
    Column,
    Integer,
    BigInteger,
    String,
    DateTime,
    select,
    func,
    Text,
    LargeBinary,
    case,
)
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from modules.utils.profile import get_profiles_for_users
from .base import (
    Base,
    get_mysql_session,
    get_userconfig_session,
    userconfig_session_maker,
    PaginatedResponse,
    PerPageOptions,
    parse_json_field,
    fetch_paginated_data,
    DCClubs,
    DCShareBalances,
    DCUsers,
    DCClubInfo,
    DCTableRows,
    DCLeagues,
    JobsBoard,
    DEFAULT_PROFILE_PIC_URL,
)

# -------------------------------------------------------------------------
# Define a dedicated Enum for allowed clubs sort fields
# -------------------------------------------------------------------------
class ClubsSortBy(str, Enum):
    club_id = "club_id"
    last_price = "last_price"
    volume_1_day = "volume_1_day"
    volume_7_day = "volume_7_day"
    balance = "balance"
    country_id = "country_id"
    value = "value"
    rating_start = "rating_start"
    available = "available"
    league_id = "league_id"
    division = "division"
    avg_wages = "avg_wages"
    total_wages = "total_wages"
    total_player_value = "total_player_value"
    avg_player_rating = "avg_player_rating"
    avg_player_rating_top21 = "avg_player_rating_top21"
    avg_shooting = "avg_shooting"
    avg_passing = "avg_passing"
    avg_tackling = "avg_tackling"
    gk_rating = "gk_rating"
    transfers_in = "transfers_in"
    transfers_out = "transfers_out"
    stadium_size_current = "stadium_size_current"
    fans_current = "fans_current"

# -------------------------------------------------------------------------
# Additional SQLAlchemy model used here
# -------------------------------------------------------------------------
class DCClubsTrading(Base):
    __tablename__ = "dc_clubs_trading"
    club_id = Column(Integer, primary_key=True)
    last_price = Column(BigInteger)
    volume_1_day = Column(BigInteger)
    volume_7_day = Column(BigInteger)
    last_7days = Column(Text)
    last_7days_price = Column(Text)
    updated_at = Column(DateTime)

# -------------------------------------------------------------------------
# Pydantic models
# -------------------------------------------------------------------------
class JobPostingResponse(BaseModel):
    club_id: int
    poster_name: str
    posted_at: int
    description: str
    created_at: Optional[datetime] = None
    last_updated: Optional[datetime] = None
    
    class Config:
        from_attributes = True

class ClubResponse(BaseModel):
    club_id: int
    last_price: int = 0
    volume_1_day: int = 0
    volume_7_day: int = 0
    last_7days: Optional[List[int]] = None
    last_7days_price: Optional[List[int]] = None
    balance: Optional[int]
    manager_name: Optional[str]
    country_id: Optional[str]
    value: Optional[int]
    rating_start: Optional[int]
    profile_pic: Optional[str] = None
    manager_last_active_unix: Optional[int] = None

    class Config:
        from_attributes = True


class ShareBalanceItem(BaseModel):
    name: str
    num: int
    profile_pic: Optional[str] = None
    last_active_unix: Optional[int] = None  # Added influencer's last active time

class JobPostedDetails(BaseModel):
    posted_at: int
    poster_name: str
    poster_last_active_unix: Optional[int] = None  # Added poster's last active time
    description: Optional[str] = None
    poster_influence: Optional[int] = None
    poster_profile_pic: Optional[str] = None

class ClubDetailedResponse(BaseModel):
    club_id: int
    balance: Optional[int]
    form: Optional[str]
    division_start: Optional[int]
    fans_start: Optional[int]
    fans_current: Optional[int]
    stadium_size_start: Optional[int]
    stadium_size_current: Optional[int]
    stadium_id: Optional[int]
    value: Optional[int]
    rating_start: Optional[int]
    manager_name: Optional[str]
    manager_last_active_unix: Optional[int] = None  # Added manager's last active time
    default_formation: Optional[int]
    penalty_taker: Optional[int]
    country_id: Optional[str]
    manager_locked: Optional[int]
    transfers_in: Optional[int]
    transfers_out: Optional[int]
    form: Optional[str]  # repeated but included from original snippet
    committed_tactics: Optional[str]
    proposed_manager: Optional[str]
    last_price: int = 0
    volume_1_day: int = 0
    volume_7_day: int = 0
    last_7days: Optional[List[int]] = None
    last_7days_price: Optional[List[int]] = None
    available: int  # Add the 'available' field
    league_id: Optional[int]
    division: Optional[int]
    avg_wages: Optional[int]
    total_wages: Optional[int]
    total_player_value: Optional[int]
    avg_player_rating: Optional[int]
    avg_player_rating_top21: Optional[int]
    avg_shooting: Optional[int]
    avg_passing: Optional[int]
    avg_tackling: Optional[int]
    gk_rating: Optional[int]
    profile_pic: Optional[str] = None
    manager_voted: Optional[int] = None
    job_posted: Optional[Union[JobPostedDetails, str]] = None   # New field for job posting information
    top_influencers: Optional[List[ShareBalanceItem]] = None  # New field for top 5 influencers
    league_position: Optional[int] = None  # Current position in league (new_position)
    previous_position: Optional[int] = None  # Previous position in league (old_position)

    class Config:
        from_attributes = True

# -------------------------------------------------------------------------
# Create an APIRouter instance for clubs
# -------------------------------------------------------------------------
clubs_router = APIRouter()

# -------------------------------------------------------------------------
# Original CommonQueryParams logic (used internally)
# -------------------------------------------------------------------------
# (This logic remains unchanged; we will now wrap it with two distinct dependency classes)
class CommonQueryParams:
    def __init__(
        self,
        page: int = Query(1, ge=1, description="Page number, starting from 1"),
        per_page: PerPageOptions = Query(
            PerPageOptions.twenty,
            description="Number of records per page (options: 5, 20, 50)",
        ),
        sort_by: Optional[ClubsSortBy] = Query(None, description="Field to sort by"),
        sort_order: Optional[str] = Query(
            "asc",
            description="Sort order: 'asc' or 'desc'",
            regex="^(asc|desc)$",
        ),
        club_id: Optional[List[int]] = Query(None, description="Filter by specific club ID(s)"),
        country_id: Optional[str] = Query(None, description="Country ID to filter clubs"),
        owned: Optional[str] = Query(None, description="Filter clubs owned by the given name"),
        manager_locked: Optional[int] = Query(None, description="Exact value for manager_locked"),
        available: Optional[int] = Query(None, description="Filter by availability status: 1 (available), 0 (not available)"),
        league_id: Optional[int] = Query(None, description="Filter by league ID"),
        division: Optional[int] = Query(None, description="Filter by division"),
        balance_min: Optional[int] = Query(None, description="Minimum balance"),
        balance_max: Optional[int] = Query(None, description="Maximum balance"),
        division_start_min: Optional[int] = Query(None, description="Minimum division_start"),
        division_start_max: Optional[int] = Query(None, description="Maximum division_start"),
        fans_start_min: Optional[int] = Query(None, description="Minimum fans_start"),
        fans_start_max: Optional[int] = Query(None, description="Maximum fans_start"),
        fans_current_min: Optional[int] = Query(None, description="Minimum fans_current"),
        fans_current_max: Optional[int] = Query(None, description="Maximum fans_current"),
        stadium_size_start_min: Optional[int] = Query(None, description="Minimum stadium_size_start"),
        stadium_size_start_max: Optional[int] = Query(None, description="Maximum stadium_size_start"),
        stadium_size_current_min: Optional[int] = Query(None, description="Minimum stadium_size_current"),
        stadium_size_current_max: Optional[int] = Query(None, description="Maximum stadium_size_current"),
        stadium_id_min: Optional[int] = Query(None, description="Minimum stadium_id"),
        stadium_id_max: Optional[int] = Query(None, description="Maximum stadium_id"),
        value_min: Optional[int] = Query(None, description="Minimum value"),
        value_max: Optional[int] = Query(None, description="Maximum value"),
        rating_start_min: Optional[int] = Query(None, description="Minimum rating"),
        rating_start_max: Optional[int] = Query(None, description="Maximum rating"),
        default_formation_min: Optional[int] = Query(None, description="Minimum default_formation"),
        default_formation_max: Optional[int] = Query(None, description="Maximum default_formation"),
        penalty_taker_min: Optional[int] = Query(None, description="Minimum penalty_taker"),
        penalty_taker_max: Optional[int] = Query(None, description="Maximum penalty_taker"),
        transfers_in_min: Optional[int] = Query(None, description="Minimum transfers_in"),
        transfers_in_max: Optional[int] = Query(None, description="Maximum transfers_in"),
        transfers_out_min: Optional[int] = Query(None, description="Minimum transfers_out"),
        transfers_out_max: Optional[int] = Query(None, description="Maximum transfers_out"),
    ):
        self.page = page
        self.per_page = per_page.value if isinstance(per_page, PerPageOptions) else per_page
        self.sort_by = sort_by
        self.sort_order = sort_order
        self.club_id = club_id
        self.country_id = country_id
        self.owned = owned
        self.manager_locked = manager_locked
        self.available = available
        self.league_id = league_id
        self.division = division

        field_names = [
            "balance",
            "division_start",
            "fans_start",
            "fans_current",
            "stadium_size_start",
            "stadium_size_current",
            "stadium_id",
            "value",
            "rating_start",
            "default_formation",
            "penalty_taker",
            "transfers_in",
            "transfers_out",
        ]
        self.field_min_max = {}
        for field in field_names:
            min_value = locals().get(f"{field}_min")
            max_value = locals().get(f"{field}_max")
            setattr(self, f"{field}_min", min_value)
            setattr(self, f"{field}_max", max_value)
            self.field_min_max[field] = (min_value, max_value)

# -------------------------------------------------------------------------
# Main function that fetches and filters clubs
# -------------------------------------------------------------------------
async def get_filtered_clubs(
    session: AsyncSession,
    params,  # Instance of CommonQueryParams (or subclass thereof)
    detailed: bool,
    userconfig_session: AsyncSession,
):
    per_page = params.per_page
    page = params.page
    sort_order = params.sort_order
    chosen_sort = params.sort_by.value if params.sort_by else None

    # Alias tables
    dc_clubs_alias = DCClubs.__table__.alias("dc_clubs_alias")
    trading_alias = DCClubsTrading.__table__.alias("trading_alias")
    share_balances_alias = DCShareBalances.__table__.alias("share_balances_alias")
    dc_users_alias = DCUsers.__table__.alias("dc_users_alias")
    dc_club_info_alias = DCClubInfo.__table__.alias("dc_club_info_alias")
    # Create an additional alias for fetching poster's last active time
    poster_users_alias = DCUsers.__table__.alias("poster_users_alias")

    # Build a list of extra filters
    extra_filters = []

    if params.owned:
        # If 'owned' is specified, pivot to share_balances as base
        base_query = share_balances_alias
        extra_filters.append(share_balances_alias.c.name == params.owned)
        # Ensure exact match including trailing spaces
        extra_filters.append(func.char_length(share_balances_alias.c.name) == len(params.owned))
        extra_filters.append(share_balances_alias.c.share_type == "club")
        extra_filters.append(share_balances_alias.c.num > 0)

        # Outer join with clubs, trading, users, club_info
        join_clause = base_query.outerjoin(
            dc_clubs_alias,
            share_balances_alias.c.share_id == dc_clubs_alias.c.club_id
        ).outerjoin(
            trading_alias,
            share_balances_alias.c.share_id == trading_alias.c.club_id
        ).outerjoin(
            dc_users_alias,
            dc_clubs_alias.c.manager_name == dc_users_alias.c.name
        ).outerjoin(
            dc_club_info_alias,
            dc_clubs_alias.c.club_id == dc_club_info_alias.c.club_id
        )

        # Select columns
        select_columns = [
            share_balances_alias.c.share_id.label("club_id"),
            trading_alias.c.last_price,
            trading_alias.c.volume_1_day,
            trading_alias.c.volume_7_day,
            trading_alias.c.last_7days,
            trading_alias.c.last_7days_price,
        ]

        if detailed:
            current_unix_timestamp = func.UNIX_TIMESTAMP()
            inactivity_threshold = 14 * 86400  # 14 days in seconds

            available_column = case(
                (
                    ((dc_clubs_alias.c.manager_name == None) | (dc_clubs_alias.c.manager_name == "")),
                    1
                ),
                (
                    (dc_users_alias.c.last_active <= current_unix_timestamp - inactivity_threshold),
                    1
                ),
                else_=0
            ).label("available")

            # Add all club columns
            select_columns.extend([dc_clubs_alias.c[field.name] for field in DCClubs.__table__.columns
                if field.name not in ["checksum", "updated_at", "home_colour", "away_colour"]])
            select_columns.append(available_column)
            # Add manager's last active time
            select_columns.append(dc_users_alias.c.last_active.label("manager_last_active_unix"))

            # Add fields from dc_club_info
            select_columns.extend([
                dc_club_info_alias.c.league_id,
                dc_club_info_alias.c.division,
                dc_club_info_alias.c.avg_wages,
                dc_club_info_alias.c.total_wages,
                dc_club_info_alias.c.total_player_value,
                dc_club_info_alias.c.avg_player_rating,
                dc_club_info_alias.c.avg_player_rating_top21,
                dc_club_info_alias.c.avg_shooting,
                dc_club_info_alias.c.avg_passing,
                dc_club_info_alias.c.avg_tackling,
                dc_club_info_alias.c.gk_rating,
            ])

            sortable_fields = {
                column.name: dc_clubs_alias.c[column.name]
                for column in DCClubs.__table__.columns
                if column.name not in ["checksum", "updated_at", "home_colour", "away_colour", "manager_name"]
            }
            sortable_fields.update({
                "club_id": share_balances_alias.c.share_id,
                "last_price": func.coalesce(trading_alias.c.last_price, 0),
                "volume_1_day": func.coalesce(trading_alias.c.volume_1_day, 0),
                "volume_7_day": func.coalesce(trading_alias.c.volume_7_day, 0),
                "available": available_column,
                "league_id": dc_club_info_alias.c.league_id,
                "division": dc_club_info_alias.c.division,
                "avg_wages": dc_club_info_alias.c.avg_wages,
                "total_wages": dc_club_info_alias.c.total_wages,
                "total_player_value": dc_club_info_alias.c.total_player_value,
                "avg_player_rating": dc_club_info_alias.c.avg_player_rating,
                "avg_player_rating_top21": dc_club_info_alias.c.avg_player_rating_top21,
                "avg_shooting": dc_club_info_alias.c.avg_shooting,
                "avg_passing": dc_club_info_alias.c.avg_passing,
                "avg_tackling": dc_club_info_alias.c.avg_tackling,
                "gk_rating": dc_club_info_alias.c.gk_rating,
            })
        else:
            # Non-detailed 'owned'
            select_columns.extend([
                dc_clubs_alias.c.balance,
                dc_clubs_alias.c.manager_name,
                dc_clubs_alias.c.country_id,
                dc_clubs_alias.c.value,
                dc_clubs_alias.c.rating_start,
                dc_users_alias.c.last_active.label("manager_last_active_unix"),
            ])
            sortable_fields = {
                "club_id": share_balances_alias.c.share_id,
                "last_price": func.coalesce(trading_alias.c.last_price, 0),
                "volume_1_day": func.coalesce(trading_alias.c.volume_1_day, 0),
                "volume_7_day": func.coalesce(trading_alias.c.volume_7_day, 0),
                "balance": dc_clubs_alias.c.balance,
                "country_id": dc_clubs_alias.c.country_id,
                "value": dc_clubs_alias.c.value,
                "rating_start": dc_clubs_alias.c.rating_start,
            }

    else:
        # Owned not specified
        if detailed:
            base_query = dc_clubs_alias
            join_clause = base_query.outerjoin(
                trading_alias,
                dc_clubs_alias.c.club_id == trading_alias.c.club_id
            ).outerjoin(
                dc_users_alias,
                dc_clubs_alias.c.manager_name == dc_users_alias.c.name
            ).outerjoin(
                dc_club_info_alias,
                dc_clubs_alias.c.club_id == dc_club_info_alias.c.club_id
            )

            current_unix_timestamp = func.UNIX_TIMESTAMP()
            inactivity_threshold = 14 * 86400

            available_column = case(
                (
                    ((dc_clubs_alias.c.manager_name == None) | (dc_clubs_alias.c.manager_name == "")),
                    1
                ),
                (
                    (dc_users_alias.c.last_active <= current_unix_timestamp - inactivity_threshold),
                    1
                ),
                else_=0
            ).label("available")

            select_columns = [
                dc_clubs_alias.c[field.name] for field in DCClubs.__table__.columns
                if field.name not in ["checksum", "updated_at", "home_colour", "away_colour"]
            ] + [
                trading_alias.c.last_price,
                trading_alias.c.volume_1_day,
                trading_alias.c.volume_7_day,
                trading_alias.c.last_7days,
                trading_alias.c.last_7days_price,
                available_column,
                # Add manager's last active time
                dc_users_alias.c.last_active.label("manager_last_active_unix"),
            ]
            select_columns.extend([
                dc_club_info_alias.c.league_id,
                dc_club_info_alias.c.division,
                dc_club_info_alias.c.avg_wages,
                dc_club_info_alias.c.total_wages,
                dc_club_info_alias.c.total_player_value,
                dc_club_info_alias.c.avg_player_rating,
                dc_club_info_alias.c.avg_player_rating_top21,
                dc_club_info_alias.c.avg_shooting,
                dc_club_info_alias.c.avg_passing,
                dc_club_info_alias.c.avg_tackling,
                dc_club_info_alias.c.gk_rating,
            ])

            sortable_fields = {
                column.name: dc_clubs_alias.c[column.name]
                for column in DCClubs.__table__.columns
                if column.name not in ["checksum", "updated_at", "home_colour", "away_colour", "manager_name"]
            }
            sortable_fields.update({
                "last_price": func.coalesce(trading_alias.c.last_price, 0),
                "volume_1_day": func.coalesce(trading_alias.c.volume_1_day, 0),
                "volume_7_day": func.coalesce(trading_alias.c.volume_7_day, 0),
                "available": available_column,
                "league_id": dc_club_info_alias.c.league_id,
                "division": dc_club_info_alias.c.division,
                "avg_wages": dc_club_info_alias.c.avg_wages,
                "total_wages": dc_club_info_alias.c.total_wages,
                "total_player_value": dc_club_info_alias.c.total_player_value,
                "avg_player_rating": dc_club_info_alias.c.avg_player_rating,
                "avg_player_rating_top21": dc_club_info_alias.c.avg_player_rating_top21,
                "avg_shooting": dc_club_info_alias.c.avg_shooting,
                "avg_passing": dc_club_info_alias.c.avg_passing,
                "avg_tackling": dc_club_info_alias.c.avg_tackling,
                "gk_rating": dc_club_info_alias.c.gk_rating,
            })
        else:
            base_query = trading_alias
            join_clause = base_query.outerjoin(
                dc_clubs_alias,
                trading_alias.c.club_id == dc_clubs_alias.c.club_id
            ).outerjoin(
                dc_users_alias,
                dc_clubs_alias.c.manager_name == dc_users_alias.c.name
            )
            select_columns = [
                trading_alias.c.club_id,
                trading_alias.c.last_price,
                trading_alias.c.volume_1_day,
                trading_alias.c.volume_7_day,
                trading_alias.c.last_7days,
                trading_alias.c.last_7days_price,
                dc_clubs_alias.c.balance,
                dc_clubs_alias.c.manager_name,
                dc_clubs_alias.c.country_id,
                dc_clubs_alias.c.value,
                dc_clubs_alias.c.rating_start,
                dc_users_alias.c.last_active.label("manager_last_active_unix"),
            ]
            sortable_fields = {
                "club_id": trading_alias.c.club_id,
                "last_price": func.coalesce(trading_alias.c.last_price, 0),
                "volume_1_day": func.coalesce(trading_alias.c.volume_1_day, 0),
                "volume_7_day": func.coalesce(trading_alias.c.volume_7_day, 0),
                "balance": dc_clubs_alias.c.balance,
                "country_id": dc_clubs_alias.c.country_id,
                "value": dc_clubs_alias.c.value,
                "rating_start": dc_clubs_alias.c.rating_start,
            }

    select_query = select(*select_columns).select_from(join_clause)
    total_query = select(func.count()).select_from(join_clause)

    # Apply any extra filters
    if extra_filters:
        for condition in extra_filters:
            select_query = select_query.where(condition)
            total_query = total_query.where(condition)

    # Apply numeric min/max filters
    for field_name, (min_value, max_value) in params.field_min_max.items():
        if hasattr(dc_clubs_alias.c, field_name):
            column = dc_clubs_alias.c[field_name]
            if column is not None:
                if min_value is not None:
                    select_query = select_query.where(column >= min_value)
                    total_query = total_query.where(column >= min_value)
                if max_value is not None:
                    select_query = select_query.where(column <= max_value)
                    total_query = total_query.where(column <= max_value)
        else:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid filter field: {field_name}_min or {field_name}_max"
            )

    # Apply club_id filter
    if params.club_id is not None:
        if params.owned:
            select_query = select_query.where(share_balances_alias.c.share_id.in_(params.club_id))
            total_query = total_query.where(share_balances_alias.c.share_id.in_(params.club_id))
        else:
            select_query = select_query.where(dc_clubs_alias.c.club_id.in_(params.club_id))
            total_query = total_query.where(dc_clubs_alias.c.club_id.in_(params.club_id))

    # Apply country_id filter
    if params.country_id is not None:
        select_query = select_query.where(dc_clubs_alias.c.country_id == params.country_id)
        total_query = total_query.where(dc_clubs_alias.c.country_id == params.country_id)

    # Apply manager_locked filter
    if params.manager_locked is not None:
        if hasattr(dc_clubs_alias.c, "manager_locked"):
            column = dc_clubs_alias.c["manager_locked"]
            select_query = select_query.where(column == params.manager_locked)
            total_query = total_query.where(column == params.manager_locked)
        else:
            raise HTTPException(status_code=400, detail="Invalid filter field: manager_locked")

    # Apply available filter (only if detailed)
    if detailed and params.available is not None:
        if params.available not in (0, 1):
            raise HTTPException(
                status_code=400,
                detail="Invalid available value, must be 0 or 1"
            )
        select_query = select_query.where(
            case(
                (
                    ((dc_clubs_alias.c.manager_name == None) | (dc_clubs_alias.c.manager_name == "")),
                    1
                ),
                (
                    (dc_users_alias.c.last_active <= func.UNIX_TIMESTAMP() - 14 * 86400),
                    1
                ),
                else_=0
            ) == params.available
        )
        total_query = total_query.where(
            case(
                (
                    ((dc_clubs_alias.c.manager_name == None) | (dc_clubs_alias.c.manager_name == "")),
                    1
                ),
                (
                    (dc_users_alias.c.last_active <= func.UNIX_TIMESTAMP() - 14 * 86400),
                    1
                ),
                else_=0
            ) == params.available
        )

    # Apply league_id filter (only if detailed)
    if detailed and params.league_id is not None:
        select_query = select_query.where(dc_club_info_alias.c.league_id == params.league_id)
        total_query = total_query.where(dc_club_info_alias.c.league_id == params.league_id)

    # Apply division filter (only if detailed)
    if detailed and params.division is not None:
        select_query = select_query.where(dc_club_info_alias.c.division == params.division)
        total_query = total_query.where(dc_club_info_alias.c.division == params.division)
        
    # Apply has_job_posting filter (only if detailed)
    if detailed and hasattr(params, 'has_job_posting') and params.has_job_posting is not None:
        if params.has_job_posting not in (0, 1):
            raise HTTPException(
                status_code=400,
                detail="Invalid has_job_posting value, must be 0 or 1"
            )
        
        # Check if userconfig database is available for job posting filter
        if userconfig_session is None:
            raise HTTPException(
                status_code=500,
                detail="Userconfig database not available - job posting filter disabled"
            )
            
        # Get all club IDs with job postings from the userconfig database
        jobs_stmt = select(JobsBoard.club_id).distinct()
        jobs_result = await userconfig_session.execute(jobs_stmt)
        club_ids_with_jobs = [row[0] for row in jobs_result.fetchall()]
        
        # No need for a subquery, we can use the list directly
        
        if params.has_job_posting == 1:
            # Only include clubs that have job postings
            if params.owned:
                select_query = select_query.where(share_balances_alias.c.share_id.in_(club_ids_with_jobs))
                total_query = total_query.where(share_balances_alias.c.share_id.in_(club_ids_with_jobs))
            else:
                select_query = select_query.where(dc_clubs_alias.c.club_id.in_(club_ids_with_jobs))
                total_query = total_query.where(dc_clubs_alias.c.club_id.in_(club_ids_with_jobs))
        else:  # params.has_job_posting == 0
            # Only include clubs that don't have job postings
            if params.owned:
                select_query = select_query.where(share_balances_alias.c.share_id.notin_(club_ids_with_jobs))
                total_query = total_query.where(share_balances_alias.c.share_id.notin_(club_ids_with_jobs))
            else:
                select_query = select_query.where(dc_clubs_alias.c.club_id.notin_(club_ids_with_jobs))
                total_query = total_query.where(dc_clubs_alias.c.club_id.notin_(club_ids_with_jobs))

    # Execute paginated fetch
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

    # Collect manager names to fetch profile pics
    manager_names_needed = set()
    for row in rows:
        manager_name = row._mapping.get("manager_name")
        if manager_name:
            manager_names_needed.add(manager_name)

    name_to_pic = await get_profiles_for_users(list(manager_names_needed), userconfig_session)

    # Get league positions for detailed view
    league_positions = {}
    if detailed:
        # Get current season
        season_query = select(func.max(DCLeagues.season_id))
        season_result = await session.execute(season_query)
        current_season = season_result.scalar() or 1
        
        # Collect club-league pairs for position lookup
        club_league_pairs = []
        for row in rows:
            club_id = row._mapping.get("club_id")
            league_id = row._mapping.get("league_id")
            if club_id and league_id:
                club_league_pairs.append((club_id, league_id))
        
        # Batch fetch positions if we have clubs with leagues
        if club_league_pairs:
            from sqlalchemy import or_
            position_conditions = [
                (DCTableRows.club_id == club_id) & 
                (DCTableRows.league_id == league_id) & 
                (DCTableRows.season_id == current_season)
                for club_id, league_id in club_league_pairs
            ]
            
            position_query = select(
                DCTableRows.club_id,
                DCTableRows.old_position,
                DCTableRows.new_position
            ).where(or_(*position_conditions))
            
            position_result = await session.execute(position_query)
            for pos_row in position_result:
                league_positions[pos_row.club_id] = {
                    'old_position': pos_row.old_position,
                    'new_position': pos_row.new_position
                }

    # Build list of responses
    items = []
    for row in rows:
        row_dict = dict(row._mapping)
        last_7days_price = parse_json_field(row_dict.get("last_7days_price"))
        last_7days = parse_json_field(row_dict.get("last_7days"))
        manager_name = row_dict.get("manager_name")
        
        # Always default to DEFAULT_PROFILE_PIC_URL
        profile_pic = DEFAULT_PROFILE_PIC_URL
        if manager_name:
            profile_pic = name_to_pic.get(manager_name, DEFAULT_PROFILE_PIC_URL)

        if detailed:
            club_response_data = {
                key: row_dict.get(key)
                for key in ClubDetailedResponse.__fields__.keys()
                if key not in ["last_7days", "last_7days_price", "committed_tactics", "profile_pic"]
            }
            club_response_data["last_7days"] = last_7days
            club_response_data["last_7days_price"] = last_7days_price
            club_response_data["last_price"] = int(row_dict.get("last_price") or 0)
            club_response_data["volume_1_day"] = int(row_dict.get("volume_1_day") or 0)
            club_response_data["volume_7_day"] = int(row_dict.get("volume_7_day") or 0)

            # Handle committed_tactics base64 encoding
            committed_tactics_bytes = row_dict.get("committed_tactics")
            if committed_tactics_bytes is not None:
                committed_tactics_base64 = base64.b64encode(committed_tactics_bytes).decode("ascii")
            else:
                committed_tactics_base64 = None
            club_response_data["committed_tactics"] = committed_tactics_base64

            club_response_data["available"] = int(row_dict.get("available") or 0)
            
            # Fetch job posting information for this club
            if detailed:
                # Check if this is a single club query (using club_id filter) or has_job_posting filter
                is_single_club_query = (hasattr(params, 'club_id') and params.club_id is not None and 
                                      isinstance(params.club_id, list) and len(params.club_id) == 1)
                # Also fetch full details when has_job_posting filter is used
                fetch_full_details = is_single_club_query or (hasattr(params, 'has_job_posting') and params.has_job_posting == 1)
                
                # Process all database operations concurrently when possible
                job_row = None
                top_influencers_rows = []
                influencer_profiles = {}
                poster_influence_row = None
                
                # Handle job posting data
                if userconfig_session is None:
                    # Set job posting info to error message when userconfig not available
                    club_response_data["job_posted"] = "error: userconfig not available"
                else:
                    # First, get the job posting info
                    if fetch_full_details:
                        job_stmt = select(JobsBoard.posted_at, JobsBoard.poster_name, JobsBoard.description)\
                            .where(JobsBoard.club_id == row_dict.get("club_id"))\
                            .order_by(JobsBoard.posted_at.desc())\
                            .limit(1)
                    else:
                        job_stmt = select(JobsBoard.posted_at, JobsBoard.poster_name)\
                            .where(JobsBoard.club_id == row_dict.get("club_id"))\
                            .order_by(JobsBoard.posted_at.desc())\
                            .limit(1)
                    
                    job_result = await userconfig_session.execute(job_stmt)
                    job_row = job_result.fetchone()
                
                    # Set job posting information
                    if job_row:
                        # Get poster's last active time
                        poster_name = job_row[1]
                        poster_last_active_stmt = select(DCUsers.last_active)\
                            .where(DCUsers.name == poster_name)
                        poster_last_active_result = await session.execute(poster_last_active_stmt)
                        poster_last_active_row = poster_last_active_result.fetchone()
                        poster_last_active = poster_last_active_row[0] if poster_last_active_row else None
                        
                        # Get poster's profile picture
                        if userconfig_session_maker is not None:
                            async with userconfig_session_maker() as poster_profile_session:
                                poster_profiles = await get_profiles_for_users([poster_name], poster_profile_session)
                                poster_profile_pic = poster_profiles.get(poster_name, DEFAULT_PROFILE_PIC_URL)
                        else:
                            poster_profile_pic = "error: userconfig not available"
                        
                        if fetch_full_details and len(job_row) > 2:
                            club_response_data["job_posted"] = {
                                "posted_at": job_row[0],
                                "poster_name": job_row[1],
                                "poster_last_active_unix": poster_last_active,
                                "description": job_row[2],
                                "poster_profile_pic": poster_profile_pic
                            }
                        else:
                            club_response_data["job_posted"] = {
                                "posted_at": job_row[0],
                                "poster_name": job_row[1],
                                "poster_last_active_unix": poster_last_active,
                                "poster_profile_pic": poster_profile_pic
                            }
                    else:
                        club_response_data["job_posted"] = None
                
                # Now handle top influencers for single club queries or has_job_posting filter
                if fetch_full_details:
                    # Get top 5 influencers
                    top_influencers_stmt = select(DCShareBalances.name, DCShareBalances.num)\
                        .where(DCShareBalances.share_id == row_dict.get("club_id"))\
                        .where(DCShareBalances.share_type == "club")\
                        .where(DCShareBalances.num > 0)\
                        .order_by(DCShareBalances.num.desc())\
                        .limit(5)
                    top_influencers_result = await session.execute(top_influencers_stmt)
                    top_influencers_rows = top_influencers_result.fetchall()
                    
                    # Create a new userconfig session for profile pics to avoid concurrent use issues
                    top_influencer_names = [row[0] for row in top_influencers_rows]
                    if top_influencer_names:
                        if userconfig_session_maker is not None:
                            async with userconfig_session_maker() as profile_session:
                                influencer_profiles = await get_profiles_for_users(top_influencer_names, profile_session)
                        else:
                            influencer_profiles = await get_profiles_for_users(top_influencer_names, None)
                    
                    # Get last active times for all influencers
                    influencer_last_active_times = {}
                    if top_influencer_names:
                        influencer_last_active_stmt = select(DCUsers.name, DCUsers.last_active)\
                            .where(DCUsers.name.in_(top_influencer_names))
                        influencer_last_active_result = await session.execute(influencer_last_active_stmt)
                        influencer_last_active_rows = influencer_last_active_result.fetchall()
                        influencer_last_active_times = {row[0]: row[1] for row in influencer_last_active_rows}
                    
                    # Build top influencers list with profile pics and last active times
                    top_influencers = []
                    for name, num in top_influencers_rows:
                        top_influencers.append({
                            "name": name,
                            "num": num,
                            "profile_pic": influencer_profiles.get(name, DEFAULT_PROFILE_PIC_URL),
                            "last_active_unix": influencer_last_active_times.get(name)
                        })
                    
                    club_response_data["top_influencers"] = top_influencers
                    
                    # Add poster influence if we have a job posting
                    if club_response_data["job_posted"]:
                        poster_name = club_response_data["job_posted"]["poster_name"]
                        # Get poster's influence on this club
                        poster_influence_stmt = select(DCShareBalances.num)\
                            .where(DCShareBalances.share_id == row_dict.get("club_id"))\
                            .where(DCShareBalances.share_type == "club")\
                            .where(DCShareBalances.name == poster_name)
                        poster_influence_result = await session.execute(poster_influence_stmt)
                        poster_influence_row = poster_influence_result.fetchone()
                        
                        if poster_influence_row:
                            club_response_data["job_posted"]["poster_influence"] = poster_influence_row[0]
                        else:
                            club_response_data["job_posted"]["poster_influence"] = 0

            for field in [
                "league_id",
                "division",
                "avg_wages",
                "total_wages",
                "total_player_value",
                "avg_player_rating",
                "avg_player_rating_top21",
                "avg_shooting",
                "avg_passing",
                "avg_tackling",
                "gk_rating",
            ]:
                value = row_dict.get(field)
                club_response_data[field] = int(value) if value is not None else None

            club_response_data["profile_pic"] = profile_pic
            
            # Add league position data if available
            club_id = row_dict.get("club_id")
            if club_id in league_positions:
                club_response_data["league_position"] = league_positions[club_id].get('new_position')
                club_response_data["previous_position"] = league_positions[club_id].get('old_position')

            club_response = ClubDetailedResponse(**club_response_data)
        else:
            club_response = ClubResponse(
                club_id=row_dict.get("club_id"),
                last_price=int(row_dict.get("last_price") or 0),
                volume_1_day=int(row_dict.get("volume_1_day") or 0),
                volume_7_day=int(row_dict.get("volume_7_day") or 0),
                last_7days=last_7days,
                last_7days_price=last_7days_price,
                balance=row_dict.get("balance"),
                manager_name=manager_name,
                country_id=row_dict.get("country_id"),
                value=row_dict.get("value"),
                rating_start=row_dict.get("rating_start"),
                profile_pic=profile_pic,
                manager_last_active_unix=row_dict.get("manager_last_active_unix"),
            )
        items.append(club_response)

    # Return paginated response
    return PaginatedResponse(
        page=page,
        per_page=per_page,
        total=total,
        total_pages=total_pages,
        items=items,
    )

# -------------------------------------------------------------------------
# Distinct Query Parameter Classes for FastAPI Documentation
# -------------------------------------------------------------------------
class BasicClubQueryParams:
    """
    Query parameters for the basic /clubs endpoint.
    Only includes filters relevant to basic market data.
    """
    def __init__(
        self,
        page: int = Query(1, ge=1, description="Page number, starting from 1"),
        per_page: PerPageOptions = Query(
            PerPageOptions.twenty,
            description="Number of records per page (options: 5, 20, 50)"
        ),
        sort_by: Optional[ClubsSortBy] = Query(None, description="Field to sort by"),
        sort_order: Optional[str] = Query("asc", description="Sort order: 'asc' or 'desc'", regex="^(asc|desc)$"),
        club_id: Optional[List[int]] = Query(None, description="Filter by specific club ID(s)"),
        country_id: Optional[str] = Query(None, description="Country ID to filter clubs"),
        owned: Optional[str] = Query(None, description="Filter clubs owned by the given name"),
        manager_locked: Optional[int] = Query(None, description="Exact value for manager_locked"),
        balance_min: Optional[int] = Query(None, description="Minimum balance"),
        balance_max: Optional[int] = Query(None, description="Maximum balance"),
    ):
        self.page = page
        self.per_page = per_page.value if isinstance(per_page, PerPageOptions) else per_page
        self.sort_by = sort_by
        self.sort_order = sort_order
        self.club_id = club_id
        self.country_id = country_id
        self.owned = owned
        self.manager_locked = manager_locked
        self.balance_min = balance_min
        self.balance_max = balance_max
        self.field_min_max = {
            "balance": (balance_min, balance_max)
        }

class DetailedClubQueryParams:
    """
    Query parameters for the /clubs/detailed endpoint.
    Includes all basic filters plus extended filters for detailed data.
    """
    def __init__(
        self,
        page: int = Query(1, ge=1, description="Page number, starting from 1"),
        per_page: PerPageOptions = Query(
            PerPageOptions.twenty,
            description="Number of records per page (options: 5, 20, 50)"
        ),
        sort_by: Optional[ClubsSortBy] = Query(None, description="Field to sort by"),
        sort_order: Optional[str] = Query("asc", description="Sort order: 'asc' or 'desc'", regex="^(asc|desc)$"),
        club_id: Optional[List[int]] = Query(None, description="Filter by specific club ID(s)"),
        country_id: Optional[str] = Query(None, description="Country ID to filter clubs"),
        owned: Optional[str] = Query(None, description="Filter clubs owned by the given name"),
        manager_locked: Optional[int] = Query(None, description="Exact value for manager_locked"),
        available: Optional[int] = Query(None, description="Availability status: 1 (available) or 0 (not available)"),
        league_id: Optional[int] = Query(None, description="Filter by league ID"),
        division: Optional[int] = Query(None, description="Filter by division"),
        has_job_posting: Optional[int] = Query(None, description="Filter clubs with job postings: 1 (has job posting), 0 (no job posting)"),
        balance_min: Optional[int] = Query(None, description="Minimum balance"),
        balance_max: Optional[int] = Query(None, description="Maximum balance"),
        division_start_min: Optional[int] = Query(None, description="Minimum division_start"),
        division_start_max: Optional[int] = Query(None, description="Maximum division_start"),
        fans_start_min: Optional[int] = Query(None, description="Minimum fans_start"),
        fans_start_max: Optional[int] = Query(None, description="Maximum fans_start"),
        fans_current_min: Optional[int] = Query(None, description="Minimum fans_current"),
        fans_current_max: Optional[int] = Query(None, description="Maximum fans_current"),
        stadium_size_start_min: Optional[int] = Query(None, description="Minimum stadium_size_start"),
        stadium_size_start_max: Optional[int] = Query(None, description="Maximum stadium_size_start"),
        stadium_size_current_min: Optional[int] = Query(None, description="Minimum stadium_size_current"),
        stadium_size_current_max: Optional[int] = Query(None, description="Maximum stadium_size_current"),
        stadium_id_min: Optional[int] = Query(None, description="Minimum stadium_id"),
        stadium_id_max: Optional[int] = Query(None, description="Maximum stadium_id"),
        value_min: Optional[int] = Query(None, description="Minimum value"),
        value_max: Optional[int] = Query(None, description="Maximum value"),
        rating_start_min: Optional[int] = Query(None, description="Minimum rating"),
        rating_start_max: Optional[int] = Query(None, description="Maximum rating"),
        default_formation_min: Optional[int] = Query(None, description="Minimum default_formation"),
        default_formation_max: Optional[int] = Query(None, description="Maximum default_formation"),
        penalty_taker_min: Optional[int] = Query(None, description="Minimum penalty_taker"),
        penalty_taker_max: Optional[int] = Query(None, description="Maximum penalty_taker"),
        transfers_in_min: Optional[int] = Query(None, description="Minimum transfers_in"),
        transfers_in_max: Optional[int] = Query(None, description="Maximum transfers_in"),
        transfers_out_min: Optional[int] = Query(None, description="Minimum transfers_out"),
        transfers_out_max: Optional[int] = Query(None, description="Maximum transfers_out"),
    ):
        self.page = page
        self.per_page = per_page.value if isinstance(per_page, PerPageOptions) else per_page
        self.sort_by = sort_by
        self.sort_order = sort_order
        self.club_id = club_id
        self.country_id = country_id
        self.owned = owned
        self.manager_locked = manager_locked
        self.available = available
        self.league_id = league_id
        self.division = division
        self.has_job_posting = has_job_posting
        self.balance_min = balance_min
        self.balance_max = balance_max
        self.division_start_min = division_start_min
        self.division_start_max = division_start_max
        self.fans_start_min = fans_start_min
        self.fans_start_max = fans_start_max
        self.fans_current_min = fans_current_min
        self.fans_current_max = fans_current_max
        self.stadium_size_start_min = stadium_size_start_min
        self.stadium_size_start_max = stadium_size_start_max
        self.stadium_size_current_min = stadium_size_current_min
        self.stadium_size_current_max = stadium_size_current_max
        self.stadium_id_min = stadium_id_min
        self.stadium_id_max = stadium_id_max
        self.value_min = value_min
        self.value_max = value_max
        self.rating_start_min = rating_start_min
        self.rating_start_max = rating_start_max
        self.default_formation_min = default_formation_min
        self.default_formation_max = default_formation_max
        self.penalty_taker_min = penalty_taker_min
        self.penalty_taker_max = penalty_taker_max
        self.transfers_in_min = transfers_in_min
        self.transfers_in_max = transfers_in_max
        self.transfers_out_min = transfers_out_min
        self.transfers_out_max = transfers_out_max

        self.field_min_max = {}
        for field in [
            "balance", "division_start", "fans_start", "fans_current", "stadium_size_start",
            "stadium_size_current", "stadium_id", "value", "rating_start", "default_formation",
            "penalty_taker", "transfers_in", "transfers_out"
        ]:
            min_value = locals().get(f"{field}_min")
            max_value = locals().get(f"{field}_max")
            setattr(self, f"{field}_min", min_value)
            setattr(self, f"{field}_max", max_value)
            self.field_min_max[field] = (min_value, max_value)

# -------------------------------------------------------------------------
# Routes for /clubs and /clubs/detailed endpoints with distinct query parameters
# -------------------------------------------------------------------------
@clubs_router.get(
    "/clubs",
    response_model=PaginatedResponse[ClubResponse],
    summary="Retrieve influence market data for clubs",
    description="Fetch a paginated list of clubs, including basic market data such as last price, trading volume, and optional filtering. Returns a PaginatedResponse of ClubResponse objects."
)
async def get_clubs(
    params: BasicClubQueryParams = Depends(),
    session: AsyncSession = Depends(get_mysql_session),
    userconfig_session: AsyncSession = Depends(get_userconfig_session),
):
    return await get_filtered_clubs(session, params, detailed=False, userconfig_session=userconfig_session)


@clubs_router.get(
    "/clubs/detailed",
    response_model=PaginatedResponse[ClubDetailedResponse],
    summary="Retrieve detailed data for clubs",
    description="Fetch a paginated list of clubs, including extended fields such as form, stadium data, fan counts, and more advanced filtering. Returns a PaginatedResponse of ClubDetailedResponse objects."
)
async def get_clubs_detailed(
    params: DetailedClubQueryParams = Depends(),
    session: AsyncSession = Depends(get_mysql_session),
    userconfig_session: AsyncSession = Depends(get_userconfig_session),
):
    return await get_filtered_clubs(session, params, detailed=True, userconfig_session=userconfig_session)


@clubs_router.get(
    "/clubs/{club_id}/job",
    response_model=JobPostingResponse,
    summary="Retrieve job posting details for a specific club",
    description="Fetch job posting information for a specific club, including poster name, posted time, and job description."
)
async def get_club_job_posting(
    club_id: int = Path(..., description="The ID of the club to get job posting for"),
    userconfig_session: AsyncSession = Depends(get_userconfig_session),
):
    # Check if userconfig database is available
    if userconfig_session is None:
        raise HTTPException(
            status_code=500,
            detail="Userconfig database not available - job posting feature disabled"
        )
    
    # Query the jobs_board table for the specific club
    stmt = select(JobsBoard).where(JobsBoard.club_id == club_id).order_by(JobsBoard.posted_at.desc()).limit(1)
    result = await userconfig_session.execute(stmt)
    job_posting = result.scalars().first()
    
    if not job_posting:
        raise HTTPException(status_code=404, detail=f"No job posting found for club ID {club_id}")
    
    return job_posting
