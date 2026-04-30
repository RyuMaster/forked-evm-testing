# modules/users.py

from sqlalchemy import Column, Integer, BigInteger, String, DateTime, select, func, Text
from typing import List, Optional
from datetime import datetime
from pydantic import BaseModel
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from enum import Enum

from .base import (
    Base,
    get_mysql_session,
    PaginatedResponse,
    PerPageOptions,
    parse_json_field,
    fetch_paginated_data,
    DCUsers,
    get_userconfig_session,
    DEFAULT_PROFILE_PIC_URL,
    DCClubs,
)
# 2) Import our new profile-pic helper
from modules.utils.profile import get_profiles_for_users

# -----------------------------------------------------------------------------
# SQLAlchemy model for dc_users_trading
# -----------------------------------------------------------------------------
class DCUsersTrading(Base):
    __tablename__ = "dc_users_trading"
    user_name = Column(String, primary_key=True)
    buy_volume_1_day = Column(BigInteger)
    buy_volume_7_day = Column(BigInteger)
    sell_volume_1_day = Column(BigInteger)
    sell_volume_7_day = Column(BigInteger)
    buy_total_volume = Column(BigInteger)
    sell_total_volume = Column(BigInteger)
    total_volume = Column(BigInteger)
    total_volume_30_day = Column(BigInteger)  # New column
    first_trade_date = Column(BigInteger)
    tenth_trade_date = Column('10th_trade_date', BigInteger)
    hundredth_trade_date = Column('100th_trade_date', BigInteger)
    thousandth_trade_date = Column('1000th_trade_date', BigInteger)
    biggest_trade = Column(BigInteger)
    last_7days = Column(Text)
    last_30days = Column(Text)  # New column

# -----------------------------------------------------------------------------
# Pydantic models for user responses
# -----------------------------------------------------------------------------
class UserResponse(BaseModel):
    name: str
    balance: Optional[int]
    last_active_unix: Optional[int]
    last_active: Optional[datetime]
    club_id: Optional[int]
    profile_pic: Optional[str] = None
    # >>> NEW FIELD <<<
    manager_voted: Optional[int] = None  # Null if no club, else the DCClubs.manager_voted

    class Config:
        from_attributes = True

class UserDetailedResponse(UserResponse):
    buy_volume_1_day: Optional[int]
    buy_volume_7_day: Optional[int]
    sell_volume_1_day: Optional[int]
    sell_volume_7_day: Optional[int]
    buy_total_volume: Optional[int]
    sell_total_volume: Optional[int]
    total_volume: Optional[int]
    total_volume_30_day: Optional[int]
    first_trade_date: Optional[int]
    tenth_trade_date: Optional[int]
    hundredth_trade_date: Optional[int]
    thousandth_trade_date: Optional[int]
    biggest_trade: Optional[int]
    last_7days: Optional[List[int]]
    last_30days: Optional[List[int]]

# -----------------------------------------------------------------------------
# Define a FastAPI router for users
# -----------------------------------------------------------------------------
users_router = APIRouter()

# -----------------------------------------------------------------------------
# Enum definitions for sorting
# -----------------------------------------------------------------------------
class UsersSortBy(str, Enum):
    name = "name"
    balance = "balance"
    last_active_unix = "last_active_unix"
    club_id = "club_id"
    manager_voted = "manager_voted"

class UsersDetailedSortBy(str, Enum):
    name = "name"
    balance = "balance"
    last_active_unix = "last_active_unix"
    club_id = "club_id"
    buy_volume_1_day = "buy_volume_1_day"
    buy_volume_7_day = "buy_volume_7_day"
    sell_volume_1_day = "sell_volume_1_day"
    sell_volume_7_day = "sell_volume_7_day"
    buy_total_volume = "buy_total_volume"
    sell_total_volume = "sell_total_volume"
    total_volume = "total_volume"
    total_volume_30_day = "total_volume_30_day"
    first_trade_date = "first_trade_date"
    tenth_trade_date = "10th_trade_date"
    hundredth_trade_date = "100th_trade_date"
    thousandth_trade_date = "1000th_trade_date"
    biggest_trade = "biggest_trade"
    manager_voted = "manager_voted"

# -----------------------------------------------------------------------------
# Distinct Query Parameter Classes for FastAPI Documentation
# -----------------------------------------------------------------------------
class BasicUserQueryParams:
    """
    Query parameters for the basic /users endpoint.
    Only includes filters relevant to basic user data.
    """
    def __init__(
        self,
        page: int = Query(1, ge=1, description="Page number, starting from 1"),
        per_page: PerPageOptions = Query(
            PerPageOptions.twenty,
            description="Number of records per page (options: 5, 20, 50, 100)"
        ),
        sort_by: Optional[UsersSortBy] = Query(None, description="Field to sort by"),
        sort_order: Optional[str] = Query(
            "asc", description="Sort order: 'asc' or 'desc'", regex="^(asc|desc)$"
        ),
        name_prefix: Optional[str] = Query(None, min_length=2, description="Name prefix to search"),
        names: Optional[List[str]] = Query(None, description="List of names to include"),
    ):
        self.page = page
        self.per_page = per_page.value if isinstance(per_page, PerPageOptions) else per_page
        self.sort_by = sort_by
        self.sort_order = sort_order
        self.name_prefix = name_prefix
        self.names = names

class DetailedUserQueryParams:
    """
    Query parameters for the /users/detailed endpoint.
    Includes all basic filters and additional fields for detailed trading data.
    """
    def __init__(
        self,
        page: int = Query(1, ge=1, description="Page number, starting from 1"),
        per_page: PerPageOptions = Query(
            PerPageOptions.twenty,
            description="Number of records per page (options: 5, 20, 50, 100)"
        ),
        sort_by: Optional[UsersDetailedSortBy] = Query(None, description="Field to sort by"),
        sort_order: Optional[str] = Query(
            "asc", description="Sort order: 'asc' or 'desc'", regex="^(asc|desc)$"
        ),
        name_prefix: Optional[str] = Query(None, min_length=2, description="Name prefix to search"),
        names: Optional[List[str]] = Query(None, description="List of names to include"),
        # >>> Additional detailed filters:
        buy_volume_min: Optional[int] = Query(None, description="Minimum buy volume (1 day)"),
        buy_volume_max: Optional[int] = Query(None, description="Maximum buy volume (1 day)"),
        sell_volume_min: Optional[int] = Query(None, description="Minimum sell volume (1 day)"),
        sell_volume_max: Optional[int] = Query(None, description="Maximum sell volume (1 day)"),
        total_volume_min: Optional[int] = Query(None, description="Minimum total volume"),
        total_volume_max: Optional[int] = Query(None, description="Maximum total volume"),
    ):
        self.page = page
        self.per_page = per_page.value if isinstance(per_page, PerPageOptions) else per_page
        self.sort_by = sort_by
        self.sort_order = sort_order
        self.name_prefix = name_prefix
        self.names = names
        # Additional filters for detailed endpoint
        self.buy_volume_min = buy_volume_min
        self.buy_volume_max = buy_volume_max
        self.sell_volume_min = sell_volume_min
        self.sell_volume_max = sell_volume_max
        self.total_volume_min = total_volume_min
        self.total_volume_max = total_volume_max

# -----------------------------------------------------------------------------
# Endpoint: GET /users (basic)
# -----------------------------------------------------------------------------
@users_router.get(
    "/users",
    response_model=PaginatedResponse[UserResponse],
    summary="Get user list and basic data",
    description="""Fetches a paginated list of users, including name, balance, last activity, 
    club ID, whether they voted for a manager, and profile picture. 
    Supports sorting by specific fields, filtering by name prefix, or a list of names."""
)
async def get_users(
    params: BasicUserQueryParams = Depends(),
    session: AsyncSession = Depends(get_mysql_session),
    userconfig_session: AsyncSession = Depends(get_userconfig_session),
):
    # Extract parameters from the dependency
    page = params.page
    per_page = params.per_page
    chosen_sort = params.sort_by.value if params.sort_by else None
    sort_order = params.sort_order

    users_alias = DCUsers.__table__.alias("users_alias")
    dc_clubs_alias = DCClubs.__table__.alias("dc_clubs_alias")

    sortable_fields_users = {
        "name": users_alias.c.name,
        "balance": users_alias.c.balance,
        "last_active_unix": users_alias.c.last_active,
        "club_id": users_alias.c.club_id,
        "manager_voted": dc_clubs_alias.c.manager_voted,
    }

    join_clause = users_alias.outerjoin(
        dc_clubs_alias,
        users_alias.c.club_id == dc_clubs_alias.c.club_id
    )

    select_query = select(
        users_alias.c.name,
        users_alias.c.balance,
        users_alias.c.last_active,
        users_alias.c.club_id,
        dc_clubs_alias.c.manager_voted.label("manager_voted"),
    ).select_from(join_clause)

    extra_filters = []
    if params.name_prefix:
        extra_filters.append(users_alias.c.name.ilike(f"{params.name_prefix}%"))
    if params.names:
        extra_filters.append(users_alias.c.name.in_(params.names))
        # Ensure exact match including trailing spaces
        extra_filters.append(func.char_length(users_alias.c.name).in_([len(n) for n in params.names]))

    if extra_filters:
        for condition in extra_filters:
            select_query = select_query.where(condition)

    total_query = select(func.count()).select_from(join_clause)
    if extra_filters:
        for condition in extra_filters:
            total_query = total_query.where(condition)

    total, total_pages, rows = await fetch_paginated_data(
        session,
        select_query,
        total_query,
        sortable_fields_users,
        chosen_sort,
        sort_order,
        page,
        per_page,
        []
    )

    # Batch fetch profile pics
    names_needed = {r.name for r in rows if r.name}
    name_to_pic = await get_profiles_for_users(list(names_needed), userconfig_session)

    items = []
    for row in rows:
        last_active_unix = row.last_active
        last_active = datetime.utcfromtimestamp(last_active_unix) if last_active_unix else None
        pic_url = name_to_pic.get(row.name, DEFAULT_PROFILE_PIC_URL)

        user_response = UserResponse(
            name=row.name,
            balance=row.balance,
            last_active_unix=last_active_unix,
            last_active=last_active,
            club_id=row.club_id,
            profile_pic=pic_url,
            manager_voted=row.manager_voted,
        )
        items.append(user_response)

    return PaginatedResponse(
        page=page,
        per_page=per_page,
        total=total,
        total_pages=total_pages,
        items=items,
    )

# -----------------------------------------------------------------------------
# Endpoint: GET /users/detailed
# -----------------------------------------------------------------------------
@users_router.get(
    "/users/detailed",
    response_model=PaginatedResponse[UserDetailedResponse],
    summary="Get detailed user and trading data",
    description="""Fetches a paginated list of users with detailed trading volume statistics 
    (daily, weekly, total), last activity, club information (manager voted), and profile picture. 
    Supports sorting and filtering by name prefix, a list of names, and additional trading filters."""
)
async def get_users_detailed(
    params: DetailedUserQueryParams = Depends(),
    session: AsyncSession = Depends(get_mysql_session),
    userconfig_session: AsyncSession = Depends(get_userconfig_session),
):
    page = params.page
    per_page = params.per_page
    chosen_sort = params.sort_by.value if params.sort_by else None
    sort_order = params.sort_order

    users_alias = DCUsers.__table__.alias("users_alias")
    trading_alias = DCUsersTrading.__table__.alias("trading_alias")
    dc_clubs_alias = DCClubs.__table__.alias("dc_clubs_alias")

    extra_filters = []
    if params.name_prefix:
        extra_filters.append(users_alias.c.name.ilike(f"{params.name_prefix}%"))
    if params.names:
        extra_filters.append(users_alias.c.name.in_(params.names))
        # Ensure exact match including trailing spaces
        extra_filters.append(func.char_length(users_alias.c.name).in_([len(n) for n in params.names]))
    # Apply additional detailed filters if provided
    if params.buy_volume_min is not None:
        extra_filters.append(trading_alias.c.buy_volume_1_day >= params.buy_volume_min)
    if params.buy_volume_max is not None:
        extra_filters.append(trading_alias.c.buy_volume_1_day <= params.buy_volume_max)
    if params.sell_volume_min is not None:
        extra_filters.append(trading_alias.c.sell_volume_1_day >= params.sell_volume_min)
    if params.sell_volume_max is not None:
        extra_filters.append(trading_alias.c.sell_volume_1_day <= params.sell_volume_max)
    if params.total_volume_min is not None:
        extra_filters.append(trading_alias.c.total_volume >= params.total_volume_min)
    if params.total_volume_max is not None:
        extra_filters.append(trading_alias.c.total_volume <= params.total_volume_max)

    sortable_fields = {
        "name": users_alias.c.name,
        "balance": users_alias.c.balance,
        "last_active_unix": users_alias.c.last_active,
        "club_id": users_alias.c.club_id,
        "buy_volume_1_day": trading_alias.c.buy_volume_1_day,
        "buy_volume_7_day": trading_alias.c.buy_volume_7_day,
        "sell_volume_1_day": trading_alias.c.sell_volume_1_day,
        "sell_volume_7_day": trading_alias.c.sell_volume_7_day,
        "buy_total_volume": trading_alias.c.buy_total_volume,
        "sell_total_volume": trading_alias.c.sell_total_volume,
        "total_volume": trading_alias.c.total_volume,
        "total_volume_30_day": trading_alias.c.total_volume_30_day,
        "first_trade_date": trading_alias.c.first_trade_date,
        "10th_trade_date": trading_alias.c['10th_trade_date'],
        "100th_trade_date": trading_alias.c['100th_trade_date'],
        "1000th_trade_date": trading_alias.c['1000th_trade_date'],
        "biggest_trade": trading_alias.c.biggest_trade,
        "manager_voted": dc_clubs_alias.c.manager_voted,
    }

    join_clause = users_alias.outerjoin(
        trading_alias,
        users_alias.c.name == trading_alias.c.user_name
    ).outerjoin(
        dc_clubs_alias,
        users_alias.c.club_id == dc_clubs_alias.c.club_id
    )

    select_query = (
        select(
            users_alias.c.name,
            users_alias.c.balance,
            users_alias.c.last_active,
            users_alias.c.club_id,
            trading_alias.c.buy_volume_1_day,
            trading_alias.c.buy_volume_7_day,
            trading_alias.c.sell_volume_1_day,
            trading_alias.c.sell_volume_7_day,
            trading_alias.c.buy_total_volume,
            trading_alias.c.sell_total_volume,
            trading_alias.c.total_volume,
            trading_alias.c.total_volume_30_day,
            trading_alias.c.first_trade_date,
            trading_alias.c['10th_trade_date'].label('tenth_trade_date'),
            trading_alias.c['100th_trade_date'].label('hundredth_trade_date'),
            trading_alias.c['1000th_trade_date'].label('thousandth_trade_date'),
            trading_alias.c.biggest_trade,
            trading_alias.c.last_7days,
            trading_alias.c.last_30days,
            dc_clubs_alias.c.manager_voted.label("manager_voted"),
        )
        .select_from(join_clause)
    )

    total_query = select(func.count()).select_from(join_clause)

    if extra_filters:
        for condition in extra_filters:
            select_query = select_query.where(condition)
            total_query = total_query.where(condition)

    total, total_pages, rows = await fetch_paginated_data(
        session,
        select_query,
        total_query,
        sortable_fields,
        chosen_sort,
        sort_order,
        page,
        per_page,
        []
    )

    names_needed = set()
    for row in rows:
        if row.name:
            names_needed.add(row.name)

    name_to_pic = await get_profiles_for_users(list(names_needed), userconfig_session)

    items = []
    seen_names = set()
    for row in rows:
        if row.name in seen_names:
            continue
        seen_names.add(row.name)

        last_active_unix = row.last_active
        last_active = datetime.utcfromtimestamp(last_active_unix) if last_active_unix else None

        last_7days = parse_json_field(row.last_7days)
        last_7days = [int(x or 0) for x in last_7days]

        last_30days = parse_json_field(row.last_30days)
        last_30days = [int(x or 0) for x in last_30days]

        pic_url = name_to_pic.get(row.name, DEFAULT_PROFILE_PIC_URL)

        user_response = UserDetailedResponse(
            name=row.name,
            balance=row.balance,
            last_active_unix=last_active_unix,
            last_active=last_active,
            club_id=row.club_id,
            profile_pic=pic_url,
            manager_voted=row.manager_voted,
            buy_volume_1_day=int(row.buy_volume_1_day or 0),
            buy_volume_7_day=int(row.buy_volume_7_day or 0),
            sell_volume_1_day=int(row.sell_volume_1_day or 0),
            sell_volume_7_day=int(row.sell_volume_7_day or 0),
            buy_total_volume=int(row.buy_total_volume or 0),
            sell_total_volume=int(row.sell_total_volume or 0),
            total_volume=int(row.total_volume or 0),
            total_volume_30_day=int(row.total_volume_30_day or 0),
            first_trade_date=row.first_trade_date,
            tenth_trade_date=row.tenth_trade_date,
            hundredth_trade_date=row.hundredth_trade_date,
            thousandth_trade_date=row.thousandth_trade_date,
            biggest_trade=row.biggest_trade,
            last_7days=last_7days,
            last_30days=last_30days,
        )
        items.append(user_response)

    return PaginatedResponse(
        page=page,
        per_page=per_page,
        total=total,
        total_pages=total_pages,
        items=items,
    )
