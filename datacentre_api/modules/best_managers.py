# best_managers.py

from enum import Enum
from sqlalchemy import Column, Integer, String, select, func
from typing import List, Optional
from pydantic import BaseModel
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime

from .base import (
    Base,
    get_mysql_session,
    get_userconfig_session,
    PaginatedResponse,
    PerPageOptions,
    fetch_paginated_data,
    DEFAULT_PROFILE_PIC_URL,
    DCUsers,
)
from modules.utils.profile import get_profiles_for_users

class BestManagersSortBy(str, Enum):
    name = "name"
    rank_old = "rank_old"
    rank_old_ranking = "rank_old_ranking"
    rank_a = "rank_a"
    rank_a_ranking = "rank_a_ranking"
    rank_b = "rank_b"
    rank_b_ranking = "rank_b_ranking"
    last_active_unix = "last_active_unix"

class DCBestManagers(Base):
    __tablename__ = 'dc_best_managers'
    id = Column(Integer, primary_key=True)
    name = Column(String(255), unique=True)
    rank_old = Column(Integer)
    rank_a = Column(Integer)
    rank_b = Column(Integer)

class BestManagersResponse(BaseModel):
    name: str
    rank_old: Optional[int]
    rank_old_ranking: Optional[int]
    rank_a: Optional[int]
    rank_a_ranking: Optional[int]
    rank_b: Optional[int]
    rank_b_ranking: Optional[int]
    profile_pic: Optional[str] = None
    last_active_unix: Optional[int] = None
    last_active: Optional[datetime] = None

    class Config:
        from_attributes = True

best_managers_router = APIRouter()

@best_managers_router.get(
    "/best_managers",
    response_model=PaginatedResponse[BestManagersResponse],
    summary="Get best managers data",
    description="""Retrieves a paginated list of the best managers from the database. Includes their ranks (old, A, B),
    computed rankings, user's last active timestamp, and associated profile pictures. Supports sorting, pagination, and optional filtering by manager name."""
)
async def get_best_managers(
    page: int = Query(1, ge=1, description="Page number, starting from 1"),
    per_page: PerPageOptions = Query(
        PerPageOptions.twenty,
        description="Number of records per page (options: 5, 20, 50)",
    ),
    sort_by: BestManagersSortBy = Query(
        BestManagersSortBy.rank_a_ranking,
        description="Field to sort by: rank_old, rank_old_ranking, rank_a, rank_a_ranking, rank_b, rank_b_ranking, last_active_unix",
    ),
    sort_order: Optional[str] = Query(
        "asc", description="Sort order: 'asc' or 'desc'", regex="^(asc|desc)$"
    ),
    name: Optional[str] = Query(None, description="Filter by specific user name (case sensitive)"),
    session: AsyncSession = Depends(get_mysql_session),
    userconfig_session: AsyncSession = Depends(get_userconfig_session),
):
    per_page = per_page.value

    # Query to get min_rank_a
    result_min_rank_a = await session.execute(select(func.min(DCBestManagers.rank_a)))
    min_rank_a = result_min_rank_a.scalar()
    if min_rank_a is None:
        min_rank_a = 0
    adjustment_rank_a = -min_rank_a if min_rank_a < 0 else 0

    # Query to get min_rank_b
    result_min_rank_b = await session.execute(select(func.min(DCBestManagers.rank_b)))
    min_rank_b = result_min_rank_b.scalar()
    if min_rank_b is None:
        min_rank_b = 0
    adjustment_rank_b = -min_rank_b if min_rank_b < 0 else 0

    # Base query with adjusted ranks and rankings
    ranked_query = select(
        DCBestManagers.name,
        DCBestManagers.rank_old,
        DCBestManagers.rank_a,
        DCBestManagers.rank_b,
        func.dense_rank().over(order_by=DCBestManagers.rank_old.desc()).label('rank_old_ranking'),
        func.dense_rank().over(order_by=(DCBestManagers.rank_a + adjustment_rank_a).desc()).label('rank_a_ranking'),
        func.dense_rank().over(order_by=(DCBestManagers.rank_b + adjustment_rank_b).desc()).label('rank_b_ranking'),
        DCUsers.last_active.label('last_active_unix'),
    ).outerjoin(
        DCUsers,
        DCBestManagers.name == DCUsers.name
    ).subquery()

    select_query = select(ranked_query)

    extra_filters = []
    if name:
        # Store exact length to ensure we match including trailing spaces
        name_len = len(name)
        extra_filters.append(ranked_query.c.name == name)
        extra_filters.append(func.char_length(ranked_query.c.name) == name_len)

    count_query = select(func.count()).select_from(ranked_query)
    if extra_filters:
        for condition in extra_filters:
            select_query = select_query.where(condition)
            count_query = count_query.where(condition)

    # Map sortable fields
    sortable_fields = {
        "name": ranked_query.c.name,
        "rank_old": ranked_query.c.rank_old,
        "rank_old_ranking": ranked_query.c.rank_old_ranking,
        "rank_a": ranked_query.c.rank_a,
        "rank_a_ranking": ranked_query.c.rank_a_ranking,
        "rank_b": ranked_query.c.rank_b,
        "rank_b_ranking": ranked_query.c.rank_b_ranking,
        "last_active_unix": ranked_query.c.last_active_unix,
    }

    # Use the user’s chosen sort_by enum value
    chosen_sort = sort_by.value

    total, total_pages, rows = await fetch_paginated_data(
        session,
        select_query,
        count_query,
        sortable_fields,
        chosen_sort,
        sort_order,
        page,
        per_page,
        extra_filters,
    )

    names_needed = {row.name for row in rows if row.name}
    name_to_pic = await get_profiles_for_users(list(names_needed), userconfig_session)

    items = []
    for row in rows:
        adjusted_rank_a = (row.rank_a or 0) + adjustment_rank_a
        adjusted_rank_b = (row.rank_b or 0) + adjustment_rank_b
        pic_url = name_to_pic.get(row.name, DEFAULT_PROFILE_PIC_URL)

        # Convert last_active_unix to datetime
        last_active_unix = row.last_active_unix
        last_active = datetime.utcfromtimestamp(last_active_unix) if last_active_unix else None

        items.append(BestManagersResponse(
            name=row.name,
            rank_old=row.rank_old,
            rank_old_ranking=row.rank_old_ranking,
            rank_a=adjusted_rank_a,
            rank_a_ranking=row.rank_a_ranking,
            rank_b=adjusted_rank_b,
            rank_b_ranking=row.rank_b_ranking,
            profile_pic=pic_url,
            last_active_unix=last_active_unix,
            last_active=last_active,
        ))

    return PaginatedResponse(
        page=page,
        per_page=per_page,
        total=total,
        total_pages=total_pages,
        items=items,
    )
