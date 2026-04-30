# File: modules/user_balance_sheet.py
import time  # Import the time module
import logging  # Import the logging module
from sqlalchemy import Column, BigInteger, String, select, func, text, case
from typing import Optional, List
from datetime import datetime, timedelta
from pydantic import BaseModel
from fastapi import APIRouter, Depends, Query, HTTPException, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession
from .base import (
    Base,
    get_archival_session,
    get_redis_client,
    PaginatedResponse,
    Blocks,
    get_mysql_session,  # to fetch the latest svc2usdc rate
)
import json

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

from enum import Enum
class PerPageOptionsExtended(int, Enum):
    five = 5
    ten = 10
    twenty = 20
    fifty = 50
    hundred = 100

class TypeAmountEntry(BaseModel):
    type: str
    amount: int

class UserBalanceSheetResponse(BaseModel):
    name: str
    amount: int
    type: str
    other_name: Optional[str] = None
    other_type: Optional[str] = None
    other_id: Optional[int] = None
    fixture_id: Optional[int] = None
    time: datetime
    unix_time: int

class UserWeeklyBalanceSheetResponse(BaseModel):
    name: str
    week_start_unix_time: int
    week_end_unix_time: int
    totals: List[TypeAmountEntry]

class EarningsPeriod(BaseModel):
    svc: int
    usdc: float

class UserEarningsResponse(BaseModel):
    svc2usdc: float
    earnings_7d: EarningsPeriod
    earnings_14d: EarningsPeriod
    earnings_30d: EarningsPeriod
    earnings_last_match_day: EarningsPeriod

class UserBalanceSheets(Base):
    __tablename__ = 'user_balance_sheets'
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    height = Column(BigInteger, index=True)
    name = Column(String(255), index=True)
    amount = Column(BigInteger)
    type = Column(String(50))
    other_name = Column(String(255))
    other_type = Column(String(50))
    other_id = Column(BigInteger)
    fixture_id = Column(BigInteger)

user_balance_sheet_router = APIRouter()

@user_balance_sheet_router.get(
    "/user_balance_sheet",
    response_model=PaginatedResponse[UserBalanceSheetResponse],
    summary="Get user balance sheet entries",
    description="""
    Retrieves transaction-based balance sheet entries for a specific user.
    Supports either a paginated query of the most recent records or a focused time range
    (up to 30 days). Each entry contains amount, type, and referenced 'other' data.
    """
)
async def get_user_balance_sheet(
    page: int = Query(1, ge=1, description="Page number, starting from 1"),
    per_page: PerPageOptionsExtended = Query(
        PerPageOptionsExtended.fifty,
        description="Number of records per page (options: 5, 20, 50, 100)",
    ),
    name: str = Query(..., description="Name of the user (exact match)"),
    from_time: Optional[int] = Query(
        None, description="Unix time integer for start of time range"
    ),
    to_time: Optional[int] = Query(
        None, description="Unix time integer for end of time range"
    ),
    session: AsyncSession = Depends(get_archival_session),
):
    per_page_value = per_page.value
    # validate time window
    if from_time is not None and to_time is not None:
        if to_time < from_time:
            raise HTTPException(400, "'to_time' must be >= 'from_time'")
        max_gap = 30 * 24 * 60 * 60   # 30 days in seconds
        if to_time - from_time > max_gap:
            raise HTTPException(400, "Time range cannot exceed 30 days")
        paginate = False
    else:
        paginate = True

    name_len = len(name)
    b = UserBalanceSheets.__table__.alias("b")
    blk = Blocks.__table__.alias("blk")

    qry = (
        select(
            b.c.name,
            b.c.amount,
            b.c.type,
            b.c.other_name,
            b.c.other_type,
            b.c.other_id,
            b.c.fixture_id,
            blk.c.date.label("unix_time"),
        )
        .select_from(b.join(blk, b.c.height == blk.c.height))
        # first filter by normal equality (uses index), then by exact length
        .where(
            b.c.name == name,
            func.char_length(b.c.name) == name_len
        )
        .order_by(b.c.height.desc())
    )

    if from_time is not None and to_time is not None:
        qry = qry.where(blk.c.date.between(from_time, to_time))
    else:
        qry = qry.offset((page - 1) * per_page_value).limit(per_page_value)

    result = await session.execute(qry)
    rows = result.fetchall()

    items: List[UserBalanceSheetResponse] = []
    for r in rows:
        ut = int(r.unix_time or 0)
        items.append(UserBalanceSheetResponse(
            name=r.name,
            amount=r.amount,
            type=r.type,
            other_name=r.other_name,
            other_type=r.other_type,
            other_id=r.other_id,
            fixture_id=r.fixture_id,
            time=datetime.utcfromtimestamp(ut),
            unix_time=ut,
        ))

    if paginate:
        return PaginatedResponse(page=page, per_page=per_page_value, items=items)
    else:
        return PaginatedResponse(page=1, per_page=len(items), items=items)


@user_balance_sheet_router.get(
    "/user_balance_sheet/weeks",
    response_model=PaginatedResponse[UserWeeklyBalanceSheetResponse],
    summary="Get weekly aggregated user balance sheets",
    description="""
    Returns a paginated set of weekly aggregated balance sheet data for a given user.
    Groups transactions by week (Monday 00:00:00 through Sunday 23:59:59) and sums the amounts by type.
    """
)
async def get_user_balance_sheet_weeks(
    page: int = Query(1, ge=1, description="Page number, starting from 1"),
    per_page: int = Query(10, ge=1, le=100, description="Number of weeks per page"),
    name: str = Query(..., description="Name of the user (exact match)"),
    session: AsyncSession = Depends(get_archival_session),
):
    # Try to get from cache first
    cache_key = f"user_balance_weeks:{name}:page:{page}:per_page:{per_page}"
    redis_cl = get_redis_client()
    if redis_cl:
        try:
            cached = await redis_cl.get(cache_key)
            if cached:
                logger.info(f"Cache hit for user_balance_weeks: {name}, page {page}")
                return json.loads(cached)
        except Exception as e:
            logger.warning(f"Cache read error: {e}")

    name_len = len(name)
    b = UserBalanceSheets.__table__.alias("b")
    blk = Blocks.__table__.alias("blk")

    # Use YEARWEEK with mode 6 (Monday start, week 1-53)
    # This naturally groups Monday-Sunday as a week
    year_week = func.YEARWEEK(func.FROM_UNIXTIME(blk.c.date), 6)

    # Get actual min/max dates from the data for accurate boundaries
    # We'll calculate these in the aggregation query
    week_start_unix = func.MIN(blk.c.date)
    week_end_unix = func.MAX(blk.c.date)

    # First get the week boundaries
    week_bounds_q = (
        select(
            year_week.label("year_week"),
            func.MIN(blk.c.date).label("week_start"),
            func.MAX(blk.c.date).label("week_end")
        )
        .select_from(b.join(blk, b.c.height == blk.c.height))
        .where(
            b.c.name == name,
            func.char_length(b.c.name) == name_len
        )
        .group_by(year_week)
    )

    # Aggregation per‐type using YEARWEEK for grouping
    agg_base = (
        select(
            b.c.name,
            year_week.label("year_week"),
            b.c.type,
            func.SUM(b.c.amount).label("total_amount"),
        )
        .select_from(b.join(blk, b.c.height == blk.c.height))
        .where(
            b.c.name == name,
            func.char_length(b.c.name) == name_len
        )
        .group_by(year_week, b.c.type)
    )

    # Count distinct weeks using YEARWEEK (much faster)
    total_q = (
        select(func.count(func.distinct(year_week)))
        .select_from(b.join(blk, b.c.height == blk.c.height))
        .where(
            b.c.name == name,
            func.char_length(b.c.name) == name_len
        )
    )
    total_weeks = (await session.execute(total_q)).scalar() or 0
    total_pages = (total_weeks + per_page - 1) // per_page if total_weeks else 0

    # Which weeks for this page? Use YEARWEEK for faster grouping
    week_list_q = (
        select(func.distinct(year_week))
        .select_from(b.join(blk, b.c.height == blk.c.height))
        .where(
            b.c.name == name,
            func.char_length(b.c.name) == name_len
        )
        .group_by(year_week)
        .order_by(year_week.desc())
        .offset((page - 1) * per_page)
        .limit(per_page)
    )
    week_rows = (await session.execute(week_list_q)).scalars().all()
    if not week_rows:
        return PaginatedResponse(
            page=page,
            per_page=per_page,
            total=total_weeks,
            total_pages=total_pages,
            items=[]
        )

    # First fetch the week boundaries
    bounds_q = week_bounds_q.where(year_week.in_(week_rows))
    bounds_result = await session.execute(bounds_q)
    week_bounds = {row.year_week: (row.week_start, row.week_end) for row in bounds_result}

    # Then fetch sums for those weeks
    agg_q = agg_base.where(year_week.in_(week_rows)).order_by(year_week.desc())
    rows = (await session.execute(agg_q)).fetchall()

    week_data = {}
    for r in rows:
        week_key = r.year_week

        # Get the actual boundaries for this week
        if week_key in week_bounds:
            ws, we = week_bounds[week_key]
        else:
            # Fallback (shouldn't happen)
            ws = we = 0

        if week_key not in week_data:
            week_data[week_key] = {
                "name": r.name,
                "week_start_unix_time": int(ws),
                "week_end_unix_time": int(we),
                "totals": []
            }
        week_data[week_key]["totals"].append(
            TypeAmountEntry(type=r.type, amount=int(r.total_amount or 0))
        )

    items = [
        UserWeeklyBalanceSheetResponse(**v)
        for v in sorted(week_data.values(), key=lambda x: x["week_start_unix_time"], reverse=True)
    ]

    result = PaginatedResponse(
        page=page,
        per_page=per_page,
        total=total_weeks,
        total_pages=total_pages,
        items=items
    )

    # Cache the result if available
    # Check if all weeks are complete (not current week)
    # Current week ends at next Sunday 00:00:00 UTC
    if redis_cl and items:
        try:
            current_time = int(time.time())
            # Get the current week's Sunday start (259200 = 3 days; epoch + 3d = Sun Jan 4 1970)
            current_week_start = current_time - ((current_time - 259200) % 604800)

            # Check if the newest week in results is before current week
            newest_week_start = items[0].week_start_unix_time if items else 0

            if newest_week_start < current_week_start:
                # All weeks are complete, cache for a long time (30 days)
                cache_ttl = 30 * 24 * 3600
            else:
                # Contains current week, cache for short time (5 minutes)
                cache_ttl = 5 * 60

            await redis_cl.setex(
                cache_key,
                cache_ttl,
                json.dumps(result.dict())
            )
            logger.info(f"Cached user_balance_weeks for {name}, page {page} (TTL: {cache_ttl}s)")
        except Exception as e:
            logger.warning(f"Cache write error: {e}")

    return result


@user_balance_sheet_router.get(
    "/user_balance_sheet/earnings",
    response_model=UserEarningsResponse,
    summary="Get aggregated SVC earnings",
    description="""
    Aggregates a user's earnings (from entries that include dividend amounts)
    in SVC for the last 7, 14, and 30 days and for the last match day.
    Also includes the latest SVC-to-USDC conversion rate and computes USDC values accordingly.
    """
)
async def get_user_earnings(
    name: str = Query(..., description="Name of the user (exact match)"),
    archival_session: AsyncSession = Depends(get_archival_session),
    mysql_session: AsyncSession = Depends(get_mysql_session),
    background_tasks: BackgroundTasks = BackgroundTasks()
):
    # Try to get from cache first
    cache_key = f"user_earnings:{name}"
    redis_cl = get_redis_client()
    if redis_cl:
        try:
            cached = await redis_cl.get(cache_key)
            if cached:
                logger.info(f"Cache hit for user_earnings: {name}")
                return UserEarningsResponse(**json.loads(cached))
        except Exception as e:
            logger.warning(f"Cache read error: {e}")
    now = int(time.time())
    p7 = now - 7*86400
    p14 = now - 14*86400
    p30 = now - 30*86400

    name_len = len(name)
    b = UserBalanceSheets.__table__.alias("b")
    blk = Blocks.__table__.alias("blk")
    join_clause = b.join(blk, b.c.height == blk.c.height)

    conditions = [
        b.c.name == name,
        func.char_length(b.c.name) == name_len,
        b.c.type.ilike("dividend%")
    ]

    earnings_q = (
        select(
            func.sum(case((blk.c.date >= p7, b.c.amount), else_=0)).label("e7"),
            func.sum(case((blk.c.date >= p14, b.c.amount), else_=0)).label("e14"),
            func.sum(b.c.amount).label("e30"),
        )
        .select_from(join_clause)
        .where(*conditions, blk.c.date >= p30)
    )
    e7, e14, e30 = (await archival_session.execute(earnings_q)).one()
    e7 = int(e7 or 0)
    e14 = int(e14 or 0)
    e30 = int(e30 or 0)

    # last match day
    last_time_q = (
        select(func.max(blk.c.date))
        .select_from(join_clause)
        .where(*conditions)
    )
    last_time = (await archival_session.execute(last_time_q)).scalar()
    if not last_time:
        lm_svc = 0
    else:
        dt = datetime.utcfromtimestamp(last_time)
        if dt.hour == 0 and dt.minute == 0 and dt.second == 0:
            dt -= timedelta(days=1)
        start = int(datetime(dt.year, dt.month, dt.day).timestamp())
        end = start + 86400 - 1
        lm_q = (
            select(func.sum(b.c.amount))
            .select_from(join_clause)
            .where(*conditions, blk.c.date.between(start, end))
        )
        lm_svc = int((await archival_session.execute(lm_q)).scalar() or 0)

    # Get SVC price using the cached service
    from .services.svc_price_service import get_svc_price

    redis_client_price = get_redis_client()

    rate = await get_svc_price(
        redis_client=redis_client_price,
        mysql_session=mysql_session,
        background_tasks=background_tasks
    )

    def to_usdc(s: int) -> float:
        return round(s / 10000 * rate, 2)

    response = UserEarningsResponse(
        svc2usdc=rate,
        earnings_7d=EarningsPeriod(svc=e7, usdc=to_usdc(e7)),
        earnings_14d=EarningsPeriod(svc=e14, usdc=to_usdc(e14)),
        earnings_30d=EarningsPeriod(svc=e30, usdc=to_usdc(e30)),
        earnings_last_match_day=EarningsPeriod(svc=lm_svc, usdc=to_usdc(lm_svc))
    )

    # Cache the result for 10 minutes
    if redis_cl:
        try:
            cache_ttl = 10 * 60  # 10 minutes
            await redis_cl.setex(
                cache_key,
                cache_ttl,
                json.dumps(response.dict())
            )
            logger.info(f"Cached user_earnings for {name} (TTL: {cache_ttl}s)")
        except Exception as e:
            logger.warning(f"Cache write error: {e}")

    return response
