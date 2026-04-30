# modules/club_balance_sheet.py

import time
import logging
from typing import List
from datetime import datetime

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy import Column, BigInteger, String, select
from sqlalchemy.ext.asyncio import AsyncSession

# Import from your 'base.py'
from .base import (
    Base,
    Blocks,
    get_archival_session,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


# ------------------------------------------------------------------------
# 1. SQLAlchemy Model for 'club_balance_sheets' (if not already defined)
# ------------------------------------------------------------------------
class ClubBalanceSheets(Base):
    __tablename__ = 'club_balance_sheets'
    id = Column(BigInteger, primary_key=True)
    height = Column(BigInteger, index=True)
    club_id = Column(BigInteger, index=True)
    season_id = Column(BigInteger)
    fixture_id = Column(BigInteger)
    amount = Column(BigInteger)
    type = Column(String(64))
    balance = Column(BigInteger)


# ------------------------------------------------------------------------
# 2. Pydantic Model for the returned weekly data
#    - Includes "season"
#    - We have no pagination, so just a list.
# ------------------------------------------------------------------------
class ClubWeeklyBalanceSheetResponse(BaseModel):
    season: int
    game_week: int
    date: int
    agent_wages: int
    cash_injection: int
    gate_receipts: int
    ground_maintenance: int
    managers_wage: int
    merchandise: int
    other_income: int
    other_outgoings: int
    player_wages: int
    prize_money: int
    shareholder_payouts: int
    shareholder_prize_money: int
    sponsor: int
    transfers_in: int
    transfers_out: int
    tv_revenue: int


# ------------------------------------------------------------------------
# 3. FastAPI Router
#    - No pagination
#    - game_week starts at 1 for the earliest date in that season
#    - returns a list of items
# ------------------------------------------------------------------------
club_balance_sheet_router = APIRouter()


@club_balance_sheet_router.get(
    "/club_balance_sheet/weeks",
    response_model=List[ClubWeeklyBalanceSheetResponse],
    summary="Get club balance sheet entries by game week",
    description="Returns a list of aggregated balance-sheet entries for the specified club and season, grouped by distinct block dates in ascending order. Each block date corresponds to a new 'game_week'. No pagination is used."
)
async def get_club_balance_sheet_weeks(
    club_id: int = Query(..., ge=1, description="Club ID"),
    season_id: int = Query(..., ge=1, description="Season ID"),
    session: AsyncSession = Depends(get_archival_session),
):
    """
    Returns a list of aggregated balance-sheet entries for the given club & season,
    grouped by distinct block dates (ascending). Each distinct block date becomes
    a new 'game_week': starting at 1 for the earliest date in that season.
    
    No pagination, so it returns all rows for that season in one list.
    """

    start_time = time.perf_counter()

    # Aliases
    cbs_alias = ClubBalanceSheets.__table__.alias("cbs_alias")
    blocks_alias = Blocks.__table__.alias("blocks_alias")

    # ----------------------------------------------------------------
    # Step 1: Find all distinct block dates for [club_id, season_id].
    #         Sort ascending (the earliest block date => game_week = 1).
    # ----------------------------------------------------------------
    join_clause = cbs_alias.join(
        blocks_alias,
        cbs_alias.c.height == blocks_alias.c.height
    )

    distinct_dates_query = (
        select(blocks_alias.c.date)
        .select_from(join_clause)
        .where(cbs_alias.c.club_id == club_id)
        .where(cbs_alias.c.season_id == season_id)
        .distinct()
        .order_by(blocks_alias.c.date.asc())
    )
    distinct_dates_result = await session.execute(distinct_dates_query)
    distinct_dates = [row[0] for row in distinct_dates_result.fetchall()]

    if not distinct_dates:
        # No data => return an empty list
        return []

    # date_to_gw maps each block date => game_week (1-based)
    date_to_gw = {}
    for i, d in enumerate(distinct_dates, start=1):
        date_to_gw[d] = i

    # ----------------------------------------------------------------
    # Step 2: Query all rows for that club_id & season_id (all dates).
    # ----------------------------------------------------------------
    select_query = (
        select(
            cbs_alias.c.type,
            cbs_alias.c.amount,
            blocks_alias.c.date.label("block_date"),
        )
        .select_from(join_clause)
        .where(cbs_alias.c.club_id == club_id)
        .where(cbs_alias.c.season_id == season_id)
    )
    results = await session.execute(select_query)
    rows = results.fetchall()

    # ----------------------------------------------------------------
    # Step 3: Prepare set of "known" types => aggregator
    #         We'll group them by block_date and sum amounts.
    # ----------------------------------------------------------------
    known_types = [
        "agent_wages", "cash_injection", "gate_receipts", "ground_maintenance",
        "managers_wage", "merchandise", "other_income", "other_outgoings",
        "player_wages", "prize_money", "shareholder_payouts",
        "shareholder_prize_money", "sponsor", "transfers_in", "transfers_out",
        "tv_revenue",
    ]

    from collections import defaultdict

    # aggregated_data[block_date] = { "agent_wages": 0, "cash_injection": 0, ... }
    aggregated_data = {}

    # Initialize each date with zeros
    for d in distinct_dates:
        aggregated_data[d] = {t: 0 for t in known_types}

    # Fill in from the DB rows
    for row in rows:
        row_type = row.type or ""
        block_date = row.block_date
        amount_value = row.amount or 0

        # If you want negative amounts left as negative, remove abs(...)
        # If you want them all positive, use abs(...)
        # Below uses abs() to ensure "all numbers are positive."
        amount_value = abs(amount_value)

        # If 'row_type' is not recognized, we'll lump it under "other_outgoings"
        if row_type not in known_types:
            row_type = "other_outgoings"

        aggregated_data[block_date][row_type] += amount_value

    # ----------------------------------------------------------------
    # Step 4: Build final response list in ascending order of block_date
    #         game_week = date_to_gw[the_date]
    #         season => just use season_id
    # ----------------------------------------------------------------
    items: List[ClubWeeklyBalanceSheetResponse] = []

    for d in distinct_dates:
        gw = date_to_gw[d]
        # Pull the aggregated amounts
        row_dict = aggregated_data[d]

        # Create a dict with the final structure
        data = {
            "season": season_id,
            "game_week": gw,
            "date": d,
            # Fill from aggregator
            "agent_wages": row_dict["agent_wages"],
            "cash_injection": row_dict["cash_injection"],
            "gate_receipts": row_dict["gate_receipts"],
            "ground_maintenance": row_dict["ground_maintenance"],
            "managers_wage": row_dict["managers_wage"],
            "merchandise": row_dict["merchandise"],
            "other_income": row_dict["other_income"],
            "other_outgoings": row_dict["other_outgoings"],
            "player_wages": row_dict["player_wages"],
            "prize_money": row_dict["prize_money"],
            "shareholder_payouts": row_dict["shareholder_payouts"],
            "shareholder_prize_money": row_dict["shareholder_prize_money"],
            "sponsor": row_dict["sponsor"],
            "transfers_in": row_dict["transfers_in"],
            "transfers_out": row_dict["transfers_out"],
            "tv_revenue": row_dict["tv_revenue"],
        }

        item = ClubWeeklyBalanceSheetResponse(**data)
        items.append(item)

    logger.info(f"[ClubBalanceSheet] club_id={club_id}, season_id={season_id}, "
                f"items_count={len(items)}, time={time.perf_counter() - start_time:.4f}s")

    return items
