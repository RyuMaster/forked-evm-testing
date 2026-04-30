import time
import logging

from sqlalchemy import select, func
from typing import List, Optional
from datetime import datetime

from pydantic import BaseModel
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from .base import (
    get_archival_session,
    get_userconfig_session,
    PaginatedResponse,
    Blocks,
    ShareTradeHistory,
    ShareTransactions,
    DEFAULT_PROFILE_PIC_URL,
)
# Import the helper for fetching profile pictures
from modules.utils.profile import get_profiles_for_users

# Configure logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Pydantic model for the response
class ShareTradeResponse(BaseModel):
    id: int
    time: datetime
    unix_time: int
    share_type: str
    share_id: int
    buyer: Optional[str]
    buyer_profile_pic: Optional[str] = None
    seller: Optional[str]
    seller_profile_pic: Optional[str] = None
    num: int
    price: int
    market_buy: bool

# Pydantic model for the transactions response
class ShareTransactionResponse(BaseModel):
    unix_time: int
    share_type: str
    share_id: int
    name: str
    num: int
    type: str
    other_name: Optional[str]

# Create an APIRouter instance
share_history_router = APIRouter()

@share_history_router.get(
    "/share_trade_history",
    response_model=PaginatedResponse[ShareTradeResponse],
    summary="Retrieve share trade history",
    description="Provides a paginated record of trades for clubs or players, optionally filtered by buyer/seller name, with optional sorting."
)
async def get_share_trade_history(
    page: int = Query(1, ge=1, description="Page number, starting from 1"),
    per_page: int = Query(50, ge=1, le=100, description="Number of records per page"),
    name: Optional[str] = Query(None, description="Name of the buyer or seller"),
    club_id: Optional[int] = Query(None, description="Club ID to filter trades"),
    player_id: Optional[int] = Query(None, description="Player ID to filter trades"),
    # --- NEW: Sorting parameters ---
    sort_by: Optional[str] = Query(
        None,
        description="Field to sort by (e.g. 'unix_time', 'price', 'num', or 'id')"
    ),
    sort_order: Optional[str] = Query(
        "desc",
        description="Sort order: 'asc' or 'desc'",
        regex="^(asc|desc)$"
    ),
    session: AsyncSession = Depends(get_archival_session),
    userconfig_session: AsyncSession = Depends(get_userconfig_session),
):
    start_time = time.perf_counter()  # Start timing

    # Alias tables
    trade_alias = ShareTradeHistory.__table__.alias("trade_alias")
    blocks_alias = Blocks.__table__.alias("blocks_alias")

    # Join share_trade_history with blocks on height
    join_clause = trade_alias.join(
        blocks_alias,
        trade_alias.c.height == blocks_alias.c.height
    )

    # Build the base select query
    select_query = (
        select(
            trade_alias.c.id,
            blocks_alias.c.date.label('unix_time'),
            trade_alias.c.share_type,
            trade_alias.c.share_id,
            trade_alias.c.buyer,
            trade_alias.c.seller,
            trade_alias.c.num,
            trade_alias.c.price,
            trade_alias.c.market_buy,
        )
        .select_from(join_clause)
    )

    # Prepare filter conditions
    filters = []

    if name:
        # Filter by exact name in either buyer or seller (including trailing spaces)
        name_len = len(name)
        buyer_filter = (trade_alias.c.buyer == name) & (func.char_length(trade_alias.c.buyer) == name_len)
        seller_filter = (trade_alias.c.seller == name) & (func.char_length(trade_alias.c.seller) == name_len)
        filters.append(buyer_filter | seller_filter)

    if club_id:
        # Filter by club shares
        filters.append((trade_alias.c.share_type == 'club') & (trade_alias.c.share_id == club_id))

    if player_id:
        # Filter by player shares
        filters.append((trade_alias.c.share_type == 'player') & (trade_alias.c.share_id == player_id))

    # Ensure at least one filter is provided
    if not filters:
        raise HTTPException(
            status_code=400,
            detail="At least one of 'name', 'club_id', or 'player_id' must be provided."
        )

    # Apply all filter conditions
    for condition in filters:
        select_query = select_query.where(condition)

    # --- NEW: Apply sorting ---
    # Map recognized fields to columns
    valid_sort_fields = {
        "unix_time": blocks_alias.c.date,    # Sorting by block time
        "price": trade_alias.c.price,
        "num": trade_alias.c.num,
        "id": trade_alias.c.id,
    }

    if sort_by and sort_by in valid_sort_fields:
        sort_col = valid_sort_fields[sort_by]
        if sort_order == "desc":
            select_query = select_query.order_by(sort_col.desc())
        else:
            select_query = select_query.order_by(sort_col.asc())
    else:
        # Default sort if no valid sort_by is provided
        # (same as old: descending by trade_alias.c.height)
        select_query = select_query.order_by(trade_alias.c.height.desc())

    # Apply pagination
    select_query = select_query.offset((page - 1) * per_page).limit(per_page)

    # Execute the query
    result = await session.execute(select_query)
    rows = result.fetchall()

    # ----------------------------------------------------------------------
    # BATCH FETCH PROFILE PICTURES for buyers & sellers
    # ----------------------------------------------------------------------
    names_to_lookup = set()
    for row in rows:
        if row.buyer:
            names_to_lookup.add(row.buyer)
        if row.seller:
            names_to_lookup.add(row.seller)

    name_to_pic = await get_profiles_for_users(list(names_to_lookup), userconfig_session)

    # Prepare the response items
    items = []
    for row in rows:
        # Convert unix_time to datetime
        unix_time = int(row.unix_time or 0)
        time_obj = datetime.utcfromtimestamp(unix_time)

        # Retrieve pic URLs (now guaranteed to be default if not found/disabled)
        buyer_pic = name_to_pic.get(row.buyer, DEFAULT_PROFILE_PIC_URL)
        seller_pic = name_to_pic.get(row.seller, DEFAULT_PROFILE_PIC_URL)

        item = ShareTradeResponse(
            id=row.id,
            time=time_obj,
            unix_time=unix_time,
            share_type=row.share_type,
            share_id=row.share_id,
            buyer=row.buyer,
            buyer_profile_pic=buyer_pic,
            seller=row.seller,
            seller_profile_pic=seller_pic,
            num=row.num,
            price=row.price,
            market_buy=bool(row.market_buy),
        )
        items.append(item)

    # Return the paginated response without total counts
    response = PaginatedResponse(
        page=page,
        per_page=per_page,
        items=items,
    )

    logger.info(f"Time taken: {time.perf_counter() - start_time:.4f}s")
    return response

@share_history_router.get(
    "/share_trade_history/all",
    response_model=List[ShareTradeResponse],
    summary="Get last 20 trades",
    description="Returns the 20 most recent trades with all details."
)
async def get_all_recent_trades(
    session: AsyncSession = Depends(get_archival_session),
    userconfig_session: AsyncSession = Depends(get_userconfig_session),
):
    start_time = time.perf_counter()
    
    # Join trades with blocks to get timestamps
    trade_alias = ShareTradeHistory.__table__.alias("t")
    blocks_alias = Blocks.__table__.alias("b")
    
    query = (
        select(
            trade_alias.c.id,
            blocks_alias.c.date.label('unix_time'),
            trade_alias.c.share_type,
            trade_alias.c.share_id,
            trade_alias.c.buyer,
            trade_alias.c.seller,
            trade_alias.c.num,
            trade_alias.c.price,
            trade_alias.c.market_buy,
        )
        .select_from(
            trade_alias.join(blocks_alias, trade_alias.c.height == blocks_alias.c.height)
        )
        .order_by(trade_alias.c.id.desc())
        .limit(20)
    )
    
    result = await session.execute(query)
    rows = result.fetchall()
    
    # Batch fetch profile pictures
    names = {r.buyer for r in rows if r.buyer} | {r.seller for r in rows if r.seller}
    name_to_pic = await get_profiles_for_users(list(names), userconfig_session)
    
    # Build response
    trades = [
        ShareTradeResponse(
            id=row.id,
            time=datetime.utcfromtimestamp(int(row.unix_time or 0)),
            unix_time=int(row.unix_time or 0),
            share_type=row.share_type,
            share_id=row.share_id,
            buyer=row.buyer,
            buyer_profile_pic=name_to_pic.get(row.buyer, DEFAULT_PROFILE_PIC_URL),
            seller=row.seller,
            seller_profile_pic=name_to_pic.get(row.seller, DEFAULT_PROFILE_PIC_URL),
            num=row.num,
            price=row.price,
            market_buy=bool(row.market_buy),
        )
        for row in rows
    ]
    
    logger.info(f"All trades fetch time: {time.perf_counter() - start_time:.4f}s")
    return trades

@share_history_router.get(
    "/share/transactions",
    response_model=List[ShareTransactionResponse],
    summary="Retrieve share transactions for a user",
    description="Returns all share transactions for a specific user, including transfers between users."
)
async def get_share_transactions(
    name: str = Query(..., description="Xaya name of the user"),
    session: AsyncSession = Depends(get_archival_session),
):
    start_time = time.perf_counter()
    
    # Join share_transactions with blocks to get timestamps
    transactions_alias = ShareTransactions.__table__.alias("t")
    blocks_alias = Blocks.__table__.alias("b")
    
    query = (
        select(
            blocks_alias.c.date.label('unix_time'),
            transactions_alias.c.share_type,
            transactions_alias.c.share_id,
            transactions_alias.c.name,
            transactions_alias.c.num,
            transactions_alias.c.type,
            transactions_alias.c.other_name,
        )
        .select_from(
            transactions_alias.join(blocks_alias, transactions_alias.c.height == blocks_alias.c.height)
        )
        .where(transactions_alias.c.name == name)
        .order_by(transactions_alias.c.id.desc())
    )
    
    result = await session.execute(query)
    rows = result.fetchall()
    
    transactions = [
        ShareTransactionResponse(
            unix_time=row.unix_time,
            share_type=row.share_type,
            share_id=row.share_id,
            name=row.name,
            num=row.num,
            type=row.type,
            other_name=row.other_name,
        )
        for row in rows
    ]
    
    logger.info(f"Share transactions fetch time: {time.perf_counter() - start_time:.4f}s")
    return transactions
