import json
import time
import logging
from typing import List, Optional
from sqlalchemy import select

from pydantic import BaseModel
from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from redis import asyncio as aioredis

from .base import get_archival_session, get_mysql_session, get_redis_client, DCClubs, Messages, Blocks, ShareTradeHistory

logger = logging.getLogger(__name__)

TICKER_CACHE_KEY = "ticker_data_v1"
TICKER_CACHE_TTL = 600  # 10 minutes


class ClubBalance(BaseModel):
    club: int
    balance: int


class ClubBalances(BaseModel):
    rich: List[ClubBalance]
    poor: List[ClubBalance]


class PlayerTransfer(BaseModel):
    date: int
    player: int
    newclub: int
    amount: int


class Share(BaseModel):
    type: str
    id: int


class ShareTrade(BaseModel):
    share: Share
    date: int
    price: int
    type: str


# Create router
ticker_router = APIRouter()


async def get_top_club_balances(
    session: AsyncSession,
    order: str,
    num: int
) -> List[ClubBalance]:
    """
    Query for top clubs with balances in the given order.
    
    Args:
        session: Database session (main database)
        order: Either "desc" for richest or "asc" for poorest
        num: Number of results to return
    """
    query = select(DCClubs.club_id, DCClubs.balance)
    
    if order == "desc":
        query = query.order_by(DCClubs.balance.desc(), DCClubs.club_id)
    else:
        query = query.order_by(DCClubs.balance.asc(), DCClubs.club_id)
    
    query = query.limit(num)
    
    result = await session.execute(query)
    rows = result.fetchall()
    
    return [ClubBalance(club=row.club_id, balance=row.balance) for row in rows]


async def get_club_balances(session: AsyncSession, num: int) -> ClubBalances:
    """
    Get richest and poorest clubs.
    
    Args:
        session: Database session (main database)
        num: Number of clubs to return for each category
    """
    rich = await get_top_club_balances(session, "desc", num)
    poor = await get_top_club_balances(session, "asc", num)
    
    return ClubBalances(rich=rich, poor=poor)


async def get_last_transfers(session: AsyncSession, num: int) -> List[PlayerTransfer]:
    """
    Get the latest player transfers.
    
    Args:
        session: Database session (archival database)
        num: Number of transfers to return
    """
    # TransferCompleted message type is 9
    query = (
        select(
            Blocks.date,
            Messages.data_1.label("player"),
            Messages.club_1.label("new_club"),
            Messages.data_2.label("amount")
        )
        .select_from(Messages)
        .join(Blocks, Messages.height == Blocks.height)
        .where(Messages.type == 9)
        .order_by(Messages.id.desc())
        .limit(num)
    )
    
    result = await session.execute(query)
    rows = result.fetchall()
    
    return [
        PlayerTransfer(
            date=row.date,
            player=row.player,
            newclub=row.new_club,
            amount=row.amount
        )
        for row in rows
    ]


async def get_share_trades(session: AsyncSession, share_type: str, num: int) -> List[ShareTrade]:
    """
    Get the latest share trades for a given share type.
    
    Args:
        session: Database session (archival database)
        share_type: Type of share ("club" or "player")
        num: Number of distinct shares to return
    """
    query = (
        select(
            ShareTradeHistory.share_type,
            ShareTradeHistory.share_id,
            ShareTradeHistory.price,
            ShareTradeHistory.market_buy,
            Blocks.date
        )
        .select_from(ShareTradeHistory)
        .join(Blocks, ShareTradeHistory.height == Blocks.height)
        .where(ShareTradeHistory.share_type == share_type)
        .order_by(ShareTradeHistory.id.desc())
    )
    
    # Explicit close in `finally` so the underlying streaming cursor +
    # connection are released back to the pool when we break out of the
    # loop early. The previous code did `result = await session.stream(query)`
    # and then `break`-ed out of the loop on line ~174 without ever closing
    # the stream, leaving the cursor + connection orphaned. Combined with
    # this function being called twice every 120s by the periodic ticker
    # cache refresher (4 workers per pod), that produced ~240 leaked stream
    # objects per pod per hour and was the root cause of the
    # red-datacentre-api OOM-restart sawtooth observed on 2026-04-08.
    #
    # NOTE: AsyncResult does NOT implement the async context manager
    # protocol despite being async-iterable, so `async with` does not work
    # here. Use try/finally with an explicit `await result.close()`.
    seen_shares = set()
    trades = []

    result = await session.stream(query)
    try:
        async for row in result:
            if row.share_id in seen_shares:
                continue
            seen_shares.add(row.share_id)

            trades.append(
                ShareTrade(
                    share=Share(type=row.share_type, id=row.share_id),
                    date=row.date,
                    price=row.price,
                    type="buy" if row.market_buy else "sell"
                )
            )

            if len(trades) >= num:
                break
    finally:
        await result.close()

    return trades


async def refresh_ticker_cache(
    r: Optional[aioredis.client.Redis],
    archival_session: AsyncSession,
    mysql_session: AsyncSession,
):
    """Fetch ticker data from DB and store in Redis cache."""
    if r is None:
        return

    start_time = time.perf_counter()

    club_balances = await get_club_balances(mysql_session, 5)
    transfers = await get_last_transfers(archival_session, 10)
    club_trades = await get_share_trades(archival_session, "club", 5)
    player_trades = await get_share_trades(archival_session, "player", 5)

    response = {
        "club_balances": club_balances.model_dump(),
        "last_transfers": [t.model_dump() for t in transfers],
        "club_trades": [t.model_dump() for t in club_trades],
        "player_trades": [t.model_dump() for t in player_trades],
    }

    await r.setex(TICKER_CACHE_KEY, TICKER_CACHE_TTL, json.dumps(response))
    logger.info(f"Ticker cache refreshed: {time.perf_counter() - start_time:.4f}s")


@ticker_router.get(
    "/ticker",
    summary="Get ticker data",
    description="Returns latest player transfers, share trades and richest/poorest clubs for the ticker display. Background-refreshed every 2 minutes."
)
async def get_ticker_data(
    r: Optional[aioredis.client.Redis] = Depends(get_redis_client),
    archival_session: AsyncSession = Depends(get_archival_session),
    mysql_session: AsyncSession = Depends(get_mysql_session),
):
    # Always serve from cache
    if r:
        cached = await r.get(TICKER_CACHE_KEY)
        if cached:
            return json.loads(cached)

    # Fallback if cache is cold (first request before background task runs)
    await refresh_ticker_cache(r, archival_session, mysql_session)

    if r:
        cached = await r.get(TICKER_CACHE_KEY)
        if cached:
            return json.loads(cached)

    return {"error": "Ticker data not available yet, please retry shortly."}
