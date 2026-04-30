from sqlalchemy import Column, BigInteger, String, select, text, or_
from typing import List
from datetime import datetime

from pydantic import BaseModel
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from .base import Base, get_archival_session, get_mysql_session, Blocks, DCTransferCounts


# Pydantic response model
class TransferBasicResponse(BaseModel):
    player_id: int
    from_club: int
    to_club: int
    cost: int  # In smallest units
    unix_time: int
    datetime: datetime


class TransferCountResponse(BaseModel):
    season_id: int
    from_club: int
    to_club: int
    transfers: int


# Create router
transfers_router = APIRouter()


@transfers_router.get(
    "/transfers/basic",
    response_model=List[TransferBasicResponse],
    summary="Get latest completed transfers",
    description="Returns the last 20 completed transfers (auctions) from the blockchain, including player ID, clubs involved, cost, and timestamp."
)
async def get_basic_transfers(
    session: AsyncSession = Depends(get_archival_session),
):
    """
    Fetch the latest 20 completed transfers from the messages table.
    Transfers are identified by type=9 in the messages table.
    """
    
    # Query to get latest transfers with block timestamps
    query = text("""
        SELECT 
            m.data_1 as player_id,
            m.club_2 as from_club,
            m.club_1 as to_club,
            m.data_2 as cost,
            b.date as unix_time
        FROM messages m
        JOIN blocks b ON m.height = b.height
        WHERE m.type = 9
        ORDER BY m.height DESC
        LIMIT 20
    """)
    
    result = await session.execute(query)
    rows = result.fetchall()
    
    if not rows:
        return []
    
    # Build response
    transfers = []
    for row in rows:
        unix_time = int(row.unix_time)
        transfer = TransferBasicResponse(
            player_id=int(row.player_id),
            from_club=int(row.from_club),
            to_club=int(row.to_club),
            cost=int(row.cost),
            unix_time=unix_time,
            datetime=datetime.utcfromtimestamp(unix_time)
        )
        transfers.append(transfer)
    
    return transfers


@transfers_router.get(
    "/transfers/counts",
    response_model=List[TransferCountResponse],
    summary="Get transfer counts for a club in a season",
    description="Returns transfer counts from dc_transfer_counts table for a given club and season, including both incoming and outgoing transfers."
)
async def get_transfer_counts(
    club_id: int = Query(..., description="Club ID"),
    season_id: int = Query(..., description="Season ID"),
    session: AsyncSession = Depends(get_mysql_session),
):
    """
    Fetch transfer counts for a specific club and season.
    Returns all entries where the club_id matches either from_club or to_club.
    """
    
    query = select(DCTransferCounts).where(
        DCTransferCounts.season_id == season_id,
        or_(
            DCTransferCounts.from_club == club_id,
            DCTransferCounts.to_club == club_id
        )
    )
    
    result = await session.execute(query)
    rows = result.scalars().all()
    
    return [
        TransferCountResponse(
            season_id=row.season_id,
            from_club=row.from_club,
            to_club=row.to_club,
            transfers=row.transfers
        )
        for row in rows
    ]