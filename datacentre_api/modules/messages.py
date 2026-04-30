from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from pydantic import BaseModel

from .base import get_archival_session, Messages, MessageIndexTable, Blocks

messages_router = APIRouter()


class MessageResponse(BaseModel):
    message_id: int
    date: int
    type: int
    sub_type: int
    club_1: int
    club_2: int
    data_1: int
    data_2: int
    data_3: int
    data_4: int
    data_5: int
    name_1: Optional[str]
    season_id: int


@messages_router.get(
    "/messages",
    response_model=List[MessageResponse],
    summary="Get messages from the blockchain",
    description="Returns messages from the archival database. Requires season_id and at least one of: country_id, comp_id, or club_id."
)
async def get_messages(
    session: AsyncSession = Depends(get_archival_session),
    season_id: int = Query(..., description="Season ID to filter messages"),
    country_id: Optional[str] = Query(None, description="Country ID to filter messages"),
    comp_id: Optional[int] = Query(None, description="Competition ID to filter messages"),
    club_id: Optional[int] = Query(None, description="Club ID to filter messages"),
):
    if not any([country_id, comp_id, club_id]):
        raise HTTPException(
            status_code=400,
            detail="At least one of country_id, comp_id, or club_id must be provided"
        )
    
    query = (
        select(
            Messages.id.label('message_id'),
            Blocks.date,
            Messages.type,
            Messages.sub_type,
            Messages.club_1,
            Messages.club_2,
            Messages.data_1,
            Messages.data_2,
            Messages.data_3,
            Messages.data_4,
            Messages.data_5,
            Messages.name_1,
            Messages.season_id
        )
        .select_from(Messages)
        .join(Blocks, Messages.height == Blocks.height)
        .where(Messages.season_id == season_id)
    )
    
    # Use EXISTS subqueries to check if message appears in message_index_table
    # with the specified filters. This allows a message to match even if the
    # filters are satisfied by different rows in message_index_table.
    if country_id is not None:
        query = query.where(
            select(MessageIndexTable.message_id)
            .where(MessageIndexTable.message_id == Messages.id)
            .where(MessageIndexTable.country_id == country_id)
            .exists()
        )
    
    if comp_id is not None:
        query = query.where(
            select(MessageIndexTable.message_id)
            .where(MessageIndexTable.message_id == Messages.id)
            .where(MessageIndexTable.competition_id == comp_id)
            .exists()
        )
    
    if club_id is not None:
        query = query.where(
            select(MessageIndexTable.message_id)
            .where(MessageIndexTable.message_id == Messages.id)
            .where(MessageIndexTable.club_id == club_id)
            .exists()
        )
    
    query = query.order_by(Blocks.date.desc(), Messages.id.desc())
    
    result = await session.execute(query)
    rows = result.fetchall()
    
    messages = []
    for row in rows:
        message = MessageResponse(
            message_id=int(row.message_id),
            date=int(row.date),
            type=int(row.type),
            sub_type=int(row.sub_type),
            club_1=int(row.club_1),
            club_2=int(row.club_2),
            data_1=int(row.data_1),
            data_2=int(row.data_2),
            data_3=int(row.data_3),
            data_4=int(row.data_4),
            data_5=int(row.data_5),
            name_1=row.name_1,
            season_id=int(row.season_id)
        )
        messages.append(message)
    
    return messages
