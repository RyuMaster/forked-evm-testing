import time
import logging

from sqlalchemy import Column, BigInteger, String, Integer, Boolean, select, func
from typing import List, Optional
from datetime import datetime

from pydantic import BaseModel
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from .base import (
    Base,
    get_archival_session,
    PaginatedResponse,
    Blocks,
)

# Configure logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Define SQLAlchemy models for match events and commentary tables
# These match the MySQL archival database schema
class MatchEvent(Base):
    __tablename__ = 'match_events'
    id = Column(BigInteger, primary_key=True)
    height = Column(BigInteger, index=True)
    event_type = Column(String(32))
    player_id = Column(BigInteger)
    club_id = Column(BigInteger)
    time = Column(Integer)  # TINYINT in MySQL, but using Integer for SQLAlchemy
    goal_type = Column(String(16))
    fixture_id = Column(BigInteger, index=True)
    season_id = Column(BigInteger)

class CommEvent(Base):
    __tablename__ = 'comm_events'
    id = Column(BigInteger, primary_key=True)
    height = Column(BigInteger, index=True)
    time = Column(Integer)  # TINYINT in MySQL
    fixture_id = Column(BigInteger, index=True)
    season_id = Column(BigInteger)

class CommSubEvent(Base):
    __tablename__ = 'comm_sub_events'
    id = Column(BigInteger, primary_key=True)
    height = Column(BigInteger, index=True)
    comm_event_id = Column(BigInteger)
    category = Column(String(32))
    player_one_id = Column(BigInteger)
    player_two_id = Column(BigInteger)
    club_one_id = Column(BigInteger)
    club_two_id = Column(BigInteger)
    data_one = Column(String(32))

# Pydantic models for responses
class MatchEventResponse(BaseModel):
    match_event_id: int
    event_type: str
    player_id: int
    club_id: int
    time: int
    goal_type: Optional[str]
    season_id: int

class MatchCommentaryResponse(BaseModel):
    comm_sub_event_id: int
    category: str
    player_one_id: Optional[int]
    player_two_id: Optional[int]
    club_one_id: Optional[int]
    club_two_id: Optional[int]
    data_one: Optional[str]
    season_id: int
    comm_event_id: int
    time: int
    fixture_id: int

# Create an APIRouter instance
commentary_router = APIRouter()

@commentary_router.get(
    "/commentary/match_events/{fixture_id}",
    response_model=List[MatchEventResponse],
    summary="Get match events for a fixture",
    description="Retrieve all match events (goals, cards, etc.) for a specific fixture, ordered by time."
)
async def get_match_events(
    fixture_id: int,
    session: AsyncSession = Depends(get_archival_session),
):
    """
    Get match events for a specific fixture ID.
    This replaces the get_match_event(fixture_id=) RPC method.
    """
    start_time = time.perf_counter()

    # Build the query for match events - matches the RPC query exactly
    # RPC query: SELECT `match_event_id`, `event_type`, `player_id`, `club_id`, `time`, `goal_type`, `season_id` FROM match_events
    select_query = (
        select(
            MatchEvent.id.label('match_event_id'),  # MySQL uses 'id', but RPC expects 'match_event_id'
            MatchEvent.event_type,
            MatchEvent.player_id,
            MatchEvent.club_id,
            MatchEvent.time,
            MatchEvent.goal_type,
            MatchEvent.season_id,
        )
        .where(MatchEvent.fixture_id == fixture_id)
        .order_by(MatchEvent.time)
    )

    # Execute the query
    result = await session.execute(select_query)
    rows = result.fetchall()

    # Prepare the response items
    items = []
    for row in rows:
        item = MatchEventResponse(
            match_event_id=row.match_event_id,
            event_type=row.event_type,
            player_id=row.player_id,
            club_id=row.club_id,
            time=row.time,
            goal_type=row.goal_type,
            season_id=row.season_id,
        )
        items.append(item)

    logger.info(f"Time taken for match events: {time.perf_counter() - start_time:.4f}s")
    return items

@commentary_router.get(
    "/commentary/match_commentary/{fixture_id}",
    response_model=List[MatchCommentaryResponse],
    summary="Get match commentary for a fixture",
    description="Retrieve all match commentary events for a specific fixture, ordered by comm_sub_event_id."
)
async def get_match_commentary(
    fixture_id: int,
    session: AsyncSession = Depends(get_archival_session),
):
    """
    Get match commentary for a specific fixture ID.
    This replaces the get_match_commentary(fixture_id=) RPC method.
    Joins the comm_events and comm_sub_events tables.
    """
    start_time = time.perf_counter()

    # Build the join query - matches the example MySQL query provided
    # Example query: SELECT * FROM `comm_events` ev INNER JOIN `comm_sub_events` sev ON ev.`id` = sev.`comm_event_id` WHERE ev.`fixture_id` = 123
    join_clause = CommEvent.__table__.join(
        CommSubEvent.__table__,
        CommEvent.id == CommSubEvent.comm_event_id
    )

    select_query = (
        select(
            CommSubEvent.id.label('comm_sub_event_id'),  # Using MySQL 'id' field
            CommSubEvent.category,
            CommSubEvent.player_one_id,
            CommSubEvent.player_two_id,
            CommSubEvent.club_one_id,
            CommSubEvent.club_two_id,
            CommSubEvent.data_one,
            CommSubEvent.comm_event_id,
            CommEvent.time,
            CommEvent.fixture_id,
            CommEvent.season_id,
        )
        .select_from(join_clause)
        .where(CommEvent.fixture_id == fixture_id)
        .order_by(CommSubEvent.id)  # Order by comm_sub_event_id as per RPC
    )

    # Execute the query
    result = await session.execute(select_query)
    rows = result.fetchall()

    # Prepare the response items
    items = []
    for row in rows:
        item = MatchCommentaryResponse(
            comm_sub_event_id=row.comm_sub_event_id,
            category=row.category,
            player_one_id=row.player_one_id,
            player_two_id=row.player_two_id,
            club_one_id=row.club_one_id,
            club_two_id=row.club_two_id,
            data_one=row.data_one,
            season_id=row.season_id,
            comm_event_id=row.comm_event_id,
            time=row.time,
            fixture_id=row.fixture_id,
        )
        items.append(item)

    logger.info(f"Time taken for match commentary: {time.perf_counter() - start_time:.4f}s")
    return items