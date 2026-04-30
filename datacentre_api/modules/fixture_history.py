import time
import logging
import zlib
import json

from sqlalchemy import Column, BigInteger, String, Integer, Boolean, LargeBinary, select, func
from typing import List, Optional, Dict, Any, Union
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

# Define SQLAlchemy model for fixture_players table
# This matches the MySQL archival database schema
class FixturePlayer(Base):
    __tablename__ = 'fixture_players'
    id = Column(BigInteger, primary_key=True)
    height = Column(BigInteger, index=True)
    team = Column(Integer)
    player_id = Column(BigInteger)
    start_ix = Column(Integer)
    time_started = Column(Integer)
    time_finished = Column(Integer)
    minutes_played = Column(Integer)
    saves = Column(Integer)
    key_tackles = Column(Integer)
    key_passes = Column(Integer)
    assists = Column(Integer)
    shots = Column(Integer)
    goals = Column(Integer)
    yellow_cards = Column(Integer)
    red_cards = Column(Integer)
    yellowred_cards = Column(Integer)
    injuries = Column(Integer)
    keeping_abilities = Column(Integer)
    tackling_abilities = Column(Integer)
    passing_abilities = Column(Integer)
    shooting_abilities = Column(Integer)
    rating = Column(Integer)
    youth_player = Column(Boolean)
    fixture_id = Column(BigInteger, index=True)
    season_id = Column(BigInteger)

# Define SQLAlchemy model for old_tactic_actions table
class OldTacticAction(Base):
    __tablename__ = 'old_tactic_actions'
    id = Column(BigInteger, primary_key=True)
    height = Column(BigInteger, index=True)
    fixture_id = Column(BigInteger, index=True)
    club_id = Column(BigInteger)
    time = Column(Integer)
    situation = Column(Integer)
    goal_margin = Column(Integer)
    formation_id = Column(Integer)
    play_style = Column(Integer)
    use_playmaker = Column(Integer)
    use_target_man = Column(Integer)
    captain = Column(Integer)
    penalty_taker = Column(Integer)
    free_kicks = Column(Integer)
    corner_taker = Column(Integer)
    playmaker = Column(Integer)
    target_man = Column(Integer)
    autopicked = Column(String(32))

# Define SQLAlchemy model for old_tactic_action_lineups table
class OldTacticActionLineup(Base):
    __tablename__ = 'old_tactic_action_lineups'
    id = Column(BigInteger, primary_key=True)
    height = Column(BigInteger, index=True)
    old_tactic_action_id = Column(BigInteger, index=True)
    player_id = Column(BigInteger)
    tackling_style = Column(Integer)
    tempo = Column(Integer)

# Define SQLAlchemy model for old_tactics table
class OldTactics(Base):
    __tablename__ = 'old_tactics'
    id = Column(BigInteger, primary_key=True)
    height = Column(BigInteger, index=True)
    fixture_id = Column(BigInteger, index=True)
    club_id = Column(BigInteger)
    set_tactics = Column(LargeBinary)
    auto_tactics = Column(LargeBinary)
    reason_for_auto = Column(String(64))

# Pydantic models for responses
class FixturePlayerResponse(BaseModel):
    team: int
    player_id: int
    start_ix: int
    time_started: int
    time_finished: int
    minutes_played: int
    saves: int
    key_tackles: int
    key_passes: int
    assists: int
    shots: int
    goals: int
    yellow_cards: int
    red_cards: int
    yellowred_cards: int
    injuries: int
    keeping_abilities: int
    tackling_abilities: int
    passing_abilities: int
    shooting_abilities: int
    rating: int
    youth_player: bool
    fixture_id: int

class LineupItem(BaseModel):
    player_id: int
    tempo: int
    tackling_style: int

class TacticActionItem(BaseModel):
    time: int
    situation: int
    goal_margin: int
    formation_id: int
    play_style: int
    use_playmaker: int
    use_target_man: int
    captain: int
    penalty_taker: int
    free_kicks: int
    corner_taker: int
    playmaker: int
    target_man: int
    autopicked: Optional[str]
    lineup: List[LineupItem]

class FixtureTacticsResponse(BaseModel):
    club_id: int
    tactic_actions: List[TacticActionItem]
    set_tactics: Any
    auto_tactics: Optional[Any]
    reason_for_auto: Optional[str]

# Create an APIRouter instance
fixture_history_router = APIRouter()

@fixture_history_router.get(
    "/fixture_history/players/{fixture_id}",
    response_model=List[FixturePlayerResponse],
    summary="Get fixture player data for a fixture",
    description="Retrieve all player performance data for a specific fixture from the archival database."
)
async def get_fixture_player_data(
    fixture_id: int,
    session: AsyncSession = Depends(get_archival_session),
):
    """
    Get fixture player data for a specific fixture ID.
    This replaces the get_fixture_player_data(fixture_id=) RPC method.
    """
    start_time = time.perf_counter()

    # Build the query for fixture players - matches the RPC query exactly
    # RPC query: SELECT `fixture_player_id`, `team`, `player_id`, `start_ix`, `time_started`, `time_finished`,
    # `saves`, `key_tackles`, `key_passes`, `assists`, `shots`, `goals`, `yellow_cards`, `red_cards`, `yellowred_cards`,
    # `injuries`, `keeping_abilities`, `tackling_abilities`, `passing_abilities`, `shooting_abilities`,
    # `rating`, `youth_player`, `fixture_id`, `season_id` FROM fixture_player WHERE fixture_id = ?1
    select_query = (
        select(
            FixturePlayer.team,
            FixturePlayer.player_id,
            FixturePlayer.start_ix,
            FixturePlayer.time_started,
            FixturePlayer.time_finished,
            FixturePlayer.minutes_played,
            FixturePlayer.saves,
            FixturePlayer.key_tackles,
            FixturePlayer.key_passes,
            FixturePlayer.assists,
            FixturePlayer.shots,
            FixturePlayer.goals,
            FixturePlayer.yellow_cards,
            FixturePlayer.red_cards,
            FixturePlayer.yellowred_cards,
            FixturePlayer.injuries,
            FixturePlayer.keeping_abilities,
            FixturePlayer.tackling_abilities,
            FixturePlayer.passing_abilities,
            FixturePlayer.shooting_abilities,
            FixturePlayer.rating,
            FixturePlayer.youth_player,
            FixturePlayer.fixture_id,
        )
        .where(FixturePlayer.fixture_id == fixture_id)
    )

    # Execute the query
    result = await session.execute(select_query)
    rows = result.fetchall()

    # Prepare the response items
    items = []
    for row in rows:
        item = FixturePlayerResponse(
            team=row.team,
            player_id=row.player_id,
            start_ix=row.start_ix,
            time_started=row.time_started,
            time_finished=row.time_finished,
            minutes_played=row.minutes_played,
            saves=row.saves,
            key_tackles=row.key_tackles,
            key_passes=row.key_passes,
            assists=row.assists,
            shots=row.shots,
            goals=row.goals,
            yellow_cards=row.yellow_cards,
            red_cards=row.red_cards,
            yellowred_cards=row.yellowred_cards,
            injuries=row.injuries,
            keeping_abilities=row.keeping_abilities,
            tackling_abilities=row.tackling_abilities,
            passing_abilities=row.passing_abilities,
            shooting_abilities=row.shooting_abilities,
            rating=row.rating,
            youth_player=row.youth_player,
            fixture_id=row.fixture_id,
        )
        items.append(item)

    logger.info(f"Time taken for fixture player data: {time.perf_counter() - start_time:.4f}s")
    return items


@fixture_history_router.get(
    "/fixture_history/tactics/{fixture_id}",
    response_model=List[FixtureTacticsResponse],
    summary="Get fixture tactics data for a fixture",
    description="Retrieve all tactics data for a specific fixture from the archival database, grouped by club_id."
)
async def get_fixture_tactics_data(
    fixture_id: int,
    session: AsyncSession = Depends(get_archival_session),
):
    """
    Get fixture tactics data for a specific fixture ID.
    Returns tactics actions and lineups grouped by club_id.
    """
    start_time = time.perf_counter()

    def decode_tactics_blob(tactics_blob: Optional[bytes]) -> Optional[Dict[str, Any]]:
        """Decompress and parse tactics blob from database."""
        if tactics_blob is None:
            return None
        decompressed = zlib.decompress(tactics_blob, -zlib.MAX_WBITS)
        return json.loads(decompressed.decode('utf-8'))

    # Query old_tactics for this fixture
    old_tactics_query = (
        select(
            OldTactics.club_id,
            OldTactics.set_tactics,
            OldTactics.auto_tactics,
            OldTactics.reason_for_auto,
        )
        .where(OldTactics.fixture_id == fixture_id)
    )

    # Execute old_tactics query
    old_tactics_result = await session.execute(old_tactics_query)
    old_tactics_rows = old_tactics_result.fetchall()

    # Build a map of club_id -> tactics data
    tactics_by_club = {}
    for tactics_row in old_tactics_rows:
        tactics_by_club[tactics_row.club_id] = {
            "set_tactics": decode_tactics_blob(tactics_row.set_tactics),
            "auto_tactics": decode_tactics_blob(tactics_row.auto_tactics),
            "reason_for_auto": tactics_row.reason_for_auto,
        }

    # Query all tactic actions for this fixture, ordered by id
    tactic_actions_query = (
        select(
            OldTacticAction.id,
            OldTacticAction.club_id,
            OldTacticAction.time,
            OldTacticAction.situation,
            OldTacticAction.goal_margin,
            OldTacticAction.formation_id,
            OldTacticAction.play_style,
            OldTacticAction.use_playmaker,
            OldTacticAction.use_target_man,
            OldTacticAction.captain,
            OldTacticAction.penalty_taker,
            OldTacticAction.free_kicks,
            OldTacticAction.corner_taker,
            OldTacticAction.playmaker,
            OldTacticAction.target_man,
            OldTacticAction.autopicked,
        )
        .where(OldTacticAction.fixture_id == fixture_id)
        .order_by(OldTacticAction.id)
    )

    # Execute tactic actions query
    tactic_actions_result = await session.execute(tactic_actions_query)
    tactic_actions_rows = tactic_actions_result.fetchall()

    # Get all tactic action IDs to query lineups
    tactic_action_ids = [row.id for row in tactic_actions_rows]

    # Group lineups by tactic action ID
    lineups_by_action = {}
    
    # Only query lineups if there are tactic actions
    if tactic_action_ids:
        # Query all lineups for these tactic actions, ordered by id
        lineups_query = (
            select(
                OldTacticActionLineup.old_tactic_action_id,
                OldTacticActionLineup.player_id,
                OldTacticActionLineup.tempo,
                OldTacticActionLineup.tackling_style,
            )
            .where(OldTacticActionLineup.old_tactic_action_id.in_(tactic_action_ids))
            .order_by(OldTacticActionLineup.id)
        )

        # Execute lineups query
        lineups_result = await session.execute(lineups_query)
        lineups_rows = lineups_result.fetchall()
        
        # Group lineups by tactic action ID
        for lineup_row in lineups_rows:
            action_id = lineup_row.old_tactic_action_id
            if action_id not in lineups_by_action:
                lineups_by_action[action_id] = []
            lineups_by_action[action_id].append({
                "player_id": lineup_row.player_id,
                "tempo": lineup_row.tempo,
                "tackling_style": lineup_row.tackling_style,
            })



    # Group tactic actions by club_id
    clubs_data = {}
    for action_row in tactic_actions_rows:
        club_id = action_row.club_id
        if club_id not in clubs_data:
            clubs_data[club_id] = []
        
        # Create lineup items
        lineup_items = [
            LineupItem(
                player_id=lineup["player_id"],
                tempo=lineup["tempo"],
                tackling_style=lineup["tackling_style"],
            )
            for lineup in lineups_by_action.get(action_row.id, [])
        ]
        
        # Create tactic action object
        tactic_action = TacticActionItem(
            time=action_row.time,
            situation=action_row.situation,
            goal_margin=action_row.goal_margin,
            formation_id=action_row.formation_id,
            play_style=action_row.play_style,
            use_playmaker=action_row.use_playmaker,
            use_target_man=action_row.use_target_man,
            captain=action_row.captain,
            penalty_taker=action_row.penalty_taker,
            free_kicks=action_row.free_kicks,
            corner_taker=action_row.corner_taker,
            playmaker=action_row.playmaker,
            target_man=action_row.target_man,
            autopicked=action_row.autopicked,
            lineup=lineup_items,
        )
        
        clubs_data[club_id].append(tactic_action)

    # Convert to final response format
    result = []
    for club_id, tactic_actions in clubs_data.items():
        tactics_data = tactics_by_club[club_id]
        result.append(FixtureTacticsResponse(
            club_id=club_id,
            tactic_actions=tactic_actions,
            set_tactics=tactics_data["set_tactics"],
            auto_tactics=tactics_data["auto_tactics"],
            reason_for_auto=tactics_data["reason_for_auto"],
        ))

    logger.info(f"Time taken for fixture tactics data: {time.perf_counter() - start_time:.4f}s")
    return result
