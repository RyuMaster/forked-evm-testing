# modules/user_activity.py

from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import (
    select,
    func,
    Table,
    Column,
    MetaData,
    BigInteger,
    Numeric,
    Text,
    LargeBinary,
)
from sqlalchemy.dialects.postgresql import INT4RANGE
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime

from modules.utils.profile import get_profiles_for_users
import modules.base as base_module

def get_subgraph_tables():
    """Create and return table objects with the current schema (looked up at startup)."""
    metadata = MetaData(schema=base_module.GRAPH_SUBGRAPH_STATS_SCHEMA)

    game_move = Table(
        'game_move',
        metadata,
        Column('vid', BigInteger, primary_key=True),
        Column('id', LargeBinary),
        Column('move', LargeBinary),
        Column('tx', LargeBinary),
        Column('game', LargeBinary),
        Column('gamemove', Text),
    )

    move = Table(
        'move',
        metadata,
        Column('vid', BigInteger, primary_key=True),
        Column('id', LargeBinary),
        Column('tx', LargeBinary),
        Column('name', LargeBinary),
        Column('move', Text),
    )

    name = Table(
        'name',
        metadata,
        Column('vid', BigInteger, primary_key=True),
        Column('id', LargeBinary),
        Column('ns', LargeBinary),
        Column('name', Text),
        Column('owner', LargeBinary),
        Column('block_range', INT4RANGE),
    )

    tx_table = Table(
        'transaction',
        metadata,
        Column('vid', BigInteger, primary_key=True),
        Column('id', LargeBinary),
        Column('height', Numeric),
        Column('timestamp', Numeric),
    )

    game = Table(
        'game',
        metadata,
        Column('vid', BigInteger, primary_key=True),
        Column('id', LargeBinary),
        Column('game', Text),
    )

    return game_move, move, name, tx_table, game

# Define the Pydantic model for the API response
class UserActivityResponse(BaseModel):
    name: str
    last_active_unix: int
    first_move: int
    rank: int
    points: int
    profile_pic: Optional[str] = None

    class Config:
        from_attributes = True

# Create an APIRouter instance for user_activity
user_activity_router = APIRouter()

@user_activity_router.get(
    "/user_activity",
    response_model=List[UserActivityResponse],
    summary="Get user activity stats",
    description="""Generates user activity statistics based on moves recorded in The Graph subgraph.
    Calculates first move time, last active time, and an overall point-based ranking.
    By default, returns the top 50 ranked users; if 'name' is specified, returns data for that user only."""
)
async def get_user_activity(
    name_param: Optional[str] = Query(None, alias="name", description="Filter by username"),
    session: AsyncSession = Depends(base_module.get_user_activity_session),
    userconfig_session: AsyncSession = Depends(base_module.get_userconfig_session),
):
    # Get table objects with the current schema
    game_move, move, name_table, tx_table, game = get_subgraph_tables()

    current_time = int(datetime.utcnow().timestamp())
    seven_days_ago = current_time - (7 * 24 * 60 * 60)
    thirty_days_ago = current_time - (30 * 24 * 60 * 60)
    week_duration = 7 * 24 * 60 * 60  # Number of seconds in a week

    # Single query: fetch (player_name, timestamp) for all moves in the game.
    # This avoids an expensive query in the database, and allows us to
    # process the data in a custom, optimised way.
    base_join = (
        game_move
        .join(move, game_move.c.move == move.c.id)
        .join(name_table, (move.c.name == name_table.c.id) & (func.coalesce(func.upper(name_table.c.block_range), 2147483647) == 2147483647))
        .join(tx_table, game_move.c.tx == tx_table.c.id)
        .join(game, game_move.c.game == game.c.id)
    )

    raw_query = (
        select(
            name_table.c.name.label('player_name'),
            tx_table.c.timestamp.label('ts'),
        )
        .select_from(base_join)
        .where(game.c.game == base_module.GAME_ID)
    )

    if session is None:
        return []

    result = await session.execute(raw_query)
    rows = result.fetchall()

    # Build per-user stats in a single pass over the rows.
    # For each user we track: min/max timestamp, and per-week move counts.
    user_first = {}       # player_name -> min timestamp
    user_last = {}        # player_name -> max timestamp
    user_week_counts = {} # player_name -> {week_num -> count}

    for player_name, ts in rows:
        ts_int = int(ts)
        prev_first = user_first.get(player_name)
        if prev_first is None or ts_int < prev_first:
            user_first[player_name] = ts_int
        prev_last = user_last.get(player_name)
        if prev_last is None or ts_int > prev_last:
            user_last[player_name] = ts_int
        weeks = user_week_counts.get(player_name)
        if weeks is None:
            weeks = {}
            user_week_counts[player_name] = weeks
        week_num = ts_int // week_duration
        weeks[week_num] = weeks.get(week_num, 0) + 1

    # Compute points per user.
    user_stats = {}
    for player_name in user_first:
        last_active = user_last[player_name]

        weekly_points = 0
        for count in user_week_counts[player_name].values():
            if count == 1:
                weekly_points += 10
            elif count > 1:
                weekly_points += 12

        points = weekly_points
        if last_active >= seven_days_ago:
            points += 100
        if last_active >= thirty_days_ago:
            points += 100

        user_stats[player_name] = (user_first[player_name], last_active, points)

    # Sort by points descending, then name ascending, and assign dense ranks.
    sorted_users = sorted(user_stats.keys(), key=lambda n: (-user_stats[n][2], n))

    ranked = []
    current_rank = 0
    prev_points = None
    for player_name in sorted_users:
        pts = user_stats[player_name][2]
        if pts != prev_points:
            current_rank += 1
            prev_points = pts
        ranked.append((player_name, current_rank, pts))

    if name_param:
        # Find the requested user
        match = None
        for player_name, rank, pts in ranked:
            if player_name == name_param and len(player_name) == len(name_param):
                match = (player_name, rank, pts)
                break
        if not match:
            raise HTTPException(status_code=404, detail="User not found")

        names_needed = {match[0]}
        name_to_pic = await get_profiles_for_users(list(names_needed), userconfig_session)

        first_move, last_active, _ = user_stats[match[0]]
        response_items = [
            UserActivityResponse(
                name=match[0],
                last_active_unix=last_active,
                first_move=first_move,
                rank=match[1],
                points=match[2],
                profile_pic=name_to_pic.get(match[0], base_module.DEFAULT_PROFILE_PIC_URL),
            )
        ]
    else:
        # Top 50
        top50 = ranked[:50]

        names_needed = {r[0] for r in top50}
        name_to_pic = await get_profiles_for_users(list(names_needed), userconfig_session)

        response_items = [
            UserActivityResponse(
                name=player_name,
                last_active_unix=user_stats[player_name][1],
                first_move=user_stats[player_name][0],
                rank=rank,
                points=pts,
                profile_pic=name_to_pic.get(player_name, base_module.DEFAULT_PROFILE_PIC_URL),
            )
            for player_name, rank, pts in top50
        ]

    return response_items
