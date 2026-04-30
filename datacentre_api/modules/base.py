# modules/base.py

import os
from typing import AsyncGenerator, Generic, TypeVar, List, Optional, Any
from datetime import datetime
import json
import time
import logging

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import func, Column, BigInteger, Integer, String, Float, DateTime, Text, LargeBinary, ForeignKey

from pydantic import BaseModel
from fastapi import HTTPException

from enum import Enum

from dotenv import load_dotenv

from redis import asyncio as aioredis
from typing import Union

# Load environment variables from .env file
load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Suppress INFO logs from gql library (only show WARNING and above).
# gql logs every request and response to INFO, we don't want that.
logging.getLogger("gql").setLevel(logging.WARNING)

# Pool debugging configuration
POOL_DEBUG = os.getenv("POOL_DEBUG", "false").lower() in ("true", "1", "yes", "debug")

# Database configuration from environment variables
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = os.getenv("MYSQL_PORT")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DB = os.getenv("MYSQL_DB")

# The pre-built database of player rating history before launch
PLAYERHISTORY_SQLITE_DB_PATH = os.getenv("PLAYERHISTORY_SQLITE_DB_PATH")

# Archival database configuration
MYSQL_ARCHIVAL_HOST = os.getenv("MYSQL_ARCHIVAL_HOST")
MYSQL_ARCHIVAL_PORT = os.getenv("MYSQL_ARCHIVAL_PORT")
MYSQL_ARCHIVAL_USER = os.getenv("MYSQL_ARCHIVAL_USER")
MYSQL_ARCHIVAL_PASSWORD = os.getenv("MYSQL_ARCHIVAL_PASSWORD")
MYSQL_ARCHIVAL_DB = os.getenv("MYSQL_ARCHIVAL_DB")

GAME_ID = os.getenv("GAME_ID", "sv")

# The Graph Postgres database configuration (for user activity / Polygon stats)
GRAPH_POSTGRES_HOST = os.getenv("GRAPH_POSTGRES_HOST")
GRAPH_POSTGRES_PORT = os.getenv("GRAPH_POSTGRES_PORT")
GRAPH_POSTGRES_USER = os.getenv("GRAPH_POSTGRES_USER")
GRAPH_POSTGRES_PASSWORD = os.getenv("GRAPH_POSTGRES_PASSWORD")
GRAPH_POSTGRES_DB = os.getenv("GRAPH_POSTGRES_DB")
GRAPH_SUBGRAPH_STATS = os.getenv("GRAPH_SUBGRAPH_STATS")  # IPFS hash, schema looked up at startup
GRAPH_SUBGRAPH_SV = os.getenv("GRAPH_SUBGRAPH_SV")  # IPFS hash for the SV subgraph (pack sales)

# Will be populated at startup by looking up the IPFS hash in deployment_schemas
GRAPH_SUBGRAPH_STATS_SCHEMA: str = None
GRAPH_SUBGRAPH_SV_SCHEMA: str = None

# Load new env variables (already loaded if you're using dotenv)
USERCONFIG_HOST = os.getenv("USERCONFIG_HOST")
USERCONFIG_PORT = os.getenv("USERCONFIG_PORT")
USERCONFIG_USER = os.getenv("USERCONFIG_USER")
USERCONFIG_PASS = os.getenv("USERCONFIG_PASS")
USERCONFIG_DB   = os.getenv("USERCONFIG_DB")

DEFAULT_PROFILE_PIC_URL = os.getenv("DEFAULT_PROFILE_PIC_URL")  # from .env

# Output folder written by the datacentre_updater DataDumpUpdater.
# Leave unset to disable the /dumps/* endpoints (they will return 503).
DUMP_OUTPUT_FOLDER = os.getenv("DUMP_OUTPUT_FOLDER", "")
if not DUMP_OUTPUT_FOLDER:
    logger.warning(
        "DUMP_OUTPUT_FOLDER not configured - /dumps/* endpoints will return 503"
    )

# >>> NEW: REDIS GLOBAL INSTANCE <<<
redis: Union[aioredis.client.Redis, None] = None

# >>> NEW: FUNCTION TO INITIALIZE REDIS CONNECTION <<<
async def connect_redis():
    global redis
    if redis is None:
        redis_host = os.getenv("REDIS_HOST")
        redis_port = os.getenv("REDIS_PORT", "6379")
        redis_db   = os.getenv("REDIS_DB", "0")
        redis_password = os.getenv("REDIS_PASSWORD", None)

        # Check if Redis is configured
        if not redis_host:
            logger.warning("Redis is not configured (REDIS_HOST not set). Running without cache - performance may be reduced.")
            return

        try:
            if redis_password:
                # If the password is not None or empty, add it
                redis_url = f"redis://:{redis_password}@{redis_host}:{redis_port}/{redis_db}"
            else:
                # No password
                redis_url = f"redis://{redis_host}:{redis_port}/{redis_db}"

            redis = aioredis.from_url(redis_url, decode_responses=True)
            # Test connection
            await redis.ping()
            logger.info(f"Connected to Redis at {redis_url}")
        except Exception as e:
            logger.warning(f"Failed to connect to Redis: {e}. Running without cache - performance may be reduced.")
            redis = None

def get_redis_client() -> Union[aioredis.client.Redis, None]:
    """Get Redis client. Returns None if Redis is not available."""
    return redis

# Create async engine for userconfig DB (optional)
if USERCONFIG_HOST:
    userconfig_connection_url = (
        f"mysql+aiomysql://{USERCONFIG_USER}:{USERCONFIG_PASS}@{USERCONFIG_HOST}:"
        f"{USERCONFIG_PORT}/{USERCONFIG_DB}"
    )
    userconfig_engine = create_async_engine(
        userconfig_connection_url,
        echo=False,
        echo_pool="debug" if POOL_DEBUG else False,
        future=True,
        isolation_level="AUTOCOMMIT",
        pool_pre_ping=True,
        pool_recycle=1800,
        pool_size=20,
        max_overflow=15,
        pool_timeout=30,
    )

    userconfig_session_maker = sessionmaker(
        userconfig_engine, expire_on_commit=False, class_=AsyncSession
    )
    
    logger.info("Userconfig database configured and connected")
else:
    userconfig_engine = None
    userconfig_session_maker = None
    logger.warning("Userconfig database not configured (USERCONFIG_HOST not set) - profile pictures and jobs board will show error messages")

# Dependency to get a session for the userconfig DB
async def get_userconfig_session() -> AsyncGenerator[AsyncSession, None]:
    if userconfig_session_maker is None:
        yield None
    else:
        async with userconfig_session_maker() as session:
            yield session


# Create async engines for MySQL with autocommit isolation level
mysql_connection_url = (
    f"mysql+aiomysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:"
    f"{MYSQL_PORT}/{MYSQL_DB}"
)
mysql_engine = create_async_engine(
    mysql_connection_url,
    echo=False,
    echo_pool="debug" if POOL_DEBUG else False,  # Configurable pool debugging
    future=True,
    isolation_level="AUTOCOMMIT",
    pool_pre_ping=True,
    pool_recycle=1800,
    pool_size=20,
    max_overflow=15,
    pool_timeout=30,
)

# Create async engine for archival MySQL database
mysql_archival_connection_url = (
    f"mysql+aiomysql://{MYSQL_ARCHIVAL_USER}:{MYSQL_ARCHIVAL_PASSWORD}@{MYSQL_ARCHIVAL_HOST}:"
    f"{MYSQL_ARCHIVAL_PORT}/{MYSQL_ARCHIVAL_DB}"
)
mysql_archival_engine = create_async_engine(
    mysql_archival_connection_url,
    echo=False,
    echo_pool="debug" if POOL_DEBUG else False,
    future=True,
    isolation_level="AUTOCOMMIT",
    pool_pre_ping=True,
    pool_recycle=1800,
    pool_size=15,
    max_overflow=10,
    pool_timeout=30,
)

# Create async engine for The Graph Postgres database (user activity)
if GRAPH_POSTGRES_HOST:
    graph_postgres_connection_url = (
        f"postgresql+asyncpg://{GRAPH_POSTGRES_USER}:{GRAPH_POSTGRES_PASSWORD}@{GRAPH_POSTGRES_HOST}:"
        f"{GRAPH_POSTGRES_PORT}/{GRAPH_POSTGRES_DB}"
    )
    graph_postgres_engine = create_async_engine(
        graph_postgres_connection_url,
        echo=False,
        echo_pool="debug" if POOL_DEBUG else False,
        future=True,
        pool_pre_ping=True,
        pool_recycle=1800,
        pool_size=15,
        max_overflow=10,
        pool_timeout=30,
    )
else:
    graph_postgres_engine = None
    logger.warning("Graph Postgres database not configured (GRAPH_POSTGRES_HOST not set) - user activity features will be disabled")

# Create async engine for SQLite
playerhistory_sqlite_connection_url = f"sqlite+aiosqlite:///{PLAYERHISTORY_SQLITE_DB_PATH}"
playerhistory_sqlite_engine = create_async_engine(playerhistory_sqlite_connection_url, echo=False, future=True)

# Create declarative base class for SQLAlchemy models
Base = declarative_base()

# Define SQLAlchemy model for dc_users
class DCUsers(Base):
    __tablename__ = "dc_users"
    name = Column(String, primary_key=True)
    balance = Column(BigInteger)
    last_active = Column(BigInteger)  # Changed from DateTime to BigInteger
    club_id = Column(Integer)

# Define DCClubInfo model
class DCClubInfo(Base):
    __tablename__ = 'dc_club_info'
    club_id = Column(Integer, primary_key=True)
    available = Column(Integer)
    country_id = Column(Integer)
    league_id = Column(Integer)
    division = Column(Integer)
    avg_wages = Column(BigInteger)
    total_wages = Column(BigInteger)
    total_player_value = Column(BigInteger)
    avg_player_rating = Column(Integer)
    avg_player_rating_top21 = Column(Integer)
    avg_shooting = Column(Integer)
    avg_passing = Column(Integer)
    avg_tackling = Column(Integer)
    gk_rating = Column(Integer)

class DCPlayers(Base):
    __tablename__ = "dc_players"
    player_id = Column(Integer, primary_key=True)
    fitness = Column(Integer)
    retired = Column(Integer)
    morale = Column(Integer)
    injured = Column(Integer)
    injury_id = Column(Integer)
    wages = Column(BigInteger)
    contract = Column(Integer)
    form = Column(Text)
    position = Column(Integer)
    multi_position = Column(Integer)
    rating = Column(Integer)
    rating_gk = Column(Integer)
    rating_tackling = Column(Integer)
    rating_passing = Column(Integer)
    rating_shooting = Column(Integer)
    rating_aggression = Column(Integer)
    rating_stamina = Column(Integer)
    ability_gk = Column(Integer)
    ability_tackling = Column(Integer)
    ability_passing = Column(Integer)
    ability_shooting = Column(Integer)
    banned = Column(Integer)
    cup_tied = Column(Integer)
    yellow_cards = Column(Integer)
    red_cards = Column(Integer)
    dob = Column(Integer)  # Date of birth as Unix timestamp
    side = Column(Text)
    value = Column(BigInteger)
    country_id = Column(String)
    club_id = Column(Integer)
    agent_name = Column(String)
    last_transfer = Column(Integer)
    desired_contract = Column(Integer)
    allow_transfer = Column(Integer)
    allow_renew = Column(Integer)
    loan_offered = Column(BigInteger)
    loan_offer_accepted = Column(BigInteger)
    loaned_to_club = Column(BigInteger)

class DCClubs(Base):
    __tablename__ = "dc_clubs"
    club_id = Column(Integer, primary_key=True)
    balance = Column(BigInteger)
    form = Column(Text)
    division_start = Column(Integer)
    fans_start = Column(Integer)
    fans_current = Column(Integer)
    stadium_size_start = Column(Integer)
    stadium_size_current = Column(Integer)
    stadium_id = Column(Integer)
    value = Column(BigInteger)
    rating_start = Column(Integer)  # Changed from 'rating' to 'rating_start'
    manager_name = Column(String(254))
    default_formation = Column(Integer)
    penalty_taker = Column(Integer)
    country_id = Column(String(3), index=True)
    committed_tactics = Column(LargeBinary)  # Binary data
    proposed_manager = Column(Text)
    manager_locked = Column(Integer)
    transfers_in = Column(Integer)
    transfers_out = Column(Integer)
    # >>> NEW FIELD <<<
    manager_voted = Column(Integer, nullable=True)  # can be NULL, 0, or 1

# Define SQLAlchemy model for dc_leagues
class DCLeagues(Base):
    __tablename__ = 'dc_leagues'
    league_id = Column(Integer, primary_key=True)
    season_id = Column(Integer)
    country_id = Column(String(20))
    level = Column(Integer)
    ticket_cost = Column(BigInteger)
    tv_money = Column(BigInteger)
    prize_money_pot = Column(BigInteger)
    ave_attendance = Column(Integer)
    num_teams = Column(Integer)
    round = Column(Integer)
    num_rounds = Column(Integer)
    comp_type = Column(Integer)

# Define SQLAlchemy model for dc_share_balances table
class DCShareBalances(Base):
    __tablename__ = "dc_share_balances"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), index=True)
    share_type = Column(String(50), index=True)
    share_id = Column(Integer, index=True)
    num = Column(BigInteger, index=True)
    checksum = Column(String(64))
    updated_at = Column(DateTime, index=True, default=func.current_timestamp(), onupdate=func.current_timestamp())

# Define SQLAlchemy model for dc_player_earnings table
class DCPlayerEarnings(Base):
    __tablename__ = "dc_player_earnings"
    player_id = Column(BigInteger, primary_key=True)
    club_id = Column(BigInteger)
    club_country = Column(String(3), index=True)
    club_division = Column(Integer, index=True)
    club_position = Column(Integer)
    player_nationality = Column(String(3))
    player_age = Column(Integer, index=True)
    player_rating = Column(Integer, index=True)
    player_position = Column(Integer, index=True)
    match_time_percentage = Column(Float(precision=53))  # DOUBLE
    current_earnings = Column(Float(precision=53))  # DOUBLE
    buyable_2s = Column(BigInteger)
    cost_2s = Column(BigInteger)
    buyable_3s = Column(BigInteger)
    cost_3s = Column(BigInteger)
    buyable_4s = Column(BigInteger)
    cost_4s = Column(BigInteger)
    buyable_5s = Column(BigInteger)
    cost_5s = Column(BigInteger)
    buyable_6s = Column(BigInteger)
    cost_6s = Column(BigInteger)
    buyable_7s = Column(BigInteger)
    cost_7s = Column(BigInteger)
    buyable_8s = Column(BigInteger)
    cost_8s = Column(BigInteger)
    buyable_10s = Column(BigInteger)
    cost_10s = Column(BigInteger)
    buyable_12s = Column(BigInteger)
    cost_12s = Column(BigInteger)
    buyable_15s = Column(BigInteger)
    cost_15s = Column(BigInteger)
    buyable_20s = Column(BigInteger)
    cost_20s = Column(BigInteger)
    updated_at = Column(DateTime, server_default=func.current_timestamp(), onupdate=func.current_timestamp())

# Define SQLAlchemy model for dc_earnings table
class DCEarnings(Base):
    __tablename__ = "dc_earnings"
    name = Column(String(255), primary_key=True)
    share_type = Column(String(50), primary_key=True)
    share_id = Column(BigInteger, primary_key=True)
    earnings_7d = Column(BigInteger, default=0)
    earnings_30d = Column(BigInteger, default=0)
    updated_at = Column(DateTime, index=True, default=func.current_timestamp(), onupdate=func.current_timestamp())

class DCTableRows(Base):
    __tablename__ = 'dc_table_rows'
    league_id = Column(Integer, primary_key=True)
    club_id = Column(Integer, primary_key=True)
    club_ix = Column(Integer)
    played = Column(Integer)
    won = Column(Integer)
    drawn = Column(Integer)
    lost = Column(Integer)
    goals_for = Column(Integer)
    goals_against = Column(Integer)
    pts = Column(Integer)
    form = Column(Text)
    old_position = Column(Integer)
    new_position = Column(Integer)
    season_id = Column(Integer, primary_key=True)
    updated_at = Column(DateTime, server_default=func.current_timestamp(), onupdate=func.current_timestamp())


# DCUsernames model for accessing profile pictures from userconfig DB
class DCUsernames(Base):
    __tablename__ = "usernames"   # name of the table in userconfig DB
    id = Column(Integer, primary_key=True)
    xayaname = Column(String(255), unique=True, nullable=False)
    profile_pic = Column(Text, nullable=True)
    profile_option = Column(Integer, nullable=True, default=1)  # 0 => do not show pic
    prof_dev_disabled = Column(Integer, nullable=False, default=0)  # 1 => forcibly disabled

# Define SQLAlchemy model for jobs_board in userconfig database
class JobsBoard(Base):
    __tablename__ = "jobs_board"
    id = Column(Integer, primary_key=True, autoincrement=True)
    club_id = Column(Integer, nullable=False, index=True)
    poster_name = Column(String(255), nullable=False)
    posted_at = Column(BigInteger, nullable=False)
    description = Column(Text, nullable=False)
    created_at = Column(DateTime, server_default=func.current_timestamp())
    last_updated = Column(DateTime, server_default=func.current_timestamp(), onupdate=func.current_timestamp())

# Create the 'Blocks' class here to avoid duplication
class Blocks(Base):
    __tablename__ = 'blocks'
    height = Column(BigInteger, primary_key=True)
    date = Column(BigInteger)  # Stored as Unix time

# Define archival database model for player loan updates
class PlayerLoanUpdates(Base):
    __tablename__ = "player_loan_updates"
    id = Column(BigInteger, primary_key=True)
    height = Column(BigInteger, nullable=False)
    season_id = Column(BigInteger, nullable=False)
    player_id = Column(BigInteger, nullable=False)
    club_id = Column(BigInteger, nullable=False)
    accepting_club_id = Column(BigInteger, nullable=True)
    fee = Column(BigInteger, nullable=True)
    action = Column(String(10), nullable=True)

# Define archival database model for player updates
class PlayerUpdates(Base):
    __tablename__ = "player_updates"
    id = Column(BigInteger, primary_key=True)
    height = Column(BigInteger, nullable=False)
    player_id = Column(BigInteger, nullable=False)
    morale = Column(Integer, nullable=True)
    desired_contract = Column(Integer, nullable=True)
    allow_transfer = Column(Integer, nullable=True)  # BOOL in MySQL maps to Integer in SQLAlchemy
    allow_renew = Column(Integer, nullable=True)  # BOOL in MySQL maps to Integer in SQLAlchemy
    rating = Column(Integer, nullable=True)
    rating_gk = Column(Integer, nullable=True)
    rating_tackling = Column(Integer, nullable=True)
    rating_passing = Column(Integer, nullable=True)
    rating_shooting = Column(Integer, nullable=True)
    rating_aggression = Column(Integer, nullable=True)
    rating_stamina = Column(Integer, nullable=True)

# Define SQLAlchemy model for player history (SQLite database)
class PlayerHistory(Base):
    __tablename__ = "player_history"
    player_history_id = Column(Integer, primary_key=True)
    player_id = Column(Integer, nullable=False)
    rating = Column(Integer, nullable=False)
    rating_gk = Column(Integer, nullable=False)
    rating_tackling = Column(Integer, nullable=False)
    rating_passing = Column(Integer, nullable=False)
    rating_shooting = Column(Integer, nullable=False)
    rating_aggression = Column(Integer, nullable=False)
    rating_stamina = Column(Integer, nullable=False)
    date_updated = Column(Integer, nullable=False)

# Define SQLAlchemy model for injury history (archival database)
class InjuryHistory(Base):
    __tablename__ = "injury_history"
    id = Column(BigInteger, primary_key=True)
    height = Column(BigInteger, nullable=False)
    player_id = Column(BigInteger, nullable=False)
    injury_id = Column(BigInteger, nullable=False)
    start_date = Column(BigInteger, nullable=False)
    end_date = Column(BigInteger, nullable=False)
    season_id = Column(BigInteger, nullable=False)

# Define SQLAlchemy model for messages (archival database)
class Messages(Base):
    __tablename__ = "messages"
    id = Column(BigInteger, primary_key=True)
    height = Column(BigInteger, nullable=False)
    type = Column(BigInteger, nullable=False)
    sub_type = Column(BigInteger, nullable=False, default=0)
    club_1 = Column(BigInteger, nullable=False, default=0)
    club_2 = Column(BigInteger, nullable=False, default=0)
    data_1 = Column(BigInteger, nullable=False, default=0)
    data_2 = Column(BigInteger, nullable=False, default=0)
    data_3 = Column(BigInteger, nullable=False, default=0)
    data_4 = Column(BigInteger, nullable=False, default=0)
    data_5 = Column(BigInteger, nullable=False, default=0)
    name_1 = Column(String(255))
    season_id = Column(BigInteger, nullable=False, default=0)

# Define SQLAlchemy model for message_index_table (archival database)
class MessageIndexTable(Base):
    __tablename__ = 'message_index_table'
    id = Column(BigInteger, primary_key=True)
    height = Column(BigInteger, nullable=False)
    message_id = Column(BigInteger, nullable=False)
    club_id = Column(BigInteger)
    country_id = Column(String(3))
    competition_id = Column(BigInteger)

# Define SQLAlchemy model for dc_transfer_counts table
class DCTransferCounts(Base):
    __tablename__ = 'dc_transfer_counts'
    season_id = Column(BigInteger, primary_key=True)
    from_club = Column(BigInteger, primary_key=True)
    to_club = Column(BigInteger, primary_key=True)
    transfers = Column(BigInteger)

# Define SQLAlchemy model for share trade history (archival database)
class ShareTradeHistory(Base):
    __tablename__ = "share_trade_history"
    id = Column(BigInteger, primary_key=True)
    height = Column(BigInteger, nullable=False, index=True)
    share_type = Column(String(6), nullable=False, index=True)
    share_id = Column(BigInteger, nullable=False)
    buyer = Column(String(255), nullable=False, index=True)
    seller = Column(String(255), index=True)
    num = Column(BigInteger, nullable=False)
    price = Column(BigInteger, nullable=False)
    market_buy = Column(Integer, nullable=False)

# Define SQLAlchemy model for SVC trades table
class SVCTrades(Base):
    __tablename__ = 'svc_trades'
    id = Column(Integer, primary_key=True)
    trade_id = Column(Integer)
    trade_ts = Column(DateTime)
    buyer = Column(String(255))
    seller = Column(String(255))
    amount_svc = Column(BigInteger)
    amount_wchi = Column(Float)
    volume_svc = Column(BigInteger)
    volume_wchi = Column(Float)
    price_usdc = Column(Float)
    volume_usdc = Column(Float)

# Define SQLAlchemy model for share transactions (archival database)
class ShareTransactions(Base):
    __tablename__ = "share_transactions"
    id = Column(BigInteger, primary_key=True)
    height = Column(BigInteger, nullable=False, index=True)
    share_type = Column(String(6), nullable=False)
    share_id = Column(BigInteger, nullable=False)
    name = Column(String(255), nullable=False, index=True)
    num = Column(BigInteger, nullable=False)
    type = Column(String(64), nullable=False)
    other_name = Column(String(255), nullable=True)

# Create async session makers
mysql_session_maker = sessionmaker(
    mysql_engine, expire_on_commit=False, class_=AsyncSession
)

# Create async session maker for archival MySQL database
mysql_archival_session_maker = sessionmaker(
    mysql_archival_engine, expire_on_commit=False, class_=AsyncSession
)

# Session maker for The Graph Postgres database
if graph_postgres_engine is not None:
    graph_postgres_session_maker = sessionmaker(
        graph_postgres_engine, expire_on_commit=False, class_=AsyncSession
    )
else:
    graph_postgres_session_maker = None

async def _resolve_subgraph_schema(session: AsyncSession, ipfs_hash: str) -> Optional[str]:
    from sqlalchemy import text
    result = await session.execute(
        text("SELECT name FROM public.deployment_schemas WHERE subgraph = :ipfs_hash"),
        {"ipfs_hash": ipfs_hash}
    )
    row = result.fetchone()
    return row[0] if row else None

async def lookup_subgraph_schemas():
    """Look up the Postgres schema names for the configured IPFS hashes."""
    global GRAPH_SUBGRAPH_STATS_SCHEMA, GRAPH_SUBGRAPH_SV_SCHEMA
    
    async with graph_postgres_session_maker() as session:
        # 1. Stats Subgraph
        if GRAPH_SUBGRAPH_STATS:
            GRAPH_SUBGRAPH_STATS_SCHEMA = await _resolve_subgraph_schema(session, GRAPH_SUBGRAPH_STATS)
            if GRAPH_SUBGRAPH_STATS_SCHEMA:
                logger.info(f"Resolved IPFS hash {GRAPH_SUBGRAPH_STATS} to schema {GRAPH_SUBGRAPH_STATS_SCHEMA}")
            else:
                logger.error(f"Could not find schema for IPFS hash {GRAPH_SUBGRAPH_STATS}")
        else:
            logger.warning("GRAPH_SUBGRAPH_STATS not configured - user activity features will be disabled")

        # 2. SV Subgraph (pack sales)
        if GRAPH_SUBGRAPH_SV:
            GRAPH_SUBGRAPH_SV_SCHEMA = await _resolve_subgraph_schema(session, GRAPH_SUBGRAPH_SV)
            if GRAPH_SUBGRAPH_SV_SCHEMA:
                logger.info(f"Resolved IPFS hash {GRAPH_SUBGRAPH_SV} to schema {GRAPH_SUBGRAPH_SV_SCHEMA}")
            else:
                logger.error(f"Could not find schema for IPFS hash {GRAPH_SUBGRAPH_SV}")
        else:
            logger.warning("GRAPH_SUBGRAPH_SV not configured - shop clubs features will be disabled")

# Create session maker for player history SQLite database
playerhistory_sqlite_session_maker = sessionmaker(
    playerhistory_sqlite_engine, expire_on_commit=False, class_=AsyncSession
)

# Dependency to get a MySQL session
async def get_mysql_session() -> AsyncGenerator[AsyncSession, None]:
    async with mysql_session_maker() as session:
        yield session

# Dependency to get a MySQL archival session
async def get_archival_session() -> AsyncGenerator[AsyncSession, None]:
    async with mysql_archival_session_maker() as session:
        yield session

# Dependency to get a session for The Graph Postgres database (user activity)
async def get_user_activity_session() -> AsyncGenerator[Optional[AsyncSession], None]:
    if graph_postgres_session_maker is None:
        yield None
    else:
        async with graph_postgres_session_maker() as session:
            yield session

# Dependency to get a player history SQLite session
async def get_playerhistory_sqlite_session() -> AsyncGenerator[AsyncSession, None]:
    async with playerhistory_sqlite_session_maker() as session:
        yield session

# Function to calculate age from Unix timestamp
def calculate_age(unix_dob: int) -> int:
    dob_datetime = datetime.utcfromtimestamp(unix_dob)
    today = datetime.utcnow()
    age = (
        today.year
        - dob_datetime.year
        - ((today.month, today.day) < (dob_datetime.month, dob_datetime.day))
    )
    return age

# Enum for per_page options
class PerPageOptions(int, Enum):
    five = 5
    ten = 10
    twenty = 20
    fifty = 50
    hundred = 100
    ten_thousand = 10000

# Generic model for paginated responses
T = TypeVar('T')

class PaginatedResponse(BaseModel, Generic[T]):
    page: int
    per_page: int
    total: Optional[int] = None       # Make total optional
    total_pages: Optional[int] = None # Make total_pages optional
    items: List[T]

    class Config:
        arbitrary_types_allowed = True  # Allow Generic types

# Utility function to parse JSON fields
def parse_json_field(field_value: Any) -> List[int]:
    if field_value:
        try:
            # Parse the JSON and convert each element to int
            return [int(value or 0) for value in json.loads(field_value)]
        except json.JSONDecodeError:
            return []
    else:
        return []

# Utility function to apply sorting to a query
def apply_sorting(query, sortable_fields, sort_by, sort_order):
    if sort_by in sortable_fields:
        sort_column = sortable_fields[sort_by]
        if sort_order == "desc":
            sort_column = sort_column.desc()
        else:
            sort_column = sort_column.asc()
        return query.order_by(sort_column)
    else:
        raise HTTPException(
            status_code=400, detail=f"Invalid sort_by field: {sort_by}"
        )

# Utility function to apply pagination to a query
def apply_pagination(query, page, per_page):
    offset = (page - 1) * per_page
    return query.offset(offset).limit(per_page)

# Utility function to fetch paginated data
async def fetch_paginated_data(
    session: AsyncSession,
    select_query,
    total_query,
    sortable_fields: dict,
    sort_by: Optional[str],
    sort_order: str,
    page: int,
    per_page: int,
    extra_filters: Optional[List[Any]] = None,
):
    start_time = time.perf_counter()

    # Apply extra filters if provided
    if extra_filters:
        for filter_condition in extra_filters:
            select_query = select_query.where(filter_condition)
            total_query = total_query.where(filter_condition)
    logger.info(f"Time after applying filters: {time.perf_counter() - start_time:.4f}s")

    # Apply sorting
    if sort_by:
        if sort_by in sortable_fields:
            sort_column = sortable_fields[sort_by]
            # Handle NULL values without using functions in ORDER BY
            if sort_order == "desc":
                sort_columns = [
                    sort_column.is_(None).asc(),
                    sort_column.desc()
                ]
            else:
                sort_columns = [
                    sort_column.is_(None).desc(),
                    sort_column.asc()
                ]
            select_query = select_query.order_by(*sort_columns)
        else:
            raise HTTPException(status_code=400, detail=f"Invalid sort_by field: {sort_by}")
    logger.info(f"Time after applying sorting: {time.perf_counter() - start_time:.4f}s")

    # Calculate total records
    total_result = await session.execute(total_query)
    total = total_result.scalar_one()
    logger.info(f"Total count from query: {total}")
    total_pages = (total + per_page - 1) // per_page if total else 0
    logger.info(f"Time after calculating total records: {time.perf_counter() - start_time:.4f}s")

    # Apply pagination
    query = apply_pagination(select_query, page, per_page)

    # Execute the query
    result = await session.execute(query)
    rows = result.fetchall()
    logger.info(f"Fetched {len(rows)} rows from database")
    logger.info(f"Time after executing query and fetching rows: {time.perf_counter() - start_time:.4f}s")

    return total, total_pages, rows

async def try_acquire_bg_lock(r, key: str, ex: int) -> bool:
    """Try to acquire a Redis NX lock. Returns True if acquired or Redis
    unavailable. Used to gate background-refresh tasks against thundering
    herd when multiple gunicorn workers see the same stale cache and would
    otherwise each fire their own refresh in parallel.
    """
    if r is None:
        return True
    acquired = await r.set(key, "1", ex=ex, nx=True)
    if not acquired:
        logger.info(f"{key}: lock held by another worker, skipping")
    return bool(acquired)


# The Graph SV Subgraph configuration
GRAPH_SUBGRAPH_SV_URL = os.getenv("GRAPH_SUBGRAPH_SV_URL")

def get_sv_subgraph_client():
    """Get a GraphQL client for the SV subgraph (referrals and pack sales).
    Returns None if the subgraph URL is not configured.
    """
    if not GRAPH_SUBGRAPH_SV_URL:
        return None
    
    from gql import Client
    from gql.transport.aiohttp import AIOHTTPTransport
    
    transport = AIOHTTPTransport(url=GRAPH_SUBGRAPH_SV_URL)
    return Client(transport=transport, fetch_schema_from_transport=False)
