# modules/market.py

import logging
import time
import json
from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, BackgroundTasks
from sqlalchemy import select, func, Column, Integer, BigInteger, String, DateTime, desc, text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.declarative import declarative_base
from pydantic import BaseModel
from typing import List, Dict, Any

from . import base as base_module
from .base import get_mysql_session, get_redis_client, get_archival_session, Blocks, get_user_activity_session, SVCTrades, try_acquire_bg_lock
from .players import DCPlayers, DCPlayersTrading
from .clubs import DCClubs, DCClubsTrading
from .services.svc_price_service import get_svc_price, _update_price_cache, get_previous_daily_average
from .users import DCUsers

# Set up a logger for this module
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

MarketBase = declarative_base()

class DCUsersCorrected(MarketBase):
    __tablename__ = "dc_users"
    name = Column(String, primary_key=True)
    balance = Column(BigInteger)
    last_active = Column(BigInteger)
    club_id = Column(Integer)

class DCUsersTrading(MarketBase):
    __tablename__ = "dc_users_trading"
    user_name = Column(String, primary_key=True)
    total_volume = Column(BigInteger)
    # Add other fields if needed

# Model for messages table in archival database
class Messages(MarketBase):
    __tablename__ = 'messages'
    __table_args__ = {'extend_existing': True}
    id = Column(BigInteger, primary_key=True)
    height = Column(BigInteger, index=True)
    type = Column(Integer, index=True)
    data_2 = Column(BigInteger)  # SVC value for transfers

market_router = APIRouter()

class MarketResponse(BaseModel):
    TotalPlayers: int
    PlayersMarketCap: int
    PlayerValues: int
    Players7dayVolume: int
    Players7dayVolumePrior: int
    Players7dayVolumeChange: float
    Players30dayVolume: int
    Players30dayVolumePrior: int
    Players30dayVolumeChange: float

    TotalClubs: int
    ClubsMarketCap: int
    ClubBalances: int
    Clubs7dayVolume: int
    Clubs7dayVolumePrior: int
    Clubs7dayVolumeChange: float
    Clubs30dayVolume: int
    Clubs30dayVolumePrior: int
    Clubs30dayVolumeChange: float

    TotalMarketCap: int
    Total7dayVolume: int
    Total7dayVolumePrior: int
    Total7dayVolumeChange: float
    Total30dayVolume: int
    Total30dayVolumePrior: int
    Total30dayVolumeChange: float

    Transfers7dayVol: int
    Transfers7dayVolPrior: int
    Transfers7dayVolChange: float
    Transfers30dayVol: int
    Transfers30dayVolPrior: int
    Transfers30dayVolChange: float

    SVCTrades7dayVolSVC: int
    SVCTrades7dayVolSVCPrior: int
    SVCTrades7dayVolSVCChange: float
    SVCTrades30dayVolSVC: int
    SVCTrades30dayVolSVCPrior: int
    SVCTrades30dayVolSVCChange: float
    SVCTrades7dayVolUSDC: float
    SVCTrades7dayVolUSDCPrior: float
    SVCTrades7dayVolUSDCChange: float
    SVCTrades30dayVolUSDC: float
    SVCTrades30dayVolUSDCPrior: float
    SVCTrades30dayVolUSDCChange: float

    UserBalances: int
    UserTotalVolume: int
    NumberOfUsers: int

    NumberAgents: int
    NumberManagers: int
    ActiveUsers: int
    ActiveManagers: int
    InactiveManagers: int

    NumberManagersLocked: int
    NumberManagersUnlocked: int

    # >>> NEW FIELD FOR SVC PRICE <<<
    SVC2USDC: float
    
    # >>> NEW: Chart data (30 days) <<<
    SVCPriceChart: List[Dict[str, Any]] = []
    PlayersVolumeChart: List[Dict[str, Any]] = []
    ClubsVolumeChart: List[Dict[str, Any]] = []
    TransfersVolumeChart: List[Dict[str, Any]] = []
    SVCTradesVolumeChart: List[Dict[str, Any]] = []
    
    # >>> NEW: Daily active users and trades (30 days) <<<
    DailyActiveUsersChart: List[Dict[str, Any]] = []
    DailyTradesChart: List[Dict[str, Any]] = []

    class Config:
        from_attributes = True

async def _fetch_and_cache_market_data(r, session, cache_key: str) -> MarketResponse:
    """
    Fetch fresh market data from the DB, build the MarketResponse,
    then store it in Redis. Returns the new MarketResponse.
    """
    logger.info("No valid (fresh) cache found for /market; fetching from database...")

    start_time = time.perf_counter()
    
    # Create a new session for background task to avoid connection leaks
    from .base import mysql_session_maker
    async with mysql_session_maker() as local_session:
        # ----------------------
        # Original logic from get_market_data, converting aggregator results to int
        # ----------------------

        # Compute TotalPlayers
        total_players_query = select(func.count()).select_from(DCPlayers)
        total_players_result = await local_session.execute(total_players_query)
        TotalPlayers = int(total_players_result.scalar_one() or 0)

        # Compute PlayersMarketCap
        players_market_cap_query = select(
            func.sum(DCPlayersTrading.last_price * 1000000)
        )
        players_market_cap_result = await local_session.execute(players_market_cap_query)
        PlayersMarketCap = int(players_market_cap_result.scalar() or 0)

        # Compute PlayerValues
        player_values_query = select(func.sum(DCPlayers.value))
        player_values_result = await local_session.execute(player_values_query)
        PlayerValues = int(player_values_result.scalar() or 0)

        # Compute Players7dayVolume
        players_7day_volume_query = select(func.sum(DCPlayersTrading.volume_7_day))
        players_7day_volume_result = await local_session.execute(players_7day_volume_query)
        Players7dayVolume = int(players_7day_volume_result.scalar() or 0)

        # Compute TotalClubs
        total_clubs_query = select(func.count()).select_from(DCClubs)
        total_clubs_result = await local_session.execute(total_clubs_query)
        TotalClubs = int(total_clubs_result.scalar_one() or 0)

        # Compute ClubsMarketCap
        clubs_market_cap_query = select(
            func.sum(DCClubsTrading.last_price * 1000000)
        )
        clubs_market_cap_result = await local_session.execute(clubs_market_cap_query)
        ClubsMarketCap = int(clubs_market_cap_result.scalar() or 0)

        # Compute ClubBalances
        club_balances_query = select(func.sum(DCClubs.balance))
        club_balances_result = await local_session.execute(club_balances_query)
        ClubBalances = int(club_balances_result.scalar() or 0)

        # Compute Clubs7dayVolume
        clubs_7day_volume_query = select(func.sum(DCClubsTrading.volume_7_day))
        clubs_7day_volume_result = await local_session.execute(clubs_7day_volume_query)
        Clubs7dayVolume = int(clubs_7day_volume_result.scalar() or 0)

        # Compute TotalMarketCap
        TotalMarketCap = PlayersMarketCap + ClubsMarketCap

        # Compute Total7dayVolume
        Total7dayVolume = Players7dayVolume + Clubs7dayVolume

        # Compute UserBalances
        user_balances_query = select(func.sum(DCUsersCorrected.balance))
        user_balances_result = await local_session.execute(user_balances_query)
        UserBalances = int(user_balances_result.scalar() or 0)

        # Compute UserTotalVolume
        user_total_volume_query = select(func.sum(DCUsersTrading.total_volume))
        user_total_volume_result = await local_session.execute(user_total_volume_query)
        UserTotalVolume = int(user_total_volume_result.scalar() or 0)

        # Compute NumberOfUsers
        number_of_users_query = select(func.count()).select_from(DCUsersCorrected)
        number_of_users_result = await local_session.execute(number_of_users_query)
        NumberOfUsers = int(number_of_users_result.scalar_one() or 0)

        # Compute NumberAgents
        number_agents_query = select(func.count()).select_from(DCPlayers).where(DCPlayers.agent_name != None)
        number_agents_result = await local_session.execute(number_agents_query)
        NumberAgents = int(number_agents_result.scalar() or 0)

        # Compute NumberManagers
        number_managers_query = select(func.count()).select_from(DCClubs).where(DCClubs.manager_name != None)
        number_managers_result = await local_session.execute(number_managers_query)
        NumberManagers = int(number_managers_result.scalar() or 0)

        # Get the maximum last_active timestamp from DCUsersCorrected
        max_last_active_query = select(func.max(DCUsersCorrected.last_active))
        max_last_active_result = await local_session.execute(max_last_active_query)
        max_last_active_timestamp = max_last_active_result.scalar() or int(datetime.utcnow().timestamp())

        current_date = datetime.utcfromtimestamp(max_last_active_timestamp)
        two_weeks_ago_unix = int((current_date - timedelta(weeks=2)).timestamp())

        # Compute ActiveUsers
        active_users_query = select(func.count()).select_from(DCUsersCorrected).where(
            DCUsersCorrected.last_active > two_weeks_ago_unix
        )
        active_users_result = await local_session.execute(active_users_query)
        ActiveUsers = int(active_users_result.scalar() or 0)

        # Compute ActiveManagers
        active_managers_query = select(func.count()).select_from(DCUsersCorrected).where(
            DCUsersCorrected.last_active > two_weeks_ago_unix,
            DCUsersCorrected.club_id != None
        )
        active_managers_result = await local_session.execute(active_managers_query)
        ActiveManagers = int(active_managers_result.scalar() or 0)

        # Compute InactiveManagers
        inactive_managers_query = select(func.count()).select_from(DCUsersCorrected).where(
            DCUsersCorrected.last_active <= two_weeks_ago_unix,
            DCUsersCorrected.club_id != None
        )
        inactive_managers_result = await local_session.execute(inactive_managers_query)
        InactiveManagers = int(inactive_managers_result.scalar() or 0)

        # Compute NumberManagersLocked
        number_managers_locked_query = select(func.count()).select_from(DCClubs).where(
            DCClubs.manager_name != None,
            DCClubs.manager_locked == 1
        )
        number_managers_locked_result = await local_session.execute(number_managers_locked_query)
        NumberManagersLocked = int(number_managers_locked_result.scalar() or 0)

        # Compute NumberManagersUnlocked
        number_managers_unlocked_query = select(func.count()).select_from(DCClubs).where(
            DCClubs.manager_name != None,
            DCClubs.manager_locked == 0
        )
        number_managers_unlocked_result = await local_session.execute(number_managers_unlocked_query)
        NumberManagersUnlocked = int(number_managers_unlocked_result.scalar() or 0)

        # Check if SVC price cache is stale and update it if needed
        if r:
            try:
                cached_value = await r.get("svc_price_weighted")
                if cached_value:
                    cache_data = json.loads(cached_value)
                    last_updated = cache_data.get("last_updated", 0)
                    # If older than 20 minutes (1200 seconds), force update
                    if (time.time() - last_updated) > 1200:
                        logger.info("SVC price cache is stale, updating it now...")
                        await _update_price_cache(r, local_session)
                else:
                    # No cache exists, create it
                    logger.info("No SVC price cache found, creating it now...")
                    await _update_price_cache(r, local_session)
            except Exception as e:
                logger.error(f"Error checking/updating SVC price cache: {e}")

        # Now get the (hopefully fresh) price
        SVC2USDC = await get_svc_price(redis_client=r, mysql_session=local_session)

        # ----------------------
        # NEW: Calculate transfer volumes and percentage changes
        # ----------------------
        from .base import mysql_archival_session_maker
        
        async with mysql_archival_session_maker() as archival_session:
            # Get the most recent block date to calculate time ranges
            max_date_query = select(func.max(Blocks.date))
            max_date_result = await archival_session.execute(max_date_query)
            max_date_unix = max_date_result.scalar()
            
            if max_date_unix:
                # Helper function to calculate percentage change
                def calculate_percentage_change(current, prior):
                    return ((current - prior) / prior * 100) if prior > 0 else 0.0
                
                # Calculate time boundaries
                current_date = datetime.utcfromtimestamp(max_date_unix)
                seven_days_ago_unix = int((current_date - timedelta(days=7)).timestamp())
                fourteen_days_ago_unix = int((current_date - timedelta(days=14)).timestamp())
                thirty_days_ago_unix = int((current_date - timedelta(days=30)).timestamp())
                sixty_days_ago_unix = int((current_date - timedelta(days=60)).timestamp())
                
                # ----------------------
                # OPTIMIZED: Fetch ALL data once for 60 days
                # ----------------------
                
                # 1. Fetch all share trades for 60 days in ONE query (with trade counts)
                all_trades_query = text("""
                    SELECT 
                        DATE(FROM_UNIXTIME(b.date)) as trade_date,
                        b.date as unix_timestamp,
                        sth.share_type,
                        SUM(sth.price * sth.num) as daily_volume,
                        COUNT(*) as trade_count
                    FROM share_trade_history sth
                    JOIN blocks b ON sth.height = b.height
                    WHERE b.date >= :sixty_days_ago
                    GROUP BY DATE(FROM_UNIXTIME(b.date)), sth.share_type
                    ORDER BY trade_date
                """)
                
                trades_result = await archival_session.execute(
                    all_trades_query,
                    {"sixty_days_ago": sixty_days_ago_unix}
                )
                
                # Build dictionaries for in-memory calculations - keep both unix and date string keys
                player_volumes = {}  # unix timestamp -> volume (for calculations)
                club_volumes = {}    # unix timestamp -> volume (for calculations)
                player_volumes_by_date = {}  # date string -> volume (for charts)
                club_volumes_by_date = {}    # date string -> volume (for charts)
                daily_trade_counts = {}  # date string -> total trade count (for DailyTradesChart)
                
                for row in trades_result:
                    date_str = row.trade_date.strftime("%Y-%m-%d")
                    volume = int(row.daily_volume or 0)
                    unix_ts = row.unix_timestamp
                    trade_count = int(row.trade_count or 0)
                    
                    # Accumulate trade counts for the day
                    if date_str not in daily_trade_counts:
                        daily_trade_counts[date_str] = 0
                    daily_trade_counts[date_str] += trade_count
                    
                    if row.share_type == 'player':
                        player_volumes[unix_ts] = volume
                        player_volumes_by_date[date_str] = volume
                    else:  # club
                        club_volumes[unix_ts] = volume
                        club_volumes_by_date[date_str] = volume
                
                # 2. Fetch all transfers for 60 days in ONE query
                all_transfers_query = text("""
                    SELECT 
                        DATE(FROM_UNIXTIME(b.date)) as transfer_date,
                        b.date as unix_timestamp,
                        SUM(m.data_2) as daily_volume
                    FROM messages m
                    JOIN blocks b ON m.height = b.height
                    WHERE m.type = 9 AND b.date >= :sixty_days_ago
                    GROUP BY DATE(FROM_UNIXTIME(b.date))
                    ORDER BY transfer_date
                """)
                
                transfers_result = await archival_session.execute(
                    all_transfers_query,
                    {"sixty_days_ago": sixty_days_ago_unix}
                )
                
                transfer_volumes = {}  # unix timestamp -> volume (for calculations)
                transfer_volumes_by_date = {}  # date string -> volume (for charts)
                for row in transfers_result:
                    date_str = row.transfer_date.strftime("%Y-%m-%d")
                    unix_ts = row.unix_timestamp
                    volume = int(row.daily_volume or 0)
                    transfer_volumes[unix_ts] = volume
                    transfer_volumes_by_date[date_str] = volume
                
                # Calculate all metrics from in-memory data
                Players7dayVolume = sum(v for ts, v in player_volumes.items() if ts >= seven_days_ago_unix)
                Players7dayVolumePrior = sum(v for ts, v in player_volumes.items() if fourteen_days_ago_unix <= ts < seven_days_ago_unix)
                Players30dayVolume = sum(v for ts, v in player_volumes.items() if ts >= thirty_days_ago_unix)
                Players30dayVolumePrior = sum(v for ts, v in player_volumes.items() if sixty_days_ago_unix <= ts < thirty_days_ago_unix)
                
                Clubs7dayVolume = sum(v for ts, v in club_volumes.items() if ts >= seven_days_ago_unix)
                Clubs7dayVolumePrior = sum(v for ts, v in club_volumes.items() if fourteen_days_ago_unix <= ts < seven_days_ago_unix)
                Clubs30dayVolume = sum(v for ts, v in club_volumes.items() if ts >= thirty_days_ago_unix)
                Clubs30dayVolumePrior = sum(v for ts, v in club_volumes.items() if sixty_days_ago_unix <= ts < thirty_days_ago_unix)
                
                Transfers7dayVol = sum(v for ts, v in transfer_volumes.items() if ts >= seven_days_ago_unix)
                Transfers7dayVolPrior = sum(v for ts, v in transfer_volumes.items() if fourteen_days_ago_unix <= ts < seven_days_ago_unix)
                Transfers30dayVol = sum(v for ts, v in transfer_volumes.items() if ts >= thirty_days_ago_unix)
                Transfers30dayVolPrior = sum(v for ts, v in transfer_volumes.items() if sixty_days_ago_unix <= ts < thirty_days_ago_unix)
                
                # Calculate percentage changes
                Players7dayVolumeChange = calculate_percentage_change(Players7dayVolume, Players7dayVolumePrior)
                Players30dayVolumeChange = calculate_percentage_change(Players30dayVolume, Players30dayVolumePrior)
                Clubs7dayVolumeChange = calculate_percentage_change(Clubs7dayVolume, Clubs7dayVolumePrior)
                Clubs30dayVolumeChange = calculate_percentage_change(Clubs30dayVolume, Clubs30dayVolumePrior)
                Transfers7dayVolChange = calculate_percentage_change(Transfers7dayVol, Transfers7dayVolPrior)
                Transfers30dayVolChange = calculate_percentage_change(Transfers30dayVol, Transfers30dayVolPrior)
                
                # Import needed for the chart generation later
                from .share_history import ShareTradeHistory
                
                # Calculate totals
                Total7dayVolumePrior = Players7dayVolumePrior + Clubs7dayVolumePrior
                Total7dayVolumeChange = calculate_percentage_change(Total7dayVolume, Total7dayVolumePrior)
                
                Total30dayVolume = Players30dayVolume + Clubs30dayVolume
                Total30dayVolumePrior = Players30dayVolumePrior + Clubs30dayVolumePrior
                Total30dayVolumeChange = calculate_percentage_change(Total30dayVolume, Total30dayVolumePrior)
            else:
                # Default values if no blocks found  
                Transfers7dayVol = 0
                Transfers7dayVolPrior = 0
                Transfers7dayVolChange = 0.0
                Transfers30dayVol = 0
                Transfers30dayVolPrior = 0
                Transfers30dayVolChange = 0.0
                Players7dayVolumePrior = Players7dayVolume
                Players7dayVolumeChange = 0.0
                Players30dayVolume = 0
                Players30dayVolumePrior = 0
                Players30dayVolumeChange = 0.0
                Clubs7dayVolumePrior = Clubs7dayVolume
                Clubs7dayVolumeChange = 0.0
                Clubs30dayVolume = 0
                Clubs30dayVolumePrior = 0
                Clubs30dayVolumeChange = 0.0
                Total7dayVolumePrior = Total7dayVolume
                Total7dayVolumeChange = 0.0
                Total30dayVolume = 0
                Total30dayVolumePrior = 0
                Total30dayVolumeChange = 0.0
                
        # Calculate SVC trading volumes (from datacentre database) - OPTIMIZED
        # Using datetime objects for the svc_trades table
        current_date = datetime.utcnow()
        seven_days_ago = current_date - timedelta(days=7)
        fourteen_days_ago = current_date - timedelta(days=14)
        thirty_days_ago = current_date - timedelta(days=30)
        sixty_days_ago = current_date - timedelta(days=60)
        
        # Fetch ALL SVC trades for 60 days in ONE query for chart data
        all_svc_trades_query = (
            select(
                func.date(SVCTrades.trade_ts).label('trade_date'),
                func.sum(SVCTrades.volume_svc).label('daily_svc_volume'),
                func.sum(SVCTrades.volume_usdc).label('daily_usdc_volume')
            )
            .where(SVCTrades.trade_ts >= sixty_days_ago)
            .group_by(func.date(SVCTrades.trade_ts))
            .order_by('trade_date')
        )
        
        all_svc_trades_result = await local_session.execute(all_svc_trades_query)
        
        # Build dictionary for chart data
        svc_volumes_by_date = {}  # date string -> svc volume
        usdc_volumes_by_date = {}  # date string -> usdc volume
        for row in all_svc_trades_result:
            date_str = row.trade_date.strftime("%Y-%m-%d")
            svc_volumes_by_date[date_str] = int(row.daily_svc_volume or 0)
            usdc_volumes_by_date[date_str] = float(row.daily_usdc_volume or 0)
        
        # Query for SVC trading volume last 7 days
        svc_vol_7day_query = (
            select(
                func.sum(SVCTrades.volume_svc),
                func.sum(SVCTrades.volume_usdc)
            )
            .where(SVCTrades.trade_ts >= seven_days_ago)
        )
        svc_vol_result = await local_session.execute(svc_vol_7day_query)
        svc_vol_row = svc_vol_result.one()
        SVCTrades7dayVolSVC = int(svc_vol_row[0] or 0)
        SVCTrades7dayVolUSDC = float(svc_vol_row[1] or 0.0)
        
        # Query for SVC trading volume prior 7 days
        svc_vol_prior_query = (
            select(
                func.sum(SVCTrades.volume_svc),
                func.sum(SVCTrades.volume_usdc)
            )
            .where(
                SVCTrades.trade_ts >= fourteen_days_ago,
                SVCTrades.trade_ts < seven_days_ago
            )
        )
        svc_vol_prior_result = await local_session.execute(svc_vol_prior_query)
        svc_vol_prior_row = svc_vol_prior_result.one()
        SVCTrades7dayVolSVCPrior = int(svc_vol_prior_row[0] or 0)
        SVCTrades7dayVolUSDCPrior = float(svc_vol_prior_row[1] or 0.0)
        
        # Query for SVC trading volume last 30 days
        svc_vol_30day_query = (
            select(
                func.sum(SVCTrades.volume_svc),
                func.sum(SVCTrades.volume_usdc)
            )
            .where(SVCTrades.trade_ts >= thirty_days_ago)
        )
        svc_vol_30day_result = await local_session.execute(svc_vol_30day_query)
        svc_vol_30day_row = svc_vol_30day_result.one()
        SVCTrades30dayVolSVC = int(svc_vol_30day_row[0] or 0)
        SVCTrades30dayVolUSDC = float(svc_vol_30day_row[1] or 0.0)
        
        # Query for SVC trading volume prior 30 days
        svc_vol_30day_prior_query = (
            select(
                func.sum(SVCTrades.volume_svc),
                func.sum(SVCTrades.volume_usdc)
            )
            .where(
                SVCTrades.trade_ts >= sixty_days_ago,
                SVCTrades.trade_ts < thirty_days_ago
            )
        )
        svc_vol_30day_prior_result = await local_session.execute(svc_vol_30day_prior_query)
        svc_vol_30day_prior_row = svc_vol_30day_prior_result.one()
        SVCTrades30dayVolSVCPrior = int(svc_vol_30day_prior_row[0] or 0)
        SVCTrades30dayVolUSDCPrior = float(svc_vol_30day_prior_row[1] or 0.0)
        
        # Calculate percentage changes for SVC trading
        if max_date_unix:
            SVCTrades7dayVolSVCChange = calculate_percentage_change(SVCTrades7dayVolSVC, SVCTrades7dayVolSVCPrior)
            SVCTrades7dayVolUSDCChange = calculate_percentage_change(SVCTrades7dayVolUSDC, SVCTrades7dayVolUSDCPrior)
            SVCTrades30dayVolSVCChange = calculate_percentage_change(SVCTrades30dayVolSVC, SVCTrades30dayVolSVCPrior)
            SVCTrades30dayVolUSDCChange = calculate_percentage_change(SVCTrades30dayVolUSDC, SVCTrades30dayVolUSDCPrior)
        else:
            SVCTrades7dayVolSVCChange = 0.0
            SVCTrades7dayVolUSDCChange = 0.0
            SVCTrades30dayVolSVCChange = 0.0
            SVCTrades30dayVolUSDCChange = 0.0

        # ----------------------
        # Generate 30-day chart data
        # ----------------------
        
        # Helper function to generate daily chart data - OPTIMIZED VERSION  
        async def generate_chart_data(player_vols_by_date=None, club_vols_by_date=None, transfer_vols_by_date=None, svc_vols_by_date=None, svc_price_by_date=None):
            charts = {
                "SVCPriceChart": [],
                "PlayersVolumeChart": [],
                "ClubsVolumeChart": [],
                "TransfersVolumeChart": [],
                "SVCTradesVolumeChart": []
            }
            
            # Build chart data from our pre-fetched data
            current_date = datetime.utcnow()
            
            for i in range(29, -1, -1):  # 30 days including today
                target_date = current_date - timedelta(days=i)
                date_str = target_date.strftime("%Y-%m-%d")
                
                # Get SVC price for this day first (we'll use it to calculate USDC values)
                svc_price = svc_price_by_date.get(date_str, 0) if svc_price_by_date else 0
                charts["SVCPriceChart"].append({"date": date_str, "value": svc_price})
                
                # Get volumes for this day from pre-fetched data using date string keys
                # Include both SVC volume and calculated USDC value
                # Note: volumes are in smallest unit, so divide by 10000 to get actual SVC amount
                players_vol = player_vols_by_date.get(date_str, 0) if player_vols_by_date else 0
                players_usdc = round((players_vol / 10000) * svc_price, 2) if svc_price > 0 else 0
                charts["PlayersVolumeChart"].append({
                    "date": date_str, 
                    "svc_volume": players_vol,
                    "usdc_value": players_usdc
                })
                
                clubs_vol = club_vols_by_date.get(date_str, 0) if club_vols_by_date else 0
                clubs_usdc = round((clubs_vol / 10000) * svc_price, 2) if svc_price > 0 else 0
                charts["ClubsVolumeChart"].append({
                    "date": date_str,
                    "svc_volume": clubs_vol,
                    "usdc_value": clubs_usdc
                })
                
                transfers_vol = transfer_vols_by_date.get(date_str, 0) if transfer_vols_by_date else 0
                transfers_usdc = round((transfers_vol / 10000) * svc_price, 2) if svc_price > 0 else 0
                charts["TransfersVolumeChart"].append({
                    "date": date_str,
                    "svc_volume": transfers_vol,
                    "usdc_value": transfers_usdc
                })
                
                svc_vol = svc_vols_by_date.get(date_str, 0) if svc_vols_by_date else 0
                # For SVC trades, the volume is already in actual SVC (not smallest units)
                # So we don't divide by 10000 here
                charts["SVCTradesVolumeChart"].append({
                    "date": date_str,
                    "svc_volume": svc_vol,
                    "usdc_value": round(svc_vol * svc_price, 2) if svc_price > 0 else 0
                })
            
            return charts
        
        # Build daily VWAP price series from svc_trades (already fetched above for 60 days)
        svc_price_by_date = {}
        for date_str, usdc_vol in usdc_volumes_by_date.items():
            svc_vol = svc_volumes_by_date.get(date_str, 0)
            if svc_vol > 0:
                svc_price_by_date[date_str] = usdc_vol / svc_vol

        # Forward-fill: seed with the VWAP of the last trading day before the 30-day window
        seed_price = await get_previous_daily_average(local_session, thirty_days_ago)
        last_price = seed_price or 0.0
        filled_price_by_date = {}
        for i in range(29, -1, -1):  # oldest to newest
            target_date = current_date - timedelta(days=i)
            date_str = target_date.strftime("%Y-%m-%d")
            if date_str in svc_price_by_date:
                last_price = svc_price_by_date[date_str]
            filled_price_by_date[date_str] = last_price
        
        # Generate chart data - pass the pre-fetched data with correct parameter names
        chart_data = await generate_chart_data(
            player_vols_by_date=player_volumes_by_date if 'player_volumes_by_date' in locals() else {},
            club_vols_by_date=club_volumes_by_date if 'club_volumes_by_date' in locals() else {},
            transfer_vols_by_date=transfer_volumes_by_date if 'transfer_volumes_by_date' in locals() else {},
            svc_vols_by_date=svc_volumes_by_date if 'svc_volumes_by_date' in locals() else {},
            svc_price_by_date=filled_price_by_date if 'filled_price_by_date' in locals() else {}
        )
        
        # ----------------------
        # NEW: Fetch daily active users from The Graph Postgres database (30 days)
        # ----------------------
        from .base import graph_postgres_session_maker
        
        daily_active_users_data = []
        if graph_postgres_session_maker is not None:
            async with graph_postgres_session_maker() as activity_session:
                # Get daily unique active users for the last 30 days
                thirty_days_ago_for_users = current_date - timedelta(days=30)
                thirty_days_ago_unix_users = int(thirty_days_ago_for_users.timestamp())

                # Query The Graph Postgres: join game_move -> move -> name -> transaction
                # Count distinct moves to match old MySQL behavior (one row per move in games table)
                schema = base_module.GRAPH_SUBGRAPH_STATS_SCHEMA
                daily_users_query = text(f"""
                    SELECT
                        DATE(TO_TIMESTAMP(t.timestamp)) as activity_date,
                        COUNT(DISTINCT n.name) as unique_users,
                        COUNT(DISTINCT gm.move) as total_transactions
                    FROM {schema}.game_move gm
                    JOIN {schema}.move m ON gm.move = m.id
                    JOIN {schema}.name n ON m.name = n.id
                    JOIN {schema}.transaction t ON gm.tx = t.id
                    JOIN {schema}.game g ON gm.game = g.id
                    WHERE t.timestamp >= :start_unix AND g.game = :game_id
                    GROUP BY DATE(TO_TIMESTAMP(t.timestamp))
                    ORDER BY activity_date
                """)

                daily_users_result = await activity_session.execute(
                    daily_users_query,
                    {"start_unix": thirty_days_ago_unix_users, "game_id": base_module.GAME_ID}
                )

                # Build dictionary for easy lookup
                users_by_date = {}
                transactions_by_date = {}
                for row in daily_users_result:
                    date_str = row.activity_date.strftime("%Y-%m-%d")
                    users_by_date[date_str] = row.unique_users
                    transactions_by_date[date_str] = row.total_transactions

                # Build the chart data for exactly 30 days
                for i in range(29, -1, -1):  # 30 days including today
                    target_date = current_date - timedelta(days=i)
                    date_str = target_date.strftime("%Y-%m-%d")

                    daily_active_users_data.append({
                        "date": date_str,
                        "unique_users": users_by_date.get(date_str, 0),
                        "total_transactions": transactions_by_date.get(date_str, 0)
                    })
        
        # ----------------------
        # NEW: Build DailyTradesChart from the trade counts we already collected
        # ----------------------
        daily_trades_data = []
        for i in range(29, -1, -1):  # 30 days including today
            target_date = current_date - timedelta(days=i)
            date_str = target_date.strftime("%Y-%m-%d")
            
            daily_trades_data.append({
                "date": date_str,
                "trade_count": daily_trade_counts.get(date_str, 0)
            })
        
        # ----------------------
        # END original logic
        # ----------------------

        data_dict = {
            "TotalPlayers": TotalPlayers,
            "PlayersMarketCap": PlayersMarketCap,
            "PlayerValues": PlayerValues,
            "Players7dayVolume": Players7dayVolume,
            "Players7dayVolumePrior": Players7dayVolumePrior,
            "Players7dayVolumeChange": Players7dayVolumeChange,
            "Players30dayVolume": Players30dayVolume,
            "Players30dayVolumePrior": Players30dayVolumePrior,
            "Players30dayVolumeChange": Players30dayVolumeChange,
            "TotalClubs": TotalClubs,
            "ClubsMarketCap": ClubsMarketCap,
            "ClubBalances": ClubBalances,
            "Clubs7dayVolume": Clubs7dayVolume,
            "Clubs7dayVolumePrior": Clubs7dayVolumePrior,
            "Clubs7dayVolumeChange": Clubs7dayVolumeChange,
            "Clubs30dayVolume": Clubs30dayVolume,
            "Clubs30dayVolumePrior": Clubs30dayVolumePrior,
            "Clubs30dayVolumeChange": Clubs30dayVolumeChange,
            "TotalMarketCap": TotalMarketCap,
            "Total7dayVolume": Total7dayVolume,
            "Total7dayVolumePrior": Total7dayVolumePrior,
            "Total7dayVolumeChange": Total7dayVolumeChange,
            "Total30dayVolume": Total30dayVolume,
            "Total30dayVolumePrior": Total30dayVolumePrior,
            "Total30dayVolumeChange": Total30dayVolumeChange,
            "Transfers7dayVol": Transfers7dayVol,
            "Transfers7dayVolPrior": Transfers7dayVolPrior,
            "Transfers7dayVolChange": Transfers7dayVolChange,
            "Transfers30dayVol": Transfers30dayVol,
            "Transfers30dayVolPrior": Transfers30dayVolPrior,
            "Transfers30dayVolChange": Transfers30dayVolChange,
            "SVCTrades7dayVolSVC": SVCTrades7dayVolSVC,
            "SVCTrades7dayVolSVCPrior": SVCTrades7dayVolSVCPrior,
            "SVCTrades7dayVolSVCChange": SVCTrades7dayVolSVCChange,
            "SVCTrades30dayVolSVC": SVCTrades30dayVolSVC,
            "SVCTrades30dayVolSVCPrior": SVCTrades30dayVolSVCPrior,
            "SVCTrades30dayVolSVCChange": SVCTrades30dayVolSVCChange,
            "SVCTrades7dayVolUSDC": SVCTrades7dayVolUSDC,
            "SVCTrades7dayVolUSDCPrior": SVCTrades7dayVolUSDCPrior,
            "SVCTrades7dayVolUSDCChange": SVCTrades7dayVolUSDCChange,
            "SVCTrades30dayVolUSDC": SVCTrades30dayVolUSDC,
            "SVCTrades30dayVolUSDCPrior": SVCTrades30dayVolUSDCPrior,
            "SVCTrades30dayVolUSDCChange": SVCTrades30dayVolUSDCChange,
            "SVCPriceChart": chart_data["SVCPriceChart"],
            "PlayersVolumeChart": chart_data["PlayersVolumeChart"],
            "ClubsVolumeChart": chart_data["ClubsVolumeChart"],
            "TransfersVolumeChart": chart_data["TransfersVolumeChart"],
            "SVCTradesVolumeChart": chart_data["SVCTradesVolumeChart"],
            "DailyActiveUsersChart": daily_active_users_data,
            "DailyTradesChart": daily_trades_data,
            "UserBalances": UserBalances,
            "UserTotalVolume": UserTotalVolume,
            "NumberOfUsers": NumberOfUsers,
            "NumberAgents": NumberAgents,
            "NumberManagers": NumberManagers,
            "ActiveUsers": ActiveUsers,
            "ActiveManagers": ActiveManagers,
            "InactiveManagers": InactiveManagers,
            "NumberManagersLocked": NumberManagersLocked,
            "NumberManagersUnlocked": NumberManagersUnlocked,
            "SVC2USDC": SVC2USDC,
        }

        payload = {
            "data": data_dict,
            "last_updated": time.time(),
        }

        # Store in Redis
        if r:
            await r.set(cache_key, json.dumps(payload))

        elapsed = time.perf_counter() - start_time
        logger.info(f"Finished DB queries and caching for /market in {elapsed:.2f} seconds")

        return MarketResponse(**data_dict)


@market_router.get(
    "/market",
    response_model=MarketResponse,
    summary="Obtain overall market metrics",
    description="Retrieves aggregated market data such as total players, clubs, volumes, user balances, and SVC2USDC price."
)
async def get_market_data(
    background_tasks: BackgroundTasks,
    session: AsyncSession = Depends(get_mysql_session),
    r = Depends(get_redis_client),
):
    cache_key = "market_data_v8"  # v8 removed balance sheet fields (moved to /trading_graph/all)
    refresh_lock_key = "bg:market_data_lock"

    # Attempt to read from cache
    cached_value = r and await r.get(cache_key)
    if cached_value:
        data_json = json.loads(cached_value)
        last_updated = data_json.get("last_updated", 0)

        # If it's fresh (< 30 minutes), return it immediately. The data
        # is a 30-day rolling daily-active-users aggregation; sub-30-min
        # freshness is much tighter than the data's own resolution and
        # was burning Aurora PG CPU on every refresh.
        if (time.time() - last_updated) < 1800:
            logger.info("Serving fresh /market data from Redis cache")
            return MarketResponse(**data_json["data"])
        else:
            # The cache is stale; update in the background, serve stale data.
            # Singleflight: with 8 gunicorn workers (4 × 2 pods), an
            # unguarded background_tasks.add_task fires up to 8 parallel
            # _fetch_and_cache_market_data calls per cache expiry — each
            # runs a 13-17s sgd100 aggregation that saturates the 2-vCPU
            # Aurora PG and starves Synapse (perf review 2026-04-29).
            # The lock TTL (60s) is generous given the query takes ~17s
            # and recovers automatically if a worker crashes mid-refresh.
            if await try_acquire_bg_lock(r, refresh_lock_key, ex=60):
                logger.info("Stale /market cache found; refreshing in background (lock acquired)")
                background_tasks.add_task(_fetch_and_cache_market_data, r, session, cache_key)
            return MarketResponse(**data_json["data"])
    else:
        # No cache at all; fetch fresh data
        logger.info("No /market cache found; fetching fresh data immediately")
        return await _fetch_and_cache_market_data(r, session, cache_key)
