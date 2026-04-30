#!/usr/bin/env python3
"""
This script shows the optimized approach for the market endpoint.
We fetch all data once for 60 days, then calculate everything in memory.
"""

import asyncio
from datetime import datetime, timedelta
from sqlalchemy import text

async def fetch_all_data_once():
    """
    Fetch all trading data for 60 days in just a few queries,
    then calculate 7-day, 30-day metrics and chart data in memory.
    """
    
    # Time boundaries
    current_date = datetime.utcnow()
    sixty_days_ago = current_date - timedelta(days=60)
    sixty_days_ago_unix = int(sixty_days_ago.timestamp())
    
    # 1. Single query for all share trades (players and clubs)
    all_trades_query = text("""
        SELECT 
            DATE(FROM_UNIXTIME(b.date)) as trade_date,
            b.date as unix_timestamp,
            sth.share_type,
            SUM(sth.price * sth.num) as daily_volume
        FROM share_trade_history sth
        JOIN blocks b ON sth.height = b.height
        WHERE b.date >= :sixty_days_ago
        GROUP BY DATE(FROM_UNIXTIME(b.date)), sth.share_type
        ORDER BY trade_date
    """)
    
    # 2. Single query for all transfers
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
    
    # 3. Single query for all SVC trades
    all_svc_trades_query = text("""
        SELECT 
            DATE(trade_ts) as trade_date,
            SUM(volume_svc) as daily_svc_volume,
            SUM(volume_usdc) as daily_usdc_volume
        FROM svc_trades
        WHERE trade_ts >= :sixty_days_ago_dt
        GROUP BY DATE(trade_ts)
        ORDER BY trade_date
    """)
    
    # 4. Single query for SVC prices
    all_prices_query = text("""
        SELECT 
            DATE(updated_at) as price_date,
            MAX(updated_at) as latest_time,
            svc2usdc
        FROM price_history
        WHERE updated_at >= :sixty_days_ago_dt
        GROUP BY DATE(updated_at), svc2usdc
        ORDER BY price_date, latest_time DESC
    """)
    
    # After fetching, process in memory:
    
    # Build dictionaries by date
    player_volumes = {}  # date -> volume
    club_volumes = {}    # date -> volume
    transfer_volumes = {} # date -> volume
    svc_volumes = {}     # date -> volume
    prices = {}          # date -> price
    
    # Calculate time boundaries for filtering
    seven_days_ago = current_date - timedelta(days=7)
    fourteen_days_ago = current_date - timedelta(days=14)
    thirty_days_ago = current_date - timedelta(days=30)
    
    # From the dictionaries, calculate:
    # - 7-day current (last 7 days)
    # - 7-day prior (days 8-14)
    # - 30-day current (last 30 days)
    # - 30-day prior (days 31-60)
    # - Chart data arrays (30 daily points)
    
    # Example calculation:
    players_7day = sum(v for d, v in player_volumes.items() if d >= seven_days_ago)
    players_7day_prior = sum(v for d, v in player_volumes.items() if fourteen_days_ago <= d < seven_days_ago)
    players_30day = sum(v for d, v in player_volumes.items() if d >= thirty_days_ago)
    players_30day_prior = sum(v for d, v in player_volumes.items() if sixty_days_ago <= d < thirty_days_ago)
    
    # Generate chart data
    chart_data = {
        "SVCPriceChart": [],
        "PlayersVolumeChart": [],
        "ClubsVolumeChart": [],
        "TransfersVolumeChart": [],
        "SVCTradesVolumeChart": []
    }
    
    # Build 30-day chart arrays
    for i in range(29, -1, -1):
        target_date = current_date - timedelta(days=i)
        date_str = target_date.strftime("%Y-%m-%d")
        
        chart_data["SVCPriceChart"].append({
            "date": date_str,
            "value": prices.get(target_date.date(), 0)
        })
        chart_data["PlayersVolumeChart"].append({
            "date": date_str,
            "value": player_volumes.get(target_date.date(), 0)
        })
        # ... etc for other charts
    
    return {
        "metrics": {
            "players_7day": players_7day,
            "players_7day_prior": players_7day_prior,
            # ... etc
        },
        "charts": chart_data
    }

if __name__ == "__main__":
    print("This is a demonstration of the optimized approach")
    print("Key benefits:")
    print("1. Fetch all 60 days of data in just 4-5 queries")
    print("2. Calculate all metrics in memory") 
    print("3. Generate chart data from same cached data")
    print("4. Much faster than 30+ individual queries")