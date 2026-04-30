#!/usr/bin/env python3
import asyncio
import os
from dotenv import load_dotenv
import redis.asyncio as aioredis

load_dotenv()

async def clear_market_cache():
    redis_host = os.getenv("REDIS_HOST", "localhost")
    redis_port = os.getenv("REDIS_PORT", "6379")
    redis_db = os.getenv("REDIS_DB", "0")
    redis_password = os.getenv("REDIS_PASSWORD", None)
    
    if redis_password:
        redis_url = f"redis://:{redis_password}@{redis_host}:{redis_port}/{redis_db}"
    else:
        redis_url = f"redis://{redis_host}:{redis_port}/{redis_db}"
    
    r = aioredis.from_url(redis_url, decode_responses=True)
    
    # Delete the market cache key
    result = await r.delete("market_data_v1")
    if result:
        print("Market cache cleared successfully!")
    else:
        print("No market cache found to clear.")
    
    await r.close()

if __name__ == "__main__":
    asyncio.run(clear_market_cache())