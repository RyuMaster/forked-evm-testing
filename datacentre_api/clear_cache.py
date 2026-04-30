import asyncio
import redis.asyncio as aioredis

async def clear_cache():
    r = aioredis.from_url('redis://10.0.23.76:6379/0', decode_responses=True)

    # Get count of keys before clearing
    key_count = await r.dbsize()
    print(f'Keys in Redis before flush: {key_count}')

    # Clear ALL keys in the current database
    await r.flushdb()
    print('All Redis keys cleared successfully!')

    # Verify
    remaining_keys = await r.dbsize()
    print(f'Keys remaining: {remaining_keys}')

    await r.close()

asyncio.run(clear_cache())

