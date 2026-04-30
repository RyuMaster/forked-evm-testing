#!/usr/bin/env python
"""
Script to clear Redis cache entries
"""

import redis
import sys
import argparse

def get_redis_client():
    """Connect to Redis"""
    try:
        client = redis.Redis(
            host='10.0.5.215',
            port=6379,
            decode_responses=True,
            socket_connect_timeout=2,
            socket_timeout=2
        )
        client.ping()
        return client
    except Exception as e:
        print(f"Failed to connect to Redis: {e}")
        sys.exit(1)

def clear_all_cache(client):
    """Clear ALL cache entries (use with caution)"""
    try:
        client.flushdb()
        print("✓ Cleared ALL cache entries")
    except Exception as e:
        print(f"Error clearing all cache: {e}")

def clear_user_balance_weeks(client, username=None):
    """Clear user_balance_weeks cache entries"""
    if username:
        pattern = f"user_balance_weeks:{username}:*"
        print(f"Clearing cache for user: {username}")
    else:
        pattern = "user_balance_weeks:*"
        print("Clearing all user_balance_weeks cache")

    count = 0
    try:
        for key in client.scan_iter(match=pattern):
            client.delete(key)
            count += 1
            print(f"  Deleted: {key}")

        print(f"✓ Cleared {count} user_balance_weeks cache entries")
    except Exception as e:
        print(f"Error clearing user_balance_weeks cache: {e}")

def clear_earnings_cache(client, username=None):
    """Clear earnings cache entries"""
    if username:
        pattern = f"user_earnings:{username}"
        print(f"Clearing earnings cache for user: {username}")
    else:
        pattern = "user_earnings:*"
        print("Clearing all earnings cache")

    count = 0
    try:
        for key in client.scan_iter(match=pattern):
            client.delete(key)
            count += 1
            print(f"  Deleted: {key}")

        print(f"✓ Cleared {count} earnings cache entries")
    except Exception as e:
        print(f"Error clearing earnings cache: {e}")

def list_cache_keys(client, pattern="*"):
    """List all cache keys matching pattern"""
    print(f"Cache keys matching '{pattern}':")
    count = 0
    try:
        for key in client.scan_iter(match=pattern):
            # Get TTL
            ttl = client.ttl(key)
            ttl_str = f"(TTL: {ttl}s)" if ttl > 0 else "(no expire)"
            print(f"  {key} {ttl_str}")
            count += 1

        print(f"Total: {count} keys")
    except Exception as e:
        print(f"Error listing cache: {e}")

def get_cache_stats(client):
    """Get cache statistics"""
    try:
        info = client.info('memory')
        db_info = client.info('keyspace')

        print("Redis Cache Statistics:")
        print(f"  Memory used: {info.get('used_memory_human', 'N/A')}")
        print(f"  Memory peak: {info.get('used_memory_peak_human', 'N/A')}")

        if 'db0' in db_info:
            db0 = db_info['db0']
            print(f"  Total keys: {db0.get('keys', 0)}")
            print(f"  Expires: {db0.get('expires', 0)}")
    except Exception as e:
        print(f"Error getting stats: {e}")

def main():
    parser = argparse.ArgumentParser(description='Manage Redis cache for datacentre API')
    parser.add_argument('action', choices=['clear', 'list', 'stats'],
                       help='Action to perform')
    parser.add_argument('--type', choices=['all', 'weeks', 'earnings'],
                       default='weeks',
                       help='Type of cache to clear (default: weeks)')
    parser.add_argument('--user', help='Specific username to clear cache for')
    parser.add_argument('--pattern', default='*', help='Pattern for listing keys')
    parser.add_argument('--force', action='store_true',
                       help='Skip confirmation for dangerous operations')

    args = parser.parse_args()

    client = get_redis_client()

    if args.action == 'clear':
        if args.type == 'all':
            if not args.force:
                response = input("⚠️  This will clear ALL cache entries. Are you sure? (yes/no): ")
                if response.lower() != 'yes':
                    print("Cancelled")
                    return
            clear_all_cache(client)
        elif args.type == 'weeks':
            clear_user_balance_weeks(client, args.user)
        elif args.type == 'earnings':
            clear_earnings_cache(client, args.user)

    elif args.action == 'list':
        if args.type == 'weeks':
            pattern = f"user_balance_weeks:{args.user}:*" if args.user else "user_balance_weeks:*"
        elif args.type == 'earnings':
            pattern = f"user_earnings:{args.user}" if args.user else "user_earnings:*"
        else:
            pattern = args.pattern
        list_cache_keys(client, pattern)

    elif args.action == 'stats':
        get_cache_stats(client)

if __name__ == "__main__":
    main()