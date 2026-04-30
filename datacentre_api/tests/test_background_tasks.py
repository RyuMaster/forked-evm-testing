"""Tests for background task deduplication via Redis locks.

When running under gunicorn with multiple workers, each worker starts its
own set of background tasks (league table updater, ticker cache updater).
To avoid redundant work and memory churn, each iteration should acquire a
short-lived Redis lock before proceeding. If the lock is already held by
another worker, the iteration should be skipped.
"""
import os

os.environ.setdefault("MYSQL_HOST", "localhost")
os.environ.setdefault("MYSQL_PORT", "3306")
os.environ.setdefault("MYSQL_USER", "test")
os.environ.setdefault("MYSQL_PASSWORD", "test")
os.environ.setdefault("MYSQL_DB", "test")
os.environ.setdefault("MYSQL_ARCHIVAL_HOST", "localhost")
os.environ.setdefault("MYSQL_ARCHIVAL_PORT", "3306")
os.environ.setdefault("MYSQL_ARCHIVAL_USER", "test")
os.environ.setdefault("MYSQL_ARCHIVAL_PASSWORD", "test")
os.environ.setdefault("MYSQL_ARCHIVAL_DB", "test")
os.environ.setdefault("SQLITE_DB_PATH", "/tmp/test.sqlite")
os.environ.setdefault("PLAYERHISTORY_SQLITE_DB_PATH", "/tmp/test_ph.sqlite")

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


@pytest.mark.asyncio
async def test_league_updater_skips_when_lock_held():
    """When another worker holds the Redis lock, the league table updater
    should skip the iteration without running the expensive query."""
    from main import periodic_league_table_cache_updater

    mock_redis = AsyncMock()
    # Lock already held — SET NX returns False
    mock_redis.set = AsyncMock(return_value=False)

    update_mock = AsyncMock()

    with patch("main.get_redis_client", return_value=mock_redis), \
         patch("main.update_all_league_table_caches", update_mock), \
         patch("main.mysql_session_maker") as mock_sm:

        # Run one iteration then cancel
        task = asyncio.create_task(periodic_league_table_cache_updater())
        await asyncio.sleep(0.05)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    # Lock was attempted with correct TTL
    mock_redis.set.assert_called()
    call_args = mock_redis.set.call_args
    assert call_args[0][0] == "bg:league_table_lock"
    assert call_args[1].get("nx") is True
    assert call_args[1].get("ex") == 60

    # The actual update should NOT have been called
    update_mock.assert_not_called()
    # Session should NOT have been created
    mock_sm.assert_not_called()


@pytest.mark.asyncio
async def test_league_updater_runs_when_lock_acquired():
    """When the Redis lock is acquired, the league table updater should
    proceed with the cache update."""
    from main import periodic_league_table_cache_updater

    mock_redis = AsyncMock()
    mock_redis.set = AsyncMock(return_value=True)

    update_mock = AsyncMock()

    mock_session = AsyncMock()
    mock_session_cm = AsyncMock()
    mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session_cm.__aexit__ = AsyncMock(return_value=False)

    with patch("main.get_redis_client", return_value=mock_redis), \
         patch("main.update_all_league_table_caches", update_mock), \
         patch("main.mysql_session_maker", return_value=mock_session_cm):

        task = asyncio.create_task(periodic_league_table_cache_updater())
        await asyncio.sleep(0.05)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    update_mock.assert_called_once()


@pytest.mark.asyncio
async def test_ticker_updater_runs_without_redis():
    """When Redis is not configured, the ticker cache updater should still
    call the refresh function (it handles r=None internally)."""
    from main import periodic_ticker_cache_updater

    refresh_mock = AsyncMock()

    mock_session = AsyncMock()
    mock_session_cm = AsyncMock()
    mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session_cm.__aexit__ = AsyncMock(return_value=False)

    with patch("main.get_redis_client", return_value=None), \
         patch("main.refresh_ticker_cache", refresh_mock), \
         patch("main.mysql_archival_session_maker", return_value=mock_session_cm), \
         patch("main.mysql_session_maker", return_value=mock_session_cm):

        task = asyncio.create_task(periodic_ticker_cache_updater())
        await asyncio.sleep(0.05)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    refresh_mock.assert_called_once()


@pytest.mark.asyncio
async def test_ticker_updater_skips_when_lock_held():
    """When another worker holds the Redis lock, the ticker cache updater
    should skip the iteration."""
    from main import periodic_ticker_cache_updater

    mock_redis = AsyncMock()
    mock_redis.set = AsyncMock(return_value=False)

    refresh_mock = AsyncMock()

    with patch("main.get_redis_client", return_value=mock_redis), \
         patch("main.refresh_ticker_cache", refresh_mock), \
         patch("main.mysql_archival_session_maker") as mock_asm, \
         patch("main.mysql_session_maker") as mock_sm:

        task = asyncio.create_task(periodic_ticker_cache_updater())
        await asyncio.sleep(0.05)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    mock_redis.set.assert_called()
    call_args = mock_redis.set.call_args
    assert call_args[0][0] == "bg:ticker_lock"
    assert call_args[1].get("nx") is True
    assert call_args[1].get("ex") == 120

    refresh_mock.assert_not_called()
    mock_asm.assert_not_called()
    mock_sm.assert_not_called()


@pytest.mark.asyncio
async def test_ticker_updater_runs_when_lock_acquired():
    """When the Redis lock is acquired, the ticker cache updater should
    proceed with the cache refresh."""
    from main import periodic_ticker_cache_updater

    mock_redis = AsyncMock()
    mock_redis.set = AsyncMock(return_value=True)

    refresh_mock = AsyncMock()

    mock_session = AsyncMock()
    mock_session_cm = AsyncMock()
    mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session_cm.__aexit__ = AsyncMock(return_value=False)

    with patch("main.get_redis_client", return_value=mock_redis), \
         patch("main.refresh_ticker_cache", refresh_mock), \
         patch("main.mysql_archival_session_maker", return_value=mock_session_cm), \
         patch("main.mysql_session_maker", return_value=mock_session_cm):

        task = asyncio.create_task(periodic_ticker_cache_updater())
        await asyncio.sleep(0.05)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    refresh_mock.assert_called_once()


@pytest.mark.asyncio
async def test_try_acquire_bg_lock_returns_true_when_no_redis():
    """The shared lock helper should return True when Redis is not configured,
    allowing the caller to proceed without locking."""
    from modules.base import try_acquire_bg_lock

    result = await try_acquire_bg_lock(None, "bg:test_lock", ex=60)
    assert result is True


@pytest.mark.asyncio
async def test_market_skips_background_refresh_when_lock_held():
    """The /market endpoint must use the singleflight lock around its
    background refresh. With 8 gunicorn workers, an unguarded
    background_tasks.add_task fires up to 8 parallel sgd100 aggregations
    against Aurora PG (perf review 2026-04-29 — root cause of slow
    Matrix sync). Regression test: if `r.set(...nx=True)` returns falsy
    (lock held by another worker), background_tasks.add_task must NOT
    be called.
    """
    import json
    import time as _time
    from fastapi import BackgroundTasks
    from modules.market import get_market_data

    # Cached value: stale (older than 30 min freshness window) so the
    # endpoint takes the refresh branch.
    stale_payload = {
        "last_updated": _time.time() - 3600,  # 1 hour ago
        "data": {
            "TotalPlayers": 0, "PlayersMarketCap": 0, "PlayerValues": 0,
            "Players7dayVolume": 0, "TotalClubs": 0, "ClubsMarketCap": 0,
            "ClubBalances": 0, "Clubs7dayVolume": 0, "TotalMarketCap": 0,
            "Total7dayVolume": 0, "UserBalances": 0,
            "DailyTradesChart": [], "DailyVolumesChart": [],
            "DailyActiveUsersChart": [], "SVCPriceChart": [],
        },
    }

    r = AsyncMock()
    r.get = AsyncMock(return_value=json.dumps(stale_payload).encode())
    r.set = AsyncMock(return_value=False)  # lock held → skip refresh

    bg = BackgroundTasks()
    add_task_mock = MagicMock(wraps=bg.add_task)
    bg.add_task = add_task_mock

    with patch("modules.market.MarketResponse", lambda **kw: kw):
        await get_market_data(background_tasks=bg, session=AsyncMock(), r=r)

    add_task_mock.assert_not_called()
    # Sanity: the lock attempt happened with nx=True
    r.set.assert_called_once()
    assert r.set.call_args.kwargs.get("nx") is True


@pytest.mark.asyncio
async def test_market_runs_background_refresh_when_lock_acquired():
    """Counterpart: when r.set(...nx=True) succeeds, the refresh IS
    scheduled."""
    import json
    import time as _time
    from fastapi import BackgroundTasks
    from modules.market import get_market_data

    # Minimal payload — the test patches out MarketResponse, so the
    # contents only need to be a dict.
    stale_payload = {"last_updated": _time.time() - 3600, "data": {"placeholder": True}}

    r = AsyncMock()
    r.get = AsyncMock(return_value=json.dumps(stale_payload).encode())
    r.set = AsyncMock(return_value=True)  # lock acquired

    bg = BackgroundTasks()
    add_task_mock = MagicMock(wraps=bg.add_task)
    bg.add_task = add_task_mock

    with patch("modules.market.MarketResponse", lambda **kw: kw):
        await get_market_data(background_tasks=bg, session=AsyncMock(), r=r)

    add_task_mock.assert_called_once()


@pytest.mark.asyncio
async def test_league_updater_runs_without_redis():
    """When Redis is not configured, the league table updater should still
    call the update function (it handles r=None internally)."""
    from main import periodic_league_table_cache_updater

    update_mock = AsyncMock()

    mock_session = AsyncMock()
    mock_session_cm = AsyncMock()
    mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session_cm.__aexit__ = AsyncMock(return_value=False)

    with patch("main.get_redis_client", return_value=None), \
         patch("main.update_all_league_table_caches", update_mock), \
         patch("main.mysql_session_maker", return_value=mock_session_cm):

        task = asyncio.create_task(periodic_league_table_cache_updater())
        await asyncio.sleep(0.05)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    update_mock.assert_called_once()
