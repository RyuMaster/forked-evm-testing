#!/usr/bin/env python3

# IMPORTANT: PROMETHEUS_MULTIPROC_DIR must be set BEFORE prometheus_client
# is imported (transitively via prometheus_fastapi_instrumentator below).
# When this env var is set at import time, the default REGISTRY uses
# MultiProcessCollector — required because we run gunicorn -w 4 and need
# accurate aggregation across worker processes. Setting it after import
# is silently ignored.
#
# The metrics HTTP endpoint is NOT mounted on the FastAPI app — it's
# served by the gunicorn master process on port 9100 (see gunicorn.conf.py),
# which is unreachable from the public ingress. The K8s Service exposes
# both 8000 (api) and 9100 (metrics) but only 8000 is wired into the
# /api(/|$)(.*) catch-all in prod-ingress.yaml. Don't add `.expose(app)`
# below — there is a regression test (tests/test_metrics.py) that fails
# CI if you do.
import os
os.environ.setdefault("PROMETHEUS_MULTIPROC_DIR", "/tmp/prometheus_multiproc")
os.makedirs(os.environ["PROMETHEUS_MULTIPROC_DIR"], exist_ok=True)

import asyncio
import time
import logging
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from prometheus_fastapi_instrumentator import Instrumentator, metrics

from modules.players import players_router
from modules.clubs import clubs_router
from modules.users import users_router
from modules.share_history import share_history_router
from modules.trading_graph import trading_graph_router
from modules.share_balances import share_balances_router
from modules.market import market_router
from modules.user_activity import user_activity_router
from modules.rich_list import rich_list_router, refresh_rich_list_cache_background
from modules.best_managers import best_managers_router 
from modules.user_balance_sheet import user_balance_sheet_router 
from modules.leagues import leagues_router, update_all_league_table_caches
from modules.countries import countries_router
from modules.club_balance_sheet import club_balance_sheet_router
from modules.leaderboards import leaderboards_router

# NEW: Import the achievements router
from modules.achievements import achievements_router
from modules.commentary import commentary_router
from modules.fixture_history import fixture_history_router
from modules.transfers import transfers_router
from modules.proposals import proposals_router
from modules.ticker import ticker_router, refresh_ticker_cache
from modules.messages import messages_router
from modules.shop_clubs import shop_clubs_router
from modules.datadumps import datadumps_router

from modules.base import (
    connect_redis,
    lookup_subgraph_schemas,
    get_mysql_session,
    get_redis_client,
    mysql_session_maker,
    mysql_archival_session_maker,
    mysql_engine,
    mysql_archival_engine,
    graph_postgres_engine,
    userconfig_engine,
    playerhistory_sqlite_engine,
    redis,
    try_acquire_bg_lock,
)

app = FastAPI(
    root_path=os.getenv("ROOT_PATH", "/")
)

# Wire prometheus-fastapi-instrumentator middleware. This adds standard
# HTTP histograms (request count, duration, in-flight) to every request
# handled by the FastAPI app, grouped by endpoint route template (so
# /clubs/{club_id} is one series, not one per concrete club_id).
#
# Critical: only `.instrument(app)` here — do NOT add `.expose(app)`.
# Calling .expose() would add /metrics as a route on port 8000, which
# is reachable via the public ingress's /api/(.*) catch-all and would
# leak operational telemetry to the world. The metrics endpoint is
# served separately by the gunicorn master on port 9100 — see
# gunicorn.conf.py and tests/test_metrics.py.
Instrumentator(
    excluded_handlers=["/metrics", "/healthz"],
    should_group_status_codes=False,
    should_ignore_untemplated=True,
    should_respect_env_var=False,
    inprogress_name="datacentre_api_http_requests_inprogress",
    inprogress_labels=True,
).add(
    # Default buckets are (0.1, 0.5, 1) — too coarse to see the slow tail.
    # Perf review 2026-04-29 found 5.5% of /leagues requests fall in the
    # `+Inf` bucket and app logs show /share_balances/detailed at 1-2s,
    # all invisible at p99 (clipped at the 1s bucket ceiling). These
    # buckets give actionable latency signal up to 10s.
    metrics.default(
        latency_lowr_buckets=(0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
    )
).instrument(app)

@app.get("/healthz", tags=["System"])
async def health_check():
    """Simple health check endpoint"""
    return {"status": "ok"}

# Set up logging
logger = logging.getLogger("datacentre_api")
logger.setLevel(logging.INFO)

# Add request logging middleware
@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = time.time()
    
    # Get real IP address (Cloudflare or other proxy)
    real_ip = request.headers.get("CF-Connecting-IP")  # Cloudflare
    if not real_ip:
        real_ip = request.headers.get("X-Real-IP")  # Nginx
    if not real_ip:
        real_ip = request.headers.get("X-Forwarded-For", "").split(",")[0].strip()  # Standard proxy
    if not real_ip:
        real_ip = request.client.host if request.client else "unknown"
    
    # Get user agent
    user_agent = request.headers.get("User-Agent", "unknown")
    
    # Log request details
    logger.info(f"Request started: {request.method} {request.url.path} - IP: {real_ip} - Query: {dict(request.query_params)} - UA: {user_agent}")
    
    # Process request
    response = await call_next(request)
    
    # Calculate request duration
    process_time = time.time() - start_time
    
    # Log response details with potential slow query warning
    log_msg = f"Request completed: {request.method} {request.url.path} - IP: {real_ip} - Status: {response.status_code} - Duration: {process_time:.3f}s"
    if process_time > 1.0:  # Warn if request takes more than 1 second
        logger.warning(f"SLOW REQUEST: {log_msg}")
    else:
        logger.info(log_msg)
    
    # Add custom header with process time
    response.headers["X-Process-Time"] = str(process_time)
    
    return response

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add gzip compression for responses larger than 1KB
# This will reduce JSON payloads by ~80% (e.g., 1.3MB -> 260KB)
app.add_middleware(GZipMiddleware, minimum_size=1000)

# Existing routers
app.include_router(players_router, tags=["Players"])
app.include_router(clubs_router, tags=["Clubs"])
app.include_router(users_router, tags=["Users"])
app.include_router(share_history_router, tags=["Share History"])
app.include_router(trading_graph_router, tags=["Trading Graph"])
app.include_router(share_balances_router, tags=["Share Balances"])
app.include_router(market_router, tags=["Market"])
app.include_router(user_activity_router, tags=["User Activity"])
app.include_router(rich_list_router, tags=["Rich List"])
app.include_router(best_managers_router, tags=["Best Managers"])
app.include_router(user_balance_sheet_router, tags=["User Balance Sheet"])
app.include_router(leagues_router, tags=["Leagues"])
app.include_router(countries_router, tags=["Countries"])
app.include_router(club_balance_sheet_router, tags=["Club Balance Sheet"])
app.include_router(leaderboards_router, tags=["Leaderboards"])

# NEW: Include the achievements router
app.include_router(achievements_router, tags=["Achievements"])
app.include_router(commentary_router, tags=["Commentary"])
app.include_router(fixture_history_router, tags=["Fixture History"])
app.include_router(transfers_router, tags=["Transfers"])
app.include_router(proposals_router, tags=["Proposals"])
app.include_router(ticker_router, tags=["Ticker"])
app.include_router(messages_router, tags=["Messages"])
app.include_router(shop_clubs_router, tags=["Shop"])
app.include_router(datadumps_router, tags=["Data Dumps"])

# Background task to update all league table caches every minute
async def periodic_league_table_cache_updater():
    while True:
        try:
            r = get_redis_client()
            if await try_acquire_bg_lock(r, "bg:league_table_lock", ex=60):
                async with mysql_session_maker() as session:
                    await update_all_league_table_caches(r, session)
        except Exception as e:
            logger.warning(f"Error updating league table caches: {e}")
        await asyncio.sleep(60)

# Background task to refresh ticker cache every 2 minutes
async def periodic_ticker_cache_updater():
    while True:
        try:
            r = get_redis_client()
            if await try_acquire_bg_lock(r, "bg:ticker_lock", ex=120):
                async with mysql_archival_session_maker() as archival_session:
                    async with mysql_session_maker() as mysql_session:
                        await refresh_ticker_cache(r, archival_session, mysql_session)
        except Exception as e:
            logger.warning(f"Error updating ticker cache: {e}")
        await asyncio.sleep(120)

# Startup event initializes Redis and schedules periodic cache updates
@app.on_event("startup")
async def startup_event():
    await connect_redis()
    if graph_postgres_engine is not None:
        await lookup_subgraph_schemas()
    asyncio.create_task(periodic_league_table_cache_updater())
    asyncio.create_task(periodic_ticker_cache_updater())
    # Pre-warm rich list cache in background
    asyncio.create_task(refresh_rich_list_cache_background())

# Shutdown event to properly dispose of database connections
@app.on_event("shutdown")
async def shutdown_event():
    """Properly dispose of all database engines and close Redis connection"""
    # Dispose all database engines
    await mysql_engine.dispose()
    await mysql_archival_engine.dispose()
    if graph_postgres_engine is not None:
        await graph_postgres_engine.dispose()
    if userconfig_engine is not None:
        await userconfig_engine.dispose()
    await playerhistory_sqlite_engine.dispose()
    
    # Close Redis connection
    if redis:
        await redis.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, workers=1)
