# modules/datadumps.py

import asyncio
import json
import logging
import os
import math
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query, Depends
from fastapi.responses import FileResponse, JSONResponse
from sqlalchemy import select, func, and_
from sqlalchemy.ext.asyncio import AsyncSession

from .base import DUMP_OUTPUT_FOLDER, get_mysql_session, DCPlayerEarnings

logger = logging.getLogger(__name__)

if not DUMP_OUTPUT_FOLDER:
    logger.warning(
        "DUMP_OUTPUT_FOLDER not configured - /dumps/* endpoints will return 503"
    )

datadumps_router = APIRouter(prefix="/dumps")

_CACHE_HEADERS = {"Cache-Control": "public, max-age=120"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _require_configured():
    """Raise 503 if DUMP_OUTPUT_FOLDER is not set."""
    if not DUMP_OUTPUT_FOLDER:
        raise HTTPException(status_code=503, detail="DUMP_OUTPUT_FOLDER not configured")


def _dump_path(filename: str) -> str:
    return os.path.join(DUMP_OUTPUT_FOLDER, filename)


def _file_response(filename: str, as_attachment: bool = False, media_type: str = "application/json") -> FileResponse:
    """Return a FileResponse for a dump file, raising 503 if the file is not yet present."""
    path = _dump_path(filename)
    if not os.path.exists(path):
        raise HTTPException(status_code=503, detail="Data not yet available")
    return FileResponse(
        path=path,
        media_type=media_type,
        headers=_CACHE_HEADERS,
        filename=filename if as_attachment else None,
    )


async def _load_json(filename: str) -> dict:
    """Load a JSON dump file from disk (off the event-loop thread)."""
    path = _dump_path(filename)
    if not os.path.exists(path):
        raise HTTPException(status_code=503, detail="Data not yet available")

    def _read():
        with open(path, "r") as f:
            return json.load(f)

    return await asyncio.to_thread(_read)


# ---------------------------------------------------------------------------
# Share orders
# ---------------------------------------------------------------------------

@datadumps_router.get(
    "/share_orders/csv",
    summary="Download share_orders as CSV",
)
async def get_share_orders_csv():
    """Download the full share_orders table as a CSV file attachment."""
    _require_configured()
    return _file_response("share_orders.csv", as_attachment=True, media_type="text/csv")


@datadumps_router.get(
    "/share_orders/json",
    summary="Download share_orders as JSON",
)
async def get_share_orders_json_download():
    """Download the full share_orders JSON (clubs + players combined) as a file attachment."""
    _require_configured()
    return _file_response("share_orders.json", as_attachment=True)


@datadumps_router.get(
    "/share_orders/clubs",
    summary="Club share orders (optionally filtered by ID)",
)
async def get_share_orders_clubs(
    ids: Optional[List[int]] = Query(None, description="Club IDs to include; omit for all"),
):
    """Serve club share orders grouped by club_id, optionally filtered to specific IDs."""
    _require_configured()
    data = await _load_json("share_orders.json")
    clubs = data.get("data", {}).get("clubs", {})
    if ids is not None:
        id_set = {str(i) for i in ids}
        clubs = {k: v for k, v in clubs.items() if k in id_set}
    return JSONResponse(
        content={"meta": data.get("meta", {}), "data": clubs},
        headers=_CACHE_HEADERS,
    )


@datadumps_router.get(
    "/share_orders/players",
    summary="Player share orders (optionally filtered by ID)",
)
async def get_share_orders_players(
    ids: Optional[List[int]] = Query(None, description="Player IDs to include; omit for all"),
):
    """Serve player share orders grouped by player_id, optionally filtered to specific IDs."""
    _require_configured()
    data = await _load_json("share_orders.json")
    players = data.get("data", {}).get("players", {})
    if ids is not None:
        id_set = {str(i) for i in ids}
        players = {k: v for k, v in players.items() if k in id_set}
    return JSONResponse(
        content={"meta": data.get("meta", {}), "data": players},
        headers=_CACHE_HEADERS,
    )


@datadumps_router.get(
    "/share_orders",
    summary="All share orders (clubs + players combined)",
)
async def get_share_orders():
    """Serve the full share_orders JSON with clubs and players grouped separately."""
    _require_configured()
    return _file_response("share_orders.json")


# ---------------------------------------------------------------------------
# Leagues
# ---------------------------------------------------------------------------

@datadumps_router.get(
    "/leagues",
    summary="All national leagues for the current season",
)
async def get_leagues():
    """Serve all national leagues (comp_type=0) for the current season."""
    _require_configured()
    return _file_response("leagues.json")


@datadumps_router.get(
    "/league_tables",
    summary="League tables for all divisions, optionally filtered to Division 1 by country",
)
async def get_league_tables(
    country_ids: Optional[List[str]] = Query(
        None,
        description="Country codes to filter by (e.g. ENG, ITA); returns Division 1 only for those countries",
    ),
):
    """Without country_ids: serve all league tables for all divisions (flat format).
    With country_ids: serve Division 1 standings for those countries in enriched format
    ({country_id, division, teams})."""
    _require_configured()

    if country_ids is None:
        return _file_response("league_tables.json")

    # Filtered path: cross-reference leagues.json to find Division 1 league_ids for the
    # requested countries, then return them in the enriched {country_id, division, teams} format.
    tables_data, leagues_data = await asyncio.gather(
        _load_json("league_tables.json"),
        _load_json("leagues.json"),
    )

    table_meta = tables_data.get("meta", {})
    tables = tables_data.get("data", {})
    leagues = leagues_data.get("data", {})
    league_fields = leagues_data.get("meta", {}).get("fields", [])
    field_idx = {f: i for i, f in enumerate(league_fields)}
    country_id_idx = field_idx.get("country_id")
    level_idx = field_idx.get("level")

    country_filter = {c.upper() for c in country_ids}

    filtered_tables = {}
    for league_id, league_info in leagues.items():
        country_id = league_info[country_id_idx]
        level = league_info[level_idx]
        # level is stored as the raw DB value: "0" = Division 1 top tier
        if str(level) == "0" and country_id in country_filter and league_id in tables:
            filtered_tables[league_id] = {
                "country_id": country_id,
                "division": 1,
                "teams": tables[league_id],
            }

    meta = {
        **table_meta,
        "filtered_by": {
            "division": 1,
            "country_ids": list(country_filter),
        },
        "total_leagues": len(filtered_tables),
    }

    return JSONResponse(
        content={"meta": meta, "data": filtered_tables},
        headers=_CACHE_HEADERS,
    )


# ---------------------------------------------------------------------------
# Precomputed earnings / packs
# ---------------------------------------------------------------------------

@datadumps_router.get(
    "/club_earnings",
    summary="Pre-calculated club earnings for buyers",
)
async def get_club_earnings():
    """Serve pre-calculated club earnings data (buyable influence and ROI by payback threshold)."""
    _require_configured()
    return _file_response("club_earnings.json")


@datadumps_router.get(
    "/club_earnings_light",
    summary="Simplified club earnings (club_id → SVC per influence per season)",
)
async def get_club_earnings_light():
    """Serve simplified club earnings: just the raw SVC generated per influence per season."""
    _require_configured()
    return _file_response("club_earnings_light.json")


@datadumps_router.get(
    "/club_seller_opportunities",
    summary="Pre-calculated club seller opportunities",
)
async def get_club_seller_opportunities():
    """Serve pre-calculated seller opportunities (premium buy orders by payback threshold)."""
    _require_configured()
    return _file_response("club_seller_opportunities.json")


@datadumps_router.get(
    "/club_packs",
    summary="Club pack deals",
)
async def get_club_packs():
    """Serve club pack deals (clubs with active packs, price, payback, and availability)."""
    _require_configured()
    return _file_response("club_packs.json")


# ---------------------------------------------------------------------------
# Player Earnings (MySQL backed)
# ---------------------------------------------------------------------------

# Position mapping constants
POSITION_BITMAP_TO_STRING = {
    1: "GK",
    2: "LB",
    4: "CB",
    8: "RB",
    16: "DML",
    32: "DMC",
    64: "DMR",
    128: "LM",
    256: "CM",
    512: "RM",
    1024: "AML",
    2048: "AMC",
    4096: "AMR",
    8192: "FL",
    16384: "FC",
    32768: "FR"
}

POSITION_STRING_TO_BITMAP = {v: k for k, v in POSITION_BITMAP_TO_STRING.items()}


def _decode_player_position(bitmap: int) -> str:
    """Decode player position bitmap to string representation."""
    if not bitmap:
        return "Unknown"

    decoded = []
    for bit, pos_name in POSITION_BITMAP_TO_STRING.items():
        if bitmap & bit:
            decoded.append(pos_name)

    return "/".join(decoded) if decoded else "Unknown"


@datadumps_router.get(
    "/player_earnings",
    summary="Player earnings and ROI analysis",
    description="Serve player earnings data with filtering, sorting, and pagination. Uses SMART match time percentages."
)
async def get_player_earnings(
    threshold: int = Query(..., description="ROI threshold in seasons (2, 3, 4, 5, 6, 7, 8, 10, 12, 15, 20)"),
    country: Optional[str] = Query(None, description="Filter by club country (e.g., 'ENG', 'ITA')"),
    division: Optional[int] = Query(None, description="Filter by club division (0-based, so 0=D1)"),
    min_rating: Optional[int] = Query(None, description="Minimum player rating"),
    max_rating: Optional[int] = Query(None, description="Maximum player rating"),
    position: Optional[str] = Query(None, description="Player position code (GK, LB, CB, RB, DML, DMC, DMR, LM, CM, RM, AML, AMC, AMR, FL, FC, FR)"),
    min_age: Optional[int] = Query(None, description="Minimum player age"),
    max_age: Optional[int] = Query(None, description="Maximum player age"),
    min_buyable: Optional[int] = Query(None, description="Minimum buyable influence at threshold"),
    min_match_time: Optional[float] = Query(None, description="Minimum match time percentage (0-100)"),
    max_match_time: Optional[float] = Query(None, description="Maximum match time percentage (0-100)"),
    sort: str = Query("payback", description="Sort field (payback, earnings, buyable, cost, rating, age, avg_price, match_time)"),
    order: str = Query("asc", regex="^(asc|desc)$", description="Sort order (asc, desc)"),
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(100, ge=1, le=500, description="Results per page"),
    session: AsyncSession = Depends(get_mysql_session),
):
    valid_thresholds = [2, 3, 4, 5, 6, 7, 8, 10, 12, 15, 20]
    if threshold not in valid_thresholds:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid threshold. Must be one of: {valid_thresholds}"
        )

    # Position filter handling
    position_bitmap = None
    if position:
        position_upper = position.upper()
        if position_upper in POSITION_STRING_TO_BITMAP:
            position_bitmap = POSITION_STRING_TO_BITMAP[position_upper]
        else:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid position '{position}'. Must be one of: {', '.join(sorted(POSITION_STRING_TO_BITMAP.keys()))}"
            )

    # Select columns
    buyable_col = getattr(DCPlayerEarnings, f"buyable_{threshold}s")
    cost_col = getattr(DCPlayerEarnings, f"cost_{threshold}s")

    # Base query for calculating values in Python later, or we can let MySQL do it
    # We will fetch the actual objects and calculate the dynamic columns
    query = select(DCPlayerEarnings)

    # Apply filters
    filters = []
    
    # Must have buyable influence at this threshold
    filters.append(buyable_col > 0)

    if country:
        filters.append(DCPlayerEarnings.club_country == country)
    if division is not None:
        filters.append(DCPlayerEarnings.club_division == division)
    if min_rating is not None:
        filters.append(DCPlayerEarnings.player_rating >= min_rating)
    if max_rating is not None:
        filters.append(DCPlayerEarnings.player_rating <= max_rating)
    if position_bitmap is not None:
        # Bitwise AND to check if the position matches
        filters.append(DCPlayerEarnings.player_position.op('&')(position_bitmap) > 0)
    if min_age is not None:
        filters.append(DCPlayerEarnings.player_age >= min_age)
    if max_age is not None:
        filters.append(DCPlayerEarnings.player_age <= max_age)
    if min_buyable is not None:
        filters.append(buyable_col >= min_buyable)
    if min_match_time is not None:
        filters.append(DCPlayerEarnings.match_time_percentage >= min_match_time)
    if max_match_time is not None:
        filters.append(DCPlayerEarnings.match_time_percentage <= max_match_time)

    if filters:
        query = query.where(and_(*filters))

    # Apply sorting dynamically
    # Since avg_price and payback are derived, we construct the expressions for MySQL
    avg_price_expr = cost_col / buyable_col
    
    # Handle payback calculation (prevent division by zero for earnings)
    payback_expr = func.coalesce(
        (cost_col / buyable_col) / func.nullif(DCPlayerEarnings.current_earnings, 0),
        999999
    )

    sort_map = {
        "payback": payback_expr,
        "earnings": DCPlayerEarnings.current_earnings,
        "buyable": buyable_col,
        "cost": cost_col,
        "rating": DCPlayerEarnings.player_rating,
        "age": DCPlayerEarnings.player_age,
        "avg_price": avg_price_expr,
        "match_time": DCPlayerEarnings.match_time_percentage
    }

    if sort not in sort_map:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid sort field. Must be one of: {list(sort_map.keys())}"
        )

    sort_column = sort_map[sort]
    
    # We use the player_id as secondary sort criterion / tie breaker.
    if order == "desc":
        query = query.order_by(sort_column.desc(), DCPlayerEarnings.player_id.asc())
    else:
        query = query.order_by(sort_column.asc(), DCPlayerEarnings.player_id.asc())

    # Count total results for pagination
    count_query = select(func.count()).select_from(DCPlayerEarnings)
    if filters:
        count_query = count_query.where(and_(*filters))
        
    total_result = await session.execute(count_query)
    total_results = total_result.scalar_one()
    total_pages = math.ceil(total_results / limit) if total_results > 0 else 0

    # Apply pagination
    query = query.offset((page - 1) * limit).limit(limit)
    result = await session.execute(query)
    players = result.scalars().all()

    # Format output to match the expected array-based format
    data = []
    for p in players:
        buyable = getattr(p, f"buyable_{threshold}s")
        cost = getattr(p, f"cost_{threshold}s")
        
        # Earnings can be a float because it's derived from match percentage / scale.
        earnings = float(p.current_earnings) if p.current_earnings is not None else 0.0
        
        # Make avg_price and payback explicit floats to match the expected JSON schema.
        avg_price = float(cost / buyable) if buyable > 0 else 0.0
        payback = float(avg_price / earnings) if earnings > 0 else 999999.0

        data.append([
            p.player_id,
            p.club_id,
            p.club_country,
            p.club_division,
            p.club_position,
            p.player_nationality,
            p.player_age,
            p.player_rating,
            _decode_player_position(p.player_position),
            p.match_time_percentage,
            earnings,
            buyable,
            cost,
            avg_price,
            payback
        ])

    return JSONResponse(
        content={
            "meta": {
                "threshold": threshold,
                "page": page,
                "limit": limit,
                "total_results": total_results,
                "total_pages": total_pages,
                "filters": {
                    "country": country,
                    "division": division,
                    "min_rating": min_rating,
                    "max_rating": max_rating,
                    "position": position,
                    "min_age": min_age,
                    "max_age": max_age,
                    "min_buyable": min_buyable,
                    "min_match_time": min_match_time,
                    "max_match_time": max_match_time
                },
                "sort": {
                    "field": sort,
                    "order": order
                },
                "fields": [
                    "player_id", "club_id", "club_country", "club_division",
                    "club_position", "player_nationality", "player_age", "player_rating",
                    "player_position", "match_time_percentage", "current_earnings",
                    "buyable", "cost", "avg_price", "payback"
                ]
            },
            "data": data
        },
        headers=_CACHE_HEADERS
    )
