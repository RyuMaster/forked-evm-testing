# modules/shop_clubs.py

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import (
    select,
    func,
    case,
    Table,
    Column,
    MetaData,
    BigInteger,
    Integer,
    Boolean,
    Text,
    LargeBinary,
    Numeric,
)
from sqlalchemy.dialects.postgresql import INT4RANGE
from pydantic import BaseModel
from typing import Optional, List
from enum import Enum

import modules.base as base_module
from modules.base import (
    get_mysql_session,
    get_user_activity_session,  # Graph Postgres session
    PaginatedResponse,
    DCClubs,
)


# ---------------------------------------------------------------------------
# Tier name <-> index bidirectional mapping.
# Keys must match the SaleTier.name values stored in the subgraph.
# ---------------------------------------------------------------------------
TIER_NAME_TO_INDEX = {
    "tier 1": 1,
    "tier 2": 2,
    "tier 3": 3,
    "tier 4": 4,
    "tier 5": 5,
}
TIER_INDEX_TO_NAME = {v: k for k, v in TIER_NAME_TO_INDEX.items()}


# ---------------------------------------------------------------------------
# Subgraph table helpers
# ---------------------------------------------------------------------------

def _is_current(table):
    """Filter for the current (live) version of a mutable subgraph entity."""
    return func.coalesce(func.upper(table.c.block_range), 2147483647) == 2147483647


def _get_subgraph_tables():
    """Create SQLAlchemy Table objects for the SV subgraph pack-sale entities."""
    metadata = MetaData(schema=base_module.GRAPH_SUBGRAPH_SV_SCHEMA)

    sale_tier = Table(
        "sale_tier", metadata,
        Column("vid", BigInteger, primary_key=True),
        Column("id", LargeBinary),
        Column("name", Text),
        Column("active", Boolean),
        Column("block_range", INT4RANGE),
    )

    sale_club = Table(
        "sale_club", metadata,
        Column("vid", BigInteger, primary_key=True),
        Column("id", LargeBinary),
        Column("club_id", Integer),
        Column("tier", LargeBinary),
        Column("paused_in_tier", LargeBinary),
        Column("minted", Integer),
        Column("tranche_index", Integer),
        Column("remaining_in_tranche", Integer),
        Column("block_range", INT4RANGE),
    )

    pack = Table(
        "pack", metadata,
        Column("vid", BigInteger, primary_key=True),
        Column("id", LargeBinary),
        Column("primary_club", LargeBinary),
        Column("max_packs", Integer),
        Column("cost", Numeric),
        Column("block_range", INT4RANGE),
    )

    pack_share_content = Table(
        "pack_share_content", metadata,
        Column("vid", BigInteger, primary_key=True),
        Column("id", LargeBinary),
        Column("pack", LargeBinary),
        Column("club", LargeBinary),
        Column("num", Integer),
        Column("block_range", INT4RANGE),
    )

    pricing_step = Table(
        "pricing_step", metadata,
        Column("vid", BigInteger, primary_key=True),
        Column("id", LargeBinary),
        Column("tier", LargeBinary),
        Column("index", Integer),
        Column("num_shares", Integer),
        Column("price", Numeric),
        Column("from_total", Integer),
        Column("to_total", Integer),
        Column("block_range", INT4RANGE),
    )

    return sale_tier, sale_club, pack, pack_share_content, pricing_step


# ---------------------------------------------------------------------------
# Pydantic response models
# ---------------------------------------------------------------------------

class ShopClubShareResponse(BaseModel):
    club_id: int
    amount: int
    tranche: int
    tranche_remaining: int
    current_price: Optional[int] = None


class ShopClubPricingResponse(BaseModel):
    num: int
    price: int


class ShopClubResponse(BaseModel):
    club_id: int
    country_id: Optional[str] = None
    tier: Optional[int] = None
    available: bool
    tranche: Optional[int] = None
    tranche_remaining: int
    current_price: Optional[int] = None
    pricePerPack: Optional[int] = None
    max_packs: int
    shares: List[ShopClubShareResponse] = []
    pricing: List[ShopClubPricingResponse] = []


class ShopClubsSortBy(str, Enum):
    club_id = "club_id"
    tier = "tier"
    pricePerPack = "pricePerPack"
    sharesAvailableSale = "sharesAvailableSale"


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------
shop_clubs_router = APIRouter(prefix="/shop")


@shop_clubs_router.get(
    "/clubs",
    response_model=PaginatedResponse[ShopClubResponse],
    summary="Retrieve pack-sale data for clubs",
    description=(
        "Fetch a paginated list of club packs from the on-chain sale, "
        "with filtering by tier, country, club ID, and price range."
    ),
)
async def get_shop_clubs(
    page: int = Query(1, ge=1, description="Page number (1-based)"),
    per_page: int = Query(20, ge=1, le=100, description="Items per page"),
    tiers: Optional[List[int]] = Query(None, description="Filter by tier (1-5)"),
    country_ids: Optional[List[str]] = Query(None, description="Filter by 3-letter country codes"),
    club_ids: Optional[List[int]] = Query(None, description="Filter by club ID"),
    pricePerPack_min: Optional[int] = Query(None, description="Min pack price (USDC, 6 decimals)"),
    pricePerPack_max: Optional[int] = Query(None, description="Max pack price (USDC, 6 decimals)"),
    sort_by: Optional[ShopClubsSortBy] = Query(None, description="Sort field"),
    sort_order: Optional[str] = Query("asc", regex="^(asc|desc)$", description="Sort direction"),
    mysql_session: AsyncSession = Depends(get_mysql_session),
    pg_session: AsyncSession = Depends(get_user_activity_session),
):
    if pg_session is None or base_module.GRAPH_SUBGRAPH_SV_SCHEMA is None:
        return PaginatedResponse(
            page=page, per_page=per_page, total=0, total_pages=0, items=[],
        )

    st, sc, p, psc, ps = _get_subgraph_tables()
    effective_tier = func.coalesce(sc.c.tier, sc.c.paused_in_tier)

    # ------------------------------------------------------------------
    # 1. Resolve country_ids filter → primary club_id list via MySQL
    # ------------------------------------------------------------------
    country_club_ids = None
    if country_ids:
        result = await mysql_session.execute(
            select(DCClubs.club_id).where(DCClubs.country_id.in_(country_ids))
        )
        country_club_ids = [row[0] for row in result.fetchall()]
        if not country_club_ids:
            return PaginatedResponse(
                page=page, per_page=per_page, total=0, total_pages=0, items=[],
            )

    # ------------------------------------------------------------------
    # 2. Build main paginated query
    # ------------------------------------------------------------------

    # The default listing is pack-centric: one row per Pack, keyed on the
    # primary SaleClub.  Only clubs that are part of a sale tier (active or
    # paused) are shown; fully-removed clubs (both tier fields NULL) are not.
    #
    # When club_ids is given the query is club-centric instead: we look up
    # each requested SaleClub directly by club_id and join to its pack via
    # PackShareContent.  This correctly handles clubs that are secondary
    # shares in a pack and clubs that are sold-out (tranche_index == -1).

    # Pack-centric join: sale_club (primary) → pack → sale_tier → pricing_step.
    # Always used; when club_ids is given an additional IN-subquery restricts
    # to packs that contain any of the requested clubs as a share.
    join_expr = (
        sc
        .outerjoin(p, (p.c.primary_club == sc.c.id) & _is_current(p))
        .outerjoin(st, (st.c.id == effective_tier) & _is_current(st))
        .outerjoin(
            ps,
            (ps.c.tier == effective_tier)
            & (ps.c.index == sc.c.tranche_index)
            & _is_current(ps),
        )
    )
    base_where = _is_current(sc) & (
        sc.c.tier.isnot(None) | sc.c.paused_in_tier.isnot(None)
    )

    data_cols = [
        sc.c.club_id,
        sc.c.tier.label("tier_fk"),
        sc.c.minted,
        sc.c.tranche_index,
        sc.c.remaining_in_tranche,
        st.c.name.label("tier_name"),
        st.c.active.label("tier_active"),
        p.c.id.label("pack_id"),
        p.c.cost,
        p.c.max_packs,
        ps.c.price.label("current_price"),
        effective_tier.label("effective_tier_id"),
    ]

    data_query = select(*data_cols).select_from(join_expr).where(base_where)
    count_query = select(func.count()).select_from(join_expr).where(base_where)

    # Filters

    # country_ids → restrict primary club
    if country_club_ids is not None:
        filt = sc.c.club_id.in_(country_club_ids)
        data_query = data_query.where(filt)
        count_query = count_query.where(filt)

    if club_ids is not None:
        # Restrict to packs that contain any of the requested clubs as a share
        # (whether primary or secondary).  Walk: club_id → sale_club.id →
        # pack_share_content.club → pack_share_content.pack → pack.primary_club.
        sc_inner = sc.alias("sc_inner")
        psc_inner = psc.alias("psc_inner")
        p_inner = p.alias("p_inner")
        primary_club_entity_ids = (
            select(p_inner.c.primary_club)
            .select_from(
                p_inner.join(
                    psc_inner,
                    (psc_inner.c.pack == p_inner.c.id) & _is_current(psc_inner),
                ).join(
                    sc_inner,
                    (sc_inner.c.id == psc_inner.c.club) & _is_current(sc_inner),
                )
            )
            .where(_is_current(p_inner))
            .where(sc_inner.c.club_id.in_(club_ids))
            .scalar_subquery()
        )
        filt = sc.c.id.in_(primary_club_entity_ids)
        data_query = data_query.where(filt)
        count_query = count_query.where(filt)

    if tiers:
        tier_names = [TIER_INDEX_TO_NAME[t] for t in tiers if t in TIER_INDEX_TO_NAME]
        if tier_names:
            filt = st.c.name.in_(tier_names)
            data_query = data_query.where(filt)
            count_query = count_query.where(filt)

    if pricePerPack_min is not None:
        filt = p.c.cost >= pricePerPack_min
        data_query = data_query.where(filt)
        count_query = count_query.where(filt)

    if pricePerPack_max is not None:
        filt = p.c.cost <= pricePerPack_max
        data_query = data_query.where(filt)
        count_query = count_query.where(filt)

    # Sorting: primary column as requested, club_id asc as tiebreaker.
    if sort_by:
        order = sort_order or "asc"
        if sort_by == ShopClubsSortBy.club_id:
            col = sc.c.club_id
        elif sort_by == ShopClubsSortBy.pricePerPack:
            col = p.c.cost
        elif sort_by == ShopClubsSortBy.tier:
            col = case(
                *[
                    (st.c.name == name, idx)
                    for name, idx in TIER_NAME_TO_INDEX.items()
                ],
                else_=0,
            )
        elif sort_by == ShopClubsSortBy.sharesAvailableSale:
            col = sc.c.minted
            # Flip: more available = less minted, so ASC available → DESC minted
            order = "desc" if order == "asc" else "asc"
        else:
            col = sc.c.club_id
        primary_col = col.desc() if order == "desc" else col.asc()
        data_query = data_query.order_by(primary_col, sc.c.club_id.asc())

    # Pagination
    offset = (page - 1) * per_page
    data_query = data_query.limit(per_page).offset(offset)

    # Execute count + data
    count_result = await pg_session.execute(count_query)
    total = count_result.scalar_one()
    total_pages = (total + per_page - 1) // per_page if total else 0

    # When club_ids is set we must not short-circuit on total==0: the fallback
    # in step 7 still needs to run for sold-out clubs that have no pack rows.
    if total == 0 and club_ids is None:
        return PaginatedResponse(
            page=page, per_page=per_page, total=0, total_pages=0, items=[],
        )

    data_result = await pg_session.execute(data_query)
    rows = data_result.fetchall() if total > 0 else []

    # Collect IDs for batch queries
    pack_ids = []
    tier_ids = set()
    primary_club_ids = []
    row_data = []

    for row in rows:
        m = dict(row._mapping)
        raw_pack_id = m["pack_id"]
        pack_id = bytes(raw_pack_id) if raw_pack_id is not None else None
        if pack_id is not None:
            pack_ids.append(pack_id)
        primary_club_ids.append(m["club_id"])
        eff_tier = m["effective_tier_id"]
        if eff_tier is not None:
            tier_ids.add(bytes(eff_tier))
        m["pack_id"] = pack_id
        m["effective_tier_id"] = bytes(eff_tier) if eff_tier is not None else None
        row_data.append(m)

    # ------------------------------------------------------------------
    # 3. Batch-fetch share contents for the paginated packs
    # ------------------------------------------------------------------
    sc_share = sc.alias("sc_share")
    ps_share = ps.alias("ps_share")
    eff_tier_share = func.coalesce(sc_share.c.tier, sc_share.c.paused_in_tier)

    shares_join = (
        psc
        .join(sc_share, (sc_share.c.id == psc.c.club) & _is_current(sc_share))
        .outerjoin(
            ps_share,
            (ps_share.c.tier == eff_tier_share)
            & (ps_share.c.index == sc_share.c.tranche_index)
            & _is_current(ps_share),
        )
    )

    shares_query = (
        select(
            psc.c.pack.label("pack_id"),
            psc.c.num.label("amount"),
            sc_share.c.club_id,
            sc_share.c.tranche_index,
            sc_share.c.remaining_in_tranche,
            ps_share.c.price.label("current_price"),
        )
        .select_from(shares_join)
        .where(_is_current(psc))
        .where(psc.c.pack.in_(pack_ids))
    )

    shares_result = await pg_session.execute(shares_query)

    # Group shares by pack_id
    shares_by_pack: dict[bytes, list[ShopClubShareResponse]] = {}
    for sr in shares_result.fetchall():
        sm = sr._mapping
        key = bytes(sm["pack_id"])
        price = int(sm["current_price"]) if sm["current_price"] is not None else None
        entry = ShopClubShareResponse(
            club_id=sm["club_id"],
            amount=sm["amount"],
            tranche=sm["tranche_index"] + 1,
            tranche_remaining=sm["remaining_in_tranche"],
            current_price=price,
        )
        shares_by_pack.setdefault(key, []).append(entry)

    # Sort shares so primary club comes first, then by club_id
    for m in row_data:
        key = m["pack_id"]
        if key in shares_by_pack:
            primary = m["club_id"]
            shares_by_pack[key].sort(
                key=lambda s: (0 if s.club_id == primary else 1, s.club_id)
            )

    # ------------------------------------------------------------------
    # 4. Batch-fetch pricing steps for relevant tiers
    # ------------------------------------------------------------------
    pricing_by_tier: dict[bytes, list[dict]] = {}
    if tier_ids:
        pricing_query = (
            select(
                ps.c.tier.label("tier_id"),
                ps.c.index,
                ps.c.num_shares,
                ps.c.price,
            )
            .where(_is_current(ps))
            .where(ps.c.tier.in_(list(tier_ids)))
            .order_by(ps.c.tier, ps.c.index)
        )
        pricing_result = await pg_session.execute(pricing_query)
        for pr in pricing_result.fetchall():
            pm = pr._mapping
            key = bytes(pm["tier_id"])
            pricing_by_tier.setdefault(key, []).append({
                "index": pm["index"],
                "num_shares": pm["num_shares"],
                "price": pm["price"],
            })

    # ------------------------------------------------------------------
    # 5. Enrich with country data from MySQL
    # ------------------------------------------------------------------
    country_result = await mysql_session.execute(
        select(DCClubs.club_id, DCClubs.country_id)
        .where(DCClubs.club_id.in_(primary_club_ids))
    )
    country_map = {row[0]: row[1] for row in country_result.fetchall()}

    # ------------------------------------------------------------------
    # 6. Assemble response items
    # ------------------------------------------------------------------
    items = []
    seen_club_ids = set()
    for m in row_data:
        tier_name = m["tier_name"]
        tier_idx = TIER_NAME_TO_INDEX.get(tier_name) if tier_name else None
        tranche_index = m["tranche_index"]
        sold_out = tranche_index == -1
        available = (not sold_out) and m["tier_fk"] is not None and m["tier_active"] is True
        remaining = m["remaining_in_tranche"]
        cost = int(m["cost"]) if m["cost"] is not None else None
        current_price = int(m["current_price"]) if m["current_price"] is not None else None

        # Build pricing array: all steps for this tier.
        eff_tier = m["effective_tier_id"]
        pricing = []
        if eff_tier is not None:
            for step in pricing_by_tier.get(eff_tier, []):
                pricing.append(ShopClubPricingResponse(
                    num=step["num_shares"],
                    price=int(step["price"]),
                ))

        items.append(ShopClubResponse(
            club_id=m["club_id"],
            country_id=country_map.get(m["club_id"]),
            tier=tier_idx,
            available=available,
            tranche=None if sold_out else tranche_index + 1,
            tranche_remaining=remaining,
            current_price=current_price,
            pricePerPack=cost,
            max_packs=0 if sold_out else (m["max_packs"] or 0),
            shares=shares_by_pack.get(m["pack_id"], []),
            pricing=pricing,
        ))
        seen_club_ids.add(m["club_id"])

    # ------------------------------------------------------------------
    # 7. For any explicitly requested club_ids not found in pack results,
    #    look up their last known SaleClub state (via max vid, ignoring
    #    block_range) and append an available=False stub.
    # ------------------------------------------------------------------
    if club_ids is not None:
        missing_ids = [cid for cid in club_ids if cid not in seen_club_ids]
        if missing_ids:
            # Fetch clubs that are still current in the subgraph but have no
            # active pack (i.e. paused_in_tier is set, meaning sold out of all
            # tranches).  Clubs not present at all in the subgraph are ignored.
            fallback_result = await pg_session.execute(
                select(
                    sc.c.club_id,
                    sc.c.tranche_index,
                    sc.c.remaining_in_tranche,
                )
                .where(_is_current(sc))
                .where(sc.c.club_id.in_(missing_ids))
            )
            fallback_rows = fallback_result.fetchall()

            # Fetch country data for missing clubs from MySQL.
            missing_country_result = await mysql_session.execute(
                select(DCClubs.club_id, DCClubs.country_id)
                .where(DCClubs.club_id.in_(missing_ids))
            )
            missing_country_map = {row[0]: row[1] for row in missing_country_result.fetchall()}

            for fr in fallback_rows:
                items.append(ShopClubResponse(
                    club_id=fr.club_id,
                    country_id=missing_country_map.get(fr.club_id),
                    tier=None,
                    available=False,
                    tranche=None,
                    tranche_remaining=fr.remaining_in_tranche,
                    current_price=None,
                    pricePerPack=None,
                    max_packs=0,
                    shares=[],
                    pricing=[],
                ))

    return PaginatedResponse(
        page=page,
        per_page=per_page,
        total=total,
        total_pages=total_pages,
        items=items,
    )
