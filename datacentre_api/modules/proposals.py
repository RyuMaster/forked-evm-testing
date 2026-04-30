import time
import logging
import json
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any, TypeVar

from sqlalchemy import Column, BigInteger, String, Integer, Boolean, Text, select, func, desc, and_, or_
from sqlalchemy.ext.asyncio import AsyncSession
from pydantic import BaseModel
from fastapi import APIRouter, Depends, Query, HTTPException

from .base import Base, get_archival_session, Blocks, apply_pagination, PaginatedResponse

# Configure logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# SQLAlchemy model for proposal_updates table
class ProposalUpdates(Base):
    __tablename__ = 'proposal_updates'
    id = Column(BigInteger, primary_key=True)
    height = Column(BigInteger, index=True)
    proposal_id = Column(BigInteger, index=True)
    share_type = Column(String(6), index=True)
    share_id = Column(BigInteger, index=True)
    start_time = Column(BigInteger)
    end_time = Column(BigInteger)
    type = Column(String(64))
    proposal_data = Column(Text)
    option_id = Column(Integer)
    option_data = Column(Text)
    votes = Column(BigInteger)
    winner = Column(Boolean)

# Pydantic models for responses
class ProposalVoteOption(BaseModel):
    option_id: int
    option_data: Optional[Dict[str, Any]] = None
    votes: int
    is_winner: bool

class ProposalResponse(BaseModel):
    proposal_id: int
    share_type: str  # 'player' or 'club'
    share_id: int
    name: str  # For now, will be the ID as string
    proposal_type: str  # 'agent', 'manager', or 'unlock'
    stage: str  # 'proposal', 'voting', or 'completed'
    manager_name: Optional[str] = None  # For unlock proposals
    start_time: datetime
    end_time: datetime
    end_time_unix: int  # Unix timestamp for end_time
    time_remaining: Optional[str] = None
    vote_options: List[ProposalVoteOption] = []
    total_votes: int = 0


# Create router
proposals_router = APIRouter()

def calculate_time_remaining(end_timestamp: int) -> str:
    """Calculate time remaining for a proposal."""
    now = int(time.time())
    if end_timestamp <= now:
        return "Ended"
    
    remaining = end_timestamp - now
    days = remaining // 86400
    hours = (remaining % 86400) // 3600
    minutes = (remaining % 3600) // 60
    
    if days > 0:
        return f"{days}d {hours}h"
    elif hours > 0:
        return f"{hours}h {minutes}m"
    else:
        return f"{minutes}m"

async def get_min_height_for_active_proposals(session: AsyncSession) -> Optional[int]:
    """Get the minimum height for active proposals.

    This is used to optimize queries by filtering rows with height >= min_height,
    which dramatically reduces the number of rows scanned (98.7% reduction).

    Returns None if there are no active proposals.
    """
    current_time = int(time.time())
    result = await session.execute(
        select(func.min(ProposalUpdates.height))
        .where(ProposalUpdates.end_time > current_time)
    )
    return result.scalar_one()

def determine_stage(proposal_type: str, start_timestamp: int, end_timestamp: int, has_votes: bool, has_multiple_candidates: bool) -> str:
    """Determine the stage of a proposal.

    Timeline:
    - Unlock: 3 days voting only
    - Agent/Manager: The stored times appear to be for the voting phase only (3 days)
      The proposal collection happens before the stored start_time
    """
    now = int(time.time())
    if end_timestamp <= now:
        return "completed"

    # Unlock proposals are always in voting stage (yes/no vote)
    if proposal_type == "unlock":
        return "voting"

    # For agent/manager proposals:
    # Since the database shows 72-hour durations, it seems the start_time and end_time
    # represent the voting period only. The proposal collection happened before start_time.
    # So if we're within the start_time to end_time window, we're in voting.
    if start_timestamp and now >= start_timestamp:
        return "voting"
    elif start_timestamp and now < start_timestamp:
        return "proposal"

    # Fallback: if we have votes, it's voting; otherwise proposal
    if has_votes:
        return "voting"
    else:
        return "proposal"

@proposals_router.get(
    "/proposals/active",
    response_model=PaginatedResponse[ProposalResponse],
    summary="Get currently active proposals",
    description="Returns all currently active proposals/votes that haven't ended yet. Sorted by end time (ending soonest first)."
)
async def get_active_proposals(
    page: int = Query(1, ge=1, description="Page number, starting from 1"),
    per_page: int = Query(100, ge=1, le=5000, description="Number of proposals per page (default 100, max 5000)"),
    proposal_type: Optional[str] = Query(None, regex="^(agent|manager|unlock)$", description="Filter by proposal type"),
    share_type: Optional[str] = Query(None, regex="^(player|club)$", description="Filter by share type"),
    stage_filter: Optional[str] = Query(None, regex="^(proposal|voting)$", description="Filter by current stage"),
    club_id: Optional[int] = Query(None, description="Filter by specific club ID"),
    player_id: Optional[int] = Query(None, description="Filter by specific player ID"),
    name: Optional[str] = Query(None, description="Filter by name (searches in manager/agent names)"),
    session: AsyncSession = Depends(get_archival_session),
):
    """Get currently active proposals that haven't ended yet."""
    start_time = time.perf_counter()

    try:
        current_time = int(time.time())

        # OPTIMIZATION: Get minimum height for active proposals to reduce rows scanned by 98.7%
        min_height = await get_min_height_for_active_proposals(session)
        if min_height is None:
            # No active proposals
            return PaginatedResponse(
                page=page,
                per_page=per_page,
                total=0,
                total_pages=0,
                items=[]
            )

        # Build base query with height optimization
        proposals_query = (
            select(
                ProposalUpdates.proposal_id,
                ProposalUpdates.share_type,
                ProposalUpdates.share_id,
                ProposalUpdates.type,
                ProposalUpdates.start_time,
                ProposalUpdates.end_time,
                ProposalUpdates.proposal_data,
                func.max(ProposalUpdates.height).label('latest_height')
            )
            .where(
                and_(
                    ProposalUpdates.height >= min_height,  # Height optimization
                    ProposalUpdates.end_time.isnot(None),
                    ProposalUpdates.type.isnot(None),
                    ProposalUpdates.end_time > current_time
                )
            )
            .group_by(
                ProposalUpdates.proposal_id,
                ProposalUpdates.share_type,
                ProposalUpdates.share_id,
                ProposalUpdates.type,
                ProposalUpdates.start_time,
                ProposalUpdates.end_time,
                ProposalUpdates.proposal_data
            )
        )

        # Apply optional filters
        if proposal_type:
            proposals_query = proposals_query.where(ProposalUpdates.type == proposal_type)
        if share_type:
            proposals_query = proposals_query.where(ProposalUpdates.share_type == share_type)
        if club_id:
            proposals_query = proposals_query.where(
                and_(ProposalUpdates.share_type == 'club', ProposalUpdates.share_id == club_id)
            )
        if player_id:
            proposals_query = proposals_query.where(
                and_(ProposalUpdates.share_type == 'player', ProposalUpdates.share_id == player_id)
            )

        # Filter by stage if requested
        if stage_filter == "proposal":
            proposals_query = proposals_query.where(ProposalUpdates.start_time > current_time)
        elif stage_filter == "voting":
            proposals_query = proposals_query.where(
                (ProposalUpdates.start_time <= current_time) |
                (ProposalUpdates.type == 'unlock')
            )

        # Name filter with height optimization
        if name:
            matching_proposals = (
                select(ProposalUpdates.proposal_id)
                .where(
                    and_(
                        ProposalUpdates.height >= min_height,  # Height optimization
                        or_(
                            ProposalUpdates.option_data.like(f'%"{name}"%'),
                            ProposalUpdates.proposal_data.like(f'%"{name}"%')
                        )
                    )
                )
                .distinct()
            )
            proposals_query = proposals_query.where(
                ProposalUpdates.proposal_id.in_(matching_proposals)
            )
        
        # Order by end_time ascending (soonest ending first)
        proposals_query = proposals_query.order_by(ProposalUpdates.end_time.asc())
        
        # Get total count before pagination
        count_query = select(func.count()).select_from(proposals_query.subquery())
        total_result = await session.execute(count_query)
        total = total_result.scalar_one()
        total_pages = (total + per_page - 1) // per_page if total else 0
        
        # Apply pagination
        proposals_query = apply_pagination(proposals_query, page, per_page)
        
        result = await session.execute(proposals_query)
        proposals = result.fetchall()
        
        # Get voting data for each proposal
        proposal_responses = []
        proposal_ids = [p.proposal_id for p in proposals]

        # Fetch ALL records for proposals with height optimization
        records_by_proposal = {}
        if proposal_ids:
            all_records_query = (
                select(ProposalUpdates)
                .where(
                    and_(
                        ProposalUpdates.height >= min_height,  # Height optimization
                        ProposalUpdates.proposal_id.in_(proposal_ids)
                    )
                )
            )

            all_records_result = await session.execute(all_records_query)
            all_records = all_records_result.scalars().all()

            # Group records by proposal_id
            for record in all_records:
                if record.proposal_id not in records_by_proposal:
                    records_by_proposal[record.proposal_id] = []
                records_by_proposal[record.proposal_id].append(record)
        
        # Build response objects
        for prop in proposals:
            # Parse proposal data
            proposal_data = json.loads(prop.proposal_data) if prop.proposal_data else {}
            manager_name = proposal_data.get('manager') if prop.type == 'unlock' else None
            
            # Get voting options for this proposal
            vote_options = []
            total_votes = 0
            has_votes = False
            has_multiple_candidates = False
            
            if prop.proposal_id in records_by_proposal:
                candidates_count = 0
                # Group records by option_id to merge data
                options_data = {}

                for record in records_by_proposal[prop.proposal_id]:
                    # Skip main record (no option_id)
                    if record.option_id is None:
                        continue

                    # Initialize or update option data
                    if record.option_id not in options_data:
                        options_data[record.option_id] = {
                            'option_data': {},
                            'votes': 0,
                            'is_winner': False
                        }

                    # Merge option_data if present
                    if record.option_data:
                        try:
                            parsed_data = json.loads(record.option_data)
                            if parsed_data:
                                options_data[record.option_id]['option_data'].update(parsed_data)
                        except:
                            pass

                    # Update votes (take the maximum in case of multiple records)
                    if record.votes is not None:
                        options_data[record.option_id]['votes'] = max(
                            options_data[record.option_id]['votes'],
                            record.votes
                        )

                    # Update winner status
                    if record.winner:
                        options_data[record.option_id]['is_winner'] = True

                # Build vote options from merged data
                for option_id, data in sorted(options_data.items()):
                    option_data = data['option_data']

                    # Count candidates
                    if prop.type in ('agent', 'manager') and option_data:
                        if (prop.type == 'agent' and option_data.get('agent')) or \
                           (prop.type == 'manager' and option_data.get('manager')):
                            candidates_count += 1

                    vote_options.append(ProposalVoteOption(
                        option_id=option_id,
                        option_data=option_data if option_data else None,
                        votes=data['votes'],
                        is_winner=data['is_winner']
                    ))

                    if data['votes'] > 0:
                        total_votes += data['votes']
                        has_votes = True

                has_multiple_candidates = candidates_count > 1
            
            
            # Determine stage
            stage = determine_stage(prop.type, prop.start_time, prop.end_time, has_votes, has_multiple_candidates)
            
            # Create response object
            proposal_responses.append(ProposalResponse(
                proposal_id=prop.proposal_id,
                share_type=prop.share_type,
                share_id=prop.share_id,
                name=f"{prop.share_type}_{prop.share_id}",
                proposal_type=prop.type,
                stage=stage,
                manager_name=manager_name,
                start_time=datetime.utcfromtimestamp(prop.start_time),
                end_time=datetime.utcfromtimestamp(prop.end_time),
                end_time_unix=prop.end_time,
                time_remaining=calculate_time_remaining(prop.end_time),
                vote_options=vote_options,
                total_votes=total_votes
            ))
        
        logger.info(f"Proposals fetch time: {time.perf_counter() - start_time:.4f}s")
        
        return PaginatedResponse(
            page=page,
            per_page=per_page,
            total=total,
            total_pages=total_pages,
            items=proposal_responses
        )
        
    except Exception as e:
        logger.error(f"Error fetching proposals: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch proposals")

# Pydantic models for historical endpoint
class DailyProposalStats(BaseModel):
    date: str  # YYYY-MM-DD format
    total_ended: int
    by_type: Dict[str, int]  # agent, manager, unlock counts
    by_share_type: Dict[str, int]  # player, club counts
    proposals: List[ProposalResponse]

class HistoricalProposalsResponse(BaseModel):
    days_back: int
    total_ended: int
    by_type: Dict[str, int]
    by_share_type: Dict[str, int]
    daily_stats: List[DailyProposalStats]
    all_proposals: List[ProposalResponse]

@proposals_router.get(
    "/proposals/historical",
    response_model=HistoricalProposalsResponse,
    summary="Get proposals that ended in the last N days with daily statistics",
    description="Returns proposals that ended in the last N days, grouped by day with statistics on types and counts."
)
async def get_historical_proposals(
    days_back: int = Query(7, ge=1, le=30, description="Number of days back to look for ended proposals (default 7)"),
    proposal_type: Optional[str] = Query(None, regex="^(agent|manager|unlock)$", description="Filter by proposal type"),
    share_type: Optional[str] = Query(None, regex="^(player|club)$", description="Filter by share type"),
    club_id: Optional[int] = Query(None, description="Filter by specific club ID"),
    player_id: Optional[int] = Query(None, description="Filter by specific player ID"),
    name: Optional[str] = Query(None, description="Filter by name (searches in manager/agent names)"),
    session: AsyncSession = Depends(get_archival_session),
):
    """Get historical proposals with daily statistics."""
    start_time = time.perf_counter()

    try:
        current_time = int(time.time())
        days_ago = current_time - (days_back * 86400)

        # OPTIMIZATION: Get minimum height for proposals in the time range
        min_height_result = await session.execute(
            select(func.min(ProposalUpdates.height))
            .where(ProposalUpdates.start_time >= days_ago)
        )
        min_height = min_height_result.scalar_one()

        if min_height is None:
            # No proposals in this time range
            return HistoricalProposalsResponse(
                days_back=days_back,
                total_ended=0,
                by_type={"agent": 0, "manager": 0, "unlock": 0},
                by_share_type={"player": 0, "club": 0},
                daily_stats=[],
                all_proposals=[]
            )

        # Build query with height optimization
        proposals_query = (
            select(
                ProposalUpdates.proposal_id,
                ProposalUpdates.share_type,
                ProposalUpdates.share_id,
                ProposalUpdates.type,
                ProposalUpdates.start_time,
                ProposalUpdates.end_time,
                ProposalUpdates.proposal_data,
                func.max(ProposalUpdates.height).label('latest_height')
            )
            .where(
                and_(
                    ProposalUpdates.height >= min_height,  # Height optimization
                    ProposalUpdates.end_time.isnot(None),
                    ProposalUpdates.type.isnot(None),
                    ProposalUpdates.end_time >= days_ago,
                    ProposalUpdates.end_time <= current_time
                )
            )
            .group_by(
                ProposalUpdates.proposal_id,
                ProposalUpdates.share_type,
                ProposalUpdates.share_id,
                ProposalUpdates.type,
                ProposalUpdates.start_time,
                ProposalUpdates.end_time,
                ProposalUpdates.proposal_data
            )
        )

        # Apply optional filters
        if proposal_type:
            proposals_query = proposals_query.where(ProposalUpdates.type == proposal_type)
        if share_type:
            proposals_query = proposals_query.where(ProposalUpdates.share_type == share_type)
        if club_id:
            proposals_query = proposals_query.where(
                and_(ProposalUpdates.share_type == 'club', ProposalUpdates.share_id == club_id)
            )
        if player_id:
            proposals_query = proposals_query.where(
                and_(ProposalUpdates.share_type == 'player', ProposalUpdates.share_id == player_id)
            )

        # Name filter with height optimization
        if name:
            matching_proposals = (
                select(ProposalUpdates.proposal_id)
                .where(
                    and_(
                        ProposalUpdates.height >= min_height,  # Height optimization
                        or_(
                            ProposalUpdates.option_data.like(f'%"{name}"%'),
                            ProposalUpdates.proposal_data.like(f'%"{name}"%')
                        )
                    )
                )
                .distinct()
            )
            proposals_query = proposals_query.where(
                ProposalUpdates.proposal_id.in_(matching_proposals)
            )
        
        # Order by end_time descending (most recent first)
        proposals_query = proposals_query.order_by(ProposalUpdates.end_time.desc())
        
        result = await session.execute(proposals_query)
        proposals = result.fetchall()
        
        # Get voting data for each proposal
        proposal_responses = []
        proposal_ids = [p.proposal_id for p in proposals]

        # Fetch ALL records for proposals with height optimization
        records_by_proposal = {}
        if proposal_ids:
            all_records_query = (
                select(ProposalUpdates)
                .where(
                    and_(
                        ProposalUpdates.height >= min_height,  # Height optimization
                        ProposalUpdates.proposal_id.in_(proposal_ids)
                    )
                )
            )

            all_records_result = await session.execute(all_records_query)
            all_records = all_records_result.scalars().all()

            # Group records by proposal_id
            for record in all_records:
                if record.proposal_id not in records_by_proposal:
                    records_by_proposal[record.proposal_id] = []
                records_by_proposal[record.proposal_id].append(record)
        
        # Process proposals and organize by day
        daily_data = {}  # date -> proposals list
        total_by_type = {"agent": 0, "manager": 0, "unlock": 0}
        total_by_share_type = {"player": 0, "club": 0}
        
        for prop in proposals:
            # Parse proposal data
            proposal_data = json.loads(prop.proposal_data) if prop.proposal_data else {}
            manager_name = proposal_data.get('manager') if prop.type == 'unlock' else None
            
            # Get voting options for this proposal
            vote_options = []
            total_votes = 0
            
            if prop.proposal_id in records_by_proposal:
                # Group records by option_id to merge data
                options_data = {}

                for record in records_by_proposal[prop.proposal_id]:
                    # Skip main record (no option_id)
                    if record.option_id is None:
                        continue

                    # Initialize or update option data
                    if record.option_id not in options_data:
                        options_data[record.option_id] = {
                            'option_data': {},
                            'votes': 0,
                            'is_winner': False
                        }

                    # Merge option_data if present
                    if record.option_data:
                        try:
                            parsed_data = json.loads(record.option_data)
                            if parsed_data:
                                options_data[record.option_id]['option_data'].update(parsed_data)
                        except:
                            pass

                    # Update votes (take the maximum in case of multiple records)
                    if record.votes is not None:
                        options_data[record.option_id]['votes'] = max(
                            options_data[record.option_id]['votes'],
                            record.votes
                        )

                    # Update winner status
                    if record.winner:
                        options_data[record.option_id]['is_winner'] = True

                # Build vote options from merged data
                for option_id, data in sorted(options_data.items()):
                    vote_options.append(ProposalVoteOption(
                        option_id=option_id,
                        option_data=data['option_data'] if data['option_data'] else None,
                        votes=data['votes'],
                        is_winner=data['is_winner']
                    ))

                    if data['votes'] > 0:
                        total_votes += data['votes']
            
            
            # Create response object
            proposal_response = ProposalResponse(
                proposal_id=prop.proposal_id,
                share_type=prop.share_type,
                share_id=prop.share_id,
                name=f"{prop.share_type}_{prop.share_id}",
                proposal_type=prop.type,
                stage="completed",  # All are completed in historical view
                manager_name=manager_name,
                start_time=datetime.utcfromtimestamp(prop.start_time),
                end_time=datetime.utcfromtimestamp(prop.end_time),
                end_time_unix=prop.end_time,
                time_remaining="Ended",
                vote_options=vote_options,
                total_votes=total_votes
            )
            proposal_responses.append(proposal_response)
            
            # Group by day
            end_date = datetime.utcfromtimestamp(prop.end_time).strftime('%Y-%m-%d')
            if end_date not in daily_data:
                daily_data[end_date] = {
                    "proposals": [],
                    "by_type": {"agent": 0, "manager": 0, "unlock": 0},
                    "by_share_type": {"player": 0, "club": 0}
                }
            
            daily_data[end_date]["proposals"].append(proposal_response)
            if prop.type in daily_data[end_date]["by_type"]:
                daily_data[end_date]["by_type"][prop.type] += 1
                total_by_type[prop.type] += 1
            if prop.share_type in daily_data[end_date]["by_share_type"]:
                daily_data[end_date]["by_share_type"][prop.share_type] += 1
                total_by_share_type[prop.share_type] += 1
        
        # Create daily stats
        daily_stats = []
        for date in sorted(daily_data.keys(), reverse=True):  # Most recent first
            daily_stats.append(DailyProposalStats(
                date=date,
                total_ended=len(daily_data[date]["proposals"]),
                by_type=daily_data[date]["by_type"],
                by_share_type=daily_data[date]["by_share_type"],
                proposals=daily_data[date]["proposals"]
            ))
        
        logger.info(f"Historical proposals fetch time: {time.perf_counter() - start_time:.4f}s")
        
        return HistoricalProposalsResponse(
            days_back=days_back,
            total_ended=len(proposal_responses),
            by_type=total_by_type,
            by_share_type=total_by_share_type,
            daily_stats=daily_stats,
            all_proposals=proposal_responses
        )
        
    except Exception as e:
        logger.error(f"Error fetching historical proposals: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch historical proposals")

# Pydantic model for graph endpoint
class ProposalChartData(BaseModel):
    date: str
    unlock: int
    agent: int
    manager: int
    total: int

class ProposalGraphResponse(BaseModel):
    days_back: int
    start_date: str
    end_date: str
    total_completed: int
    by_type_total: Dict[str, int]
    chart_data: List[ProposalChartData]

@proposals_router.get(
    "/proposals/graph",
    response_model=ProposalGraphResponse,
    summary="Get daily proposal completion counts for graphing",
    description="Returns daily counts of completed proposals by type for the last N days in chart format."
)
async def get_proposals_graph(
    days_back: int = Query(30, ge=1, le=90, description="Number of days back to look for completed proposals (default 30)"),
    session: AsyncSession = Depends(get_archival_session),
):
    """Get daily proposal completion counts for graphing."""
    start_time = time.perf_counter()

    try:
        current_time = int(time.time())
        days_ago = current_time - (days_back * 86400)

        # OPTIMIZATION: Get minimum height for proposals in the time range
        min_height_result = await session.execute(
            select(func.min(ProposalUpdates.height))
            .where(ProposalUpdates.start_time >= days_ago)
        )
        min_height = min_height_result.scalar_one()

        if min_height is None:
            # No proposals in this time range
            start_date = datetime.utcfromtimestamp(days_ago)
            end_date = datetime.utcfromtimestamp(current_time)
            chart_data = []
            current_date = start_date
            while current_date <= end_date:
                chart_data.append(ProposalChartData(
                    date=current_date.strftime('%Y-%m-%d'),
                    unlock=0,
                    agent=0,
                    manager=0,
                    total=0
                ))
                current_date += timedelta(days=1)

            return ProposalGraphResponse(
                days_back=days_back,
                start_date=start_date.strftime('%Y-%m-%d'),
                end_date=end_date.strftime('%Y-%m-%d'),
                total_completed=0,
                by_type_total={"unlock": 0, "agent": 0, "manager": 0},
                chart_data=chart_data
            )

        # Build query with height optimization
        proposals_query = (
            select(
                ProposalUpdates.type,
                ProposalUpdates.end_time,
                func.count(ProposalUpdates.proposal_id.distinct()).label('count')
            )
            .where(
                and_(
                    ProposalUpdates.height >= min_height,  # Height optimization
                    ProposalUpdates.end_time.isnot(None),
                    ProposalUpdates.type.isnot(None),
                    ProposalUpdates.end_time >= days_ago,
                    ProposalUpdates.end_time <= current_time
                )
            )
            .group_by(
                ProposalUpdates.type,
                ProposalUpdates.end_time
            )
            .order_by(ProposalUpdates.end_time)
        )

        result = await session.execute(proposals_query)
        proposals = result.fetchall()
        
        # Organize data by date and type
        daily_counts = {}
        total_by_type = {"unlock": 0, "agent": 0, "manager": 0}
        
        for prop in proposals:
            # Convert timestamp to date string
            date_str = datetime.utcfromtimestamp(prop.end_time).strftime('%Y-%m-%d')
            
            if date_str not in daily_counts:
                daily_counts[date_str] = {"unlock": 0, "agent": 0, "manager": 0, "total": 0}
            
            if prop.type in daily_counts[date_str]:
                daily_counts[date_str][prop.type] += prop.count
                daily_counts[date_str]["total"] += prop.count
                total_by_type[prop.type] += prop.count
        
        # Generate chart data for all days in range (including days with 0 votes)
        chart_data = []
        
        # Create a list of all dates in the range
        start_date = datetime.utcfromtimestamp(days_ago)
        end_date = datetime.utcfromtimestamp(current_time)
        current_date = start_date
        
        while current_date <= end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            
            # Get counts for this date (default to 0 if no data)
            if date_str in daily_counts:
                unlock_count = daily_counts[date_str]["unlock"]
                agent_count = daily_counts[date_str]["agent"]
                manager_count = daily_counts[date_str]["manager"]
                total_count = daily_counts[date_str]["total"]
            else:
                unlock_count = agent_count = manager_count = total_count = 0
            
            # Add combined data point
            chart_data.append(ProposalChartData(
                date=date_str,
                unlock=unlock_count,
                agent=agent_count,
                manager=manager_count,
                total=total_count
            ))
            
            # Move to next day
            current_date += timedelta(days=1)
        
        logger.info(f"Proposals graph fetch time: {time.perf_counter() - start_time:.4f}s")
        
        return ProposalGraphResponse(
            days_back=days_back,
            start_date=start_date.strftime('%Y-%m-%d'),
            end_date=end_date.strftime('%Y-%m-%d'),
            total_completed=sum(total_by_type.values()),
            by_type_total=total_by_type,
            chart_data=chart_data
        )
        
    except Exception as e:
        logger.error(f"Error fetching proposals graph: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch proposals graph")