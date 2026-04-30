from fastapi import APIRouter, Depends, Query, HTTPException
from typing import Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, or_, Column, Integer, String, DateTime
from pydantic import BaseModel
from datetime import datetime
import logging

from modules.base import (
    DEFAULT_PROFILE_PIC_URL,
    get_archival_session,
    get_userconfig_session,
    get_sv_subgraph_client,
)
from modules.share_history import ShareTradeHistory
from modules.utils.profile import get_profiles_for_users

from gql import gql
from gql.transport.exceptions import TransportQueryError

logger = logging.getLogger(__name__)

# For user endpoints, we use the table 'notification_endpoints' from userconfig DB.
from modules.base import Base

class NotificationEndpoint(Base):
    __tablename__ = "notification_endpoints"
    id = Column(Integer, primary_key=True, autoincrement=True)
    xayaname = Column(String(255), index=True)
    endpoint_type = Column(String(50))
    endpoint_identifier = Column(String(255))
    endpoint_display_name = Column(String(255))
    verification_code = Column(String(50))
    verified = Column(String(10))  # e.g. "1" indicates verified
    created_at = Column(DateTime)
    updated_at = Column(DateTime)
    last_notified_at = Column(DateTime)
    weekly_notif_sent = Column(DateTime)
    error = Column(String(50))
    count = Column(String(50))
    notif_enabled = Column(String(10))  # "1" means enabled

# Define the response model for achievements
class AchievementsResponse(BaseModel):
    username: str
    update_profile_picture: int
    refer_a_friend: int
    complete_a_trade: int
    buy_a_pack: int
    link_discord: int
    enable_social_notifications: int
    profile_pic: Optional[str] = None

    class Config:
        orm_mode = True

achievements_router = APIRouter()


async def check_subgraph_achievements(session, username: str) -> tuple[int, int]:
    """
    Check referral and pack purchase achievements from The Graph subgraph.
    
    Args:
        session: Active GraphQL session
        username: User's xayaname
        
    Returns:
        Tuple of (refer_a_friend, buy_a_pack) where each is 0 or 1
    """
    try:
        # Single query to check both referrals and pack purchases
        query = gql("""
            query CheckAchievements($username: String!) {
                referrers(where: { account: $username }, first: 1) {
                    id
                    currentTotal {
                        usdSpent
                    }
                }
                packsBoughts(where: { receiver: $username }, first: 1) {
                    id
                }
            }
        """)
        
        result = await session.execute(query, variable_values={"username": username})
        
        # Check referrals
        referrers = result.get("referrers", [])
        refer_a_friend = 0
        if referrers and referrers[0].get("currentTotal"):
            referrals_spend = int(referrers[0]["currentTotal"]["usdSpent"])
            refer_a_friend = 1 if referrals_spend > 0 else 0
        
        # Check pack purchases
        packs_bought = result.get("packsBoughts", [])
        buy_a_pack = 1 if len(packs_bought) > 0 else 0
        
        return refer_a_friend, buy_a_pack
        
    except TransportQueryError as e:
        logger.error(f"GraphQL query error checking achievements for {username}: {e}")
        return 0, 0
    except Exception as e:
        logger.error(f"Error checking subgraph achievements for {username}: {e}")
        return 0, 0


@achievements_router.get(
    "/achievements",
    response_model=AchievementsResponse,
    summary="Get user achievements status",
    description="Returns the status of achievements for a user in the 'Complete your profile' section."
)
async def get_achievements(
    username: str = Query(..., description="User's xayaname"),
    userconfig_session: AsyncSession = Depends(get_userconfig_session),
    archival_session: AsyncSession = Depends(get_archival_session)
):
    if userconfig_session is None:
        logger.warning(f"Userconfig database not configured - setting related achievements to 0 for {username}")
        profile_pic = None
        update_profile_picture = 0
        complete_a_trade = 0
        link_discord = 0
        enable_social_notifications = 0
    else:
        # Get the user's profile picture (using the existing helper)
        pics = await get_profiles_for_users([username], userconfig_session)
        profile_pic = pics.get(username, DEFAULT_PROFILE_PIC_URL)
        # Achievement: Update Profile Picture is unlocked if user has a non-default picture.
        update_profile_picture = 1 if profile_pic != DEFAULT_PROFILE_PIC_URL else 0

        # Achievement: Complete a Trade – check if user has been a buyer or seller in share_trade_history.
        trade_result = await archival_session.execute(
            select(ShareTradeHistory).where(
                or_(ShareTradeHistory.buyer == username, ShareTradeHistory.seller == username)
            )
        )
        trade = trade_result.scalar()
        complete_a_trade = 1 if trade is not None else 0

        # Achievement: Link Discord – check if user has a verified Discord endpoint in notification_endpoints.
        # Achievement: Enable Social Notifications – check if any endpoint has notif_enabled = "1".
        # The notification_endpoints table may not exist in all environments (e.g. test).
        link_discord = 0
        enable_social_notifications = 0
        try:
            notif_endpoint_result = await userconfig_session.execute(
                select(NotificationEndpoint).where(
                    NotificationEndpoint.xayaname == username,
                    NotificationEndpoint.endpoint_type == "discord",
                    NotificationEndpoint.verified == "1"
                )
            )
            notif_endpoint = notif_endpoint_result.scalar()
            link_discord = 1 if notif_endpoint is not None else 0

            social_notif_result = await userconfig_session.execute(
                select(NotificationEndpoint).where(
                    NotificationEndpoint.xayaname == username,
                    NotificationEndpoint.endpoint_type.in_(["telegram", "discord", "whatsapp"]),
                    NotificationEndpoint.notif_enabled == "1"
                )
            )
            social_notif = social_notif_result.scalar()
            enable_social_notifications = 1 if social_notif is not None else 0
        except Exception:
            logger.warning(f"notification_endpoints table not available - setting discord/social achievements to 0 for {username}")

    # Query subgraph for referrals and pack purchases
    subgraph_client = get_sv_subgraph_client()
    if subgraph_client is None:
        logger.warning(f"SV Subgraph not configured - setting referral and pack achievements to 0 for {username}")
        refer_a_friend = 0
        buy_a_pack = 0
    else:
        async with subgraph_client as session:
            refer_a_friend, buy_a_pack = await check_subgraph_achievements(session, username)

    return AchievementsResponse(
        username=username,
        update_profile_picture=update_profile_picture,
        refer_a_friend=refer_a_friend,
        complete_a_trade=complete_a_trade,
        buy_a_pack=buy_a_pack,
        link_discord=link_discord,
        enable_social_notifications=enable_social_notifications,
        profile_pic=profile_pic
    )
