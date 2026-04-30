"""
Debug script to test the get_profiles_for_users function with edge cases
"""
import asyncio
from sqlalchemy.ext.asyncio import AsyncSession
from modules.utils.profile import get_profiles_for_users
from modules.base import get_userconfig_session

async def test_get_profiles():
    """Test get_profiles_for_users with different inputs"""
    async for userconfig_session in get_userconfig_session():
        # Test with empty list
        result1 = await get_profiles_for_users([], userconfig_session)
        print("Empty list:", result1)

        # Test with list containing None
        result2 = await get_profiles_for_users([None], userconfig_session)
        print("List with None:", result2)

        # Test with list containing empty string
        result3 = await get_profiles_for_users([""], userconfig_session)
        print("List with empty string:", result3)

        # Test with valid username
        result4 = await get_profiles_for_users(["validuser"], userconfig_session)
        print("Valid username:", result4)

        # Test with mixed list
        result5 = await get_profiles_for_users(["validuser", None, ""], userconfig_session)
        print("Mixed list:", result5)
        
        # We're done
        break

if __name__ == "__main__":
    asyncio.run(test_get_profiles())