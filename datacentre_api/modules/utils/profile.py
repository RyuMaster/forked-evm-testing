from typing import List, Dict, Optional, Union
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from modules.base import DCUsernames, DEFAULT_PROFILE_PIC_URL

async def get_profiles_for_users(
    xayanames: List[str], session: Optional[AsyncSession]
) -> Dict[str, Optional[str]]:
    """
    Returns a dictionary mapping xayaname -> the correct profile pic URL or None.
    
    This function handles both string and bytes usernames to ensure compatibility
    across different environments. The logic for profile pictures is:
    - If session is None (userconfig DB not available) => "error: userconfig not available"
    - If no row found => DEFAULT_PROFILE_PIC_URL
    - If row exists but profile_option == 0 => DEFAULT_PROFILE_PIC_URL
    - If row exists but prof_dev_disabled == 1 => DEFAULT_PROFILE_PIC_URL
    - Otherwise => row.profile_pic or DEFAULT_PROFILE_PIC_URL
    """

    if not xayanames:
        return {}
    
    # Handle case where userconfig database is not available
    if session is None:
        return {name: "error: userconfig not available" for name in xayanames}
    
    # Handle byte strings - convert to str if needed
    str_names = []
    name_mapping = {}
    for name in xayanames:
        if isinstance(name, bytes):
            # If it's bytes, decode to str
            str_name = name.decode('utf-8')
            str_names.append(str_name)
            name_mapping[str_name] = name
        else:
            # Already a string
            str_names.append(name)
            name_mapping[name] = name

    # Use ORM approach for simplicity
    try:
        # Query the usernames table for all matching xayanames
        query = select(DCUsernames).where(DCUsernames.xayaname.in_(str_names))
        result = await session.execute(query)
        records = result.scalars().all()
        
        # Build dictionary from results
        output = {}
        row_lookup = {r.xayaname: r for r in records}
        
        for name in str_names:
            row = row_lookup.get(name)
            
            if not row:
                # No row found => fallback to default
                output[name] = DEFAULT_PROFILE_PIC_URL
                continue
            
            disabled = row.prof_dev_disabled == 1 or row.profile_option == 0
            if disabled:
                output[name] = DEFAULT_PROFILE_PIC_URL
            else:
                output[name] = row.profile_pic or DEFAULT_PROFILE_PIC_URL
    
    except Exception as e:
        # Log the error for debugging
        import logging
        logger = logging.getLogger(__name__)
        logger.error(f"Error fetching profiles: {e}")
        # Fall back to default pics for all names
        output = {name: DEFAULT_PROFILE_PIC_URL for name in str_names}
    
    # Convert keys in output to match input format (bytes or str)
    final_output = {}
    for str_name, pic_url in output.items():
        original_name = name_mapping.get(str_name)
        if original_name is not None:
            final_output[original_name] = pic_url
    
    # Add defaults for any missing names
    for name in xayanames:
        if name not in final_output:
            final_output[name] = DEFAULT_PROFILE_PIC_URL
    
    return final_output
