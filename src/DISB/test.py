import os
from typing import  Optional, Tuple
import string  # For nickname generation
import random
import asyncio
def _generate_random_nickname(length=8) -> str:
    """Generates a random alphanumeric nickname."""
    characters = string.ascii_letters + string.digits
    return ''.join(random.choice(characters) for i in range(length))
async def _determine_root_name(local_path: str, custom_root_name: Optional[str] = None,
                                   user_mention: str = "") -> Tuple[str, str, bool]:
        """
        Returns: root_upload_name, original_root_name_for_db, is_nicknamed_flag_for_db
        """
        effective_root_name_from_path = os.path.basename(local_path)
        original_root_name_for_db = ""
        is_nicknamed_flag_for_db = False

        if custom_root_name:
            if len(custom_root_name) > 60:
                raise ValueError(
                    f"{user_mention}, Error: Your provided 'upload_name' ('{custom_root_name}') is too long. Please keep it 60 characters or less.")
            root_upload_name = custom_root_name
            if custom_root_name != effective_root_name_from_path:
                original_root_name_for_db = effective_root_name_from_path
                is_nicknamed_flag_for_db = True
            return root_upload_name, original_root_name_for_db, is_nicknamed_flag_for_db

        # Auto-nickname if >60 chars
        if len(effective_root_name_from_path) > 60:
            root_upload_name = _generate_random_nickname()
            original_root_name_for_db = effective_root_name_from_path
            is_nicknamed_flag_for_db = True
        else:
            root_upload_name = effective_root_name_from_path

        return root_upload_name, original_root_name_for_db, is_nicknamed_flag_for_db
print(asyncio.run(_determine_root_name(local_path = "/files/report.pdf")))