import random
import sys
import time
import functools
import aiohttp
import discord
from discord.ext import commands
from discord import app_commands
import asyncio
import datetime
import os
import io
import traceback
import logging
from typing import List, Dict, Optional, Union, cast, Tuple, Any
import re  # Added for robust part number extraction in download logic
import string  # For nickname generation
# Encryption imports
from cryptography.fernet import Fernet, InvalidToken  # Moved InvalidToken here
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.backends import default_backend
import base64  # For encoding/decoding Fernet keys and hashes to store as strings
import hashlib  # For hashing user-provided seeds
import shutil  # For removing directories in cleanup
from database import DatabaseManager
from versioning import VersioningManager
@dataclass
class DownloadContext:
    interaction: discord.Interaction
    user_id: int
    user_mention: str
    target_path: str
    normalized_target_path: str
    download_folder: str
    base_download_dir: str

    overall_parts_downloaded: int = 0
    overall_total_parts: int = 0

    local_cleanup_path: Optional[str] = None
    multiple_versions: bool = False
    should_create_target_folder: bool = False
    download_successful: bool = True
