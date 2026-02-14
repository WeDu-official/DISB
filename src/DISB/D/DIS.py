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
# Define intents globally as it's used when initializing the bot
intents = discord.Intents.default()
intents.message_content = True
intents.members = True  # Ensure this intent is enabled for fetching members if needed
log = logging.getLogger('DISB_Bot')
log.setLevel(logging.INFO)  # Set to INFO, WARNING, or DEBUG depending on verbosity
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s')
handler.setFormatter(formatter)
log.addHandler(handler)
# 2. Configure discord.py's internal logger
#    This is the crucial part for suppressing the verbose tracebacks
discord_logger = logging.getLogger('discord')
discord_logger.setLevel(logging.WARNING)  # Only show WARNING, ERROR, CRITICAL messages
# Change to logging.ERROR if WARNING is still too verbose
discord_logger.addHandler(handler)  # Use the same handler to output to console
# 3. Configure aiohttp's logger (discord.py uses aiohttp for network requests)
aiohttp_logger = logging.getLogger('aiohttp')
aiohttp_logger.setLevel(logging.WARNING)  # Same as discord_logger, to reduce network noise
aiohttp_logger.addHandler(handler)


class FileBotAPI(commands.Bot):
    def __init__(self, *, intents: discord.Intents):
        # noinspection PyTypeChecker
        super().__init__(command_prefix=self.get_prefix, intents=intents)
        self.upload_semaphore = asyncio.Semaphore(3)
        self._upload_semaphore_initial_capacity = 3
        self.download_queue = asyncio.Queue()
        self.download_semaphore = asyncio.Semaphore(3)
        self.delete_task_queue = asyncio.Queue()
        self.deletion_task = None
        self.user_uploading: Dict[int, List[str]] = {}  # Track root_upload_names per user
        self.user_downloading: Dict[int, str] = {}  # Track root_upload_names per user
        self.http_session = None
        self.log_prefix = "[FileBotAPI]"
        self.total_parts_cache: Dict[
            str, int] = {}  # Stores total parts for a given lnaocal file path (e.g., 'path/to/file.txt')
        self._db_table_init_status: Dict[str, bool] = {}
        self.discord_api_delay = 0.05
        self.batch_size_discord_checks = 50
        self.batch_delay_discord_checks = 2.0
        self.log = logging.getLogger('DISB_Bot')
        # Define PowerDB table schema for 'files' table (Table ID 0)
        # Column 0: fileid (unique ID for each part, per row)
        # Column 1: base_filename (e.g., 'image.jpg', 'document.pdf', or "_DIR_" for a folder)
        # Column 2: part_number (e.g., 1, 2, 3, or "0" for a folder)
        # Column 3: total_parts (e.g., 5, or "0" for a folder)
        # Column 4: message_id (Discord message ID, or "0" for a folder)
        # Column 5: channel_id (Discord channel ID, or "0" for a folder)
        # Column 6: relative_path_in_archive (e.g., 'subfolder/images', '.' for root, or the folder path itself for folder entry)
        # Column 7: root_upload_name (e.g., 'MyProjectFolder', 'single_file.txt')
        # Column 8: upload_timestamp (Timestamp of upload)
        # NEW for Nicknaming (Idea 1 - Root level):
        # Column 9: original_root_name (Original name if root_upload_name was nicknamed, empty otherwise)
        # Column 10: is_nicknamed (String "True" or "False", indicates if root_upload_name is a generated nickname)
        # NEW for Item-level Nicknaming (Sub-file/folder names):
        # Column 11: original_base_filename (Original name if base_filename was nicknamed (for files) or if folder name was long (for _DIR_ entries), empty otherwise)
        # Column 12: is_base_filename_nicknamed (String "True" or "False", indicates if base_filename or folder name was processed for length)
        # NEW for Encryption (Idea 2):
        # Column 13: encryption_mode (String "off", "automatic", "not_automatic")
        # Column 14: encryption_key_auto (Base64 string of Fernet key for automatic mode, empty otherwise)
        # Column 15: password_seed_hash (SHA256 hash of user's seed for not_automatic mode, empty otherwise)
        # NEW for Zero-Knowledge Encryption (save_hash option)
        # Column 16: store_hash_flag (String "True" or "False", indicates if password_seed_hash is stored)
        self.file_table_columns = [
            'fileid', 'base_filename', 'part_number', 'total_parts',
            'message_id', 'channel_id', 'relative_path_in_archive', 'root_upload_name', 'upload_timestamp',
            'original_root_name', 'is_nicknamed',
            'original_base_filename', 'is_base_filename_nicknamed',
            'encryption_mode', 'encryption_key_auto', 'password_seed_hash',
            'store_hash_flag',
            'version'  # Added new column for versioning
        ]
        self.db = DatabaseManager(file_table_columns=self.file_table_columns, log=self.log)
        self.version_manager = VersioningManager(db_read_func=self.db._db_read_sync, log=self.log)
        # Fixed salt and info for HKDF (should be constant and unique to your bot's encryption)
        self._HKDF_SALT = b"my_bot_encryption_salt_12345"
        self._HKDF_INFO = b"discord_file_bot_encryption_info"

    async def get_prefix(self, message: discord.Message) -> List[str]:
        return ["/"]

    async def setup_hook(self):
        self.http_session = aiohttp.ClientSession()
        try:
            synced = await self.tree.sync()
            self.log.info("Synced {len(synced)} slash commands for {self.user}")
        except discord.DiscordException as e:
            self.log.error(f"Error syncing slash commands: {e}")

    async def on_ready(self):
        self.log.info(f'Logged in as {self.user.name} (ID: {self.user.id})')
        # Clear any lingering upload/download states from previous sessions
        # This handles cases where the bot might have crashed mid-operation.
        # While semaphores are more robust for busy checks, clearing these on startup is still good practice.
        self.user_uploading.clear()
        self.user_downloading.clear()
        self.log.info("Cleared user_uploading and user_downloading states on startup.")

    async def on_message(self, message: discord.Message):
        if message.author == self.user:
            return
        # This will process commands defined with @bot.command() (not slash commands)
        # For slash commands, interaction processing is handled automatically by discord.py
        await self.process_commands(message)

    async def send_message_robustly(self, channel_id: int, content: Optional[str] = None,
                                    file: Optional[discord.File] = None, ephemeral: bool = False) -> Optional[
        discord.Message]:
        """
        Sends a message to a channel, handling common Discord API errors robustly.
        Retries on rate limits and attempts to fetch channel if not in cache.
        Uses enhanced retry logic with exponential backoff and jitter.
        """
        target_channel = self.get_channel(channel_id)
        if not target_channel:
            try:
                target_channel = await self.fetch_channel(channel_id)
            except (discord.NotFound, discord.Forbidden, discord.HTTPException) as e:
                self.log.error(f"Error fetching channel {channel_id}: {e}")
                return None
        if not target_channel:
            self.log.error(f"Could not resolve channel for ID: {channel_id}. Message not sent.")
            return None
        # --- Enhanced Retry Parameters from _send_file_part_to_discord ---
        max_send_attempts = 15
        initial_send_delay = 2  # seconds, for the first retry
        max_send_delay = 60  # seconds, cap the exponential backoff delay to prevent extremely long waits
        # --- End Retry Parameters ---
        for send_attempt in range(1, max_send_attempts + 1):  # Start from 1 for logging clarity
            try:
                if isinstance(target_channel, (discord.TextChannel, discord.DMChannel, discord.Thread)):
                    if content and file:
                        message = await target_channel.send(content=content, file=file)
                    elif content:
                        message = await target_channel.send(content=content)
                    elif file:
                        message = await target_channel.send(file=file)
                    else:
                        self.log.warning(f"Warning: Attempted to send empty message to channel {channel_id}.")
                        return None
                    self.log.info(
                        f"Send attempt {send_attempt}/{max_send_attempts} successful for channel '{channel_id}', Message ID: {message.id}")
                    return message  # Success!
                else:
                    self.log.warning(
                        f"Cannot send message to non-text channel type: {type(target_channel)}. Channel ID: {channel_id}")
                    return None
            except discord.errors.Forbidden as e:
                self.log.error(f"Forbidden to send message to channel {channel_id}: {e}. No further retries.")
                # No retry on Forbidden as it's a permanent permission issue
                return None
            except (discord.errors.HTTPException, asyncio.TimeoutError) as e:
                # Catch Discord API errors (including rate limits) and network timeouts
                self.log.error(
                    f"Send attempt {send_attempt}/{max_send_attempts} for channel '{channel_id}' failed: {type(e).__name__}: {e!r}")
                if send_attempt < max_send_attempts:
                    # Calculate exponential backoff with jitter
                    # current_delay will be 2, 4, 8, 16, 32, 60, 60...
                    current_delay = min(max_send_delay, initial_send_delay * (2 ** (send_attempt - 1)))
                    jitter = random.uniform(0, current_delay * 0.25)  # Add up to 25% random jitter
                    wait_time = current_delay + jitter
                    self.log.debug(f"Retrying send in {wait_time:.2f} seconds...")
                    await asyncio.sleep(wait_time)
                else:
                    self.log.error(
                        f"Max send attempts ({max_send_attempts}) reached for channel '{channel_id}', returning None.")
                    break  # Exit loop if max attempts reached
            except Exception as e:
                # Catch any other unexpected errors
                self.log.error(
                    f"Unexpected error during send attempt {send_attempt}/{max_send_attempts} for channel '{channel_id}': {type(e).__name__}: {e!r}")
                if send_attempt < max_send_attempts:
                    # Apply backoff for unexpected errors too
                    current_delay = min(max_send_delay, initial_send_delay * (2 ** (send_attempt - 1)))
                    jitter = random.uniform(0, current_delay * 0.25)
                    wait_time = current_delay + jitter
                    self.log.debug(f"Retrying send in {wait_time:.2f} seconds due to unexpected error...")
                    await asyncio.sleep(wait_time)
                else:
                    self.log.error(
                        f"Failed to send message to channel {channel_id} after {max_send_attempts} attempts due to unexpected error.")
                    break  # Exit loop if max attempts reached
        self.log.error(f"Failed to send message to channel {channel_id} after {max_send_attempts} attempts.")
        return None

    async def _send_file_part_to_discord(self, target_channel: discord.abc.Messageable, part_data: bytes,
                                         filename_to_send: str) -> Optional[discord.Message]:
        """
        Sends a file part (raw bytes) to a Discord channel with significantly enhanced retry logic.
        - Implements exponential backoff with jitter for retries.
        - Sets an overall timeout for attempting to send a single part.
        - Recreates BytesIO object for each retry to avoid "I/O operation on closed file."
        """
        max_send_attempts = 15
        initial_send_delay = 2  # seconds, for the first retry
        max_send_delay = 60  # seconds, cap the exponential backoff delay to prevent extremely long waits
        start_time = time.time()  # Start timer for overall part sending timeout
        for send_attempt in range(1, max_send_attempts + 1):  # Start from 1 for logging clarity
            try:
                # Recreate BytesIO for each attempt to ensure it's open and reset
                file_object = io.BytesIO(part_data)
                discord_file = discord.File(file_object, filename=filename_to_send)
                # Use target_channel.send to send the file
                message = await target_channel.send(file=discord_file)
                self.log.info(
                    f"Send attempt {send_attempt}/{max_send_attempts} successful for '{filename_to_send}', Message ID: {message.id}")
                return message  # Success!
            except (aiohttp.ClientError, asyncio.TimeoutError, discord.errors.DiscordException) as e:
                # Catch network errors, timeouts, and Discord API errors
                self.log.error(
                    f"Send attempt {send_attempt}/{max_send_attempts} for '{filename_to_send}' failed: {type(e).__name__}: {e!r}")
                if send_attempt < max_send_attempts:
                    # Calculate exponential backoff with jitter
                    # current_delay will be 2, 4, 8, 16, 32, 60, 60...
                    current_delay = min(max_send_delay, initial_send_delay * (2 ** (send_attempt - 1)))
                    jitter = random.uniform(0, current_delay * 0.25)  # Add up to 25% random jitter
                    wait_time = current_delay + jitter
                    self.log.debug(f"Retrying send in {wait_time:.2f} seconds...")
                    await asyncio.sleep(wait_time)
                else:
                    self.log.error(
                        f"Max send attempts ({max_send_attempts}) reached for '{filename_to_send}', returning None.")
                    break  # Exit loop if max attempts reached
            except Exception as e:
                # Catch any other unexpected errors
                self.log.error(
                    f"Unexpected error during send attempt {send_attempt}/{max_send_attempts} for '{filename_to_send}': {type(e).__name__}: {e!r}")
                # For unexpected errors, you might want to break immediately or use a different retry strategy
                # For now, we'll break to allow the higher-level logic to handle it
                break
        # If the loop finishes without returning a message, it means all attempts failed or timed out
        self.log.error(f"Failed to send '{filename_to_send}' after all attempts or timeout.")
        return None

    # --- Encryption Helpers ---
    def _derive_key_from_seed(self, seed: str) -> bytes:
        """Derives a Fernet key from a user-provided seed using HKDF."""
        if not seed:
            raise ValueError("Encryption seed cannot be empty.")
        kdf = HKDF(
            algorithm=hashes.SHA256(),
            length=32,
            salt=self._HKDF_SALT,
            info=self._HKDF_INFO,
            backend=default_backend()
        )
        # Use a strong hash of the seed as the input key material
        input_key_material = hashlib.sha256(seed.encode('utf-8')).digest()
        key = base64.urlsafe_b64encode(kdf.derive(input_key_material))
        self.log.debug(f"Derived Fernet key from seed.")
        return key

    def _encrypt_data(self, data: bytes, key: bytes) -> bytes:
        """Encrypts data using Fernet."""
        f = Fernet(key)
        return f.encrypt(data)

    def _decrypt_data(self, encrypted_data: bytes, key: bytes) -> bytes:
        """Decrypts data using Fernet."""
        f = Fernet(key)
        return f.decrypt(encrypted_data)

    def _generate_random_nickname(self, length=8) -> str:
        """Generates a random alphanumeric nickname."""
        characters = string.ascii_letters + string.digits
        return ''.join(random.choice(characters) for i in range(length))

    # --- Database Helper Methods (UPDATED BASED ON TEST RESULTS) ---
    async def is_folder_entry_in_db(self, db_file: str, root_name: str, relative_path: str, version: str) -> bool:
        """CHECKS IF A FOLDER EXISTS IN THE DB FOR A GIVEN VERSION"""
        query = {
            "root_upload_name": root_name,
            "relative_path_in_archive": relative_path,
            "base_filename": "_DIR_",
            "version": version  # Include version in query
        }
        entries = await self.db._db_read_sync(db_file, query)
        return len(entries) > 0

    async def _check_duplicate_root_upload_name(self, db_file: str, root_upload_name: str, version: str) -> bool:
        """
        Checks if a root_upload_name (nickname) with the SAME version already exists as a top-level file or folder.
        """
        if not os.path.exists(db_file):
            return False

        # Check for existing folder with this root_upload_name at the root level AND same version
        folder_query = {
            "root_upload_name": root_upload_name,
            "relative_path_in_archive": "",  # Root level folder
            "base_filename": "_DIR_",
            "version": version
        }
        existing_folders = await self.db._db_read_sync(db_file, folder_query)
        if existing_folders:
            self.log.debug(f"Duplicate check: Root folder '{root_upload_name}' version '{version}' already exists.")
            return True

        # Check for existing single file with this root_upload_name AND same version
        file_query = {
            "root_upload_name": root_upload_name,
            "relative_path_in_archive": "",  # Top-level file (no subfolder path)
            "base_filename": root_upload_name,  # File's base_filename matches its root_upload_name
            "version": version
        }
        existing_files = await self.db._db_read_sync(db_file, file_query)
        if existing_files:
            self.log.debug(f"Duplicate check: Top-level file '{root_upload_name}' version '{version}' already exists.")
            return True

        self.log.debug(f"Duplicate check: No existing item found for '{root_upload_name}'.")
        return False


    async def _resolve_path_to_db_entry_keys(self, target_path: str, all_db_entries: List[Dict[str, Any]]) -> Optional[
        Tuple[str, str, str, bool]]:
        """
        When a user gives a path like "MyProject/Subfolder/File.txt", you can figure out which DB entry it corresponds to, without considering versions.
        """
        normalized_target_path = os.path.normpath(target_path).replace(os.path.sep, '/').strip('/')
        if not normalized_target_path:
            return None  # Should be handled as global scan, not path resolution

        # Try to match as a specific file
        for entry in all_db_entries:
            r_name = entry.get("root_upload_name")
            r_path = (entry.get("relative_path_in_archive") or "").replace(os.path.sep, '/').strip('/')
            b_name = entry.get("base_filename")

            if b_name == "_DIR_":  # Skip folder entries for file matching
                continue

            current_file_conceptual_path = f"{r_name}/{r_path}/{b_name}"
            current_file_conceptual_path = re.sub(r'/{2,}', '/', current_file_conceptual_path).strip('/')

            if normalized_target_path == current_file_conceptual_path:
                self.log.debug(f"Resolved '{target_path}' as file: ({r_name}, {r_path}, {b_name}, False)")
                return r_name, r_path, b_name, False  # is_folder_target = False

        # Try to match as a specific folder (including root folder)
        # Sort by path length descending to prioritize more specific paths (e.g., 'A/B' before 'A')
        for entry in sorted(all_db_entries, key=lambda e: len(e.get("relative_path_in_archive", "")), reverse=True):
            r_name = entry.get("root_upload_name")
            r_path = (entry.get("relative_path_in_archive") or "").replace(os.path.sep, '/').strip('/')
            b_name = entry.get("base_filename")

            if b_name != "_DIR_":  # Skip file entries for folder matching
                continue

            current_folder_conceptual_path = f"{r_name}/{r_path}"
            current_folder_conceptual_path = re.sub(r'/{2,}', '/', current_folder_conceptual_path).strip('/')

            if normalized_target_path == current_folder_conceptual_path:
                self.log.debug(f"Resolved '{target_path}' as folder: ({r_name}, {r_path}, {b_name}, True)")
                return r_name, r_path, b_name, True  # is_folder_target = True

        # Try to match as a top-level root_upload_name (could be folder or single file upload)
        # This is a fallback if the full path didn't match a nested item.
        if '/' not in normalized_target_path:
            # Check if it's a root folder
            for entry in all_db_entries:
                if entry.get("root_upload_name") == normalized_target_path and \
                        (entry.get("relative_path_in_archive") == "" or entry.get(
                            "relative_path_in_archive") is None) and \
                        entry.get("base_filename") == "_DIR_":
                    self.log.debug(
                        f"Resolved '{target_path}' as top-level root folder: ({normalized_target_path}, '', '_DIR_', True)")
                    return normalized_target_path, "", "_DIR_", True  # is_folder_target = True

            # Check if it's a top-level single file upload (where root_upload_name == base_filename and no relative path)
            for entry in all_db_entries:
                if entry.get("root_upload_name") == normalized_target_path and \
                        (entry.get("relative_path_in_archive") == "" or entry.get(
                            "relative_path_in_archive") is None) and \
                        entry.get("base_filename") == normalized_target_path and \
                        entry.get("base_filename") != "_DIR_":
                    self.log.debug(
                        f"Resolved '{target_path}' as top-level single file: ({normalized_target_path}, '', {normalized_target_path}, False)")
                    return normalized_target_path, "", normalized_target_path, False  # is_folder_target = False

        self.log.debug(f"Could not resolve '{target_path}' to any specific database entry.")
        return None



    async def _store_file_metadata(self, root_upload_name: str, base_filename: str,
                                   part_number: int,
                                   total_parts: int, message_id: int, channel_id: int,
                                   relative_path_in_archive: str, database_file: str,
                                   original_root_name: str = "", is_nicknamed: bool = False,
                                   original_base_filename: str = "", is_base_filename_nicknamed: bool = False,
                                   encryption_mode: str = "off", encryption_key_auto: bytes = b"",
                                   password_seed_hash: str = "",
                                   store_hash_flag: bool = True,
                                   version: str = "0.0.0.1"):
        """Stores metadata for a single file part in the database, including version."""
        raw = f"{root_upload_name}|{relative_path_in_archive}|{base_filename}|{part_number}|{version}"
        fileid = hashlib.sha256(raw.encode()).hexdigest()
        if relative_path_in_archive == '.' or relative_path_in_archive is None:
            relative_path_in_archive = ''
        relative_path_in_archive = relative_path_in_archive.replace(os.path.sep, '/')
        entry = {
            'fileid': fileid,
            'base_filename': base_filename,
            'part_number': part_number,
            'total_parts': total_parts,
            'message_id': message_id,
            'channel_id': channel_id,
            'relative_path_in_archive': relative_path_in_archive,
            'root_upload_name': root_upload_name,
            'upload_timestamp': datetime.datetime.now().isoformat(),
            'original_root_name': original_root_name,
            'is_nicknamed': is_nicknamed,
            'original_base_filename': original_base_filename,
            'is_base_filename_nicknamed': is_base_filename_nicknamed,
            'encryption_mode': encryption_mode,
            'encryption_key_auto': encryption_key_auto,
            'password_seed_hash': password_seed_hash,
            'store_hash_flag': store_hash_flag,
            'version': version
        }
        await self.db._db_insert_sync(database_file, entry)

    async def _store_folder_metadata(self, root_upload_name: str, relative_folder_path: str,
                                     database_file: str,
                                     original_root_name: str = "", is_nicknamed: bool = False,
                                     original_base_filename: str = "", is_base_filename_nicknamed: bool = False,
                                     encryption_mode: str = "off", encryption_key_auto: bytes = b"",
                                     password_seed_hash: str = "",
                                     store_hash_flag: bool = True,
                                     version: str = "0.0.0.1"):
        """Stores metadata for a folder (directory) in the database, including version."""
        if relative_folder_path == '.' or relative_folder_path is None:
            relative_folder_path = ''
        relative_folder_path = relative_folder_path.replace(os.path.sep, '/')
        folder_fileid = f"{root_upload_name}_{relative_folder_path}_DIR_v{version}"
        entry = {
            'fileid': folder_fileid,
            'base_filename': "_DIR_",
            'part_number': 0,
            'total_parts': 0,
            'message_id': 0,
            'channel_id': 0,
            'relative_path_in_archive': relative_folder_path,
            'root_upload_name': root_upload_name,
            'upload_timestamp': datetime.datetime.now().isoformat(),
            'original_root_name': original_root_name,
            'is_nicknamed': is_nicknamed,
            'original_base_filename': original_base_filename,
            'is_base_filename_nicknamed': is_base_filename_nicknamed,
            'encryption_mode': encryption_mode,
            'encryption_key_auto': encryption_key_auto,
            'password_seed_hash': password_seed_hash,
            'store_hash_flag': store_hash_flag,
            'version': version
        }
        # Only insert if the folder entry doesn't already exist for this specific version
        if not await self.is_folder_entry_in_db(database_file, root_upload_name, relative_folder_path, version):
            await self.db._db_insert_sync(database_file, entry)
            self.log.info(
                f"Stored metadata for folder: '{root_upload_name}/{relative_folder_path}' version '{version}'")
        else:
            self.log.warning(
                f"Skipped storing duplicate metadata for folder: '{root_upload_name}/{relative_folder_path}' version '{version}'")

    async def _store_root_folder_metadata(self, root_upload_name: str, DB_FILE: str,
                                          original_root_name: str = "", is_nicknamed: bool = False,
                                          encryption_mode: str = "off", encryption_key: Optional[bytes] = None,
                                          password_seed_hash: str = "", store_hash_flag: bool = True,
                                          version: str = "0.0.0.1"):
        """Stores metadata for the root folder in DB."""
        await self._store_folder_metadata(
            root_upload_name=root_upload_name,
            relative_folder_path='',
            database_file=DB_FILE,
            original_root_name=original_root_name,
            is_nicknamed=is_nicknamed,
            original_base_filename=original_root_name if is_nicknamed else "",
            is_base_filename_nicknamed=is_nicknamed,
            encryption_mode=encryption_mode,
            encryption_key_auto=encryption_key if encryption_mode == "automatic" else b"",
            password_seed_hash=password_seed_hash,
            store_hash_flag=store_hash_flag,
            version=version
        )
        self.log.info(f"Stored metadata for root folder: '{root_upload_name}' version '{version}'")

    # ----------------------------
    # Helper functions for upload
    # ----------------------------

    def _resolve_file_nickname(self, file_path: str, root_upload_name: str,
                               is_top_level: bool, is_nicknamed_flag: bool,
                               original_root_name: str) -> tuple[str, str, bool]:
        """
        Determines the final filename for DB, handles nicknaming if needed.
        Returns: (final_base_filename_for_db, original_base_filename_for_db, is_base_filename_nicknamed_flag)
        """
        original_name = os.path.basename(file_path)
        final_name = original_name
        original_for_db = ""
        is_nicknamed_flag_for_db = False

        if is_top_level and is_nicknamed_flag and original_root_name:
            final_name = root_upload_name
            original_for_db = original_name
            is_nicknamed_flag_for_db = True
            self.log.info(f"Top-level file '{original_name}' nicknamed to '{root_upload_name}'.")
        elif len(original_name) > 60:
            final_name = self._generate_random_nickname()
            original_for_db = original_name
            is_nicknamed_flag_for_db = True
            self.log.info(f"File name '{original_name}' >60 chars. Auto-nicknamed to '{final_name}'.")
        return final_name, original_for_db, is_nicknamed_flag_for_db

    def _compute_display_path(self, root_upload_name: str, relative_path_in_archive: str,
                              base_filename: str, is_base_nicknamed: bool,
                              original_base_filename: str, is_nicknamed_flag: bool,
                              original_root_name: str, is_top_level_file: bool, version: str) -> str:
        """
        Computes a user-facing display path including nicknames and version.
        """
        path_parts = []

        if is_nicknamed_flag and original_root_name:
            path_parts.append(f"{root_upload_name} (Original: {original_root_name})")
        elif is_top_level_file:
            path_parts.append(root_upload_name)
        else:
            if relative_path_in_archive:
                path_parts.append(relative_path_in_archive)
            display_base = f"{base_filename} (Original: {original_base_filename})" \
                if is_base_nicknamed and original_base_filename else base_filename
            path_parts.append(display_base)

        display_path = os.path.join(*path_parts).replace(os.path.sep, '/')
        return f"{display_path} (v{version})"

    def _compute_file_parts(self, file_path: str, chunk_size: int) -> tuple[int, int]:
        """
        Returns (file_size, total_parts) given a file and chunk size.
        """
        file_size = os.path.getsize(file_path)
        total_parts = (file_size + chunk_size - 1) // chunk_size
        return file_size, max(total_parts, 1)

    async def _read_file_chunks(self, file_path: str, chunk_size: int):
        """
        Async generator yielding chunks of a file.
        """
        with open(file_path, 'rb') as f:
            part_number = 1
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                yield part_number, chunk
                part_number += 1

    def _encrypt_chunk_if_needed(self, chunk_data: bytes, encryption_mode: str,
                                 encryption_key: Optional[bytes]) -> bytes:
        """
        Encrypts chunk if needed, otherwise returns the original data.
        """
        if encryption_mode != "off" and encryption_key is not None:
            return self._encrypt_data(chunk_data, encryption_key)
        return chunk_data

    async def _upload_chunk_to_discord(self, interaction: discord.Interaction, channel_id: int,
                                       part_data: bytes, part_filename: str,
                                       root_upload_name: str, base_filename: str, part_number: int,
                                       total_parts: int, relative_path_in_archive: str, DB_FILE: str,
                                       original_root_name: str, is_nicknamed: bool,
                                       original_base_filename: str, is_base_filename_nicknamed: bool,
                                       encryption_mode: str, encryption_key: Optional[bytes],
                                       password_seed_hash: str, store_hash_flag: bool,
                                       version: str,
                                       overall_parts_uploaded_ref: Optional[List[int]] = None,
                                       overall_total_parts: int = 0):
        """
        Sends a file part to Discord and stores metadata in DB.
        """
        channel = self.get_channel(channel_id)
        file_part_message = await self._send_file_part_to_discord(channel, part_data, part_filename)
        if not file_part_message:
            raise Exception(f"Failed to upload part {part_number} of '{base_filename}'.")

        await self._store_file_metadata(
            root_upload_name=root_upload_name,
            base_filename=base_filename,
            part_number=part_number,
            total_parts=total_parts,
            message_id=file_part_message.id,
            channel_id=channel_id,
            relative_path_in_archive=relative_path_in_archive,
            database_file=DB_FILE,
            original_root_name=original_root_name,
            is_nicknamed=is_nicknamed,
            original_base_filename=original_base_filename,
            is_base_filename_nicknamed=is_base_filename_nicknamed,
            encryption_mode=encryption_mode,
            encryption_key_auto=encryption_key if encryption_mode == "automatic" else b"",
            password_seed_hash=password_seed_hash,
            store_hash_flag=store_hash_flag,
            version=version
        )

        if overall_parts_uploaded_ref is not None:
            overall_parts_uploaded_ref[0] += 1
            overall_percentage = (overall_parts_uploaded_ref[
                                      0] / overall_total_parts) * 100 if overall_total_parts > 0 else 0
            user_mention = interaction.user.mention
            await self.send_message_robustly(
                channel_id,
                content=f"{user_mention}, Completed part {part_number}/{total_parts} of `{base_filename}`. Overall: {overall_parts_uploaded_ref[0]}/{overall_total_parts} parts ({overall_percentage:.0f}%)."
            )

    async def upload_single_file(self, interaction: discord.Interaction, file_path: str,
                                 DB_FILE: str, channel_id: int, root_upload_name: str,
                                 relative_path_in_archive: str = '',
                                 overall_parts_uploaded_ref: Optional[List[int]] = None,
                                 overall_total_parts: int = 0, is_top_level_single_file_upload: bool = False,
                                 original_root_name_for_db: str = "", is_nicknamed_flag_for_db: bool = False,
                                 encryption_mode: str = "off", encryption_key: Optional[bytes] = None,
                                 password_seed_hash: str = "", store_hash_flag: bool = True,
                                 current_chunk_size: int = 0, version: str = "0.0.0.1"):

        user_mention = interaction.user.mention

        # 1️⃣ Resolve nicknames
        final_base, original_base, is_base_nicknamed = self._resolve_file_nickname(
            file_path, root_upload_name, is_top_level_single_file_upload,
            is_nicknamed_flag_for_db, original_root_name_for_db
        )

        # 2️⃣ Compute display path
        display_path = self._compute_display_path(
            root_upload_name, relative_path_in_archive, final_base,
            is_base_nicknamed, original_base, is_nicknamed_flag_for_db,
            original_root_name_for_db, is_top_level_single_file_upload, version
        )

        try:
            # 3️⃣ Compute file size and total parts
            file_size, total_parts = self._compute_file_parts(file_path, current_chunk_size)

            self.log.info(
                f"Uploading '{file_path}' as '{final_base}' with {total_parts} parts (chunk size {current_chunk_size}).")
            await self.send_message_robustly(channel_id,
                                             content=f"{user_mention}, Starting upload of `{display_path}` ({total_parts} parts).")

            # 4️⃣ Read chunks, encrypt, upload
            async for part_number, chunk in self._read_file_chunks(file_path, current_chunk_size):
                chunk = self._encrypt_chunk_if_needed(chunk, encryption_mode, encryption_key)
                part_filename = f"{final_base}.part{part_number:03d}"
                await self._upload_chunk_to_discord(
                    interaction, channel_id, chunk, part_filename,
                    root_upload_name, final_base, part_number, total_parts, relative_path_in_archive,
                    DB_FILE, original_root_name_for_db, is_nicknamed_flag_for_db,
                    original_base, is_base_nicknamed, encryption_mode, encryption_key,
                    password_seed_hash, store_hash_flag, version,
                    overall_parts_uploaded_ref, overall_total_parts
                )

            self.log.info(f"Finished uploading '{file_path}' version '{version}'.")

        except Exception as e:
            self.log.error(f"Error uploading '{file_path}': {e}")
            self.log.error(traceback.format_exc())
            await self.send_message_robustly(channel_id, f"{user_mention}, Error uploading `{display_path}`: {e}")
            raise

    async def _process_subfolder(self, root_upload_name: str, parent_relative_path: str,
                                 dir_name: str, DB_FILE: str, original_root_name: str,
                                 is_nicknamed_flag: bool, encryption_mode: str,
                                 encryption_key: Optional[bytes], password_seed_hash: str,
                                 store_hash_flag: bool, version: str) -> str:
        """
        Handles a single subfolder:
        - Nicknames if >60 chars
        - Stores folder metadata
        Returns the nicknamed folder name for further path calculations.
        """
        nicknamed_name = dir_name
        is_dir_nicknamed = False
        original_name_for_db = ""

        if len(dir_name) > 60:
            nicknamed_name = self._generate_random_nickname()
            is_dir_nicknamed = True
            original_name_for_db = dir_name
            self.log.debug(f"Sub-folder '{dir_name}' >60 chars. Auto-nicknamed to '{nicknamed_name}'.")

        folder_relative_path = os.path.join(parent_relative_path, nicknamed_name).replace(os.path.sep, '/')

        await self._store_folder_metadata(
            root_upload_name=root_upload_name,
            relative_folder_path=folder_relative_path,
            database_file=DB_FILE,
            original_root_name=original_root_name,
            is_nicknamed=is_nicknamed_flag,
            original_base_filename=original_name_for_db,
            is_base_filename_nicknamed=is_dir_nicknamed,
            encryption_mode=encryption_mode,
            encryption_key_auto=encryption_key if encryption_mode == "automatic" else b"",
            password_seed_hash=password_seed_hash,
            store_hash_flag=store_hash_flag,
            version=version
        )

        return nicknamed_name

    async def _process_files_in_folder(self, interaction: discord.Interaction, folder_path: str,
                                       files: list[str], DB_FILE: str, channel_id: int,
                                       root_upload_name: str, relative_folder_path: str,
                                       overall_parts_uploaded_ref: List[int],
                                       overall_total_parts: int,
                                       is_nicknamed_flag: bool, original_root_name: str,
                                       encryption_mode: str, encryption_key: Optional[bytes],
                                       password_seed_hash: str, store_hash_flag: bool,
                                       current_chunk_size: int, version: str):
        """
        Uploads all files in a single folder.
        """
        for file_name in files:
            file_path = os.path.join(folder_path, file_name)
            await self.upload_single_file(
                interaction=interaction,
                file_path=file_path,
                DB_FILE=DB_FILE,
                channel_id=channel_id,
                root_upload_name=root_upload_name,
                relative_path_in_archive=relative_folder_path.replace(os.path.sep, '/'),
                overall_parts_uploaded_ref=overall_parts_uploaded_ref,
                overall_total_parts=overall_total_parts,
                is_top_level_single_file_upload=False,
                original_root_name_for_db=original_root_name,
                is_nicknamed_flag_for_db=is_nicknamed_flag,
                encryption_mode=encryption_mode,
                encryption_key=encryption_key,
                password_seed_hash=password_seed_hash,
                store_hash_flag=store_hash_flag,
                current_chunk_size=current_chunk_size,
                version=version
            )
            self.log.info(f"Uploaded file '{file_path}' in folder '{relative_folder_path}' version '{version}'.")

    async def _walk_and_upload(self, interaction: discord.Interaction, local_folder_path: str,
                               DB_FILE: str, channel_id: int, root_upload_name: str,
                               overall_parts_uploaded_ref: List[int], overall_total_parts: int,
                               original_root_name: str = "", is_nicknamed_flag: bool = False,
                               encryption_mode: str = "off", encryption_key: Optional[bytes] = None,
                               password_seed_hash: str = "", store_hash_flag: bool = True,
                               current_chunk_size: int = 0, version: str = "0.0.0.1"):
        """
        Walks through all subfolders and files, uploading them.
        """
        for root, dirs, files in os.walk(local_folder_path, followlinks=False):
            # Compute relative path from top folder
            top_level_folder_name = os.path.basename(local_folder_path)
            relative_folder_path = os.path.relpath(root, local_folder_path)

            if relative_folder_path == '.':
                # top folder itself
                relative_folder_path = top_level_folder_name
            else:
                # prepend top folder to nested paths
                relative_folder_path = os.path.join(top_level_folder_name, relative_folder_path).replace(os.path.sep,
                                                                                                         '/')

            # Process subfolders
            for i, dir_name in enumerate(dirs):
                nicknamed_name = await self._process_subfolder(
                    root_upload_name=root_upload_name,
                    parent_relative_path=relative_folder_path,
                    dir_name=dir_name,
                    DB_FILE=DB_FILE,
                    original_root_name=original_root_name,
                    is_nicknamed_flag=is_nicknamed_flag,
                    encryption_mode=encryption_mode,
                    encryption_key=encryption_key,
                    password_seed_hash=password_seed_hash,
                    store_hash_flag=store_hash_flag,
                    version=version
                )
                # Update the dirs list so os.walk continues with the nicknamed folder name
                dirs[i] = nicknamed_name

            # Upload files in current folder
            await self._process_files_in_folder(
                interaction=interaction,
                folder_path=root,
                files=files,
                DB_FILE=DB_FILE,
                channel_id=channel_id,
                root_upload_name=root_upload_name,
                relative_folder_path=relative_folder_path,
                overall_parts_uploaded_ref=overall_parts_uploaded_ref,
                overall_total_parts=overall_total_parts,
                is_nicknamed_flag=is_nicknamed_flag,
                original_root_name=original_root_name,
                encryption_mode=encryption_mode,
                encryption_key=encryption_key,
                password_seed_hash=password_seed_hash,
                store_hash_flag=store_hash_flag,
                current_chunk_size=current_chunk_size,
                version=version
            )

    async def upload_folder_contents(self, interaction: discord.Interaction, local_folder_path: str,
                                     DB_FILE: str, channel_id: int, root_upload_name: str,
                                     overall_uploaded_parts_ref: List[int], overall_total_parts: int,
                                     original_root_name_for_db: str = "", is_nicknamed_flag_for_db: bool = False,
                                     encryption_mode: str = "off", encryption_key: Optional[bytes] = None,
                                     password_seed_hash: str = "", store_hash_flag: bool = True,
                                     current_chunk_size: int = 0, version: str = "0.0.0.1"):
        """
        Uploads an entire folder (including subfolders and files) in a modular fashion.
        """
        user_mention = interaction.user.mention
        total_files = sum(len(f) for _, _, f in os.walk(local_folder_path))
        self.log.info(
            f"Starting upload of folder '{root_upload_name}' version '{version}'. Total files: {total_files}. Overall parts: {overall_total_parts}")

        # 1️⃣ Store root folder metadata
        await self._store_root_folder_metadata(
            root_upload_name=root_upload_name,
            DB_FILE=DB_FILE,
            original_root_name=original_root_name_for_db,
            is_nicknamed=is_nicknamed_flag_for_db,
            encryption_mode=encryption_mode,
            encryption_key=encryption_key,
            password_seed_hash=password_seed_hash,
            store_hash_flag=store_hash_flag,
            version=version
        )

        # 2️⃣ Walk and upload everything
        await self._walk_and_upload(
            interaction=interaction,
            local_folder_path=local_folder_path,
            DB_FILE=DB_FILE,
            channel_id=channel_id,
            root_upload_name=root_upload_name,
            overall_parts_uploaded_ref=overall_uploaded_parts_ref,
            overall_total_parts=overall_total_parts,
            original_root_name=original_root_name_for_db,
            is_nicknamed_flag=is_nicknamed_flag_for_db,
            encryption_mode=encryption_mode,
            encryption_key=encryption_key,
            password_seed_hash=password_seed_hash,
            store_hash_flag=store_hash_flag,
            current_chunk_size=current_chunk_size,
            version=version
        )

        await self.send_message_robustly(
            channel_id,
            content=f"{user_mention}, Completed processing all files and folders in '{root_upload_name}' version '{version}'. Finalizing upload..."
        )
        self.log.info(f"Finished uploading all contents of folder '{root_upload_name}' version '{version}'.")

    async def _determine_root_name(self, local_path: str, custom_root_name: Optional[str] = None,
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
            root_upload_name = self._generate_random_nickname()
            original_root_name_for_db = effective_root_name_from_path
            is_nicknamed_flag_for_db = True
        else:
            root_upload_name = effective_root_name_from_path

        return root_upload_name, original_root_name_for_db, is_nicknamed_flag_for_db

    async def _is_root_upload_a_file(self, all_db_entries: List[Dict[str, Any]], root_upload_name: str) -> bool:
        """Checks if the given root_upload_name corresponds to a top-level file."""
        for entry in all_db_entries:
            if entry.get('root_upload_name') == root_upload_name and \
                    entry.get('relative_path_in_archive') == '' and \
                    entry.get('base_filename') != '_DIR_':
                return True
        return False

    async def _resolve_target_item_for_new_version(
            self, DB_FILE: str, target_item_path: str, interaction: discord.Interaction, user_mention: str,
            is_folder: bool
    ) -> Tuple[str, str, str, bytes, str, str, bool, str, bool]:
        """
        Returns:
        target_root_name, target_relative_path, target_base_filename,
        inherited_encryption_key, inherited_password_seed_hash,
        inherited_encryption_mode, inherited_store_hash_flag,
        current_version_for_upload, user_seed
        """
        all_db_entries = await self.db._db_read_sync(DB_FILE, {})
        resolved_target_info = await self._resolve_path_to_db_entry_keys(target_item_path, all_db_entries)

        if not resolved_target_info:
            raise ValueError(f"{user_mention}, Error: Target item '{target_item_path}' not found in DB.")

        target_root_name, target_relative_path, target_base_filename, _ = resolved_target_info
        is_target_folder_in_db = (target_base_filename == "_DIR_")
        if is_folder != is_target_folder_in_db:
            raise ValueError(f"{user_mention}, Error: Type mismatch between local path and target DB item.")

        latest_existing_item = await self.version_manager._get_item_metadata_for_versioning(
            DB_FILE, target_root_name, target_relative_path, target_base_filename
        )

        if not latest_existing_item:
            raise ValueError(f"{user_mention}, Error: Could not retrieve existing metadata for '{target_item_path}'.")

        # Inherit encryption settings
        inherited_encryption_mode = latest_existing_item.get('encryption_mode', 'off')
        inherited_store_hash_flag = latest_existing_item.get('store_hash_flag', True)
        inherited_encryption_key: Optional[bytes] = None
        inherited_password_seed_hash: str = ""
        user_seed: str = ""

        if inherited_encryption_mode == "automatic":
            inherited_encryption_key = latest_existing_item.get('encryption_key_auto')
            if not inherited_encryption_key:
                inherited_encryption_key = Fernet.generate_key()
        return (
            target_root_name,  # str
            target_relative_path,  # str
            target_base_filename,  # str
            inherited_encryption_key or b"",  # bytes, fallback to empty bytes if None
            inherited_password_seed_hash,  # str
            str(inherited_encryption_mode),  # str
            bool(inherited_store_hash_flag),  # bool
            "0.0.0.1",  # str
            user_seed or ""  # str, fallback if None
        )

    async def _derive_encryption_for_not_automatic(
            self, user_seed: Optional[str], interaction: discord.Interaction, user_id: int,
            user_mention: str, inherited_store_hash_flag: bool, target_item_path: str
    ) -> Tuple[bytes, str]:
        """
        Returns: derived_key, derived_hash
        """
        if not user_seed:
            user_seed = self._generate_random_nickname(length=32)
            await interaction.followup.send(
                f"{user_mention}, sending a random password seed for upload '{target_item_path}' to your DMs...",
                ephemeral=True
            )
            await interaction.user.send(
                f"Your random password seed for '{target_item_path}' is: ||`{user_seed}`||. Save it securely!"
            )
            self.log.info(f"Generated random seed for '{target_item_path}' for user {user_id}. Sent via DM.")

        derived_key = self._derive_key_from_seed(user_seed)
        derived_hash = hashlib.sha256(user_seed.encode('utf-8')).hexdigest() if inherited_store_hash_flag else ""
        return derived_key, derived_hash

    async def _determine_version_string(
            self, DB_FILE: str, target_root_name: str, target_relative_path: str, target_base_filename: str,
            new_version_string: Optional[str] = None
    ) -> str:
        if new_version_string:
            return new_version_string
        return self.version_manager._generate_next_version_string(
            DB_FILE, target_root_name, target_relative_path, target_base_filename
        )

    def _normalize_db_file_path(self, DB_FILE: str) -> str:
        """Ensure DB_FILE ends with .db and return absolute normalized path."""
        if not DB_FILE.lower().endswith('.db'):
            DB_FILE += '.db'
        return os.path.abspath(os.path.normpath(DB_FILE))

    async def _check_duplicate_root(self, DATABASE_FILE: str, root_upload_name: str,
                                    new_version_string: Optional[str], interaction: discord.Interaction,
                                    user_mention: str, user_id: int) -> bool:
        """Return True if a top-level root upload name already exists in DB."""
        if await self._check_duplicate_root_upload_name(DATABASE_FILE, root_upload_name, new_version_string):
            await interaction.followup.send(
                content=f"{user_mention}, An item with the name '{root_upload_name}' already exists. "
                        f"Choose a different name or use `upload_mode: new_version`.",
                ephemeral=False
            )
            self.log.info(
                f">>> [UPLOAD] User {user_id} attempted duplicate root upload '{root_upload_name}'. Upload aborted.")
            return True
        return False

    async def _acquire_user_upload_slot(self, user_id: int, root_upload_name: str,
                                        interaction: discord.Interaction) -> bool:
        """
        Async version: returns True if upload slot acquired; False if already uploading.
        Sends user feedback via interaction.
        """
        user_mention = interaction.user.mention

        if user_id not in self.user_uploading:
            self.user_uploading[user_id] = []

        if root_upload_name in self.user_uploading[user_id]:
            await interaction.followup.send(
                content=f"{user_mention}, You are already uploading '{root_upload_name}'. Wait for completion or choose a different name.",
                ephemeral=False
            )
            self.log.info(f">>> [UPLOAD] User {user_id} already uploading '{root_upload_name}'.")
            return False

        self.user_uploading[user_id].append(root_upload_name)
        self.log.info(
            f">>> [UPLOAD] User {user_id} started upload of '{root_upload_name}'. Current uploads: {self.user_uploading[user_id]}"
        )
        return True

    def _get_chunk_size(self, encryption_mode: str) -> int:
        """
        Returns chunk size in bytes.
        10 MB for no encryption, 7 MB if encrypted.
        """
        return 10 * 1024 * 1024 if encryption_mode == "off" else 7 * 1024 * 1024

    async def _precalculate_total_parts(self, local_path: str, chunk_size: int) -> int:
        """Walk a file or folder and calculate total number of chunks to upload."""
        total_parts = 0
        if os.path.isdir(local_path):
            for root, _, files in os.walk(local_path):
                for file_name in files:
                    full_file_path = os.path.join(root, file_name)
                    try:
                        file_size = os.path.getsize(full_file_path)
                        parts_for_file = (file_size + chunk_size - 1) // chunk_size
                        total_parts += max(parts_for_file, 1)
                    except FileNotFoundError:
                        self.log.warning(f"File not found during pre-calculation: {full_file_path}. Skipping.")
        else:
            file_size = os.path.getsize(local_path)
            total_parts = max((file_size + chunk_size - 1) // chunk_size, 1)
        return total_parts

    async def _upload_item(self, interaction: discord.Interaction, local_path: str, DATABASE_FILE: str,
                           channel_id: int, root_upload_name: str, is_folder: bool,
                           original_root_name_for_db: str, is_nicknamed_flag_for_db: bool,
                           inherited_encryption_mode: str, inherited_encryption_key: Optional[bytes],
                           inherited_password_seed_hash: str, inherited_store_hash_flag: bool,
                           current_chunk_size: int, current_version_for_upload: str,
                           overall_uploaded_parts_ref: List[int], overall_total_parts: int):
        """
        Handles actual uploading of a file or folder.
        """
        if is_folder:
            await self.upload_folder_contents(
                interaction, local_path, DATABASE_FILE, channel_id, root_upload_name,
                overall_uploaded_parts_ref, overall_total_parts,
                original_root_name_for_db=original_root_name_for_db,
                is_nicknamed_flag_for_db=is_nicknamed_flag_for_db,
                encryption_mode=inherited_encryption_mode,
                encryption_key=inherited_encryption_key,
                password_seed_hash=inherited_password_seed_hash,
                store_hash_flag=inherited_store_hash_flag,
                current_chunk_size=current_chunk_size,
                version=current_version_for_upload
            )
        else:
            await self.upload_single_file(
                interaction, local_path, DATABASE_FILE, channel_id, root_upload_name,
                relative_path_in_archive='',
                overall_parts_uploaded_ref=overall_uploaded_parts_ref,
                overall_total_parts=overall_total_parts,
                is_top_level_single_file_upload=True,
                original_root_name_for_db=original_root_name_for_db,
                is_nicknamed_flag_for_db=is_nicknamed_flag_for_db,
                encryption_mode=inherited_encryption_mode,
                encryption_key=inherited_encryption_key,
                password_seed_hash=inherited_password_seed_hash,
                store_hash_flag=inherited_store_hash_flag,
                current_chunk_size=current_chunk_size,
                version=current_version_for_upload
            )

    async def _handle_incomplete_upload(
            self,
            interaction: discord.Interaction,
            root_upload_name: str,
            DATABASE_FILE: str,
            current_version_for_upload: str,
            is_folder: bool,
            uploaded_parts: int,
            total_parts: int
    ):
        """
        Sends an interactive message to the user if an upload is incomplete,
        allowing them to remove the incomplete upload or keep it.
        """
        user_mention = interaction.user.mention

        # Build the message
        problem_message_prefix = f"{user_mention}, **Upload Incomplete!** "
        problem_message_detail = (
            f"The upload process for `{root_upload_name}` (Version: {current_version_for_upload}) did not complete 100%. "
            f"Only {uploaded_parts} of {total_parts} parts were successfully uploaded. "
            f"This could be due to network issues, Discord API limits, or an unexpected error.\n\n"
        )
        problem_message_suffix = (
            f"The incomplete metadata for `{root_upload_name}` (Version: {current_version_for_upload}) "
            f"is still in your database (`{DATABASE_FILE}`)."
        )
        problem_message = problem_message_prefix + problem_message_detail + problem_message_suffix

        # Create Discord UI View
        view = discord.ui.View(timeout=180)

        # --- Remove Button ---
        remove_button = discord.ui.Button(
            label="Remove Incomplete Upload",
            style=discord.ButtonStyle.red,
            emoji="🗑️",
            custom_id="remove_incomplete_upload"
        )

        async def remove_upload_callback(button_interaction: discord.Interaction):
            await button_interaction.response.defer(ephemeral=True)
            # Call self's deletion method
            await self.delete_specific_item_version_entry(
                button_interaction,
                root_upload_name,
                DATABASE_FILE,
                current_version_for_upload,
                is_folder
            )
            for item in view.children:
                item.disabled = True
            await button_interaction.message.edit(view=view)
            await button_interaction.followup.send("Operation completed.", ephemeral=True)

        remove_button.callback = remove_upload_callback
        view.add_item(remove_button)

        # --- Cancel Button ---
        cancel_button = discord.ui.Button(
            label="Keep Files (Cancel)",
            style=discord.ButtonStyle.grey,
            emoji="❌",
            custom_id="cancel_incomplete_upload"
        )

        async def cancel_upload_callback(button_interaction: discord.Interaction):
            await button_interaction.response.send_message(
                f"{user_mention}, Okay, the incomplete upload entries for `{root_upload_name}` (Version: {current_version_for_upload}) will remain in your database.",
                ephemeral=True
            )
            for item in view.children:
                item.disabled = True
            await button_interaction.message.edit(view=view)

        cancel_button.callback = cancel_upload_callback
        view.add_item(cancel_button)

        # --- Retry sending the message ---
        max_attempts = 15
        initial_delay = 5
        max_delay = 60
        sent = False

        for attempt in range(1, max_attempts + 1):
            try:
                await interaction.followup.send(content=problem_message, view=view)
                self.log.info(
                    f"Successfully sent incomplete upload message with buttons on attempt {attempt}/{max_attempts}.")
                sent = True
                break
            except (discord.HTTPException, aiohttp.ClientError, asyncio.TimeoutError) as e:
                self.log.warning(
                    f"Failed to send incomplete upload message (Attempt {attempt}/{max_attempts}): {type(e).__name__}: {e!r}")
                if attempt < max_attempts:
                    current_delay = min(max_delay, initial_delay * (2 ** (attempt - 1)))
                    jitter = random.uniform(0, current_delay * 0.25)
                    await asyncio.sleep(current_delay + jitter)
                else:
                    self.log.error(
                        f"Max attempts reached for sending incomplete upload message for '{root_upload_name}'.")
            except Exception as e:
                self.log.error(
                    f"Unexpected error sending incomplete upload message (Attempt {attempt}/{max_attempts}): {type(e).__name__}: {e!r}")
                self.log.error(traceback.format_exc())
                break

        if not sent:
            self.log.warning(f"Incomplete upload message with buttons was NOT sent for '{root_upload_name}'.")
            await interaction.followup.send(
                content=f"{user_mention}, **Upload Incomplete!** (Failed to send interactive cleanup options). "
                        f"The upload for `{root_upload_name}` (Version: {current_version_for_upload}) did not complete. "
                        f"Its incomplete metadata is still in your database (`{DATABASE_FILE}`). "
                        f"You can use `/delete` to remove it manually if needed."
            )

    async def _release_upload_slot(self, user_id: int, root_upload_name: str):
        """Release the upload semaphore for a user and clean up `user_uploading` state."""
        # Release semaphore if it was acquired
        if hasattr(self, 'upload_semaphore') and self.upload_semaphore._value < getattr(self,
                                                                                        '_upload_semaphore_initial_capacity',
                                                                                        1):
            self.upload_semaphore.release()
            self.log.info(
                f">>> [UPLOAD] Released upload semaphore for user {user_id} ('{root_upload_name}'). Available permits: {self.upload_semaphore._value}"
            )
        else:
            self.log.warning(
                f">>> [UPLOAD] Semaphore not released for user {user_id} ('{root_upload_name}'). It may not have been acquired or was already released."
            )

        # Remove from user_uploading dict
        if hasattr(self, 'user_uploading') and user_id in self.user_uploading:
            if root_upload_name in self.user_uploading[user_id]:
                self.user_uploading[user_id].remove(root_upload_name)
            if not self.user_uploading[user_id]:
                del self.user_uploading[user_id]

        self.log.info(
            f">>> [UPLOAD] User {user_id} upload state cleaned up for '{root_upload_name}'."
        )

    async def _prepare_encryption_and_version(
            self,
            interaction: discord.Interaction,
            DB_FILE: str,
            upload_mode: str,
            target_item_path: Optional[str] = None,
            new_version_string: Optional[str] = None,
            user_seed: Optional[str] = None,
            random_seed: bool = False,
            encryption_mode: str = "off",
            save_hash: bool = True,
            root_upload_name: str = "",
            is_folder: bool = False,
            original_root_name_for_db: str = ""
    ) -> Tuple[str, str, Optional[bytes], str, bool, str]:
        """
        Prepares encryption, password hash, and version for an upload.

        Returns:
            current_version_for_upload: str
            inherited_encryption_mode: str
            inherited_encryption_key: Optional[bytes]
            inherited_password_seed_hash: str
            user_seed: str
        """
        inherited_encryption_key: Optional[bytes] = None
        inherited_password_seed_hash: str = ""
        current_version_for_upload: str = "0.0.0.1"
        inherited_encryption_mode: str = encryption_mode
        user_seed_final = user_seed or ""

        # --- New version ---
        if upload_mode == "new_version":
            if not target_item_path:
                await interaction.followup.send(
                    f"{interaction.user.mention}, Error: target_item_path required for new_version.",
                    ephemeral=False
                )
                raise ValueError("target_item_path required for new_version")

            all_db_entries = await self.db._db_read_sync(DB_FILE, {})
            resolved_info = await self._resolve_target_item_for_new_version(target_item_path, all_db_entries, DB_FILE)

            (target_root_name, target_relative_path, target_base_filename,
             inherited_encryption_key, inherited_password_seed_hash,
             inherited_encryption_mode, _, _, user_seed_final) = resolved_info

            is_target_folder_in_db = target_base_filename == "_DIR_"
            if is_target_folder_in_db != is_folder:
                await interaction.followup.send(
                    f"{interaction.user.mention}, Error: Local path type does not match target DB item type.",
                    ephemeral=False
                )
                raise ValueError("Local path type mismatch")

            current_version_for_upload = new_version_string or self.version_manager._generate_next_version_string(
                DB_FILE, target_root_name, target_relative_path, target_base_filename
            )

            if await self.version_manager._check_item_version_exists(
                    DB_FILE, target_root_name, target_relative_path, target_base_filename, current_version_for_upload
            ):
                await interaction.followup.send(
                    f"{interaction.user.mention}, Error: Version '{current_version_for_upload}' already exists for '{target_item_path}'.",
                    ephemeral=False
                )
                raise ValueError("Version already exists")

        # --- New upload ---
        else:
            current_version_for_upload = new_version_string or "0.0.0.1"

            if inherited_encryption_mode == "automatic":
                inherited_encryption_key = Fernet.generate_key()
                self.log.info(f"Generated automatic encryption key for '{root_upload_name}'.")

            elif inherited_encryption_mode == "not_automatic":
                if random_seed:
                    user_seed_final = self._generate_random_nickname(length=32)
                    await interaction.followup.send(
                        f"{interaction.user.mention}, sending random password seed for '{root_upload_name}' via DM...",
                        ephemeral=True
                    )
                    await interaction.user.send(
                        f"Your random password seed for '{root_upload_name}': ||`{user_seed_final}`||. Save it securely!"
                    )
                    self.log.info(f"Generated random seed for '{root_upload_name}' for user {interaction.user.id}.")
                elif not user_seed_final:
                    await interaction.followup.send(
                        f"{interaction.user.mention}, Error: Provide password seed or set random_seed=True for not_automatic encryption.",
                        ephemeral=False
                    )
                    raise ValueError("Password seed required")

                try:
                    inherited_encryption_key = self._derive_key_from_seed(user_seed_final)
                    if save_hash:
                        inherited_password_seed_hash = hashlib.sha256(user_seed_final.encode()).hexdigest()
                        self.log.info(f"Derived encryption key and saved hash for '{root_upload_name}'.")
                    else:
                        inherited_password_seed_hash = ""
                except Exception as e:
                    self.log.error(f"Error deriving encryption key: {e}")
                    raise

        return (
            current_version_for_upload,
            inherited_encryption_mode,
            inherited_encryption_key,
            inherited_password_seed_hash,
            save_hash,  # ✅ bool
            user_seed_final  # str
        )

    async def _start_upload_process(
            self,
            interaction: discord.Interaction,
            local_path: str,
            DB_FILE: str,
            channel_id: int,
            custom_root_name: Optional[str] = None,
            encryption_mode: str = "off",
            user_seed: Optional[str] = None,
            random_seed: bool = False,
            save_hash: bool = True,
            upload_mode: str = "new_upload",
            target_item_path: Optional[str] = None,
            new_version_string: Optional[str] = None
    ):
        """
        Orchestrates the entire upload process for a file or folder.
        Delegates tasks to helper functions for clarity and maintainability.
        """
        user_id = interaction.user.id
        user_mention = interaction.user.mention
        is_folder = os.path.isdir(local_path)

        # Normalize DB_FILE path
        DATABASE_FILE = self._normalize_db_file_path(DB_FILE)

        # --- Step 1: Determine root_upload_name and nicknaming ---
        root_upload_name, original_root_name_for_db, is_nicknamed_flag_for_db = \
            await self._determine_root_name(local_path, custom_root_name, interaction)

        if not root_upload_name:
            return  # Early exit if root name determination failed

        # --- Step 2: Determine encryption key and versioning ---
        (
            current_version_for_upload,
            inherited_encryption_mode,
            inherited_encryption_key,
            inherited_password_seed_hash,
            inherited_store_hash_flag,
            user_seed
        ) = await self._prepare_encryption_and_version(
            interaction,
            DB_FILE,
            upload_mode,
            target_item_path,
            new_version_string,
            user_seed,
            random_seed,
            encryption_mode,
            save_hash,
            root_upload_name,
            is_folder,
            original_root_name_for_db
        )
        print(inherited_store_hash_flag)

        # --- Step 3: Check for duplicate root uploads (only for new_upload) ---
        if upload_mode == "new_upload":
            if await self._check_duplicate_root_upload_name(DATABASE_FILE, root_upload_name, new_version_string):
                await interaction.followup.send(
                    f"{user_mention}, An item with the name '{root_upload_name}' already exists. Choose a different name or use `upload_mode: new_version`.",
                    ephemeral=False
                )
                self.log.info(f">>> [UPLOAD] Duplicate root '{root_upload_name}' found. Aborting.")
                return

        # --- Step 4: Concurrency control ---
        if not await self._acquire_user_upload_slot(user_id, root_upload_name, interaction):
            return

        # --- Step 5: Determine chunk size ---
        current_chunk_size = self._get_chunk_size(inherited_encryption_mode)
        self.log.info(
            f"Using {current_chunk_size / (1024 * 1024):.0f} MB chunk size for '{inherited_encryption_mode}' upload of '{root_upload_name}'.")

        # --- Step 6: Pre-calculate total parts ---
        overall_uploaded_parts_ref = [0]
        overall_total_parts = await self._precalculate_total_parts(local_path, current_chunk_size)

        if overall_total_parts is None:
            await self._release_upload_slot(user_id, root_upload_name)
            return

        # --- Step 7: Acquire upload semaphore and start actual upload ---
        upload_successful = True
        try:
            await self.upload_semaphore.acquire()
            self.log.info(
                f">>> [UPLOAD] Acquired upload semaphore for user {user_id} ('{root_upload_name}'). Available permits: {self.upload_semaphore._value}")

            if is_folder:
                await self.upload_folder_contents(
                    interaction,
                    local_path,
                    DATABASE_FILE,
                    channel_id,
                    root_upload_name,
                    overall_uploaded_parts_ref,
                    overall_total_parts,
                    original_root_name_for_db=original_root_name_for_db,
                    is_nicknamed_flag_for_db=is_nicknamed_flag_for_db,
                    encryption_mode=inherited_encryption_mode,
                    encryption_key=inherited_encryption_key,
                    password_seed_hash=inherited_password_seed_hash,
                    store_hash_flag=inherited_store_hash_flag,
                    current_chunk_size=current_chunk_size,
                    version=current_version_for_upload
                )
            else:
                await self.upload_single_file(
                    interaction,
                    local_path,
                    DATABASE_FILE,
                    channel_id,
                    root_upload_name,
                    relative_path_in_archive='',
                    overall_parts_uploaded_ref=overall_uploaded_parts_ref,
                    overall_total_parts=overall_total_parts,
                    is_top_level_single_file_upload=True,
                    original_root_name_for_db=original_root_name_for_db,
                    is_nicknamed_flag_for_db=is_nicknamed_flag_for_db,
                    encryption_mode=inherited_encryption_mode,
                    encryption_key=inherited_encryption_key,
                    password_seed_hash=inherited_password_seed_hash,
                    store_hash_flag=inherited_store_hash_flag,
                    current_chunk_size=current_chunk_size,
                    version=current_version_for_upload
                )

            # --- Step 8: Validate upload completion ---
            if overall_uploaded_parts_ref[0] != overall_total_parts:
                upload_successful = False
                self.log.warning(
                    f"Upload of '{root_upload_name}' version '{current_version_for_upload}' incomplete. Uploaded {overall_uploaded_parts_ref[0]} of {overall_total_parts} parts."
                )
            else:
                await self.send_message_robustly(
                    channel_id,
                    f"{user_mention}, All parts of '{root_upload_name}' (Version: {current_version_for_upload}) have been uploaded successfully!"
                )
                self.log.info(
                    f"Successfully completed upload for '{root_upload_name}' version '{current_version_for_upload}'.")

        except Exception as e:
            self.log.critical(
                f"Critical error during upload of '{root_upload_name}' version '{current_version_for_upload}': {e}")
            self.log.critical(traceback.format_exc())
            await interaction.followup.send(
                f"{user_mention}, A critical error occurred during the upload of '{root_upload_name}' (Version: {current_version_for_upload}): {e}. Please report and try again."
            )
            upload_successful = False

        finally:
            # --- Step 9: Handle incomplete uploads ---
            if not upload_successful:
                await self._handle_incomplete_upload(
                    interaction,
                    root_upload_name,
                    DATABASE_FILE,
                    current_version_for_upload,
                    is_folder,
                    uploaded_parts=overall_uploaded_parts_ref[0],
                    total_parts=overall_total_parts
                )

            # --- Step 10: Release semaphore and clean up user_uploading state ---
            await self._release_upload_slot(user_id, root_upload_name)

#-------------------------------
#-SETTING UP THE SLASH COMMANDS-
#-------------------------------
bot = FileBotAPI(intents=intents)
@bot.event
async def on_ready():
    """Event that fires when the bot is ready."""
    bot.log.info(f'{bot.user} has connected to Discord!')
    # Sync commands on ready; consider bot.tree.copy_global_to_guild(guild=discord.Object(id=YOUR_GUILD_ID))
    # for faster testing in a dev guild.
    await bot.tree.sync()
    bot.log.info("Commands synced globally.")

@bot.tree.command(
    name="upload",
    description="Uploads a file or folder with advanced options (versioning, encryption, custom name, etc.)."
)
@app_commands.describe(
    local_path="Path to the file or folder on the bot's system.",
    database_file="Database file (e.g., myfiles.db).",
    encryption_mode="Select the encryption mode for your upload.",
    upload_name="Optional: A custom name for this upload (max 60 chars).",
    password_seed="Required for 'Not Automatic' encryption. Your seed password.",
    random_seed="Automatically generate a random password seed (for Not Automatic encryption).",
    save_hash="Store a hash of your password seed for zero-knowledge verification.",
    upload_mode="Choose whether this is a new upload or a new version of an existing item.",
    target_item_path="For new versions, the path of the existing item in the database.",
    new_version_string="Optional: Override the version string for this upload."
)
@app_commands.choices(
    encryption_mode=[
        app_commands.Choice(name="Off (No Encryption)", value="off"),
        app_commands.Choice(name="Automatic (Bot-managed Key)", value="automatic"),
        app_commands.Choice(name="Not Automatic (User-provided Password Seed)", value="not_automatic")
    ],
    upload_mode=[
        app_commands.Choice(name="New Upload", value="new_upload"),
        app_commands.Choice(name="New Version", value="new_version")
    ]
)
async def upload_command(
    interaction: discord.Interaction,
    local_path: str,
    database_file: str,
    encryption_mode: Optional[app_commands.Choice[str]] = None,
    upload_name: Optional[str] = None,
    password_seed: Optional[str] = None,
    random_seed: bool = False,
    save_hash: bool = True,
    upload_mode: Optional[app_commands.Choice[str]] = None,
    target_item_path: Optional[str] = None,
    new_version_string: Optional[str] = None
):
    user_mention = interaction.user.mention
    bot.log.debug(f"Starting /upload command for {interaction.user.id}")

    # Defer response
    try:
        await interaction.response.defer(ephemeral=False)
        bot.log.debug(f"Deferred /upload command for {interaction.user.id}")
    except discord.errors.NotFound as e:
        await interaction.followup.send(
            f"Interaction error: `{e}`. Please try again.",
            ephemeral=True
        )
        return
    except Exception as e:
        await interaction.followup.send(
            f"Unexpected error before processing `/upload`: `{e}`",
            ephemeral=True
        )
        return

    # Validate path
    if not os.path.exists(local_path):
        bot.log.error(f"Local path '{local_path}' does not exist for user {interaction.user.id}")
        await interaction.edit_original_response(
            content=f"{user_mention}, Error: The path '{local_path}' does not exist on the bot."
        )
        return

    # Resolve encryption mode
    encryption_mode_value = encryption_mode.value if encryption_mode else "automatic"

    if encryption_mode_value == "not_automatic" and not password_seed and not random_seed:
        await interaction.edit_original_response(
            content=f"{user_mention}, Error: For 'Not Automatic' encryption, either provide `password_seed` or enable `random_seed`."
        )
        return

    if encryption_mode_value != "not_automatic" and password_seed:
        await interaction.followup.send(
            content=f"{user_mention}, Warning: A `password_seed` was provided but encryption mode is not 'not_automatic'. It will be ignored.",
            ephemeral=False
        )

    # Resolve upload mode
    upload_mode_value = upload_mode.value if upload_mode else "new_upload"

    # Create the upload task
    bot.loop.create_task(bot._start_upload_process(
        interaction=interaction,
        local_path=local_path,
        DB_FILE=database_file,
        channel_id=interaction.channel_id,
        custom_root_name=upload_name,
        encryption_mode=encryption_mode_value,
        user_seed=password_seed,
        random_seed=random_seed,
        save_hash=save_hash,
        upload_mode=upload_mode_value,
        target_item_path=target_item_path,
        new_version_string=new_version_string
    ))

    bot.log.info(f"Initiated upload task for '{local_path}' by user {interaction.user.id}")
bot.run('TOKEN')