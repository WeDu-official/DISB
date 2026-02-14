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
import sqlite3
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
from upload import uploadmanager
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
        super().__init__(command_prefix=["/"], intents=intents)
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
        # Fixed salt and info for HKDF (should be constant and unique to your bot's encryption)
        self.db = DatabaseManager(file_table_columns=self.file_table_columns, log=self.log)
        self.version_manager = VersioningManager(db_read_func=self.db._db_read_sync, log=self.log)
        self._HKDF_SALT = b"my_bot_encryption_salt_12345"
        self._HKDF_INFO = b"discord_file_bot_encryption_info"

    async def setup_hook(self):
        self.http_session = aiohttp.ClientSession()
        try:
            synced = await self.tree.sync()
            self.log.info(f"Synced {len(synced)} slash commands for {self.user}")
        except discord.DiscordException as e:
            self.log.error(f"Error syncing slash commands: {e}")
        # Start the deletion task when the bot is ready
        # NOTE: Ensure you have _start_deletion_task defined elsewhere if it's more complex.
        # For this context, it's a placeholder.
        self.deletion_task = self.loop.create_task(self._deletion_worker())

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

    async def _get_file_parts_from_discord(self, channel_id: int, message_ids: List[int],
                                           encryption_key: Optional[bytes] = None) -> Dict[int, bytes]:
        """
        Fetches and optionally decrypts file parts from Discord messages with robust retry logic.
        - Implements exponential backoff with jitter for retries.
        - Attempts to fetch each message part multiple times.
        """
        part_data = {}
        max_download_attempts = 15
        initial_download_delay = 1.0  # seconds, for the first retry
        max_download_delay = 60.0  # seconds, cap the exponential backoff delay

        for msg_id in message_ids:
            filename = ""
            for attempt in range(max_download_attempts):
                try:
                    channel = self.get_channel(channel_id) or await self.fetch_channel(channel_id)
                    if not isinstance(channel, (discord.TextChannel, discord.DMChannel, discord.Thread)):
                        self.log.warning(
                            f"Channel {channel_id} is not a text-based channel. Cannot fetch message {msg_id}. Skipping part.")
                        break  # No point retrying if channel type is wrong

                    message = await channel.fetch_message(msg_id)
                    if not message.attachments:
                        self.log.warning(f"Message {msg_id} has no attachments. Skipping this part.")
                        break  # No attachments, no point retrying

                    attachment = message.attachments[0]
                    downloaded_bytes = await attachment.read()

                    # --- Decryption ---
                    if encryption_key is not None:
                        try:
                            self.log.debug(
                                f"DEBUG: Key being passed to _decrypt_data: '{encryption_key}' (Type: {type(encryption_key)})")
                            downloaded_bytes = self._decrypt_data(downloaded_bytes, encryption_key)
                        except (InvalidToken, ValueError) as e:  # Catch specific decryption errors
                            self.log.warning(f"Decryption failed for message {msg_id}: {e}. Skipping this part.")
                            break  # Decryption failed, no point retrying this part
                        except Exception as e:
                            self.log.error(
                                f"Unexpected error during decryption for message {msg_id}: {e}. Skipping this part.")
                            self.log.error(traceback.format_exc())
                            break  # Unexpected decryption error, stop retrying

                    # Extract part number from filename (e.g., 'filename.part001')
                    part_num = 0  # Default to 0 if not found
                    try:
                        filename = attachment.filename
                        match = re.search(r'\.part(\d+)', filename)
                        if match:
                            part_num = int(match.group(1))
                        else:
                            self.log.warning(
                                f"WARNING: Could not extract part number from filename '{filename}' for message {msg_id}. Defaulting to 0.")
                    except ValueError:
                        self.log.warning(
                            f"WARNING: Could not parse part number from filename '{filename}' for message {msg_id}. Defaulting to 0.")

                    part_data[part_num] = downloaded_bytes
                    self.log.debug(f"Fetched part {part_num} from message {msg_id} on attempt {attempt + 1}.")
                    break  # Successfully downloaded, move to next message_id

                except (discord.NotFound, discord.Forbidden) as e:
                    self.log.warning(
                        f"Message {msg_id} in channel {channel_id} not found or forbidden on attempt {attempt + 1}/{max_download_attempts}: {e}. Skipping this part permanently.")
                    break  # These are typically non-retryable errors
                except (discord.HTTPException, asyncio.TimeoutError, aiohttp.ClientError) as e:
                    # Catch Discord API errors, network timeouts, and aiohttp client errors
                    self.log.warning(
                        f"Failed to download part (Message ID: {msg_id}) "
                        f"on attempt {attempt + 1}/{max_download_attempts} due to: {e}. "
                        f"Retrying..."
                    )
                    if attempt < max_download_attempts - 1:  # Only calculate delay if more attempts are left
                        retry_delay = self._calculate_retry_delay(attempt, initial_download_delay, max_download_delay)
                        self.log.debug(f"Retrying download in {retry_delay:.2f} seconds...")
                        await asyncio.sleep(retry_delay)
                    else:
                        self.log.error(
                            f"Failed to download part (Message ID: {msg_id}) "
                            f"after {max_download_attempts} attempts. This part will be missing."
                        )
                        break  # Max attempts reached for this part
                except Exception as e:
                    self.log.error(
                        f"An unexpected error occurred while fetching message {msg_id} on attempt {attempt + 1}/{max_download_attempts}: {type(e).__name__}: {e!r}. No further retries for this part due to unexpected error.")
                    self.log.error(traceback.format_exc())
                    break  # Break out of retry loop for unexpected errors
            else:  # This else block executes if the inner loop completes without a 'break'
                self.log.error(
                    f"Failed to download part (Message ID: {msg_id}) after {max_download_attempts} attempts. This part will be missing (loop completed).")
        return part_data

    async def _cleanup_incomplete_download_files(self, user_mention: str, channel_id: int,
                                                 folder_or_file_to_remove: str):
        """
        Removes the specified folder or file and its contents from the local file system.
        Sends confirmation/error messages to the user.
        """
        self.log.info(f">>> [CLEANUP] Attempting to remove: '{folder_or_file_to_remove}'")
        try:
            if os.path.isdir(folder_or_file_to_remove):
                shutil.rmtree(folder_or_file_to_remove)
                await self.send_message_robustly(
                    channel_id,
                    content=f"{user_mention}, Successfully removed the incomplete download folder located at `{folder_or_file_to_remove}`."
                )
                self.log.info(f">>> [CLEANUP] Successfully removed folder '{folder_or_file_to_remove}'.")
            elif os.path.isfile(folder_or_file_to_remove):
                os.remove(folder_or_file_to_remove)
                await self.send_message_robustly(
                    channel_id,
                    content=f"{user_mention}, Successfully removed the incomplete download file located at `{folder_or_file_to_remove}`."
                )
                self.log.info(f">>> [CLEANUP] Successfully removed file '{folder_or_file_to_remove}'.")
            else:
                await self.send_message_robustly(
                    channel_id,
                    content=f"{user_mention}, The item `{folder_or_file_to_remove}` does not exist or has already been removed."
                )
                self.log.warning(f">>> [CLEANUP] Item '{folder_or_file_to_remove}' not found for removal.")
        except OSError as e:
            await self.send_message_robustly(
                channel_id,
                content=f"{user_mention}, Error removing incomplete download at `{folder_or_file_to_remove}`: {e}. You may need to delete it manually."
            )
            self.log.error(f">>> [CLEANUP] Error removing '{folder_or_file_to_remove}': {e}")
            self.log.error(traceback.format_exc())
        except Exception as e:
            self.log.error(f">>> [CLEANUP] Unexpected error during cleanup of '{folder_or_file_to_remove}': {e}")
            self.log.error(traceback.format_exc())
            await self.send_message_robustly(
                channel_id,
                content=f"{user_mention}, An unexpected error occurred during cleanup of `{folder_or_file_to_remove}`: {e}. You may need to delete it manually."
            )

    def _get_original_path_components(self, all_db_entries: List[Dict[str, Any]],
                                      root_upload_name_db: str,
                                      relative_path_in_archive_db: str,
                                      base_filename_db: str,
                                      is_file: bool) -> Tuple[str, List[str], str]:
        """
        Converts internal DB path components (potentially nicknamed) into their original,
        human-readable counterparts for disk paths and display.

        Returns: (original_root_name, original_relative_path_segments, original_base_filename)
        """
        original_root_name = root_upload_name_db
        original_relative_path_segments = []
        original_base_filename = base_filename_db

        # Find the root entry to get its original name if nicknamed
        root_entry = next((e for e in all_db_entries
                           if e.get('root_upload_name') == root_upload_name_db and
                           e.get('relative_path_in_archive') == '' and
                           e.get('base_filename') == '_DIR_'), None)
        if root_entry and root_entry.get('is_nicknamed') and root_entry.get('original_root_name'):
            original_root_name = root_entry['original_root_name']

        # Process relative path segments
        if relative_path_in_archive_db:
            current_db_path_segment_for_lookup = ""
            # Handle the case where relative_path_in_archive_db might be just a single segment, which is a file's parent folder
            # or a nested folder itself.
            path_segments_to_process = relative_path_in_archive_db.split('/')

            for i, segment_db in enumerate(path_segments_to_process):
                # Construct the full DB path for this segment to look up its _DIR_ entry
                # This is tricky because root_upload_name_db is the *actual* root, not part of relative_path_in_archive

                # If it's the first segment, its parent is the root_upload_name_db
                if i == 0:
                    db_path_for_segment_lookup = os.path.join(root_upload_name_db, segment_db).replace(os.path.sep, '/')
                else:
                    db_path_for_segment_lookup = os.path.join(root_upload_name_db, current_db_path_segment_for_lookup, segment_db).replace(os.path.sep, '/')

                folder_entry_for_segment = next((e for e in all_db_entries
                                                 if e.get('root_upload_name') == root_upload_name_db and
                                                 e.get('relative_path_in_archive') == db_path_for_segment_lookup and
                                                 e.get('base_filename') == '_DIR_'), None)

                if folder_entry_for_segment and folder_entry_for_segment.get('is_base_filename_nicknamed') and \
                        folder_entry_for_segment.get('original_base_filename'):
                    original_relative_path_segments.append(folder_entry_for_segment['original_base_filename'])
                else:
                    original_relative_path_segments.append(segment_db)

                # Update current_db_path_segment_for_lookup for the next iteration
                if i == 0:
                    current_db_path_segment_for_lookup = segment_db
                else:
                    current_db_path_segment_for_lookup = os.path.join(current_db_path_segment_for_lookup, segment_db)


        # Process base filename (if it's a file, not a directory marker)
        if is_file and base_filename_db != '_DIR_':
            # Find the file entry to get its original name if nicknamed
            # We look for any part of the file, as long as it matches the base_filename and relative_path
            file_entry = next((e for e in all_db_entries
                               if e.get('root_upload_name') == root_upload_name_db and
                               e.get('relative_path_in_archive') == relative_path_in_archive_db and
                               e.get('base_filename') == base_filename_db and
                               e.get('part_number') >= 0),
                              None)  # Use part_number >= 0 to identify the file entry itself
            if file_entry and file_entry.get('is_base_filename_nicknamed') and file_entry.get('original_base_filename'):
                original_base_filename = file_entry['original_base_filename']
            elif not original_base_filename:  # Fallback if base_filename_db was empty or not found in entry
                original_base_filename = "unknown_file.bin"

        return original_root_name, original_relative_path_segments, original_base_filename

    # noinspection PyUnresolvedReferences
    async def download_filea(self, interaction: discord.Interaction, target_path: str, DB_FILE: str,
                             download_folder: str,
                             decryption_password_seed: Optional[Dict[Tuple[str, str, str, str], bytes]] = None,
                             # Key is now a tuple
                             version_param: Optional[str] = None,  # NEW
                             start_version_param: Optional[str] = None,  # NEW
                             end_version_param: Optional[str] = None,  # NEW
                             all_versions_param: bool = False,  # NEW
                             can_apply_version_filters: bool = False  # NEW
                             ):
        """
        Downloads a specific file or an entire folder structure by its full path in the archive.
        Now supports resolving nicknamed sub-files and sub-folders, and includes incomplete download handling.
        Ensures downloaded files and folders retain original names and correct directory structure.
        Also supports version selection, range downloads, and organizing multiple versions.
        """
        user_mention = interaction.user.mention
        user_id = interaction.user.id

        decryption_password_seed = decryption_password_seed or {}  # Ensure it's a dict even if None passed

        # Normalize target_path for internal logic and comparison
        normalized_target_path = os.path.normpath(target_path).replace(os.path.sep, '/').strip('/')
        # FIX: Handle '.' as a global download indicator
        if normalized_target_path == ".":
            normalized_target_path = ""  # Treat '.' as empty for global download

        # Initialize variables to prevent potential NameErrors or IDE warnings
        base_download_target_dir_on_disk = os.path.abspath(os.path.normpath(download_folder))  # Always assigned here
        should_create_target_named_folder = False  # Default initialization
        local_base_path_for_cleanup = None  # Default initialization, will be set more precisely later

        # Initialize variables that might be referenced inside the loop's context
        # but also given initial values here for safety if the loop doesn't run or breaks early.
        total_expected_parts = 0
        display_path_for_message = ""
        final_output_filepath_on_disk = ""
        channel_id_for_file = 0  # Initialize to a default invalid channel ID
        message_ids_map_for_file = {}  # Initialize to an empty dict
        current_file_encryption_key = None  # Initialize encryption key for the current file

        # Initialize variables that were reported as potentially unassigned
        multiple_versions_of_single_item_being_downloaded = False
        original_target_root = None
        original_target_rel_path_segments = []

        if not normalized_target_path:  # If target_path was empty or just '/' or '.'
            normalized_target_path = ""  # Standardize empty path for global download
        self.user_downloading[user_id] = target_path
        self.log.info(f">>> [DOWNLOAD] User {user_id} started download of '{target_path}'.")
        acquired_semaphore = False
        download_successful = True  # Overall success flag for the entire download operation
        overall_parts_downloaded_counter = 0
        overall_total_parts_to_download = 0
        try:  # This outermost try ensures self.user_downloading state is always cleared
            # The initial deferral is handled in download_command.
            # If this is called from the modal, the modal's interaction was already deferred.
            # We might need to send a "starting download" message here if not already sent.
            # For now, assume the modal handles the initial user feedback.

            try:
                await self.download_semaphore.acquire()
                acquired_semaphore = True
                self.log.info(
                    f">>> [DOWNLOAD] Acquired download semaphore for user {user_id} ('{target_path}'). Available permits: {self.download_semaphore._value}")
                # Corrected database file extension check
                if not DB_FILE.lower().endswith('.db'):
                    DB_FILE += '.db'
                DATABASE_FILE = os.path.abspath(os.path.normpath(DB_FILE))
                if not os.path.exists(DATABASE_FILE):
                    await interaction.followup.send(
                        content=f"{user_mention}, the database file '{DB_FILE}' was not found.",
                        ephemeral=False)
                    self.log.error(f">>> [DOWNLOAD] ERROR: Database file not found at '{DATABASE_FILE}'.")
                    download_successful = False
                    return
                all_entries = await self.db._db_read_sync(DATABASE_FILE, {})
                if not all_entries:
                    await interaction.followup.send(
                        content=f"{user_mention}, No items found in database '{DB_FILE}'.",
                        ephemeral=False)
                    self.log.info(f"No entries found in database: '{DB_FILE}'.")
                    download_successful = False
                    return

                # --- Path Resolution Helper ---
                # Moved to top-level class method or defined within class, not here.
                # Use self._resolve_path_to_db_entry_keys below.

                # Initialize these to None/False for default state before resolution attempt
                resolved_root_upload_name = None
                resolved_relative_path_in_archive = None
                resolved_base_filename = None
                is_target_a_folder = False
                # should_create_target_named_folder is already initialized at the top

                if normalized_target_path == "":  # Special case for downloading the entire database (all root uploads)
                    self.log.info(f"Target path is empty, preparing to download entire database (newest versions).")
                    is_target_a_folder = True  # Treat as a folder-like download for cleanup purposes
                    should_create_target_named_folder = False  # For global download, each root becomes its own folder
                    # For global download, we don't need resolved_root_upload_name, etc.
                    # We will iterate through all root_upload_names later.
                else:  # Only attempt to resolve path if it's not empty
                    # Use the _resolve_path_to_db_entry_keys from self
                    resolved_target_info = await self._resolve_path_to_db_entry_keys(normalized_target_path,
                                                                                     all_entries)
                    if not resolved_target_info:
                        await interaction.followup.send(
                            f"{user_mention}, Could not find any item matching '{target_path}' in `{DATABASE_FILE}`."
                        )
                        self.log.info(
                            f">>> [DOWNLOAD] No item found for '{target_path}' in '{DATABASE_FILE}' after resolution.")
                        download_successful = False
                        return
                    resolved_root_upload_name, resolved_relative_path_in_archive, resolved_base_filename, is_target_a_folder = resolved_target_info
                    # When downloading a specific folder, we create a folder with its name
                    if is_target_a_folder:
                        should_create_target_named_folder = True
                    else:  # When downloading a single file, we do NOT create an extra folder for it
                        should_create_target_named_folder = False

                # Now, collect relevant file entries based on the resolved DB identifiers and versioning parameters
                relevant_file_entries_raw = []

                if normalized_target_path == "":  # Global download
                    # For global download, we get the NEWEST version of each file/folder.
                    # The versioning parameters (`version`, `start_version`, `end_version`, `all_versions`) are IGNORED.
                    # Iterate through unique root_upload_name/relative_path_in_archive/base_filename combinations
                    # to get the latest version of each unique item.
                    unique_item_keys = set()
                    for entry in all_entries:
                        item_key = (entry.get('root_upload_name'), entry.get('relative_path_in_archive'),
                                    entry.get('base_filename'))
                        if item_key not in unique_item_keys:
                            unique_item_keys.add(item_key)
                            # Get only the newest version for each item in a global download
                            latest_item_entry = await self.version_manager._get_relevant_item_versions(
                                DATABASE_FILE,
                                item_key[0], item_key[1], item_key[2],
                                version_param=None, start_version_param=None, end_version_param=None,
                                all_versions_param=False
                            )
                            relevant_file_entries_raw.extend(latest_item_entry)
                    relevant_file_entries_raw = [e for e in relevant_file_entries_raw if
                                                 e.get('base_filename') != '_DIR_']  # Filter out folder entries

                elif is_target_a_folder:
                    self.log.info(
                        f"DEBUG: Collecting relevant files for folder download. Resolved root: '{resolved_root_upload_name}', relative path: '{resolved_relative_path_in_archive}'")

                    if can_apply_version_filters:  # If it's a specific sub-folder and version filters apply
                        # Get entries for the specific target folder and its children, applying version filters
                        # For a specific folder, `_get_relevant_item_versions` will return versions of the _DIR_ entry
                        # and then we need to get children within that version scope. This requires a bit more logic.
                        # For simplicity, if version filters apply, we assume the user specified them for the *folder itself*
                        # and its contents will inherit the newest versions if not specified.

                        # Let's re-evaluate: "when downloading for example a specific folder that got version and you didn't specify the version it would download the newest one only"
                        # This implies that even if `can_apply_version_filters` is True for a sub-folder, if no explicit version params
                        # are given, we download the newest *individual* versions of its children.
                        # The `can_apply_version_filters` is mainly to ALLOW the user to select specific versions of THAT item.

                        # If user specified version parameters for a folder (i.e., this sub-folder entry itself)
                        if version_param or start_version_param or end_version_param or all_versions_param:
                            # Get the specific version(s) of the *folder itself*
                            folder_version_entries = await self.version_manager._get_relevant_item_versions(
                                DATABASE_FILE, resolved_root_upload_name, resolved_relative_path_in_archive, "_DIR_",
                                version_param, start_version_param, end_version_param, all_versions_param
                            )
                            # For each version of the folder, fetch all *files* within that specific version scope
                            for folder_entry in folder_version_entries:
                                current_folder_version = folder_entry.get('version')
                                files_in_this_folder_version = [
                                    e for e in all_entries
                                    if e.get('root_upload_name') == resolved_root_upload_name and
                                       e.get('base_filename') != '_DIR_' and
                                       e.get('version') == current_folder_version and  # Match version
                                       (e.get('relative_path_in_archive') == resolved_relative_path_in_archive or
                                        e.get('relative_path_in_archive', '').startswith(
                                            resolved_relative_path_in_archive + '/'))
                                ]
                                relevant_file_entries_raw.extend(files_in_this_folder_version)

                        else:  # No version parameters for this specific sub-folder, download newest of its children
                            # Get all files within this folder scope, then filter to newest version for each file
                            files_in_folder_scope = [
                                e for e in all_entries
                                if e.get('root_upload_name') == resolved_root_upload_name and
                                   e.get('base_filename') != '_DIR_' and
                                   (e.get('relative_path_in_archive') == resolved_relative_path_in_archive or
                                    e.get('relative_path_in_archive', '').startswith(
                                        resolved_relative_path_in_archive + '/'))
                            ]
                            # Now, for each unique file within this scope, get its newest version
                            unique_files_in_scope_keys = set()
                            for file_entry in files_in_folder_scope:
                                file_key = (
                                    file_entry.get('root_upload_name'), file_entry.get('relative_path_in_archive'),
                                    file_entry.get('base_filename'))
                                if file_key not in unique_files_in_scope_keys:
                                    unique_files_in_scope_keys.add(file_key)
                                    latest_file_version = await self.version_manager._get_relevant_item_versions(
                                        DATABASE_FILE, file_key[0], file_key[1], file_key[2],
                                        version_param=None, start_version_param=None, end_version_param=None,
                                        all_versions_param=False
                                    )
                                    relevant_file_entries_raw.extend(latest_file_version)

                else:  # Target is a specific file
                    self.log.info(
                        f"DEBUG: Collecting relevant file parts for specific file download. Resolved root: '{resolved_root_upload_name}', relative path: '{resolved_relative_path_in_archive}', base filename: '{resolved_base_filename}'")
                    # Use _get_relevant_item_versions to apply version filters for this specific file
                    relevant_file_entries_raw = await self.version_manager._get_relevant_item_versions(
                        DATABASE_FILE, resolved_root_upload_name, resolved_relative_path_in_archive,
                        resolved_base_filename,
                        version_param, start_version_param, end_version_param, all_versions_param
                    )
                    self.log.info(
                        f"DEBUG: Found {len(relevant_file_entries_raw)} entries for target file with version filters.")

                if not relevant_file_entries_raw:
                    await interaction.followup.send(
                        f"{user_mention}, The item '{target_path}' was found but contains no downloadable file parts for the specified version criteria. It might be an empty folder or a folder with only sub-folders, or no matching versions."
                    )
                    self.log.info(f">>> [DOWNLOAD] Target '{target_path}' has no file parts for specified versions.")
                    download_successful = False
                    return

                # Group parts by file AND version
                # The key for files_to_download_grouped now includes version
                files_to_download_grouped: Dict[
                    Tuple[str, str, str, str], Dict[str, Any]] = {}  # (base_filename, rel_path, root_name, version)

                # Populate files_to_download_grouped
                for entry in relevant_file_entries_raw:
                    base_filename = entry['base_filename']
                    rel_path = entry['relative_path_in_archive']
                    root_name_from_entry = entry['root_upload_name']
                    version_from_entry = entry['version']  # NEW

                    file_key = (base_filename, rel_path, root_name_from_entry, version_from_entry)  # NEW key

                    if file_key not in files_to_download_grouped:
                        files_to_download_grouped[file_key] = {
                            'parts': {},
                            'total_expected_parts': int(entry.get('total_parts', '1')),
                            'channel_id': int(entry.get('channel_id', '0')),
                            'root_upload_name': root_name_from_entry,
                            'relative_path_in_archive': rel_path,
                            'original_root_name': entry.get('original_root_name', ''),
                            'is_nicknamed': entry.get('is_nicknamed', False),
                            'original_file_name': entry.get('original_base_filename', ''),
                            'is_file_nicknamed': entry.get('is_base_filename_nicknamed', False),
                            'encryption_mode': entry.get('encryption_mode', 'off'),
                            'encryption_key_auto': entry.get('encryption_key_auto', b''),
                            'password_seed_hash': entry.get('password_seed_hash', ''),
                            'store_hash_flag': entry.get('store_hash_flag', False),  # Default changed to False
                            'version': version_from_entry  # NEW
                        }
                    files_to_download_grouped[file_key]['parts'][int(entry.get('part_number', '0'))] = int(
                        entry.get('message_id', '0'))

                self.log.debug(f"DEBUG: files_to_download_grouped count: {len(files_to_download_grouped)}")

                if not files_to_download_grouped:
                    await interaction.followup.send(
                        f"{user_mention}, No downloadable file parts found for '{target_path}' with the specified version criteria.",
                        ephemeral=False)
                    self.log.info(f"No downloadable items found for '{target_path}' with version criteria.")
                    download_successful = False
                    return

                overall_total_parts_to_download = sum(
                    file_data['total_expected_parts'] for file_data in files_to_download_grouped.values())
                if overall_total_parts_to_download == 0:
                    await interaction.followup.send(
                        content=f"{user_mention}, No actual file parts found to download within '{target_path}' with the specified version criteria.",
                        ephemeral=False)
                    self.log.info(f"No file parts to download for target: '{target_path}' with version criteria.")
                    download_successful = False
                    return

                # Determine the base download directory on disk
                os.makedirs(base_download_target_dir_on_disk, exist_ok=True)
                self.log.info(f"Ensured base download directory exists: '{base_download_target_dir_on_disk}'.")

                # Set local_base_path_for_cleanup for the entire download operation
                # This will be the top-level directory created/used for the download.
                # Only if a single item is targeted for multi-version download, create a parent folder for versions.
                multiple_versions_of_single_item_being_downloaded = (
                        can_apply_version_filters and  # The target path is a specific item
                        (version_param is None and not (
                                start_version_param and end_version_param) and all_versions_param) or  # All versions
                        (start_version_param and end_version_param and not version_param)  # Version range
                )

                if multiple_versions_of_single_item_being_downloaded:
                    # Get original name of the target item (file or folder)
                    original_target_root, original_target_rel_path_segments, original_target_base_filename = \
                        self._get_original_path_components(
                            all_entries, resolved_root_upload_name, resolved_relative_path_in_archive,
                            resolved_base_filename, not is_target_a_folder  # is_file if not a folder
                        )

                    item_display_name = original_target_base_filename if not is_target_a_folder else os.path.basename(
                        original_target_rel_path_segments[
                            -1]) if original_target_rel_path_segments else original_target_root

                    # Create a new top-level folder for all versions of this single item
                    versioned_output_parent_folder = os.path.join(base_download_target_dir_on_disk,
                                                                  f"{item_display_name}_Versions")
                    os.makedirs(versioned_output_parent_folder, exist_ok=True)
                    local_base_path_for_cleanup = versioned_output_parent_folder
                    self.log.info(f"Created versioned output parent folder: '{versioned_output_parent_folder}'.")
                    await interaction.followup.send(
                        content=f"{user_mention}, Preparing to download multiple versions of `{item_display_name}` to `{os.path.basename(versioned_output_parent_folder)}/` in `{download_folder}`."
                    )
                elif normalized_target_path == "":  # Global download
                    local_base_path_for_cleanup = base_download_target_dir_on_disk  # Cleanup refers to the download_folder itself
                elif should_create_target_named_folder:  # Targeted single folder download (not multiple versions of a single item)
                    # Get the original name of the target folder for the top-level folder on disk
                    original_target_root_name, original_target_rel_path_segments, _ = self._get_original_path_components(
                        all_entries, resolved_root_upload_name, resolved_relative_path_in_archive,
                        resolved_base_filename, False  # False because we are getting folder path
                    )
                    # The actual folder name on disk will be the last segment of the original path, or the root if it's top-level
                    if original_target_rel_path_segments:
                        top_level_folder_name_on_disk = original_target_rel_path_segments[-1]
                    else:  # It's the root itself
                        top_level_folder_name_on_disk = original_target_root_name

                    local_base_path_for_cleanup = os.path.join(base_download_target_dir_on_disk,
                                                               top_level_folder_name_on_disk)
                    os.makedirs(local_base_path_for_cleanup, exist_ok=True)
                    self.log.info(f"Created top-level download folder for target: '{local_base_path_for_cleanup}'.")
                    await interaction.followup.send(
                        content=f"{user_mention}, Creating download folder: `{os.path.basename(local_base_path_for_cleanup)}/` in `{download_folder}`.")
                else:  # Targeted single file download (not multiple versions of a single item)
                    # For a single file download, the cleanup path is the file itself.
                    # We need to derive its full target path on disk.
                    # We can use the first (and only) file's data for this.
                    first_file_data = list(files_to_download_grouped.values())[0]
                    original_root_name, original_relative_path_segments, original_base_filename = self._get_original_path_components(
                        all_entries,
                        first_file_data.get('root_upload_name', ''),
                        first_file_data.get('relative_path_in_archive', ''),
                        first_file_data.get('base_filename', ''),
                        True  # It's a file
                    )
                    file_disk_path_parts = [base_download_target_dir_on_disk]
                    if original_root_name and not original_relative_path_segments:  # Only add root folder if it's a file directly under root
                        file_disk_path_parts.append(
                            original_root_name)  # this will be the root folder if it's a sub-file
                    file_disk_path_parts.extend(original_relative_path_segments)
                    file_disk_path_parts.append(original_base_filename)
                    local_base_path_for_cleanup = os.path.join(*file_disk_path_parts)
                    # Ensure parent directory for the single file exists
                    os.makedirs(os.path.dirname(local_base_path_for_cleanup), exist_ok=True)
                    self.log.info(f"Calculated cleanup path for single file: '{local_base_path_for_cleanup}'.")

                overall_download_successful = True  # Master success flag

                # Iterate files in sorted order by root_upload_name, relative_path_in_archive, and THEN version
                sorted_files_to_download = sorted(files_to_download_grouped.items(),
                                                  key=lambda item: (item[0][2], item[0][1], self.version_manager.parse_version(
                                                      item[0][3])))  # item[0][3] is version

                for (base_filename_stored, relative_path_in_archive_stored,
                     root_upload_name_stored,
                     version_stored), file_data in sorted_files_to_download:  # NEW: version_stored
                    self.log.debug(
                        f"DEBUG: Processing file_data for version {version_stored}: {file_data}")  # Added debug log

                    # Re-initialize these variables for each file in the loop
                    total_expected_parts = file_data.get('total_expected_parts', 0)
                    channel_id_for_file = file_data.get('channel_id', 0)
                    message_ids_map_for_file = file_data.get('parts', {})
                    current_file_encryption_key = None  # Reset for each file
                    current_file_download_successful = True  # Flag for the current file

                    # Determine encryption key for the current file and version
                    encryption_mode_for_file = file_data.get('encryption_mode', 'off')
                    password_seed_hash_for_file = file_data.get('password_seed_hash', '')
                    store_hash_flag_for_file = file_data.get('store_hash_flag', False)

                    if encryption_mode_for_file == 'automatic':
                        current_file_encryption_key = file_data.get('encryption_key_auto')
                        if not isinstance(current_file_encryption_key, bytes):  # Ensure it's bytes from DB
                            current_file_encryption_key = current_file_encryption_key.encode(
                                'utf-8') if current_file_encryption_key else b''

                    elif encryption_mode_for_file == 'not_automatic':
                        # Key for lookup in decryption_password_seed is now a tuple (root, rel_path, base_filename, version)
                        decryption_key_lookup_key = (
                            root_upload_name_stored, relative_path_in_archive_stored, base_filename_stored,
                            version_stored)
                        derived_key_from_user_input = decryption_password_seed.get(decryption_key_lookup_key)

                        if not derived_key_from_user_input:
                            self.log.warning(
                                f"File '{base_filename_stored}' version '{version_stored}' requires 'not_automatic' decryption but no key found. Skipping this file.")
                            current_file_download_successful = False
                            overall_download_successful = False
                            # Do not add to overall_parts_downloaded_counter here, it's handled when the loop continues.
                            await interaction.followup.send(
                                content=f"{user_mention}, Skipping `{file_data.get('original_file_name', base_filename_stored)}` version `{version_stored}` because no decryption key was provided or it was incorrect."
                            )
                            continue  # Skip to next file

                        current_file_encryption_key = derived_key_from_user_input

                    # Determine original names for this specific file
                    original_root_name_for_file_item, original_relative_path_segments_for_file_item, original_base_filename_for_file_item = \
                        self._get_original_path_components(
                            all_entries,
                            root_upload_name_stored,
                            relative_path_in_archive_stored,
                            base_filename_stored,
                            True  # It's a file entry
                        )

                    # Determine output path on disk for this specific file/version
                    current_file_output_path_components = []

                    if multiple_versions_of_single_item_being_downloaded:
                        # Path: download_folder/OriginalItemName_Versions/vX.Y.Z/OriginalSubPath/OriginalFileName.ext
                        current_file_output_path_components.append(
                            local_base_path_for_cleanup)  # This is the _Versions folder
                        current_file_output_path_components.append(f"v{version_stored}")  # Add version folder

                        # Append the path relative to the root of the original item
                        # This means if OriginalItemName was "MyFile.txt", its path would be vX.Y.Z/MyFile.txt
                        # If OriginalItemName was "MyFolder", and file is "MyFolder/Sub/File.txt",
                        # its path would be vX.Y.Z/Sub/File.txt
                        if resolved_base_filename == "_DIR_":  # The targeted item was a folder
                            # We need the relative path of this *current file* from the *targeted folder's original path*
                            target_original_full_path_for_relative_calc = os.path.join(
                                original_target_root,
                                *original_target_rel_path_segments).replace(os.path.sep, '/')

                            # Original conceptual path of the current file from its root
                            current_file_full_conceptual_path = os.path.join(
                                original_root_name_for_file_item,
                                *original_relative_path_segments_for_file_item).replace(os.path.sep, '/')

                            if current_file_full_conceptual_path.startswith(
                                    target_original_full_path_for_relative_calc):
                                relative_to_target_folder = current_file_full_conceptual_path[
                                    len(target_original_full_path_for_relative_calc):].strip(
                                    '/')
                                if relative_to_target_folder:
                                    current_file_output_path_components.extend(relative_to_target_folder.split('/'))
                        else:  # The targeted item was a file
                            # No relative path needed, just add the file itself after the version folder
                            pass  # Nothing to add here, as original_base_filename_for_file_item is added next

                        current_file_output_path_components.append(original_base_filename_for_file_item)

                    elif normalized_target_path == "":  # Global download
                        # Path: download_folder/OriginalRootName/OriginalSubPath/OriginalFileName.ext
                        current_file_output_path_components.append(base_download_target_dir_on_disk)
                        if not await self._is_root_upload_a_file(all_entries,
                                                                 root_upload_name_stored):  # If the top-level item is a folder, create its folder
                            current_file_output_path_components.append(original_root_name_for_file_item)
                        current_file_output_path_components.extend(original_relative_path_segments_for_file_item)
                        current_file_output_path_components.append(original_base_filename_for_file_item)

                    elif should_create_target_named_folder:  # Targeted single folder download (not multiple versions)
                        # Path: download_folder/OriginalTargetFolderName/OriginalSubPathRelativeToTarget/OriginalFileName.ext
                        current_file_output_path_components.append(
                            local_base_path_for_cleanup)  # This is download_folder/OriginalTargetFolderName

                        # Get the original conceptual path of the downloaded target folder (e.g., "MyRoot/SubFolderA")
                        _, resolved_original_rel_path_segments, _ = self._get_original_path_components(
                            all_entries, resolved_root_upload_name, resolved_relative_path_in_archive,
                            resolved_base_filename, False
                        )
                        resolved_original_full_path_conceptual = os.path.join(resolved_root_upload_name,
                                                                              *resolved_original_rel_path_segments).replace(
                            os.path.sep, '/')

                        # Get the full conceptual path of the current file (e.g., "MyRoot/SubFolderA/SubSubFolder/File.txt")
                        current_file_full_conceptual_path_parts = [
                                                                      original_root_name_for_file_item] + original_relative_path_segments_for_file_item
                        current_file_full_conceptual_path = os.path.join(
                            *current_file_full_conceptual_path_parts).replace(os.path.sep, '/')

                        if current_file_full_conceptual_path.startswith(resolved_original_full_path_conceptual):
                            remaining_path = current_file_full_conceptual_path[
                                len(resolved_original_full_path_conceptual):].strip('/')
                            if remaining_path:
                                current_file_output_path_components.extend(remaining_path.split('/'))
                        current_file_output_path_components.append(original_base_filename_for_file_item)

                    else:  # Targeted single file download (not multiple versions)
                        # Path: download_folder/OriginalRootName/OriginalSubPath/OriginalFileName.ext (or just download_folder/OriginalFileName.ext)
                        # local_base_path_for_cleanup already contains the full intended path to the file.
                        current_file_output_path_components = [os.path.dirname(local_base_path_for_cleanup)]
                        current_file_output_path_components.append(original_base_filename_for_file_item)

                    final_output_filepath_on_disk = os.path.join(*current_file_output_path_components)

                    # Ensure the directory for the current file exists
                    os.makedirs(os.path.dirname(final_output_filepath_on_disk), exist_ok=True)

                    # Display path for messages (human-readable, uses original names and version)
                    display_path_for_message = ""
                    if multiple_versions_of_single_item_being_downloaded:
                        # Show path relative to the created version folder, including the version.
                        # E.g., "v1.0.0.1/MySubFolder/MyFile.txt"
                        if resolved_base_filename == "_DIR_":  # The targeted item was a folder
                            target_original_full_path_for_relative_calc = os.path.join(
                                original_target_root,
                                *original_target_rel_path_segments).replace(os.path.sep, '/')

                            current_file_full_conceptual_path = os.path.join(
                                original_root_name_for_file_item,
                                *original_relative_path_segments_for_file_item).replace(os.path.sep, '/')

                            if current_file_full_conceptual_path.startswith(
                                    target_original_full_path_for_relative_calc):
                                relative_to_target_folder = current_file_full_conceptual_path[
                                    len(target_original_full_path_for_relative_calc):].strip(
                                    '/')
                                if relative_to_target_folder:
                                    display_path_for_message = os.path.join(relative_to_target_folder,
                                                                            original_base_filename_for_file_item).replace(
                                        os.path.sep, '/')
                                else:
                                    display_path_for_message = original_base_filename_for_file_item
                            else:
                                # Fallback, should not happen
                                display_path_for_message = os.path.join(original_root_name_for_file_item,
                                                                        *original_relative_path_segments_for_file_item,
                                                                        original_base_filename_for_file_item).replace(
                                    os.path.sep, '/')
                        else:  # The targeted item was a file
                            display_path_for_message = original_base_filename_for_file_item

                        display_path_for_message = f"v{version_stored}/{display_path_for_message}"

                    else:  # Not multiple versions of a single item, use standard display logic
                        if normalized_target_path == "":  # Global download
                            # Show full conceptual path starting from original root name
                            if not await self._is_root_upload_a_file(all_entries, root_upload_name_stored):
                                display_path_for_message = os.path.join(original_root_name_for_file_item,
                                                                        *original_relative_path_segments_for_file_item,
                                                                        original_base_filename_for_file_item).replace(
                                    os.path.sep, '/')
                            else:
                                display_path_for_message = original_base_filename_for_file_item
                        elif not is_target_a_folder:  # Target was a specific file (not a folder download)
                            # Show full conceptual path starting from original root name
                            display_path_for_message = os.path.join(original_root_name_for_file_item,
                                                                    *original_relative_path_segments_for_file_item,
                                                                    original_base_filename_for_file_item).replace(
                                os.path.sep, '/')
                        else:  # Target was a folder (so `normalized_target_path` is a folder path)
                            # Show path relative to the conceptual target folder
                            resolved_original_root_name, resolved_original_rel_path_segments, _ = self._get_original_path_components(
                                all_entries, resolved_root_upload_name, resolved_relative_path_in_archive,
                                resolved_base_filename, False
                            )
                            resolved_original_full_path = os.path.join(
                                resolved_original_root_name, *resolved_original_rel_path_segments).replace(os.path.sep,
                                                                                                           '/')

                            current_original_full_path_components = [
                                                                        original_root_name_for_file_item] + original_relative_path_segments_for_file_item
                            current_original_full_path = os.path.join(
                                *current_original_full_path_components).replace(os.path.sep, '/')

                            if current_original_full_path.startswith(resolved_original_full_path):
                                relative_part_after_target = current_original_full_path[
                                    len(resolved_original_full_path):].strip('/')
                                if relative_part_after_target:
                                    display_path_for_message = os.path.join(relative_part_after_target,
                                                                            original_base_filename_for_file_item).replace(
                                        os.path.sep, '/')
                                else:  # File is directly in the targeted folder
                                    display_path_for_message = original_base_filename_for_file_item
                            else:  # Fallback, should not happen if logic is correct
                                display_path_for_message = os.path.join(original_root_name_for_file_item,
                                                                        *original_relative_path_segments_for_file_item,
                                                                        original_base_filename_for_file_item).replace(
                                    os.path.sep, '/')
                        display_path_for_message = f"{display_path_for_message} (v{version_stored})"

                    # Clean up double slashes etc.
                    display_path_for_message = re.sub(r'/{2,}', '/', display_path_for_message).strip('/')
                    if not display_path_for_message:  # Fallback for empty display path
                        display_path_for_message = "unknown_file_display_path"  # This should ideally not be hit with correct logic.

                    self.log.info(
                        f"Downloading file: '{display_path_for_message}'. Expected parts: {total_expected_parts}. Local path: {final_output_filepath_on_disk}")
                    # Initial message for the file
                    await interaction.followup.send(
                        content=f"{user_mention}, Downloading file: `{display_path_for_message}` (Total {total_expected_parts} parts)...")
                    downloaded_parts_data = {}
                    # Get the Discord channel object once for this file
                    target_discord_channel = self.get_channel(channel_id_for_file)
                    if not target_discord_channel:
                        try:
                            target_discord_channel = await self.fetch_channel(channel_id_for_file)
                        except (discord.NotFound, discord.Forbidden, discord.HTTPException) as e:
                            self.log.error(
                                f"Error fetching channel {channel_id_for_file}: {e}. Cannot download parts for '{display_path_for_message}'.")
                            await interaction.followup.send(
                                content=f"{user_mention}, Error accessing channel for `{display_path_for_message}`. Skipping this file.",
                                ephemeral=False)
                            download_successful = False
                            current_file_download_successful = False  # Mark current file as failed
                            # overall_parts_downloaded_counter += total_expected_parts  # Count parts as "skipped"
                            continue
                    if not isinstance(target_discord_channel,
                                      (discord.TextChannel, discord.DMChannel, discord.Thread)):
                        self.log.warning(
                            f"Channel {channel_id_for_file} is not a text-based channel. Cannot download parts for '{display_path_for_message}'.")
                        await interaction.followup.send(
                            content=f"{user_mention}, Channel for `{display_path_for_message}` is not text-based. Skipping this file.",
                            ephemeral=False)
                        download_successful = False
                        current_file_download_successful = False  # Mark current file as failed
                        # overall_parts_downloaded_counter += total_expected_parts  # Count parts as "skipped"
                        continue

                    # Track success for the current file's parts
                    current_file_parts_successfully_fetched = True  # Refers to actual fetching, not decryption
                    for p_num in range(1, total_expected_parts + 1):
                        msg_id = message_ids_map_for_file.get(p_num)
                        if msg_id:
                            try:
                                # Send progress message for each part
                                await self.send_message_robustly(
                                    interaction.channel_id,  # Send to the command's channel
                                    content=f"{user_mention}, Fetching part {p_num}/{total_expected_parts} for `{display_path_for_message}`..."
                                )
                                # Fetch and potentially decrypt the part
                                # _get_file_parts_from_discord will now raise InvalidToken if decryption fails
                                fetched_parts_dict = await self._get_file_parts_from_discord(
                                    channel_id_for_file, [msg_id], encryption_key=current_file_encryption_key
                                )
                                # If part_data is not empty and contains the expected part
                                if p_num in fetched_parts_dict:  # Check if the specific part was successfully fetched
                                    downloaded_parts_data[p_num] = fetched_parts_dict[p_num]
                                else:  # Fetch failed or internal error in _get_file_parts_from_discord
                                    self.log.warning(
                                        f"Part {p_num} for '{display_path_for_message}' (Msg ID: {msg_id}) not retrieved from Discord (no attachment or error occurred).")
                                    current_file_parts_successfully_fetched = False
                                    break  # Stop fetching parts for this file
                            except InvalidToken:  # This is the crucial point for zero-knowledge decryption failure
                                self.log.warning(f"Decryption failed for part {p_num} of '{display_path_for_message}'.")
                                await interaction.followup.send(
                                    f"{user_mention}, Decryption failed for `{display_path_for_message}`. The password might be incorrect.",
                                    ephemeral=False
                                )
                                # Present options to the user
                                zk_failure_view = ZeroKnowledgeDecryptionFailureView(
                                    self, interaction, local_base_path_for_cleanup,
                                    overall_parts_downloaded_counter, overall_total_parts_to_download,
                                    total_expected_parts
                                )
                                await interaction.followup.send(
                                    content=f"{user_mention}, What would you like to do about `{display_path_for_message}`?",
                                    view=zk_failure_view, ephemeral=False
                                )
                                # Wait for user interaction
                                await zk_failure_view.wait()  # This makes the function pause until user acts
                                self.log.info(f"User chose: {zk_failure_view.choice}")

                                if zk_failure_view.choice == "cancel_all":
                                    download_successful = False
                                    raise Exception(
                                        "User cancelled all downloads due to decryption failure.")  # Break main loop
                                elif zk_failure_view.choice == "cancel_keep":
                                    download_successful = False
                                    raise Exception(
                                        "User cancelled current download but kept files.")  # Break main loop
                                elif zk_failure_view.choice == "continue":
                                    current_file_download_successful = False
                                    overall_download_successful = False  # Mark overall as partial success
                                    overall_parts_downloaded_counter += total_expected_parts  # Count parts as "skipped"
                                    self.log.info(f"User chose to continue. Skipping '{display_path_for_message}'.")
                                    break  # Break from part loop, move to next file
                                else:  # Timeout or unexpected, treat as cancel all
                                    download_successful = False
                                    raise Exception("Decryption failure interaction timed out or unexpected choice.")

                            except Exception as e:  # Other errors during fetching parts
                                self.log.error(
                                    f"ERROR (caught): Fetching message {msg_id} for part {p_num} of '{display_path_for_message}': {e}")
                                self.log.error(traceback.format_exc())
                                current_file_parts_successfully_fetched = False
                                break  # Stop fetching parts for this file
                        else:  # Missing message ID
                            self.log.warning(
                                f"Warning: Missing message ID for part {p_num} of '{display_path_for_message}'. Marking file as incomplete.")
                            current_file_parts_successfully_fetched = False
                            break  # Stop fetching parts for this file

                    # Reconstruct the file if all parts were successfully downloaded and retrieved
                    if current_file_download_successful and current_file_parts_successfully_fetched and len(
                            downloaded_parts_data) == total_expected_parts:
                        try:
                            with open(final_output_filepath_on_disk, 'wb') as outfile:
                                for write_p_num in sorted(
                                        downloaded_parts_data.keys()):  # Ensure parts are written in order
                                    outfile.write(downloaded_parts_data[write_p_num])
                            self.log.info(f"Successfully downloaded and reconstructed: {display_path_for_message}")
                            await interaction.followup.send(
                                content=f"{user_mention}, Successfully downloaded and reconstructed: `{display_path_for_message}`"
                            )
                        except Exception as e:
                            self.log.error(
                                f"ERROR: Could not write/reconstruct file '{display_path_for_message}': {e}")
                            self.log.error(traceback.format_exc())
                            await interaction.followup.send(
                                content=f"{user_mention}, Error reconstructing `{display_path_for_message}` after download: {e}"
                            )
                            download_successful = False  # Mark overall as failed
                    else:  # File download was not 100% successful (either fetch issue or decryption issue handled above)
                        if current_file_download_successful:  # If current_file_download_successful is True, means it wasn't skipped due to ZK failure
                            missing_parts = [p for p in range(1, total_expected_parts + 1) if
                                             p not in downloaded_parts_data]
                            self.log.warning(
                                f"Could not reconstruct '{display_path_for_message}'. Missing parts: {missing_parts}. Marking download as incomplete.")
                            await interaction.followup.send(
                                content=f"{user_mention}, Could not fully download and reconstruct `{display_path_for_message}`. Missing parts: {', '.join(map(str, missing_parts))}"
                            )
                            download_successful = False
                        # If current_file_download_successful is already False (e.g., from ZK failure or no key provided),
                        # the message about skipping it was already sent.
                    overall_parts_downloaded_counter += len(
                        downloaded_parts_data)  # Only add successfully downloaded and decrypted parts
                    overall_percentage = (
                                                 overall_parts_downloaded_counter / overall_total_parts_to_download) * 100 if overall_total_parts_to_download > 0 else 0
                    await interaction.followup.send(
                        content=f"{user_mention}, Overall Download Progress: {overall_parts_downloaded_counter}/{overall_total_parts_to_download} parts ({overall_percentage:.0f}%)"
                    )
            except Exception as e:  # Catch exceptions that might bubble up from user choice or other issues
                self.log.error(f"Error during core download process for '{target_path}': {e}")
                self.log.error(traceback.format_exc())
                download_successful = False  # Mark overall as failed
            # Final check to determine if the download was truly successful after all files are processed
            if overall_parts_downloaded_counter != overall_total_parts_to_download:
                download_successful = False

            if download_successful:
                final_message_content = f"{user_mention}, Download process complete."
                if multiple_versions_of_single_item_being_downloaded and local_base_path_for_cleanup:
                    final_message_content += f"\nYour versioned files have been downloaded to: `{local_base_path_for_cleanup}`"
                    self.log.info(
                        f"Download process complete for '{target_path}'. Versioned files saved to: '{local_base_path_for_cleanup}'")
                elif normalized_target_path == "":
                    final_message_content += f"\nYour files have been downloaded to: `{base_download_target_dir_on_disk}`"
                    self.log.info(
                        f"Download process complete for '{target_path}'. Files saved to: '{base_download_target_dir_on_disk}'")
                elif should_create_target_named_folder and local_base_path_for_cleanup:
                    final_message_content += f"\nYour files have been downloaded to: `{local_base_path_for_cleanup}`"
                    self.log.info(
                        f"Download process complete for '{target_path}'. Files saved to: '{local_base_path_for_cleanup}'")
                elif not should_create_target_named_folder and local_base_path_for_cleanup:
                    parent_dir_of_file = os.path.dirname(local_base_path_for_cleanup)
                    final_message_content += f"\nYour file has been downloaded to: `{parent_dir_of_file}`"
                    self.log.info(
                        f"Download process complete for '{target_path}'. File saved to: '{parent_dir_of_file}'")
                else:
                    final_message_content += f"\nCheck your specified download folder: `{download_folder}`."
                    self.log.info(
                        f"Download process complete for '{target_path}'. Files saved to: '{download_folder}' (fallback).")
                await interaction.followup.send(content=final_message_content)
            else:
                problem_message_prefix = f"{user_mention}, **Download Incomplete!** "
                if overall_parts_downloaded_counter != overall_total_parts_to_download:
                    problem_message_detail = (
                        f"The download process for `{target_path}` did not complete 100%. "
                        f"Only {overall_parts_downloaded_counter} of {overall_total_parts_to_download} parts were successfully downloaded. "
                        f"This could be due to corrupted file parts on Discord, internet issues, or an incorrect decryption key.\n\n"
                    )
                else:
                    problem_message_detail = (
                        f"An unexpected error occurred during the download of `{target_path}`. "
                        f"The download might be incomplete or corrupted.\n\n"
                    )
                display_cleanup_path = "the specified download location"
                if local_base_path_for_cleanup:
                    display_cleanup_path = f"`{local_base_path_for_cleanup}`"
                problem_message_suffix = f"Your partially downloaded content might be located at: {display_cleanup_path}."
                problem_message = problem_message_prefix + problem_message_detail + problem_message_suffix
                view = discord.ui.View(timeout=180)
                remove_button = discord.ui.Button(
                    label="Remove Incomplete Download", style=discord.ButtonStyle.red, emoji="🗑️",
                    custom_id="remove_incomplete_download"
                )

                async def remove_callback(button_interaction: discord.Interaction):
                    # noinspection PyUnresolvedReferences
                    await button_interaction.response.defer(ephemeral=True)
                    await self._cleanup_incomplete_download_files(
                        user_mention,
                        button_interaction.channel_id,
                        local_base_path_for_cleanup
                    )
                    for item in view.children:
                        item.disabled = True
                    await button_interaction.message.edit(view=view)
                    await button_interaction.followup.send(f"Operation completed.",
                                                           ephemeral=True)

                remove_button.callback = remove_callback
                view.add_item(remove_button)
                cancel_button = discord.ui.Button(
                    label="Keep Files (Cancel)", style=discord.ButtonStyle.grey, emoji="❌",
                    custom_id="cancel_incomplete_download"
                )

                async def cancel_callback(button_interaction: discord.Interaction):
                    # noinspection PyUnresolvedReferences
                    await button_interaction.response.send_message(
                        f"{user_mention}, Okay, the partially downloaded files will remain on your machine.",
                        ephemeral=True
                    )
                    for item in view.children:
                        item.disabled = True
                    await button_interaction.message.edit(view=view)

                cancel_button.callback = cancel_callback
                view.add_item(cancel_button)
                await interaction.followup.send(content=problem_message, view=view)
                self.log.warning(
                    f"Download incomplete for '{target_path}'. {overall_parts_downloaded_counter}/{overall_total_parts_to_download} parts downloaded.")
        except Exception as e:
            self.log.critical(f"Critical error during download process for '{target_path}': {e}")
            self.log.critical(traceback.format_exc())
            await interaction.followup.send(
                content=f"{user_mention}, A critical and unexpected error occurred during the download of '{target_path}': {e}. Please report this and try again."
            )
            if local_base_path_for_cleanup and os.path.exists(local_base_path_for_cleanup):
                problem_message = (
                    f"{user_mention}, **Download Failed Due to Critical Error!** "
                    f"A serious error occurred during the download of `{target_path}`: {e}.\n\n"
                    f"Your partially downloaded content might be located at: `{local_base_path_for_cleanup}`."
                )
                view = discord.ui.View(timeout=180)
                remove_button = discord.ui.Button(
                    label="Remove Downloaded Files", style=discord.ButtonStyle.red, emoji="🗑️",
                    custom_id="remove_incomplete_download_critical"
                )

                async def remove_callback_critical_error(button_interaction: discord.Interaction):
                    # noinspection PyUnresolvedReferences
                    await button_interaction.response.defer(ephemeral=True)
                    await self._cleanup_incomplete_download_files(
                        user_mention,
                        button_interaction.channel_id,
                        local_base_path_for_cleanup
                    )
                    for item in view.children: item.disabled = True
                    await button_interaction.message.edit(view=view)
                    await button_interaction.followup.send(f"Operation completed.", ephemeral=True)

                remove_button.callback = remove_callback_critical_error
                view.add_item(remove_button)
                cancel_button = discord.ui.Button(
                    label="Keep Files (Cancel)", style=discord.ButtonStyle.grey, emoji="❌",
                    custom_id="cancel_incomplete_download_critical"
                )

                async def cancel_callback_critical_error(button_interaction: discord.Interaction):
                    # noinspection PyUnresolvedReferences
                    await button_interaction.response.send_message(
                        f"{user_mention}, Okay, the partially downloaded files will remain on your machine.",
                        ephemeral=True
                    )
                    for item in view.children: item.disabled = True
                    await button_interaction.message.edit(view=view)

                cancel_button.callback = cancel_callback_critical_error
                view.add_item(cancel_button)
                await interaction.followup.send(content=problem_message, view=view)
            else:
                await interaction.followup.send(
                    content=f"{user_mention}, No partial download found to offer cleanup for, or an error occurred before creating local files."
                )
        finally:
            if acquired_semaphore:
                self.download_semaphore.release()
                self.log.info(
                    f">>> [DOWNLOAD] Released download semaphore for user {user_id} ('{target_path}'). Available permits: {self.download_semaphore._value}")
            if user_id in self.user_downloading:  # Ensure it's cleared even if download failed mid-way
                del self.user_downloading[user_id]
                self.log.info(
                    f">>> [DOWNLOAD] User {user_id} finished download of '{target_path}'. user_downloading state cleared.")

    async def _get_items_requiring_password_for_download(self, database_file: str,
                                                         target_path: str,
                                                         version_param: Optional[str],
                                                         start_version_param: Optional[str],
                                                         end_version_param: Optional[str],
                                                         all_versions_param: bool,
                                                         can_apply_version_filters: bool) -> List[
        Dict[str, Any]]:  # Changed return type to Any
        """
        Identifies items (files or folders) that are encrypted with 'not_automatic' mode
        and are within the scope of the requested download (including versioning).
        Returns a list of dictionaries, each containing 'root_upload_name', 'relative_path_in_archive',
        'base_filename', 'version', 'password_seed_hash', 'display_name' (original name if nicknamed),
        and 'store_hash_flag'.
        """
        self.log.info(f"Checking for items requiring password for download: '{target_path}' with version filters.")
        if not database_file.lower().endswith('.db'):
            database_file += '.db'
        DATABASE_FILE = os.path.abspath(os.path.normpath(database_file))

        if not os.path.exists(DATABASE_FILE):
            self.log.warning(f"Database file not found: {DATABASE_FILE}. No items to check.")
            return []

        all_db_entries = await self.db._db_read_sync(DATABASE_FILE, {})
        if not all_db_entries:
            self.log.info("No entries in database. No items requiring password.")
            return []

        normalized_target_path = os.path.normpath(target_path).replace(os.path.sep, '/').strip('/')
        is_global_download = (normalized_target_path == '') or (normalized_target_path == '.')

        items_requiring_password: Dict[
            Tuple[str, str, str, str], Dict[str, Any]] = {}  # Key: (root, rel_path, base_name, version)

        # First, filter to items that are within the `target_path` scope.
        # This duplicates some logic from download_filea, but it's needed here to get conceptual paths.
        scoped_items_raw = []
        if is_global_download:
            # For global downloads, check all file entries (not folder markers)
            scoped_items_raw = [e for e in all_db_entries if e.get('base_filename') != '_DIR_']
        else:
            resolved_target_info = await self._resolve_path_to_db_entry_keys(normalized_target_path, all_db_entries)
            if not resolved_target_info:
                self.log.warning(f"Target path '{target_path}' not resolved for password check.")
                return []

            resolved_root_name, resolved_rel_path, resolved_base_name, is_target_a_folder = resolved_target_info

            if is_target_a_folder:
                # If target is a folder, consider all files within that folder's scope (all its versions)
                scoped_items_raw = [
                    e for e in all_db_entries
                    if e.get('root_upload_name') == resolved_root_name and
                       e.get('base_filename') != '_DIR_' and  # Only files
                       (e.get('relative_path_in_archive') == resolved_rel_path or
                        e.get('relative_path_in_archive', '').startswith(resolved_rel_path + '/'))
                ]
            else:  # Target is a specific file
                scoped_items_raw = [
                    e for e in all_db_entries
                    if e.get('root_upload_name') == resolved_root_name and
                       e.get('relative_path_in_archive') == resolved_rel_path and
                       e.get('base_filename') == resolved_base_name and
                       e.get('base_filename') != '_DIR_'  # Ensure it's a file
                ]

        if not scoped_items_raw:
            self.log.info(f"No files found in scope for password check for '{target_path}'.")
            return []

        # Now, apply version filtering if `can_apply_version_filters` is True and parameters are provided for a single item.
        # This is the tricky part: version filters only apply to the *top-level item* of the download, not recursively.
        final_items_to_check = []
        if can_apply_version_filters:
            # For a single targeted file/folder, we use the specific version parameters
            # This logic must align with how `download_filea` gets its `relevant_file_entries_raw`
            if not is_target_a_folder:  # If the target is a specific file
                final_items_to_check.extend(await self.version_manager._get_relevant_item_versions(
                    DATABASE_FILE, resolved_root_name, resolved_rel_path, resolved_base_name,
                    version_param, start_version_param, end_version_param, all_versions_param
                ))
            else:  # If the target is a specific folder, and version filters apply to the folder itself
                if version_param or start_version_param or end_version_param or all_versions_param:
                    # Get the specific version(s) of the *folder itself* (e.g. MyFolder/ v1.0)
                    folder_version_entries = await self.version_manager._get_relevant_item_versions(
                        DATABASE_FILE, resolved_root_name, resolved_rel_path, "_DIR_",
                        # target base_filename is _DIR_ for folder itself
                        version_param, start_version_param, end_version_param, all_versions_param
                    )
                    # For each version of the folder, fetch all *files* within that specific version scope
                    for folder_entry in folder_version_entries:
                        current_folder_version = folder_entry.get('version')
                        files_in_this_folder_version = [
                            e for e in all_db_entries
                            if e.get('root_upload_name') == resolved_root_name and
                               e.get('base_filename') != '_DIR_' and
                               e.get('version') == current_folder_version and  # Match version
                               (e.get('relative_path_in_archive') == resolved_rel_path or
                                e.get('relative_path_in_archive', '').startswith(resolved_rel_path + '/'))
                        ]
                        final_items_to_check.extend(files_in_this_folder_version)
                else:  # Specific folder, but no explicit version params, so get newest of children files
                    unique_files_in_scope_keys = set()
                    for file_entry in scoped_items_raw:  # Use scoped_items_raw which is already filtered by path
                        file_key = (file_entry.get('root_upload_name'), file_entry.get('relative_path_in_archive'),
                                    file_entry.get('base_filename'))
                        if file_key not in unique_files_in_scope_keys:
                            unique_files_in_scope_keys.add(file_key)
                            latest_file_version = await self.version_manager._get_relevant_item_versions(
                                DATABASE_FILE, file_key[0], file_key[1], file_key[2],
                                version_param=None, start_version_param=None, end_version_param=None,
                                all_versions_param=False
                            )
                            final_items_to_check.extend(latest_file_version)
        else:  # Version filters are NOT applicable (global download or top-level folder)
            # In these cases, we always get the newest version of each individual file within the scope.
            unique_items_keys = set()
            for entry in scoped_items_raw:
                item_key = (entry.get('root_upload_name'), entry.get('relative_path_in_archive'),
                            entry.get('base_filename'))
                if item_key not in unique_items_keys:
                    unique_items_keys.add(item_key)
                    # Get only the newest version for each item
                    latest_item_entry = await self.version_manager._get_relevant_item_versions(
                        DATABASE_FILE, item_key[0], item_key[1], item_key[2],
                        version_param=None, start_version_param=None, end_version_param=None, all_versions_param=False
                    )
                    final_items_to_check.extend(latest_item_entry)

        for entry in final_items_to_check:
            if entry.get('encryption_mode') == 'not_automatic':
                root_name = entry.get('root_upload_name')
                rel_path = entry.get('relative_path_in_archive')
                base_name = entry.get('base_filename')
                version = entry.get('version')

                # Use original names for display in the modal
                original_root, original_rel_path_segments, original_base_file = self._get_original_path_components(
                    all_db_entries, root_name, rel_path, base_name, (base_name != '_DIR_')
                )

                # Construct display name for the modal
                display_name_parts = []
                if original_root:
                    display_name_parts.append(original_root)
                display_name_parts.extend(original_rel_path_segments)
                if original_base_file and base_name != '_DIR_':
                    display_name_parts.append(original_base_file)

                display_name = os.path.join(*display_name_parts).replace(os.path.sep, '/')
                if not display_name:  # Fallback for root-level items or very short names
                    display_name = original_root or "Unknown Item"

                # Add version to the display name for clarity
                display_name_with_version = f"{display_name} (v{version})"

                item_key = (root_name, rel_path, base_name, version)  # Full key for password dict

                items_requiring_password[item_key] = {
                    'root_upload_name': root_name,
                    'relative_path_in_archive': rel_path,
                    'base_filename': base_name,
                    'version': version,
                    'password_seed_hash': entry.get('password_seed_hash', ''),
                    'display_name': display_name_with_version,  # Display original path + version
                    'store_hash_flag': entry.get('store_hash_flag', False)
                }

        # Convert to list of dicts for the modal, sorted for consistent display
        sorted_items = sorted(items_requiring_password.values(),
                              key=lambda x: (x['root_upload_name'], x['relative_path_in_archive'], x['base_filename'],
                                             self.version_manager.parse_version(x['version'])))
        self.log.info(f"Identified {len(sorted_items)} unique item versions requiring passwords.")
        return sorted_items
    # --- Delete Logic ---
    async def _deletion_worker(self):
        """Worker that processes deletion tasks from the queue."""
        self.log.info(">>> [DELETION_WORKER] Deletion worker started.")
        while True:
            try:
                task = await self.delete_task_queue.get()
                if task is None:  # Sentinel value to stop the worker
                    self.log.info(">>> [DELETION_WORKER] Received stop signal. Stopping worker.")
                    break
                channel_id, message_id = task
                success = await self._remove_discord_message(channel_id, message_id)
                if not success:
                    self.log.warning(
                        f"WARNING: Deletion of Discord message {message_id} in channel {channel_id} failed via worker.")
                self.delete_task_queue.task_done()
            except Exception as e:
                self.log.error(f"ERROR in deletion worker: {e}")
                self.log.error(traceback.format_exc())
                self.delete_task_queue.task_done()  # Mark as done even on error to prevent blocking
    async def _add_to_deletion_queue(self, channel_id: int, message_id: int):
        """Adds a Discord message deletion task to the queue."""
        await self.delete_task_queue.put((channel_id, message_id))
        self.log.info(f"Added message {message_id} from channel {channel_id} to deletion queue.")
    async def _remove_discord_message(self, channel_id: int, message_id: int) -> bool:
        """
        Deletes a Discord message. If the message is not found, it's considered successfully "removed"
        from the perspective of this operation (i.e., no action needed, it's already gone).
        """
        try:
            channel = self.get_channel(int(channel_id))
            if not channel:
                channel = await self.fetch_channel(int(channel_id))
            if channel:
                message = await channel.fetch_message(int(message_id))
                await message.delete()
                self.log.info(f"Successfully deleted Discord message {message_id} from channel {channel_id}.")
                return True
            return False
        except discord.NotFound:
            self.log.info(
                f"Discord message {message_id} not found in channel {channel_id} (already deleted or never existed). Ignoring and proceeding).")
            return True  # Consider it removed if it's not found
        except discord.Forbidden:
            self.log.error(
                f"ERROR: Bot lacks permissions to delete message {message_id} in channel {channel_id}. Cannot delete.")
            return False
        except discord.HTTPException as e:
            self.log.error(f"ERROR: HTTP error deleting message {message_id} in channel {channel_id}: {e}")
            return False
        except Exception as e:
            self.log.error(f"ERROR: Unexpected error deleting message {message_id} in channel {channel_id}: {e}")
            return False
    # --- START OF MODIFIED delete FUNCTION ---
    async def delete(self, interaction: discord.Interaction, target_path: str, database_file: str,
                     include_sub_folders: bool = True):
        """
        Deletes a specific file, folder, or an entire top-level uploaded item from the database
        and attempts to delete its corresponding Discord messages.
        If a Discord message is not found, it is ignored.
        """
        user_mention = interaction.user.mention
        if self.download_semaphore._value != 3 or self.upload_semaphore._value != 3:
            await interaction.followup.send(
                content=f"{user_mention}, System is currently busy with uploads or downloads. Please try again later.",
                ephemeral=False)
            return
        self.log.info(f">>> [DELETE] Attempting to delete '{target_path}' from '{database_file}'. "
                      f"Include sub-folders: {include_sub_folders}.")
        try:
            if not database_file.lower().endswith('.db'):
                database_file += '.db'
            DATABASE_FILE = os.path.abspath(os.path.normpath(database_file))
            if not os.path.exists(DATABASE_FILE):
                await interaction.followup.send(
                    content=f"{user_mention}, The database file '{database_file}' was not found. Cannot delete.",
                    ephemeral=False)
                self.log.error(f">>> [DELETE] ERROR: Database file not found at '{DATABASE_FILE}'.")
                return
            all_entries = await self.db._db_read_sync(DATABASE_FILE, {})  # Initial read
            if not all_entries:
                await interaction.followup.send(
                    content=f'{user_mention}, No files found in database: "{database_file}".', ephemeral=False)
                return
            normalized_target_path = os.path.normpath(target_path).replace(os.path.sep, '/').strip('/')
            is_global_scan = (normalized_target_path == '') or (normalized_target_path == '.')
            target_entry_found = None
            if not is_global_scan:
                # Attempt 1: Match target_path as a root_upload_name (top-level deletion)
                if '/' not in normalized_target_path:
                    matching_root_entries = [
                        entry for entry in all_entries
                        if entry.get("root_upload_name") == normalized_target_path
                    ]
                    if matching_root_entries:
                        is_root_folder = any(
                            e.get("relative_path_in_archive") == "" and e.get("base_filename") == "_DIR_" for e in
                            matching_root_entries)
                        is_root_single_file = any(
                            e.get("relative_path_in_archive") == "" and e.get("base_filename") == normalized_target_path
                            for e in matching_root_entries)
                        target_entry_found = {
                            "root_upload_name": normalized_target_path,
                            "relative_path_in_archive": "",
                            "base_filename": "_DIR_" if is_root_folder else (
                                normalized_target_path if is_root_single_file else "_UNKNOWN_")
                        }
                        self.log.info(f"DEBUG: Target '{normalized_target_path}' matched as a top-level root upload.")
                if not target_entry_found:
                    # Attempt 2: Exact path matching for nested files
                    for entry in sorted(all_entries, key=lambda e: len(
                            e.get("root_upload_name", "") + e.get("relative_path_in_archive", "") + e.get(
                                "base_filename",
                                "")),
                                        reverse=True):
                        r_name = entry.get("root_upload_name")
                        r_path = (entry.get("relative_path_in_archive") or "").replace(os.path.sep, '/').strip('/')
                        b_name = entry.get("base_filename")
                        if b_name == "_DIR_":
                            continue
                        current_item_conceptual_path = f"{r_name}/{r_path}/{b_name}"
                        current_item_conceptual_path = re.sub(r'/{2,}', '/', current_item_conceptual_path).strip('/')
                        if normalized_target_path == current_item_conceptual_path:
                            target_entry_found = entry
                            self.log.info(f"DEBUG: Target '{normalized_target_path}' matched as a specific file path.")
                            break
                if not target_entry_found:
                    # Attempt 3: Exact path matching for nested folders
                    for entry in sorted(all_entries, key=lambda e: len(
                            e.get("root_upload_name", "") + e.get("relative_path_in_archive", "")),
                                        reverse=True):
                        r_name = entry.get("root_upload_name")
                        r_path = (entry.get("relative_path_in_archive") or "").replace(os.path.sep, '/').strip('/')
                        b_name = entry.get("base_filename")
                        if b_name != "_DIR_":
                            continue
                        current_item_conceptual_path = f"{r_name}/{r_path}"
                        current_item_conceptual_path = re.sub(r'/{2,}', '/', current_item_conceptual_path).strip('/')
                        if normalized_target_path == current_item_conceptual_path:
                            target_entry_found = entry
                            self.log.info(
                                f"DEBUG: Target '{normalized_target_path}' matched as a specific folder path.")
                            break
                if not target_entry_found:
                    await interaction.followup.send(
                        content=f"{user_mention}, The target '{target_path}' was not found as a specific file or folder in the database. Please ensure you provide the full path (e.g., `RootName/SubFolder/File.txt` or `RootName/SubFolder`).",
                        ephemeral=False)
                    self.log.info(
                        f">>> [DELETE] INFO: Target '{target_path}' not found in the database. No exact file or folder match after all attempts.")
                    return
            items_to_delete_keys = set()
            # Changed: `removed_file_paths_display` to `deleted_items_for_paginator`
            deleted_items_for_paginator = []

            for entry in all_entries:
                r_name = entry.get("root_upload_name")
                r_path = (entry.get("relative_path_in_archive") or "").replace(os.path.sep, '/').strip('/')
                b_name = entry.get("base_filename")
                is_in_scope = False
                if is_global_scan:
                    if include_sub_folders:
                        is_in_scope = True  # Affects all files and all folders (their entries)
                    else:  # Only affect top-level 'loose' files (where root_upload_name is the file itself)
                        if b_name != "_DIR_" and r_path == "" and r_name == b_name:
                            is_in_scope = True
                else:  # Specific target_path provided, and we have target_entry_found
                    if r_name != target_entry_found.get("root_upload_name"):
                        continue
                    is_targeting_full_root_upload = (
                            target_entry_found.get("relative_path_in_archive") == "" and
                            (target_entry_found.get("base_filename") == "_DIR_" or
                             target_entry_found.get("base_filename") == target_entry_found.get("root_upload_name") or
                             target_entry_found.get("base_filename") == "_UNKNOWN_")
                    )
                    if is_targeting_full_root_upload:  # Target is a top-level root upload (e.g., 'MyUpload' as folder or file)
                        if include_sub_folders:
                            # If target is 'MyUpload' as root and include_sub_folders is True, delete everything under it
                            # This covers the target entry itself and all its children
                            current_entry_full_relative_path_from_root = f"{r_path}/{b_name}" if b_name != "_DIR_" else r_path
                            current_entry_full_relative_path_from_root = re.sub(r'/{2,}', '/',
                                                                                current_entry_full_relative_path_from_root).strip(
                                '/')
                            # Check if current entry is the root itself (r_path is empty)
                            if r_name == normalized_target_path and r_path == "":
                                is_in_scope = True
                            # Check if current entry is inside the root upload
                            elif r_name == normalized_target_path and r_path.startswith(
                                    normalized_target_path + '/') or (
                                    r_path != "" and r_name == normalized_target_path):  # Ensure it's under the targeted root
                                is_in_scope = True
                        else:  # Only affect files directly under the targeted root upload (no sub-folders)
                            if r_name == normalized_target_path and b_name != "_DIR_" and r_path == "":
                                is_in_scope = True  # Only files directly under the targeted root upload name
                    else:  # Target is a specific nested file or folder (e.g., 'Root/Sub/File.txt' or 'Root/Sub')
                        target_is_folder_from_found = (target_entry_found.get("base_filename") == "_DIR_")
                        if target_is_folder_from_found:  # The target specified by user is a specific nested folder
                            # Example: target_path='TorBrowser/Data'
                            if include_sub_folders:
                                # Delete target folder entry and everything inside it recursively
                                current_entry_full_relative_path = f"{r_name}/{r_path}"
                                if b_name != "_DIR_":
                                    current_entry_full_relative_path = f"{current_entry_full_relative_path}/{b_name}"
                                current_entry_full_relative_path = re.sub(r'/{2,}', '/',
                                                                          current_entry_full_relative_path).strip('/')
                                target_full_relative_path = f"{target_entry_found.get('root_upload_name')}/{target_entry_found.get('relative_path_in_archive')}"
                                target_full_relative_path = re.sub(r'/{2,}', '/', target_full_relative_path).strip('/')
                                if current_entry_full_relative_path == target_full_relative_path:  # The folder itself
                                    is_in_scope = True
                                elif current_entry_full_relative_path.startswith(
                                        target_full_relative_path + '/'):  # Anything inside it
                                    is_in_scope = True
                            else:  # Only affect files directly in the target folder (no sub-folders/their entries)
                                # Example: target_path='TorBrowser/Data', include_sub_folders=False
                                # Only 'file_in_data.txt' should be affected. 'TorBrowser/Data/Browser' should not.
                                if b_name != "_DIR_" and \
                                        r_name == target_entry_found.get("root_upload_name") and \
                                        r_path == target_entry_found.get("relative_path_in_archive"):
                                    is_in_scope = True  # Direct files only
                        else:  # The target specified by user is a specific nested file
                            # Must be an exact match for the identified file
                            if r_name == target_entry_found.get("root_upload_name") and \
                                    b_name == target_entry_found.get("base_filename") and \
                                    r_path == target_entry_found.get("relative_path_in_archive"):
                                is_in_scope = True
                if is_in_scope:
                    items_to_delete_keys.add((r_name, r_path, b_name))
                    # Prepare item data for the paginator
                    item_data_for_display = {
                        'is_folder': (b_name == "_DIR_"),
                        'root_name': r_name,
                        'relative_path': r_path,
                        'base_filename': b_name,
                        'original_root_name': entry.get("original_root_name", ""),
                        'is_nicknamed': entry.get("is_nicknamed", False),
                        'original_base_filename': entry.get("original_base_filename", ""),
                        'is_base_filename_nicknamed': entry.get("is_base_filename_nicknamed", False),
                    }
                    # Add dummy values for file-specific fields if it's not a folder, for paginator compatibility
                    if b_name != "_DIR_":
                        item_data_for_display.update({
                            'total_expected_parts': 1,
                            'current_parts_found_set': {1},
                            'part_discord_info': {},
                            'actual_found_parts_for_display': 1,
                            'is_damaged_by_discord_absence': False
                        })
                    deleted_items_for_paginator.append(item_data_for_display)

            if not items_to_delete_keys:
                if is_global_scan:
                    await interaction.followup.send(
                        content=f"{user_mention}, No items found in the entire database matching the criteria. No action taken.",
                        ephemeral=False)
                    self.log.info(f">>> [DELETE] INFO: No items found for global delete scan.")
                else:
                    await interaction.followup.send(
                        content=f"{user_mention}, No items found at '{target_path}' (and its sub-folders if specified) matching the criteria. No action taken.",
                        ephemeral=False)
                    self.log.info(f">>> [DELETE] INFO: No items found for '{target_path}'.")
                return

            initial_response_content = f"{user_mention}, Found {len(items_to_delete_keys)} item(s) to delete. Attempting to remove associated Discord messages and database entries (this may take a moment)..."
            await interaction.followup.send(content=initial_response_content, ephemeral=False)

            total_db_entries_deleted_count = 0
            if items_to_delete_keys:
                # Convert items_to_delete_keys set to a list of dicts for _db_delete_sync
                bulk_deletion_conditions = [
                    {"root_upload_name": r, "relative_path_in_archive": p, "base_filename": f}
                    for r, p, f in items_to_delete_keys
                ]
                try:
                    total_db_entries_deleted_count = await self.db._db_delete_sync(DATABASE_FILE, bulk_deletion_conditions)
                    self.log.info(f"Successfully removed {total_db_entries_deleted_count} database entries in bulk.")
                except Exception as e:
                    self.log.error(f"ERROR: Failed to perform bulk database deletion: {e}")
            else:
                self.log.info("No bulk database deletion conditions to process.")

            # Add Discord deletion tasks to queue
            for item_key in items_to_delete_keys:
                r_name, r_path, b_name = item_key
                if b_name != "_DIR_":  # Only files have associated Discord messages
                    item_db_entries = [
                        entry for entry in all_entries
                        if entry.get("root_upload_name") == r_name and
                           (entry.get("relative_path_in_archive") or "").replace(os.path.sep, '/').strip(
                               '/') == r_path and
                           entry.get("base_filename") == b_name
                    ]
                    for part_entry in item_db_entries:
                        message_id = part_entry.get("message_id")
                        channel_id = part_entry.get("channel_id")
                        if message_id and message_id != "0" and channel_id and channel_id != "0":
                            try:
                                await self._add_to_deletion_queue(int(channel_id), int(message_id))
                            except ValueError:
                                self.log.warning(
                                    f"WARNING: Invalid message_id or channel_id '{message_id}'/'{channel_id}' for part of '{b_name}'. Skipping Discord deletion for this part.")

            # Ensure all deletion tasks are processed before continuing to cleanup folders from DB
            await self.delete_task_queue.join()  # Wait for all Discord deletions to complete
            self.log.info(f"Completed Discord message deletion attempts via worker.")

            # --- Post-deletion empty folder cleanup ---
            self.log.info(">>> [DELETE] Starting post-deletion empty folder cleanup (scoped).")
            cleaned_up_folders_count = 0
            # NEW CONDITION: Skip cleanup if global scan AND not including sub-folders
            if is_global_scan and not include_sub_folders:
                self.log.info(
                    ">>> [DELETE] Skipping post-deletion empty folder cleanup due to global scan with include_sub_folders=False.")
                cleaned_up_folders_count = 0  # Ensure this is 0 if skipped
            else:
                cleanup_scope_root_name = None
                cleanup_scope_relative_path_prefix = None
                if not is_global_scan:
                    cleanup_scope_root_name = target_entry_found.get("root_upload_name")
                    target_base_filename = target_entry_found.get("base_filename")
                    target_relative_path = (target_entry_found.get("relative_path_in_archive") or "").replace(
                        os.path.sep,
                        '/').strip(
                        '/')
                    if target_base_filename == "_DIR_":
                        cleanup_scope_relative_path_prefix = target_relative_path
                    else:  # If target was a file, clean up its parent directories
                        cleanup_scope_relative_path_prefix = os.path.dirname(target_relative_path).replace(os.path.sep,
                                                                                                           '/').strip(
                            '/')
                while True:
                    current_entries = await self.db._db_read_sync(DATABASE_FILE, {})

                    # Re-implement _has_direct_children internally for cleanup if needed, or pass current_entries
                    # For this cleanup logic, we need to know if a *folder in the DB* is truly empty
                    # i.e., it has no other files or _DIR_ entries that use it as a parent (excluding its own _DIR_ entry)
                    def _is_db_folder_truly_empty(folder_r_name, folder_r_path, current_db_entries):
                        normalized_folder_r_path = (folder_r_path or "").replace(os.path.sep, '/').strip('/')
                        for entry_in_db in current_db_entries:
                            e_r_name = entry_in_db.get("root_upload_name")
                            e_r_path = (entry_in_db.get("relative_path_in_archive") or "").replace(os.path.sep,
                                                                                                   '/').strip('/')
                            e_b_name = entry_in_db.get("base_filename")
                            if e_r_name != folder_r_name:
                                continue
                            # Check if any other file or subfolder exists within this folder's path
                            # An item is "within" if its r_path is same OR starts with folder's r_path + '/'
                            if e_r_path == normalized_folder_r_path and e_b_name != "_DIR_":  # Direct file
                                return False
                            if e_r_path.startswith(normalized_folder_r_path + '/'):  # Nested item
                                # Ensure it's not the folder itself, which is handled by previous condition
                                return False
                        return True

                    all_folder_keys = set()
                    for entry in current_entries:
                        if entry.get("base_filename") == "_DIR_":
                            all_folder_keys.add((entry.get("root_upload_name"),
                                                 (entry.get("relative_path_in_archive") or "").replace(os.path.sep,
                                                                                                       '/').strip('/'),
                                                 "_DIR_"))
                    empty_folders_to_delete_in_this_round = set()
                    for folder_key in all_folder_keys:
                        f_r_name, f_r_path, _ = folder_key
                        if _is_db_folder_truly_empty(f_r_name, f_r_path, current_entries):
                            # Add to deletion list ONLY if it's within the originally defined cleanup scope
                            folder_key_root = folder_key[0]
                            folder_key_path = folder_key[1]
                            if cleanup_scope_root_name is None:  # Global cleanup (e.g., after 'delete .' with include_sub_folders=True)
                                empty_folders_to_delete_in_this_round.add(folder_key)
                            elif folder_key_root == cleanup_scope_root_name:
                                if not cleanup_scope_relative_path_prefix:  # Cleanup for a top-level root upload or global
                                    empty_folders_to_delete_in_this_round.add(folder_key)
                                elif folder_key_path == cleanup_scope_relative_path_prefix or \
                                        folder_key_path.startswith(cleanup_scope_relative_path_prefix + '/'):
                                    empty_folders_to_delete_in_this_round.add(folder_key)
                    if not empty_folders_to_delete_in_this_round:
                        break
                    bulk_folder_deletion_conditions = []
                    for folder_key_to_delete in empty_folders_to_delete_in_this_round:
                        f_r_name, f_r_path, _ = folder_key_to_delete
                        bulk_folder_deletion_conditions.append({
                            "root_upload_name": f_r_name,
                            "relative_path_in_archive": f_r_path,
                            "base_filename": "_DIR_"
                        })
                    if bulk_folder_deletion_conditions:
                        try:
                            deleted_this_round = await self.db._db_delete_sync(DATABASE_FILE,
                                                                            bulk_folder_deletion_conditions)
                            cleaned_up_folders_count += deleted_this_round
                            self.log.info(f"Cleaned up {deleted_this_round} empty folder entries in this round.")
                        except Exception as e:
                            self.log.error(f"ERROR: Failed to cleanup empty folders in bulk: {e}")
            self.log.info(
                f">>> [DELETE] Completed post-deletion empty folder cleanup. Removed {cleaned_up_folders_count} empty folder entries.")

            # --- Replacing direct message with Paginator for deleted items ---
            if deleted_items_for_paginator:
                # Send a summary message before the paginator for context
                summary_message_prefix = (
                    f"{user_mention}, **Deletion Summary:**\n"
                    f"Successfully removed {len(items_to_delete_keys)} item(s) (total {total_db_entries_deleted_count} database entries)."
                )
                if cleaned_up_folders_count > 0:
                    summary_message_prefix += f"\nAlso, {cleaned_up_folders_count} empty folder entries were cleaned up from the database."
                summary_message_prefix += "\n\nHere is a list of the items that were removed:"

                await interaction.followup.send(content=summary_message_prefix, ephemeral=False)

                paginator = ListViewPaginator(
                    user_mention=user_mention,
                    all_items_to_display=deleted_items_for_paginator,
                    total_pages=1,  # Paginator will update this internally
                    items_per_page=25,  # Number of items per page
                    initial_page=0,
                    target_path_display=None,  # Not relevant for a deletion list
                    show_original_name=True,  # Always show original name for deleted items if available
                    bot_instance=self,
                    check_existance=False,  # No need to check existence for deleted items
                    check_all_pre_checked=True,  # No need to re-check existence in paginator
                    is_deletion_list=True  # New flag to indicate this is a deletion list
                )
                paginator.total_pages = (
                                                    len(paginator.all_items_to_display) + paginator.items_per_page - 1) // paginator.items_per_page
                if paginator.total_pages == 0: paginator.total_pages = 1

                initial_content_paginator = await paginator._get_page_content(0, interaction)
                # Send the paginator as a new followup message
                sent_message_paginator = await interaction.followup.send(content=initial_content_paginator,
                                                                         view=paginator)
                paginator.message = sent_message_paginator
            else:
                final_message = (
                    f"{user_mention}, No items were found to be deleted from '{target_path if not is_global_scan else 'the entire database'}'. "
                    f"Total {total_db_entries_deleted_count} database entries and {cleaned_up_folders_count} empty folder entries were processed/cleaned up."
                )
                await interaction.followup.send(content=final_message, ephemeral=False)

            self.log.info(
                f">>> [DELETE] Completed deletion of {len(items_to_delete_keys)} items ({total_db_entries_deleted_count} DB entries) for '{target_path}'.")
        except Exception as e:
            self.log.error(f">>> [DELETE] ERROR: General exception in delete: {e}")
            self.log.error(traceback.format_exc())
            await interaction.followup.send(
                content=f"{user_mention}, An unexpected error occurred while attempting to delete items: {e}",
                ephemeral=False)
    async def _check_discord_message_existence(self, channel_id: int, message_id: int) -> bool:
        """
        Checks if a Discord message attachment still exists on Discord.
        Returns True if the message exists and is accessible, False otherwise.
        """
        # Removed `asyncio.sleep` here; relying on batching and discord.py's internal rate limiting.
        try:
            channel = self.get_channel(channel_id)
            if not channel:
                channel = await self.fetch_channel(channel_id)
            if not channel:
                self.log.debug(f"DEBUG: Channel {channel_id} not found/accessible for message {message_id}.")
                return False
            if not isinstance(channel,
                              (discord.TextChannel, discord.VoiceChannel, discord.Thread, discord.StageChannel)):
                self.log.debug(f"DEBUG: Channel {channel_id} is not a text-based channel for message {message_id}.")
                return False
            message = await channel.fetch_message(message_id)
            return True
        except discord.NotFound:
            self.log.debug(f"DEBUG: Discord message {message_id} in channel {channel_id} not found (deleted).")
            return False
        except discord.Forbidden:
            self.log.debug(f"DEBUG: Bot forbidden from accessing channel {channel_id} for message {message_id}.")
            return False
        except discord.HTTPException as e:
            self.log.debug(f"DEBUG: HTTPException checking message {message_id} in channel {channel_id}: {e}")
            return False
        except Exception as e:
            self.log.error(f"ERROR: Unexpected error checking message {message_id} in channel {channel_id}: {e}")
            self.log.error(traceback.format_exc())
            return False

    async def remove_damaged(self, interaction: discord.Interaction, target_path: str, database_file: str,
                             include_sub_folders: bool = True, check_attachments_existance: bool = False):
        """
        Scans for damaged file entries (missing Discord messages for parts or corrupted entries)
        and removes them from the database.
        Includes progress updates during attachment existence checks.
        """
        user_mention = interaction.user.mention
        if self.download_semaphore._value != 3 or self.upload_semaphore._value != 3:
            await interaction.followup.send(
                content=f"{user_mention}, System is currently busy with uploads or downloads. Please try listing files again later.",
                ephemeral=False)
            return
        self.log.info(f">>> [REMOVE_DAMAGED] Scanning for damaged items in '{target_path}' from '{database_file}'. "
                      f"Include sub-folders: {include_sub_folders}. Check attachments: {check_attachments_existance}.")
        try:
            if not database_file.lower().endswith('.db'):
                database_file += '.db'
            DATABASE_FILE = os.path.abspath(os.path.normpath(database_file))
            if not os.path.exists(DATABASE_FILE):
                await interaction.followup.send(
                    content=f"{user_mention}, The database file '{database_file}' was not found. Cannot check for damaged items.",
                    ephemeral=False)
                self.log.error(f">>> [REMOVE_DAMAGED] ERROR: Database file not found at '{DATABASE_FILE}'.")
                return
            all_entries = await self.db._db_read_sync(DATABASE_FILE, {})
            if not all_entries:
                await interaction.followup.send(
                    content=f'{user_mention}, No entries found in database: "{database_file}".', ephemeral=False)
                return
            normalized_target_path = os.path.normpath(target_path).replace(os.path.sep, '/').strip('/')
            is_global_scan = (normalized_target_path == '') or (normalized_target_path == '.')
            # We need to aggregate all parts of a file to check for damage
            # Dictionary key: (root_upload_name, relative_path_in_archive, base_filename)
            # Value: List of database entries (parts) for that file
            aggregated_data: Dict[tuple, List[Dict]] = {}
            # Keep track of actual root_upload_name and path_within_root_upload for targeted scan
            actual_root_name = None
            path_within_root_upload = None
            # For target-specific scans, find the exact target entry (file or folder)
            target_entry_found = None
            if not is_global_scan:
                # First, try to match as a root_upload_name (top-level item)
                if '/' not in normalized_target_path:
                    for entry in all_entries:
                        if entry.get("root_upload_name") == normalized_target_path:
                            target_entry_found = {
                                "root_upload_name": normalized_target_path,
                                "relative_path_in_archive": "",
                                "base_filename": entry.get("base_filename")
                            }  # Dummy base_filename, will be refined
                            actual_root_name = normalized_target_path
                            path_within_root_upload = ""
                            break
                # If not a root_upload_name or already found, try full path match
                if not target_entry_found:
                    for entry in all_entries:
                        r_name = entry.get("root_upload_name")
                        r_path = (entry.get("relative_path_in_archive") or "").replace(os.path.sep, '/').strip('/')
                        b_name = entry.get("base_filename")
                        current_item_conceptual_path = f"{r_name}/{r_path}"
                        if b_name != "_DIR_":
                            current_item_conceptual_path = f"{current_item_conceptual_path}/{b_name}"
                        current_item_conceptual_path = re.sub(r'/{2,}', '/', current_item_conceptual_path).strip(
                            '/')
                        if normalized_target_path == current_item_conceptual_path:
                            target_entry_found = entry
                            actual_root_name = r_name
                            # If target is a file, path_within_root is its relative_path_in_archive
                            # If target is a folder, path_within_root is its relative_path_in_archive
                            path_within_root_upload = r_path
                            break
            else:  # Global scan
                actual_root_name = None  # Not constrained to a single root
                path_within_root_upload = None  # Not constrained to a single path
            for entry in all_entries:
                root_name_entry = entry.get("root_upload_name")
                relative_path_entry = (entry.get("relative_path_in_archive") or "").replace(os.path.sep, '/').strip(
                    '/')
                base_filename_entry = entry.get("base_filename")
                if not root_name_entry or not base_filename_entry:
                    # Skip malformed entries
                    continue
                # We only aggregate data for files, as folders are not "damaged" in the same way
                # (they don't have parts, etc.). Folder entries are considered for removal based on scope later.
                if base_filename_entry != "_DIR_":
                    # Determine if the current file entry is within the target scope
                    is_in_scope = False
                    if is_global_scan:
                        if include_sub_folders:
                            is_in_scope = True
                        else:  # Only affect top-level 'loose' damaged files
                            if relative_path_entry == "" and root_name_entry == base_filename_entry:
                                is_in_scope = True
                    else:  # Specific target_path
                        if root_name_entry != actual_root_name:
                            continue  # Not even in the same top-level upload
                        if include_sub_folders:
                            # If target is a folder, check if current file is within or below it
                            if target_entry_found and target_entry_found.get("base_filename") == "_DIR_":
                                target_full_path_for_scope = f"{actual_root_name}/{path_within_root_upload}"
                                current_file_full_path = f"{root_name_entry}/{relative_path_entry}"
                                if current_file_full_path == target_full_path_for_scope or \
                                        current_file_full_path.startswith(target_full_path_for_scope + '/'):
                                    is_in_scope = True
                            # If target is a specific file, only match that file
                            elif target_entry_found and base_filename_entry == target_entry_found.get(
                                    "base_filename") and \
                                    relative_path_entry == target_entry_found.get("relative_path_in_archive"):
                                is_in_scope = True
                            elif target_entry_found and target_entry_found.get(
                                    "root_upload_name") == normalized_target_path:
                                # This handles cases where target_path IS the root upload name and it's a file or folder.
                                # If it's a folder, include all its contents. If a file, just that file.
                                # Example: target_path='MyRootFolder', include_sub_folders=True
                                # Example: target_path='MyFile.txt', include_sub_folders=True (only MyFile.txt is covered)
                                if normalized_target_path == root_name_entry:  # All files under this root
                                    is_in_scope = True
                        else:  # Not including sub-folders
                            # Only target files directly in the specified path (if target is a folder)
                            # Or only the specific file if target is a file
                            if target_entry_found and target_entry_found.get("base_filename") == "_DIR_":
                                if relative_path_entry == target_entry_found.get("relative_path_in_archive"):
                                    is_in_scope = True  # File is directly in the targeted folder
                            elif target_entry_found and base_filename_entry == target_entry_found.get(
                                    "base_filename") and \
                                    relative_path_entry == target_entry_found.get("relative_path_in_archive"):
                                is_in_scope = True  # Exact file match
                    if is_in_scope:
                        key = (root_name_entry, relative_path_entry, base_filename_entry)
                        if key not in aggregated_data:
                            aggregated_data[key] = []
                        aggregated_data[key].append(entry)
            if not aggregated_data:
                await interaction.followup.send(
                    content=f"{user_mention}, No file entries found matching the specified path and sub-folder criteria in '{database_file}'.",
                    ephemeral=False)
                return
            damaged_files_to_remove = []
            files_to_check_count = len(aggregated_data)  # Total number of unique files found for checking
            checked_files_progress = 0  # Counter for files processed for progress update
            PROGRESS_UPDATE_INTERVAL = 10  # Update progress every 10 files
            # Use this list to display what was removed
            removed_file_paths_display = []
            for (r_name, r_path, b_name), entries in aggregated_data.items():
                checked_files_progress += 1
                # Send progress update
                if checked_files_progress % PROGRESS_UPDATE_INTERVAL == 0 or checked_files_progress == files_to_check_count:
                    # Changed to followup.send()
                    await interaction.followup.send(
                        content=f"{user_mention}, Scanning for damaged items... {checked_files_progress}/{files_to_check_count} files checked."
                    )
                total_parts = int(
                    entries[0].get("total_parts", 1))  # Assume total_parts is same for all parts of a file
                # Check for missing parts
                found_part_numbers = {int(e.get("part_number")) for e in entries if e.get("part_number")}
                if len(found_part_numbers) != total_parts or not all(
                        p in found_part_numbers for p in range(1, total_parts + 1)):
                    self.log.warning(
                        f"Damaged: Missing parts for '{r_name}/{r_path}/{b_name}'. Found {sorted(list(found_part_numbers))}, expected {total_parts}.")
                    for entry_to_add in entries:
                        damaged_files_to_remove.append((entry_to_add))  # Add all parts to be removed
                    full_display_path = f"{r_name}/{r_path}/{b_name}"
                    full_display_path = re.sub(r'/{2,}', '/', full_display_path).strip('/')
                    removed_file_paths_display.append(f"`{full_display_path}` (missing parts)")
                    continue
                # Check Discord message existence (if enabled)
                if check_attachments_existance:
                    discord_check_tasks_for_file = []  # Tasks for the current file's parts
                    for entry_part in entries:
                        message_id = entry_part.get("message_id")
                        channel_id = entry_part.get("channel_id")
                        if message_id and message_id != "0" and channel_id and channel_id != "0":
                            try:
                                discord_check_tasks_for_file.append(
                                    self._check_discord_message_existence(int(channel_id), int(message_id)))
                            except ValueError:
                                self.log.warning(
                                    f"WARNING: Invalid message_id or channel_id '{message_id}'/'{channel_id}' for part of '{b_name}'. Assuming damaged.")
                                # Treat as damaged if IDs are invalid
                                for entry_to_add in entries:
                                    if entry_to_add not in damaged_files_to_remove:  # Avoid duplicates
                                        damaged_files_to_remove.append((entry_to_add))
                                full_display_path = f"{r_name}/{r_path}/{b_name}"
                                full_display_path = re.sub(r'/{2,}', '/', full_display_path).strip('/')
                                if f"`{full_display_path}` (Discord message missing or invalid)" not in removed_file_paths_display:
                                    removed_file_paths_display.append(
                                        f"`{full_display_path}` (Discord message missing or invalid)")
                                continue  # Move to next file if already marked damaged
                    # Execute Discord checks in batches
                    is_damaged_by_discord_absence = False
                    if discord_check_tasks_for_file:
                        for i in range(0, len(discord_check_tasks_for_file), self.batch_size_discord_checks):
                            batch = discord_check_tasks_for_file[i:i + self.batch_size_discord_checks]
                            results = await asyncio.gather(*batch, return_exceptions=True)
                            for j, result in enumerate(results):
                                if isinstance(result, Exception) or not result:  # If exception or message not found
                                    self.log.warning(
                                        f"Damaged: Discord message missing for part of '{r_name}/{r_path}/{b_name}'.")
                                    is_damaged_by_discord_absence = True
                                    # We don't decrement actual_found_parts here, that's done in list_files.
                                    # Here, we just mark for removal.
                                    break  # No need to check more parts for this file if one is missing
                            if is_damaged_by_discord_absence:
                                for entry_to_add in entries:
                                    if entry_to_add not in damaged_files_to_remove:  # Avoid duplicates
                                        damaged_files_to_remove.append((entry_to_add))
                                full_display_path = f"{r_name}/{r_path}/{b_name}"
                                full_display_path = re.sub(r'/{2,}', '/', full_display_path).strip('/')
                                if f"`{full_display_path}` (Discord message missing or invalid)" not in removed_file_paths_display:
                                    removed_file_paths_display.append(
                                        f"`{full_display_path}` (Discord message missing or invalid)")
                                break  # Stop processing batches for this file
                            # Add a delay between batches
                            if i + self.batch_size_discord_checks < len(discord_check_tasks_for_file):
                                await asyncio.sleep(self.batch_delay_discord_checks)
            if not damaged_files_to_remove:
                # Final message if no damaged files were found
                # Changed to followup.send()
                await interaction.followup.send(
                    content=f"{user_mention}, No damaged files found in '{target_path if not is_global_scan else 'the entire database'}'. Total files checked: {len(aggregated_data)}."
                )
                self.log.info(f">>> [REMOVE_DAMAGED] No damaged files found.")
                return
            # Perform bulk deletion of damaged files from the database
            bulk_deletion_conditions = []
            for entry_to_remove in damaged_files_to_remove:
                bulk_deletion_conditions.append({
                    "root_upload_name": entry_to_remove.get("root_upload_name"),
                    "relative_path_in_archive": entry_to_remove.get("relative_path_in_archive"),
                    "base_filename": entry_to_remove.get("base_filename"),
                    "part_number": entry_to_remove.get("part_number")
                })
            total_db_entries_deleted_count = 0
            if bulk_deletion_conditions:
                try:
                    total_db_entries_deleted_count = await self.db._db_delete_sync(DATABASE_FILE,
                                                                                bulk_deletion_conditions)
                    self.log.info(
                        f"Successfully removed {total_db_entries_deleted_count} damaged database entries in bulk.")
                except Exception as e:
                    self.log.error(f"ERROR: Failed to perform bulk database deletion for damaged files: {e}")
            else:
                self.log.info("No bulk database deletion conditions for damaged files to process.")
            # Post-deletion empty folder cleanup for remove_damaged (scoped to target if not global)
            self.log.info(">>> [REMOVE_DAMAGED] Starting post-deletion empty folder cleanup (scoped).")
            cleaned_up_folders_count = 0
            # If global scan and not including sub-folders, skip folder cleanup (consistent with delete)
            if is_global_scan and not include_sub_folders:
                self.log.info(
                    ">>> [REMOVE_DAMAGED] Skipping post-deletion empty folder cleanup due to global scan with include_sub_folders=False.")
                cleaned_up_folders_count = 0
            else:
                cleanup_scope_root_name = None
                cleanup_scope_relative_path_prefix = None
                if not is_global_scan:
                    cleanup_scope_root_name = actual_root_name
                    # If target was a file, clean up its parent directories
                    # If target was a folder, clean up within that folder's scope
                    if target_entry_found and target_entry_found.get("base_filename") != "_DIR_":
                        cleanup_scope_relative_path_prefix = os.path.dirname(path_within_root_upload).replace(
                            os.path.sep, '/').strip('/')
                    elif target_entry_found and target_entry_found.get("base_filename") == "_DIR_":
                        cleanup_scope_relative_path_prefix = path_within_root_upload
                while True:
                    current_entries = await self.db._db_read_sync(DATABASE_FILE, {})

                    def _is_db_folder_truly_empty(folder_r_name, folder_r_path, current_db_entries):
                        normalized_folder_r_path = (folder_r_path or "").replace(os.path.sep, '/').strip('/')
                        for entry_in_db in current_db_entries:
                            e_r_name = entry_in_db.get("root_upload_name")
                            e_r_path = (entry_in_db.get("relative_path_in_archive") or "").replace(os.path.sep,
                                                                                                   '/').strip('/')
                            e_b_name = entry_in_db.get("base_filename")
                            if e_r_name != folder_r_name:
                                continue
                            if e_r_path == normalized_folder_r_path and e_b_name != "_DIR_":  # Direct file
                                return False
                            if e_r_path.startswith(normalized_folder_r_path + '/'):  # Nested item
                                return False
                        return True

                    all_folder_keys = set()
                    for entry in current_entries:
                        if entry.get("base_filename") == "_DIR_":
                            all_folder_keys.add((entry.get("root_upload_name"),
                                                 (entry.get("relative_path_in_archive") or "").replace(os.path.sep,
                                                                                                       '/').strip(
                                                     '/'),
                                                 "_DIR_"))
                    empty_folders_to_delete_in_this_round = set()
                    for folder_key in all_folder_keys:
                        f_r_name, f_r_path, _ = folder_key
                        if _is_db_folder_truly_empty(f_r_name, f_r_path, current_entries):
                            folder_key_root = folder_key[0]
                            folder_key_path = folder_key[1]
                            if cleanup_scope_root_name is None:
                                empty_folders_to_delete_in_this_round.add(folder_key)
                            elif folder_key_root == cleanup_scope_root_name:
                                if not cleanup_scope_relative_path_prefix:
                                    empty_folders_to_delete_in_this_round.add(folder_key)
                                elif folder_key_path == cleanup_scope_relative_path_prefix or \
                                        folder_key_path.startswith(cleanup_scope_relative_path_prefix + '/'):
                                    empty_folders_to_delete_in_this_round.add(folder_key)
                    if not empty_folders_to_delete_in_this_round:
                        break
                    bulk_folder_deletion_conditions = []
                    for folder_key_to_delete in empty_folders_to_delete_in_this_round:
                        f_r_name, f_r_path, _ = folder_key_to_delete
                        bulk_folder_deletion_conditions.append({
                            "root_upload_name": f_r_name,
                            "relative_path_in_archive": f_r_path,
                            "base_filename": "_DIR_"
                        })
                    if bulk_folder_deletion_conditions:
                        try:
                            deleted_this_round = await self.db._db_delete_sync(DATABASE_FILE,
                                                                            bulk_folder_deletion_conditions)
                            cleaned_up_folders_count += deleted_this_round
                            self.log.info(f"Cleaned up {deleted_this_round} empty folder entries in this round.")
                        except Exception as e:
                            self.log.error(f"ERROR: Failed to cleanup empty folders in bulk (remove_damaged): {e}")
            self.log.info(
                f">>> [REMOVE_DAMAGED] Completed post-deletion empty folder cleanup. Removed {cleaned_up_folders_count} empty folder entries.")
            removed_summary_display = ""
            if removed_file_paths_display:
                removed_summary_display = "\n**Removed Damaged Items:**\n" + "\n".join(
                    sorted(list(set(removed_file_paths_display))))
                if len(removed_summary_display) > 1900:
                    removed_summary_display = removed_summary_display[:1890] + "\n... (truncated list)"
            final_message = (
                f"{user_mention}, Removed {len(damaged_files_to_remove)} damaged database entries (from {files_to_check_count} files checked) "
                f"from '{target_path if not is_global_scan else 'the entire database'}'.{removed_summary_display}"
            )
            if cleaned_up_folders_count > 0:
                final_message += f"\nAlso, {cleaned_up_folders_count} empty folder entries were cleaned up from the database."
            # Changed to followup.send()
            await interaction.followup.send(content=final_message)
            self.log.info(f">>> [REMOVE_DAMAGED] Completed. Removed {len(damaged_files_to_remove)} damaged entries.")
        except Exception as e:
            self.log.error(f">>> [REMOVE_DAMAGED] ERROR: General exception in remove_damaged: {e}")
            self.log.error(traceback.format_exc())
            await interaction.followup.send(
                content=f"{user_mention}, An unexpected error occurred while attempting to remove damaged items: {e}",
                ephemeral=False)

    async def reindex_database(self, interaction: discord.Interaction, database_file: str):
        """
        Reindexes the specified database file to optimize performance.
        """
        user_mention = interaction.user.mention
        self.log.info(f">>> [REINDEX] Attempting to reindex database: '{database_file}'")
        try:
            if not database_file.lower().endswith('.db'):
                database_file += '.db'
            DATABASE_FILE = os.path.abspath(os.path.normpath(database_file))
            if not os.path.exists(DATABASE_FILE):
                await interaction.followup.send(
                    content=f"{user_mention}, The database file '{database_file}' was not found. Cannot reindex.",
                    ephemeral=False)
                self.log.error(f">>> [REINDEX] ERROR: Database file not found at '{DATABASE_FILE}'.")
                return
            # Changed to followup.send()
            await interaction.followup.send(
                content=f"{user_mention}, reindexing database (it may take some time)... `{database_file}`.",
                ephemeral=False)
            # Execute the reindex operation using PowerDB.
            await self.db._db_vacuum_sync(database_file)
            # Changed to followup.send()
            await interaction.followup.send(
                content=f"{user_mention}, Successfully reindexed database: `{database_file}`.")
            self.log.info(f">>> [REINDEX] Successfully reindexed database: '{database_file}'.")
        except Exception as e:
            self.log.error(f">>> [REINDEX] ERROR: An error occurred during reindex: {e}")
            self.log.error(traceback.format_exc())
            await interaction.followup.send(
                content=f"{user_mention}, An unexpected error occurred while reindexing `{database_file}`: {e}",
                ephemeral=False)

    async def _perform_existence_checks(
            self, interaction: discord.Interaction, user_mention: str,
            all_display_items_raw: list, check_all: bool, check_all_versions: bool
    ):
        """
        Performs Discord existence checks for files in all_display_items_raw.
        Updates 'actual_found_parts_for_display' and 'is_damaged_by_discord_absence' for each file.
        """
        # Aggregate all file entries to check
        items_to_check: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}  # key=(root, rel_path, filename, version)

        for display_item in all_display_items_raw:
            if display_item.get('is_folder'):
                continue  # Skip folders

            key = (display_item['root_name'], display_item['relative_path'], display_item['base_filename'],
                   display_item['version'])
            items_to_check[key] = {
                'total_expected_parts': display_item.get('total_expected_parts', 1),
                'current_parts_found_set': set(display_item.get('current_parts_found_set', set())),
                'part_discord_info': dict(display_item.get('part_discord_info', {})),
                'actual_found_parts_for_display': 0,
                'is_damaged_by_discord_absence': False
            }

        total_files = len(items_to_check)
        await interaction.followup.send(
            content=f"{user_mention}, performing Discord existence checks for {total_files} file(s)..."
        )

        for idx, (file_key, file_data) in enumerate(items_to_check.items(), start=1):
            temp_found_parts = set(file_data['current_parts_found_set'])
            discord_tasks = []

            # Prepare Discord check tasks
            for part_num, (channel_id, message_id) in file_data['part_discord_info'].items():
                if channel_id != 0 and message_id != 0:
                    discord_tasks.append((part_num, self._check_discord_message_existence(channel_id, message_id)))
                else:
                    temp_found_parts.discard(part_num)
                    file_data['is_damaged_by_discord_absence'] = True

            # Run checks in batches
            batch_size = self.batch_size_discord_checks
            for i in range(0, len(discord_tasks), batch_size):
                batch = discord_tasks[i:i + batch_size]
                part_nums, tasks = zip(*batch) if batch else ([], [])
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for part_num, result in zip(part_nums, results):
                    if isinstance(result, Exception) or not result:
                        temp_found_parts.discard(part_num)
                        file_data['is_damaged_by_discord_absence'] = True
                        self.log.warning(f"Discord check failed for part {part_num} of '{file_key}'")

                if i + batch_size < len(discord_tasks):
                    await asyncio.sleep(self.batch_delay_discord_checks)

            file_data['actual_found_parts_for_display'] = len(temp_found_parts)

            if idx % 10 == 0 or idx == total_files:
                await interaction.followup.send(
                    content=f"{user_mention}, Discord existence check progress: {idx}/{total_files} files processed."
                )

        # Update display items with the results
        for display_item in all_display_items_raw:
            if display_item.get('is_folder'):
                continue
            key = (display_item['root_name'], display_item['relative_path'], display_item['base_filename'],
                   display_item['version'])
            if key in items_to_check:
                checked = items_to_check[key]
                display_item['actual_found_parts_for_display'] = checked['actual_found_parts_for_display']
                display_item['is_damaged_by_discord_absence'] = checked['is_damaged_by_discord_absence']
            else:
                # If item not checked, assume all parts exist
                display_item['actual_found_parts_for_display'] = display_item.get('total_expected_parts', 1)
                display_item['is_damaged_by_discord_absence'] = False
    async def _get_items_for_list_display(
            self,
            database_file: str,
            target_display_path: Optional[str],
            version_param: Optional[str],
            start_version_param: Optional[str],
            end_version_param: Optional[str],
            all_versions_param: bool,
            check_all_versions_param: bool
    ) -> List[Dict[str, Any]]:

        all_db_entries = await self.db._db_read_sync(database_file, {})
        if not all_db_entries:
            return []

        normalized_target = (target_display_path or "").strip("/")
        self.log.info(f"[GET ITEMS] Normalized target path: '{normalized_target}'")

        def version_allowed(v: str) -> bool:
            if all_versions_param:
                return True
            if version_param and v != version_param:
                return False
            if start_version_param and self.version_manager.parse_version(v) < self.version_manager.parse_version(start_version_param):
                return False
            if end_version_param and self.version_manager.parse_version(v) > self.version_manager.parse_version(end_version_param):
                return False
            return True

        aggregated: Dict[Tuple[str, str, str, bool], Dict[str, Any]] = {}

        for entry in all_db_entries:
            v = entry.get("version", "")
            if not version_allowed(v):
                continue

            root = entry.get("root_upload_name", "") or ""
            base = entry.get("base_filename", "") or ""
            raw = (entry.get("relative_path_in_archive") or "").strip("/")  # as stored in DB

            # --- Decide whether this DB row represents a folder or a file, and compute parent/name ---
            if base == "_DIR_":
                # DB row for a folder: relative_path_in_archive holds the folder's full path (relative to root)
                if raw == "" or raw == root:
                    # root-level folder entry (Takeout)
                    parent = ""  # folder sits at root
                    name = root  # display name should be root (Takeout)
                else:
                    parts = raw.split("/")
                    name = parts[-1]  # folder name (Profile)
                    parent = "/".join(parts[:-1])  # parent's path (e.g., "Takeout")
            else:
                # DB row for a file: relative_path_in_archive is the parent directory of the file
                parent = raw  # parent directory (could be "" for root)
                name = base  # filename

            # DEBUG each mapping (very helpful)
            self.log.debug(
                f"[GET ITEMS] DB row -> root='{root}' base='{base}' raw='{raw}'  => parent='{parent}' name='{name}' version='{v}'")

            # TARGET FILTER: only include items whose parent equals the requested target
            if parent != normalized_target:
                continue

            key = (root, parent, name, base == "_DIR_")
            if key not in aggregated:
                aggregated[key] = {
                    "is_folder": (base == "_DIR_"),
                    "root_name": root,
                    "relative_path": parent,
                    "base_filename": name,
                    "version": v,
                    "total_expected_parts": entry.get("total_parts", 1) if base != "_DIR_" else 0,
                    "current_parts_found_set": set(),
                    "part_discord_info": {},
                    "actual_found_parts_for_display": 0,
                    "is_damaged_by_discord_absence": False,
                    "total_versions": set(),
                }

            aggregated[key]["total_versions"].add(v)

            # Collect parts (files only)
            if base != "_DIR_":
                pn = int(entry.get("part_number", 0) or 0)
                mid = int(entry.get("message_id", 0) or 0)
                cid = int(entry.get("channel_id", 0) or 0)
                if pn:
                    aggregated[key]["current_parts_found_set"].add(pn)
                if mid and cid:
                    aggregated[key]["part_discord_info"][pn] = (cid, mid)

        # finalize versions count
        for item in aggregated.values():
            item["total_versions"] = len(item["total_versions"])

        result = list(aggregated.values())
        # sort: folders first, then alphabetical, then by version
        result.sort(key=lambda x: (
            not x["is_folder"],  # folders first (False < True)
            x["base_filename"].lower(),
            self.version_manager.parse_version(x.get("version", "0.0.0.0"))
        ))

        # debug dump
        self.log.info(f"[GET ITEMS] Final result count: {len(result)}")
        for r in result:
            self.log.info(f"[GET ITEMS] -> {r}")

        return result
    async def list_files(
            self, interaction: discord.Interaction, DB_FILE: str,
            target_display_path: Optional[str] = None,
            check_existance: bool = True,
            show_original_name: bool = False,
            check_all: bool = False,
            version: Optional[str] = None,
            start_version: Optional[str] = None,
            end_version: Optional[str] = None,
            all_versions: bool = False,
            check_all_versions: bool = False
    ):
        user_mention = interaction.user.mention

        if self.download_semaphore._value != 3 or self.upload_semaphore._value != 3:
            await interaction.followup.send(
                content=f"{user_mention}, System is currently busy with uploads or downloads. Please try again later.",
                ephemeral=False
            )
            return

        self.log.info(f">>> [LIST FILES] Listing target: '{target_display_path or ''}' "
                      f"(version={version}, start={start_version}, end={end_version}, all_versions={all_versions})")

        try:
            if not DB_FILE.lower().endswith('.db'):
                DB_FILE += '.db'
            DATABASE_FILE = os.path.abspath(os.path.normpath(DB_FILE))
            if not os.path.exists(DATABASE_FILE):
                await interaction.followup.send(
                    content=f"{user_mention}, the database file '{DB_FILE}' was not found.", ephemeral=False
                )
                self.log.error(f">>> [LIST FILES] Database file not found: '{DATABASE_FILE}'")
                return

            all_entries_raw = await self.db._db_read_sync(DATABASE_FILE, {})
            if not all_entries_raw:
                await interaction.followup.send(
                    content=f"{user_mention}, no files or folders have been uploaded yet.", ephemeral=False
                )
                self.log.info(f">>> [LIST FILES] No entries found in database.")
                return

            # --- Validate version params ---
            if version and (start_version or end_version or all_versions):
                await interaction.followup.send(
                    f"{user_mention}, Error: Cannot combine single version with range or all_versions.", ephemeral=False
                )
                return
            if (start_version or end_version) and not (start_version and end_version):
                await interaction.followup.send(
                    f"{user_mention}, Error: Both start_version and end_version must be specified for a range.",
                    ephemeral=False
                )
                return
            if start_version and end_version and self.version_manager.parse_version(start_version) >= self.version_manager.parse_version(end_version):
                await interaction.followup.send(
                    f"{user_mention}, Error: start_version must be earlier than end_version.", ephemeral=False
                )
                return

            # --- Get display items ---
            all_display_items_raw = await self._get_items_for_list_display(
                DATABASE_FILE, target_display_path,
                version, start_version, end_version, all_versions, check_all_versions
            )

            if not all_display_items_raw:
                await interaction.followup.send(
                    content=f"{user_mention}, No items found for '{target_display_path or 'All Roots'}'.",
                    ephemeral=False
                )
                self.log.info(f">>> [LIST FILES] No displayable items for target.")
                return

            # --- Existence checks ---
            if check_existance:
                await self._perform_existence_checks(interaction, user_mention, all_display_items_raw, check_all,
                                                     check_all_versions)

            # --- Pagination ---
            items_per_page = 25 if not show_original_name else 10
            total_pages = (len(all_display_items_raw) + items_per_page - 1) // items_per_page

            self.log.info(f"=== list_files DEBUG ===")
            self.log.info(f"target_display_path: {target_display_path}")
            self.log.info(f"Items to display: {len(all_display_items_raw)}")
            for item in all_display_items_raw:
                self.log.info(
                    f" - {item['root_name']}/{item['relative_path']} ({item['base_filename']}) v{item['version']}")
            self.log.info("=== END DEBUG ===")

            paginator = ListViewPaginator(
                user_mention=user_mention,
                all_items_to_display=all_display_items_raw,
                total_pages=total_pages,
                items_per_page=items_per_page,
                initial_page=0,
                target_path_display=target_display_path or "",  # ✅ ROOT IS EMPTY STRING
                show_original_name=show_original_name,
                check_existance=check_existance,
                bot_instance=self,
                check_all_pre_checked=check_all,
                is_deletion_list=False
            )

            initial_content = await paginator._get_page_content(0, interaction)
            sent_message_paginator = await interaction.followup.send(content=initial_content, view=paginator)
            paginator.message = sent_message_paginator

            self.log.info(f">>> [LIST FILES] Sent paginated list. Total pages: {total_pages}")

        except Exception as e:
            self.log.error(f">>> [LIST FILES] ERROR: {e}")
            self.log.error(traceback.format_exc())
            await interaction.followup.send(content=f"{user_mention}, unexpected error: {e}", ephemeral=False)


class GoToPageModal(discord.ui.Modal, title="Go to Page"):
    def __init__(self, paginator_view: 'ListViewPaginator'):
        super().__init__()
        self.paginator_view = paginator_view
        self.page_number_input = discord.ui.TextInput(
            label="Page Number",
            placeholder=f"Enter a number between 1 and {paginator_view.total_pages}",
            required=True,
            max_length=5  # Max pages reasonable for typical Discord use, adjust if needed
        )
        self.add_item(self.page_number_input)
    async def on_submit(self, interaction: discord.Interaction):
        try:
            page_num = int(self.page_number_input.value) - 1  # Convert to 0-indexed
            if 0 <= page_num < self.paginator_view.total_pages:
                self.paginator_view.current_page = page_num
                await self.paginator_view.update_message(interaction)
                # noinspection PyUnresolvedReferences
                await interaction.response.send_message(f"Navigated to page {page_num + 1}.", ephemeral=False)
            else:
                # noinspection PyUnresolvedReferences
                await interaction.response.send_message(
                    f"Invalid page number. Please enter a number between 1 and {self.paginator_view.total_pages}.",
                    ephemeral=False
                )
        except ValueError:
            # noinspection PyUnresolvedReferences
            await interaction.response.send_message("Invalid input. Please enter a valid number.", ephemeral=False)
        except Exception as e:
            print(f"Error in GoToPageModal on_submit: {e}")
            # noinspection PyUnresolvedReferences
            await interaction.response.send_message("An unexpected error occurred.", ephemeral=False)

class DecryptionPasswordModal(discord.ui.Modal, title="Enter Decryption Passwords"):
    def __init__(self, bot_instance: 'FileBotAPI', original_interaction: discord.Interaction,
                 required_passwords_info: List[Dict[str, Any]], database_file: str,
                 target_path: str, download_folder: str,
                 version_param: Optional[str] = None, # NEW
                 start_version_param: Optional[str] = None, # NEW
                 end_version_param: Optional[str] = None, # NEW
                 all_versions_param: bool = False, # NEW
                 can_apply_version_filters: bool = False, # NEW
                 initial_passwords: Optional[Dict[Tuple[str, str, str, str], str]] = None, # Key is tuple
                 error_messages: Optional[Dict[Tuple[str, str, str, str], str]] = None): # Key is tuple
        super().__init__(timeout=300)
        self.bot = bot_instance
        self.original_interaction = original_interaction # Store the original command interaction
        self.required_passwords_info = required_passwords_info
        self.database_file = database_file
        self.target_path = target_path
        self.download_folder = download_folder
        # New versioning parameters for re-passing
        self.version_param = version_param
        self.start_version_param = start_version_param
        self.end_version_param = end_version_param
        self.all_versions_param = all_versions_param
        self.can_apply_version_filters = can_apply_version_filters

        self.entered_passwords: Dict[Tuple[str, str, str, str], str] = initial_passwords or {}
        self.error_messages: Dict[Tuple[str, str, str, str], str] = error_messages or {}
        self.decrypted_keys: Dict[Tuple[str, str, str, str], bytes] = {} # Store derived keys per unique item+version

        self._add_password_inputs()

    def _add_password_inputs(self):
        self.clear_items()

        for item_info in self.required_passwords_info:
            root_upload_name = item_info['root_upload_name']
            relative_path_in_archive = item_info['relative_path_in_archive']
            base_filename = item_info['base_filename']
            version = item_info['version']

            display_name = item_info['display_name'] # This now includes version
            store_hash = item_info.get('store_hash_flag', False)

            item_key = (root_upload_name, relative_path_in_archive, base_filename, version) # Full key
            error_msg = self.error_messages.get(item_key, "")

            # --- MODIFIED: Ensure label is 45 characters or fewer for display_name ---
            label_prefix = "Password for: "
            max_label_length = 45 # Discord limit

            # Truncate display_name if too long, considering the prefix
            if len(label_prefix) + len(display_name) > max_label_length:
                # Calculate how much of display_name can fit
                available_space = max_label_length - len(label_prefix) - 3 # -3 for ellipsis
                if available_space < 0: available_space = 0 # Should not happen, but safety
                truncated_display_name = display_name[:available_space] + "..."
            else:
                truncated_display_name = display_name

            final_label = f"{label_prefix}{truncated_display_name}"

            placeholder_text = f"Password for {display_name}"
            if not store_hash:
                placeholder_text += " (Zero-Knowledge)"
            if error_msg:
                placeholder_text = f"INCORRECT: {error_msg}"

            # Generate a unique custom_id using a hash of the item_key to avoid exceeding 100 char limit
            # This is important if item_key parts (paths) become very long.
            unique_id_hash = hashlib.sha1(str(item_key).encode('utf-8')).hexdigest()
            custom_id = f"password_{unique_id_hash}" # Max 100 chars, so a hash is safe.

            text_input = discord.ui.TextInput(
                label=final_label,
                placeholder=placeholder_text,
                required=True,
                max_length=256,
                style=discord.TextStyle.short,
                custom_id=custom_id
            )
            if item_key in self.entered_passwords:
                text_input.default = self.entered_passwords[item_key]
            self.add_item(text_input)

    async def on_submit(self, interaction: discord.Interaction):
        self.bot.log.info("DecryptionPasswordModal submitted. Validating passwords...")
        # noinspection PyUnresolvedReferences
        await interaction.response.defer(ephemeral=True) # Defer the modal submission interaction

        new_entered_passwords: Dict[Tuple[str, str, str, str], str] = {}
        current_errors: Dict[Tuple[str, str, str, str], str] = {}
        temp_decrypted_keys: Dict[Tuple[str, str, str, str], bytes] = {}
        all_passwords_correct = True # This flag refers to successful *derivation/hash-check*, not final decryption

        for item_info in self.required_passwords_info:
            root_upload_name = item_info['root_upload_name']
            relative_path_in_archive = item_info['relative_path_in_archive']
            base_filename = item_info['base_filename']
            version = item_info['version']
            db_password_hash = item_info['password_seed_hash']
            store_hash = item_info.get('store_hash_flag', False)

            item_key = (root_upload_name, relative_path_in_archive, base_filename, version)

            unique_id_hash = hashlib.sha1(str(item_key).encode('utf-8')).hexdigest()
            custom_id = f"password_{unique_id_hash}"

            text_input: Optional[discord.ui.TextInput] = next(
                (c for c in self.children if
                 isinstance(c, discord.ui.TextInput) and c.custom_id == custom_id),
                None
            )

            if not text_input:
                self.bot.log.error(f"Could not find TextInput for {item_key} in modal with custom_id {custom_id}.")
                current_errors[item_key] = "Internal error: Input missing."
                all_passwords_correct = False
                continue

            entered_password = text_input.value
            new_entered_passwords[item_key] = entered_password


            if not entered_password:
                current_errors[item_key] = "Password cannot be empty."
                all_passwords_correct = False
                continue

            try:
                derived_key = self.bot._derive_key_from_seed(entered_password)
                provided_seed_hash = hashlib.sha256(entered_password.encode('utf-8')).hexdigest()

                if store_hash: # If hash is stored, validate against it
                    if provided_seed_hash == db_password_hash:
                        temp_decrypted_keys[item_key] = derived_key
                        self.bot.log.debug(f"Password for '{item_key}' is CORRECT (hash matched).")
                    else:
                        current_errors[item_key] = "Incorrect password."
                        all_passwords_correct = False
                        self.bot.log.warning(f"Password for '{item_key}' is INCORRECT (hash mismatch).")
                else: # Zero-knowledge: hash not stored, assume correct for now. Real test happens during download.
                    temp_decrypted_keys[item_key] = derived_key
                    self.bot.log.debug(f"Password for '{item_key}' accepted (zero-knowledge mode, no hash check).")

            except ValueError as e:
                current_errors[item_key] = f"Key derivation error: {e}"
                all_passwords_correct = False
                self.bot.log.error(f"Key derivation error for '{item_key}': {e}")
            except Exception as e:
                current_errors[item_key] = f"Unexpected error: {e}"
                all_passwords_correct = False
                self.bot.log.error(f"Unexpected error during password validation for '{item_key}': {e}")
                self.bot.log.error(traceback.format_exc())

        # Merge new_entered_passwords with existing self.entered_passwords
        # This ensures that if a user corrected one password but another was already correct,
        # the already correct one is retained.
        self.entered_passwords.update(new_entered_passwords)
        self.error_messages = current_errors
        self.decrypted_keys.update(temp_decrypted_keys)  # Update with successfully derived keys

        if all_passwords_correct:
            self.bot.log.info("All passwords correct. Initiating download.")
            # Delete the modal response (the original modal interaction response)
            await interaction.delete_original_response()
            # Re-call download_filea with the original interaction and all collected keys
            await self.bot.download_filea(
                self.original_interaction, # Use the original command interaction
                self.target_path,
                self.database_file,
                self.download_folder,
                decryption_password_seed=self.decrypted_keys,  # Pass all collected keys
                version_param=self.version_param,
                start_version_param=self.start_version_param,
                end_version_param=self.end_version_param,
                all_versions_param=self.all_versions_param,
                can_apply_version_filters=self.can_apply_version_filters
            )
        else:
            self.bot.log.warning("Some passwords were incorrect or derivation failed. Re-presenting modal via button.")
            new_modal = DecryptionPasswordModal(
                self.bot,
                self.original_interaction, # Still pass the original interaction
                self.required_passwords_info,  # Still need all required info for the new modal
                self.database_file,
                self.target_path,
                self.download_folder,
                version_param=self.version_param, # Re-pass versioning parameters
                start_version_param=self.start_version_param,
                end_version_param=self.end_version_param,
                all_versions_param=self.all_versions_param,
                can_apply_version_filters=self.can_apply_version_filters,
                initial_passwords=self.entered_passwords,
                error_messages=self.error_messages
            )

            error_message_content = (
                f"{interaction.user.mention}, Some passwords were incorrect. Please review and re-enter. "
                f"Incorrect items: {', '.join([item_info['display_name'] for item_info in self.required_passwords_info if (item_info['root_upload_name'], item_info['relative_path_in_archive'], item_info['base_filename'], item_info['version']) in current_errors])}"
            )

            retry_view = discord.ui.View(timeout=180)
            retry_button = discord.ui.Button(label="Try Again", style=discord.ButtonStyle.red, emoji="❌")

            async def retry_callback(button_interaction: discord.Interaction):
                # This is a new interaction from the retry button
                # noinspection PyUnresolvedReferences
                await button_interaction.response.send_modal(new_modal)
                retry_button.disabled = True
                await button_interaction.message.edit(view=retry_view)

            retry_button.callback = retry_callback
            retry_view.add_item(retry_button)

            # Edit the original response of the modal submission to show the error and retry button
            await interaction.edit_original_response(content=error_message_content, view=retry_view)

class DecryptionPasswordTriggerView(discord.ui.View):
    def __init__(self, bot_instance: 'FileBotAPI', original_interaction: discord.Interaction,
                 required_passwords_info: List[Dict[str, Any]], database_file: str, # Changed Any
                 target_path: str, download_folder: str,
                 version_param: Optional[str] = None, # NEW
                 start_version_param: Optional[str] = None, # NEW
                 end_version_param: Optional[str] = None, # NEW
                 all_versions_param: bool = False, # NEW
                 can_apply_version_filters: bool = False): # NEW
        super().__init__(timeout=180)
        self.bot = bot_instance
        self.original_interaction = original_interaction
        self.required_passwords_info = required_passwords_info
        self.database_file = database_file
        self.target_path = target_path
        self.download_folder = download_folder
        # Store new versioning parameters
        self.version_param = version_param
        self.start_version_param = start_version_param
        self.end_versitarget_path_displayon_param = end_version_param
        self.all_versions_param = all_versions_param
        self.can_apply_version_filters = can_apply_version_filters

    @discord.ui.button(label="Enter Passwords", style=discord.ButtonStyle.primary, custom_id="trigger_password_modal")
    async def trigger_modal_button(self, button_interaction: discord.Interaction, button: discord.ui.Button):
        # noinspection PyUnresolvedReferences
        await button_interaction.response.send_modal(DecryptionPasswordModal(
            bot_instance=self.bot,
            original_interaction=self.original_interaction,
            required_passwords_info=self.required_passwords_info,
            database_file=self.database_file,
            target_path=self.target_path,
            download_folder=self.download_folder,
            version_param=self.version_param, # Pass through versioning parameters
            start_version_param=self.start_version_param,
            end_version_param=self.end_version_param,
            all_versions_param=self.all_versions_param,
            can_apply_version_filters=self.can_apply_version_filters
        ))
        button.disabled = True
        await button_interaction.message.edit(view=self)

class ZeroKnowledgeDecryptionFailureView(discord.ui.View):
    def __init__(self, bot_instance: 'FileBotAPI', original_interaction: discord.Interaction,
                 local_base_path_for_cleanup: str,
                 overall_parts_downloaded_counter_ref: int, overall_total_parts_to_download_ref: int,
                 current_file_total_parts: int):
        super().__init__(timeout=180)
        self.bot = bot_instance
        self.original_interaction = original_interaction
        self.local_base_path_for_cleanup = local_base_path_for_cleanup
        self.overall_parts_downloaded_counter_ref = overall_parts_downloaded_counter_ref
        self.overall_total_parts_to_download_ref = overall_total_parts_to_download_ref
        self.current_file_total_parts = current_file_total_parts
        self.choice = None # Will store user's choice: "cancel_all", "cancel_keep", "continue"

    async def _disable_all_buttons(self, interaction: discord.Interaction):
        for item in self.children:
            item.disabled = True
        await interaction.message.edit(view=self)

    @discord.ui.button(label="Cancel All & Remove Files", style=discord.ButtonStyle.red, emoji="🗑️", custom_id="cancel_all_remove")
    async def cancel_all_remove_button(self, button_interaction: discord.Interaction, button: discord.ui.Button):
        self.choice = "cancel_all"
        await self._disable_all_buttons(button_interaction)
        await button_interaction.response.send_message(
            f"{button_interaction.user.mention}, Cancelling download and removing partially downloaded files. This may take a moment...",
            ephemeral=True
        )
        # This will cause the download_filea to raise an exception, which will trigger cleanup.
        self.stop() # Stop the wait() in download_filea

    @discord.ui.button(label="Cancel & Keep Files", style=discord.ButtonStyle.grey, emoji="❌", custom_id="cancel_keep_files")
    async def cancel_keep_files_button(self, button_interaction: discord.Interaction, button: discord.ui.Button):
        self.choice = "cancel_keep"
        await self._disable_all_buttons(button_interaction)
        await button_interaction.response.send_message(
            f"{button_interaction.user.mention}, Cancelling download. Partially downloaded files will remain on your machine.",
            ephemeral=True
        )
        self.stop() # Stop the wait() in download_filea

    @discord.ui.button(label="Continue (Skip Item)", style=discord.ButtonStyle.green, emoji="➡️", custom_id="continue_skip_item")
    async def continue_skip_item_button(self, button_interaction: discord.Interaction, button: discord.ui.Button):
        self.choice = "continue"
        await self._disable_all_buttons(button_interaction)
        await button_interaction.response.send_message(
            f"{button_interaction.user.mention}, Skipping this item and continuing with the rest of the download.",
            ephemeral=True
        )
        self.stop() # Stop the wait() in download_file


class ListViewPaginator(discord.ui.View):
    def __init__(self, user_mention: str, all_items_to_display: List[Dict], total_pages: int, items_per_page: int,
                 initial_page: int, target_path_display: Optional[str] = None,
                 show_original_name: bool = False, bot_instance: 'FileBotAPI' = None,
                 check_existance: bool = False,
                 check_all_pre_checked: bool = False,  # NEW PARAMETER
                 is_deletion_list: bool = False):  # NEW PARAMETER
        super().__init__(timeout=300)  # Timeout after 5 minutes of inactivity
        self.user_mention = user_mention
        self.all_items_to_display = all_items_to_display  # Now list of dicts
        print(all_items_to_display)
        self.current_page = initial_page
        self.items_per_page = items_per_page
        self.total_pages = total_pages
        self.target_path_display = target_path_display
        self.show_original_name = show_original_name
        self.bot = bot_instance  # Store bot instance to access _check_discord_message_existence
        self.check_existance = check_existance  # Store check_existance flag
        self.check_all_pre_checked = check_all_pre_checked  # Store the new parameter
        self.is_deletion_list = is_deletion_list  # Store the new parameter
        self.message: Optional[discord.Message] = None
        self.root_name = (
            all_items_to_display[0]['root_name']
            if all_items_to_display
            else None
        )
        self.update_buttons()

    def get_items_for_current_folder(self):
        """
        Returns all items (files and folders) that should be displayed
        in the current folder based on target_path_display.
        """

        items_to_display = []
        print(f'l1: {self.all_items_to_display}')
        # ROOT folder
        if self.target_path_display == '':
            for item in self.all_items_to_display:
                if item['relative_path'] == "":
                    if item['base_filename'] == "_DIR_":
                        folder_name = item['fileid'].split("__DIR_")[0]
                        new_item = item.copy()
                        new_item['display_name'] = folder_name
                        items_to_display.append(new_item)
                    # FILE
                    else:
                        items_to_display.append(item)
            return items_to_display

        # INSIDE A FOLDER
        current_path = self.target_path_display  # e.g., "Takeout"
        for item in self.all_items_to_display:
            # FILES
            if item['base_filename'] != "_DIR_" and item['relative_path'] == current_path:
                items_to_display.append(item)
                continue

            # FOLDERS
            if item['base_filename'] == "_DIR_":
                rel_path = item['relative_path']  # e.g., "Takeout/Profile"
                if "/" in rel_path:
                    parent_path, folder_name = rel_path.rsplit("/", 1)
                    if parent_path == current_path:
                        new_item = item.copy()
                        new_item['display_name'] = folder_name
                        items_to_display.append(new_item)
                else:
                    # Edge case: folder directly in current_path
                    if rel_path == current_path:
                        folder_name = item['fileid'].split("__DIR_")[0]
                        new_item = item.copy()
                        new_item['display_name'] = folder_name
                        items_to_display.append(new_item)
        print(items_to_display)
        return items_to_display

    def clear_folder_buttons(self):
        for child in list(self.children):
            if getattr(child, 'row', 0) == 1:
                self.remove_item(child)

    async def add_folder_buttons(self, interaction: discord.Interaction, folders_on_page):
        """
        Adds buttons for each folder to navigate into it
        """
        # Remove old folder buttons
        self.clear_folder_buttons()

        for folder_item in folders_on_page:
            name = folder_item['root_name'] if folder_item['relative_path'] == "" else os.path.basename(
                folder_item['relative_path'])
            button = discord.ui.Button(
                label=f"{name}/",
                style=discord.ButtonStyle.primary,
                row=1
            )

            # Folder click callback
            async def folder_callback(inter: discord.Interaction, folder_name=name, folder_item=folder_item):
                # Update the target path
                new_path = folder_item['relative_path'] if folder_item['relative_path'] else folder_item['root_name']
                self.target_path_display = new_path
                self.current_page = 0
                # Recalculate total pages for this folder
                total_items = len(self.get_items_for_current_folder())
                self.total_pages = (total_items + self.items_per_page - 1) // self.items_per_page or 1
                self.update_buttons()
                await self.update_message(inter)

            button.callback = folder_callback
            self.add_item(button)

    async def _get_page_content(self, page_num: int, interaction: discord.Interaction) -> str:
        """
        Returns the formatted content for the requested page.
        Shows files as text (with parts/DAMAGED info) and folders as clickable buttons.
        Performs Discord existence checks on the page if enabled.
        """

        # Get all items in current folder scope
        items_in_current_folder = self.get_items_for_current_folder()

        # Slice for pagination
        start_index = page_num * self.items_per_page
        end_index = start_index + self.items_per_page
        page_items = items_in_current_folder[start_index:end_index]

        # Separate folders and files
        folders_on_page = [i for i in page_items if i.get('is_folder')]
        files_on_page = [i for i in page_items if not i.get('is_folder')]

        # Add folder buttons
        if folders_on_page:
            await self.add_folder_buttons(interaction, folders_on_page)

        # Perform Discord existence checks if enabled
        if self.check_existance and not self.check_all_pre_checked and files_on_page and self.bot:
            self.bot.log.info(f"Performing Discord existence checks for Page {page_num + 1}.")
            tasks_with_context = []

            for file_data in files_on_page:
                file_data['_temp_found_parts'] = set(file_data.get('current_parts_found_set', set()))
                file_data['is_damaged_by_discord_absence'] = False

                version_to_check = file_data.get('current_version', None)
                if not version_to_check:
                    continue

                part_discord_info = file_data.get('versions', {}).get(version_to_check, {})
                for part_num, (channel_id, message_id) in part_discord_info.items():
                    if channel_id and message_id:
                        tasks_with_context.append(
                            (self.bot._check_discord_message_existence(channel_id, message_id), file_data, part_num)
                        )
                    else:
                        file_data['_temp_found_parts'].discard(part_num)
                        file_data['is_damaged_by_discord_absence'] = True

            progress_msg_func = self.message.edit if self.message else interaction.edit_original_response
            await progress_msg_func(
                content=f"{self.user_mention}, Checking Discord existence for items on Page {page_num + 1}/{self.total_pages}...",
                view=self
            )

            for i in range(0, len(tasks_with_context), self.bot.batch_size_discord_checks):
                batch_contexts = tasks_with_context[i:i + self.bot.batch_size_discord_checks]
                batch_tasks = [task[0] for task in batch_contexts]
                results = await asyncio.gather(*batch_tasks, return_exceptions=True)

                for j, result in enumerate(results):
                    _, file_data_ref, part_num_ref = batch_contexts[j]
                    if isinstance(result, Exception) or not result:
                        file_data_ref['is_damaged_by_discord_absence'] = True
                        file_data_ref['_temp_found_parts'].discard(part_num_ref)

                if i + self.bot.batch_size_discord_checks < len(tasks_with_context):
                    await asyncio.sleep(self.bot.batch_delay_discord_checks)

            for file_data in files_on_page:
                file_data['actual_found_parts_for_display'] = len(file_data['_temp_found_parts'])
                del file_data['_temp_found_parts']

        # Format files as text (with parts/DAMAGED)
        files_formatted = []
        for item in files_on_page:
            name = item['base_filename']
            if self.show_original_name and item.get('is_base_filename_nicknamed') and item.get(
                    'original_base_filename'):
                name = f"{name} (Original: {item['original_base_filename']})"

            if self.is_deletion_list:
                files_formatted.append(f"`{name}`")
            else:
                total_parts = item.get('total_expected_parts', 1)
                actual_parts = item.get('actual_found_parts_for_display', total_parts)
                parts_suffix = "part" if total_parts == 1 else "parts"
                damaged = " ~DAMAGED~" if item.get('is_damaged_by_discord_absence', False) else ""
                files_formatted.append(f"`{name}` ({actual_parts}/{total_parts} {parts_suffix}){damaged}")

        files_formatted.sort()

        # Build header
        if self.is_deletion_list:
            header = f"**{self.user_mention}, Successfully Removed Items (Page {page_num + 1}/{self.total_pages}):**\n"
        elif self.target_path_display:
            header = f"**{self.user_mention}, Contents of `{self.target_path_display}/` (Page {page_num + 1}/{self.total_pages}):**\n"
        else:
            header = f"**{self.user_mention}, Uploaded Items (Top-level Folders/Files) (Page {page_num + 1}/{self.total_pages}):**\n"

        # Combine content: only files as text, folders are buttons
        content_parts = []
        if files_formatted:
            content_parts.append("\n**📄 Files:**")
            content_parts.extend([f"- {f}" for f in files_formatted])

        final_content = f"{header}\n" + "\n".join(content_parts)
        if not files_formatted and not folders_on_page:
            final_content = f"{header}\nNo items found on this page."

        if len(final_content) > 1900:
            return f"{header}\nContent too long for one message. Please reduce item name length."

        return final_content

    def update_buttons(self):
        self.prev_button.disabled = self.current_page == 0
        self.next_button.disabled = self.current_page == self.total_pages - 1
        self.go_to_page_button.label = f"Page {self.current_page + 1}/{self.total_pages}"

    async def update_message(self, interaction: discord.Interaction):
        if self.message:
            await self.message.edit(
                content=self._get_page_content(self.current_page),
                view=self
            )
        else:
            # Fallback if message wasn't captured, should ideally not happen in normal flow.
            await interaction.followup.send(content=self._get_page_content(self.current_page), view=self,
                                            ephemeral=False)

    @discord.ui.button(label="Previous", style=discord.ButtonStyle.blurple, emoji="⬅️", row=0)
    async def prev_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        if self.current_page > 0:
            self.current_page -= 1
            self.update_buttons()
            await self.update_message(interaction)
        else:
            await interaction.followup.send("You are already on the first page.", ephemeral=False)

    @discord.ui.button(label="Page X/Y", style=discord.ButtonStyle.grey, row=0)
    async def go_to_page_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = GoToPageModal(self)  # Reusing GoToPageModal as it's generic enough
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Next", style=discord.ButtonStyle.blurple, emoji="➡️", row=0)
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        if self.current_page < self.total_pages - 1:
            self.current_page += 1
            self.update_buttons()
            await self.update_message(interaction)
        else:
            await interaction.followup.send("You are already on the last page.", ephemeral=False)

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True
        if self.message:
            try:
                await self.message.edit(view=self)
            except Exception as e:
                self.bot.log.error(f"Could not disable buttons on timeout: {e}")

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

@bot.tree.command(name="download", description="Download a file or a whole folder by its path in the archive.")
@app_commands.describe(
    target_path="Path to the file or folder (e.g., 'MyFolder/Subfolder/File.txt' or 'MyFolder').",
    database_file="Database file (e.g., myfiles.db).",
    download_folder="Folder to download the item(s) to."
)
async def download_command(interaction: discord.Interaction, target_path: str, database_file: str,
                           download_folder: str):
    user_mention = interaction.user.mention
    bot.log.debug(f"Before defer for /download by {interaction.user.id}")
    try:
        # noinspection PyUnresolvedReferences
        await interaction.response.defer(ephemeral=False)
        bot.log.debug(f"After successful defer for /download by {interaction.user.id}")
    except discord.errors.NotFound as e:
        bot.log.debug(f"Defer failed for /download by {interaction.user.id}: {e}")
        await interaction.followup.send(
            f"An error occurred while trying to acknowledge your `/download` command. The interaction might have timed out. Please try again. Error: `{e}`",
            ephemeral=True)
        return
    except Exception as e:
        bot.log.debug(f"Unexpected error during defer for /download by {interaction.user.id}: {e}")
        await interaction.followup.send(
            f"An unexpected error occurred before processing your `/download` command. Error: `{e}`",
            ephemeral=True)
        return

    if not os.path.isdir(download_folder):
        bot.log.debug(
            f"Download folder '{download_folder}' does not exist for user {interaction.user.id}. Attempting to create.")
        try:
            os.makedirs(download_folder, exist_ok=True)
            bot.log.debug(f"Successfully created download folder: {download_folder} for user {interaction.user.id}")
            await interaction.followup.send(
                content=f"{user_mention}, Created download folder: `{download_folder}`.")
        except OSError as e:
            bot.log.debug(
                f"Failed to create download folder '{download_folder}' for user {interaction.user.id}: {e}")
            await interaction.edit_original_response(
                content=f"{user_mention}, Error: Could not create download folder '{download_folder}': {e}. Please check the path and permissions.")
            bot.log.error(f"Could not create download folder '{download_folder}': {e}")
            return

    # --- NEW LOGIC FOR DECRYPTION PASSWORD MODAL ---
    required_passwords_info = await bot._get_items_requiring_password_for_download(database_file, target_path)

    if required_passwords_info:
        bot.log.info(f"Items requiring password found. Presenting modal for user {interaction.user.id}.")
        modal = DecryptionPasswordModal(
            bot_instance=bot,
            original_interaction=interaction,  # Pass the original interaction
            required_passwords_info=required_passwords_info,
            database_file=database_file,
            target_path=target_path,
            download_folder=download_folder
        )
        try:
            # Attempt to send the modal directly.
            # This might fail if interaction.response has already been used (e.g., by the initial followup.send).
            # noinspection PyUnresolvedReferences
            await interaction.response.send_modal(modal)
            bot.log.debug(f"Modal sent to user {interaction.user.id}.")
        except discord.errors.InteractionResponded:
            # If the interaction was already responded to, send a message with a button to trigger the modal.
            bot.log.warning(
                f"Interaction already responded to. Sending button to trigger modal for user {interaction.user.id}.")
            await interaction.followup.send(
                content=f"{user_mention}, Some items require decryption passwords. Please click the button below to enter them.",
                view=DecryptionPasswordTriggerView(bot, interaction, required_passwords_info, database_file,
                                                   target_path, download_folder)
            )
        except Exception as e:
            bot.log.error(f"Error sending decryption password modal to user {interaction.user.id}: {e}")
            await interaction.followup.send(
                f"{user_mention}, An error occurred while trying to get decryption passwords. Please try again later. Error: `{e}`",
                ephemeral=True
            )
    else:
        bot.log.debug(f"No items requiring password for '{target_path}'. Proceeding with download_filea.")
        await bot.download_filea(interaction, target_path, database_file, download_folder)


@bot.tree.command(name="listfiles", description="Lists uploaded files or contents of a folder, with version control.")
@app_commands.describe(database_file="Database file (e.g., myfiles.db).",
                       target_root_name="Optional: Path to the file or folder to list (e.g., 'MyFolder' or 'MyFolder/SubFolder/File.txt'). Use '.' for all root items.",
                       check_attachments_existance="Optional: If True, checks Discord attachments for file parts. Defaults to False.",
                       show_original_name="Optional: If True, displays original names (if available) alongside nicknames. Defaults to False.",
                       version="Optional: Specific version to list/check (for folders, lists contents of that version; for files, checks that version).",
                       start_version="Optional: Start of a version range to check existence for.",
                       end_version="Optional: End of a version range to check existence for. Requires start_version.",
                       all_versions="Optional: If True, checks existence for all versions of the target item(s). Overrides 'version' and range parameters.",
                       check_all_versions="Optional: If True, and no other explicit version is given, checks all versions' existence. Defaults to False.")
async def listfiles_command(interaction: discord.Interaction, database_file: str,
                            target_root_name: Optional[str] = None, check_attachments_existance: bool = False,
                            show_original_name: bool = False,
                            version: Optional[str] = None,
                            start_version: Optional[str] = None,
                            end_version: Optional[str] = None,
                            all_versions: Optional[bool] = False,  # Changed type from Choice to bool
                            check_all_versions: Optional[bool] = False):  # Changed type from Choice to bool
    bot.log.debug(f"Before defer for /listfiles by {interaction.user.id}")
    try:
        # noinspection PyUnresolvedReferences
        await interaction.response.defer(ephemeral=False)
        bot.log.debug(f"After successful defer for /listfiles by {interaction.user.id}")
    except discord.errors.NotFound as e:
        bot.log.debug(f"Defer failed for /listfiles by {interaction.user.id}: {e}")
        await interaction.followup.send(
            f"An error occurred while trying to acknowledge your `/listfiles` command. The interaction might have timed out. Please try again. Error: `{e}`",
            ephemeral=True)
        return
    except Exception as e:
        bot.log.debug(f"Unexpected error during defer for /listfiles by {interaction.user.id}: {e}")
        await interaction.followup.send(
            f"An unexpected error occurred before processing your `/listfiles` command. Error: `{e}`",
            ephemeral=True)
        return

    bot.log.debugo(
        f"Calling bot.list_files for '{database_file}' (root: '{target_root_name}', show_original_name: {show_original_name}) by user {interaction.user.id}.")
    await bot.list_files(
        interaction,
        database_file,
        target_root_name,
        check_attachments_existance,
        show_original_name,
        False,
        # check_all parameter is no longer directly controlled by user from this command due to complex versioning checks. It's now internal.
        version=version,
        start_version=start_version,
        end_version=end_version,
        all_versions=all_versions,
        check_all_versions=check_all_versions
    )

# @bot.tree.command(name="listversions", description="Lists all versions of a specific file or folder.")
# @app_commands.describe(
#     database_file="Database file (e.g., myfiles.db).",
#     target_path="The exact path to the file or folder (e.g., 'MyProject/MyDocument.txt' or 'MyProject/MyFolder')."
# )
# async def listversions_command(interaction: discord.Interaction, database_file: str, target_path: str):
#     bot.log.debug(f"Before defer for /listversions by {interaction.user.id}")
#     try:
#         await interaction.response.defer(ephemeral=False)
#         bot.log.debug(f"After successful defer for /listversions by {interaction.user.id}")
#     except discord.errors.NotFound as e:
#         bot.log.debug(f"Defer failed for /listversions by {interaction.user.id}: {e}")
#         await interaction.followup.send(
#             f"An error occurred while trying to acknowledge your `/listversions` command. The interaction might have timed out. Please try again. Error: `{e}`",
#             ephemeral=True)
#         return
#     except Exception as e:
#         bot.log.debug(f"Unexpected error during defer for /listversions by {interaction.user.id}: {e}")
#         await interaction.followup.send(
#             f"An unexpected error occurred before processing your `/listversions` command. Error: `{e}`",
#             ephemeral=True)
#         return
#
#     user_mention = interaction.user.mention
#
#     if not database_file.lower().endswith('.db'):
#         database_file += '.db'
#     DATABASE_FILE = os.path.abspath(os.path.normpath(database_file))
#
#     if not os.path.exists(DATABASE_FILE):
#         await interaction.followup.send(
#             content=f"{user_mention}, The database file '{database_file}' was not found. Cannot list versions.",
#             ephemeral=False)
#         bot.log.error(f">>> [LIST VERSIONS] ERROR: Database file not found at '{DATABASE_FILE}'.")
#         return
#
#     all_db_entries = await bot.db._db_read_sync(DATABASE_FILE, {})
#     if not all_db_entries:
#         await interaction.followup.send(
#             content=f'{user_mention}, No entries found in database: "{database_file}".', ephemeral=False)
#         return
#
#     normalized_target_path = os.path.normpath(target_path).replace(os.path.sep, '/').strip('/')
#
#     # Resolve the target path to its root_upload_name, relative_path_in_archive, base_filename
#     resolved_target_info = await bot._resolve_path_to_db_entry_keys(normalized_target_path, all_db_entries)
#
#     if not resolved_target_info:
#         await interaction.followup.send(
#             content=f"{user_mention}, The target item '{target_path}' was not found in the database. Please ensure the path is correct.",
#             ephemeral=False)
#         bot.log.info(f">>> [LIST VERSIONS] INFO: Target '{target_path}' not found for version listing.")
#         return
#
#     target_r_name, target_r_path, target_b_name, is_target_a_folder = resolved_target_info
#
#     # Fetch all versions for the identified item
#     all_versions_for_item = await bot.version_manager._get_relevant_item_versions(
#         DATABASE_FILE,
#         target_r_name,
#         target_r_path,
#         target_b_name,
#         version_param=None,
#         start_version_param=None,
#         end_version_param=None,
#         all_versions_param=True  # This is the key: get ALL versions
#     )
#
#     if not all_versions_for_item:
#         await interaction.followup.send(
#             content=f"{user_mention}, No versions found for '{target_path}'.",
#             ephemeral=False)
#         bot.log.info(f">>> [LIST VERSIONS] INFO: No versions found for '{target_path}'.")
#         return
#
#     # Extract just the version strings and sort them again (though _get_relevant_item_versions already sorts)
#     version_strings = [entry.get('version', 'Unknown') for entry in all_versions_for_item]
#     # Filter out "Unknown" versions if they exist (shouldn't happen with proper data)
#     version_strings = [v for v in version_strings if v != 'Unknown']
#
#     if not version_strings:
#         await interaction.followup.send(
#             content=f"{user_mention}, No valid version strings found for '{target_path}'.",
#             ephemeral=False)
#         bot.log.info(f">>> [LIST VERSIONS] INFO: No valid version strings found for '{target_path}'.")
#         return
#
#     # Use the new VersionListPaginator
#     paginator = (
#         user_mention=user_mention,
#         item_path=target_path,
#         versions=version_strings,
#         bot_instance=bot
#     )
#
#     initial_content = paginator._get_page_content(0)
#     sent_message_paginator = await interaction.followup.send(content=initial_content, view=paginator)
#     paginator.message = sent_message_paginator
#
#     bot.log.info(
#         f">>> [LIST VERSIONS] INFO: Sent paginated version list for '{target_path}'. Total pages: {paginator.total_pages}.")

@bot.tree.command(name="delete", description="Deletes a specific file, folder, or entire upload by its path.")
@app_commands.describe(
    target_path="Path to the file or folder to delete (e.g., 'MyFolder/Subfolder/File.txt' or 'MyFolder/Subfolder').",
    database_file="Database file (e.g., myfiles.db).",
    include_sub_folders="Set to False to only delete items directly in the specified folder, not sub-folders (only applies when deleting a folder).")
async def delete_command(interaction: discord.Interaction, target_path: str, database_file: str,
                         include_sub_folders: bool = True):
    bot.log.debug(f"Before defer for /delete by {interaction.user.id}")
    try:
        # noinspection PyUnresolvedReferences
        await interaction.response.defer(ephemeral=False)
        bot.log.debug(f"After successful defer for /delete by {interaction.user.id}")
    except discord.errors.NotFound as e:
        bot.log.debug(f"Defer failed for /delete by {interaction.user.id}: {e}")
        await interaction.followup.send(
            f"An error occurred while trying to acknowledge your `/delete` command. The interaction might have timed out. Please try again. Error: `{e}`",
            ephemeral=True)
        return
    except Exception as e:
        bot.log.debug(f"Unexpected error during defer for /delete by {interaction.user.id}: {e}")
        await interaction.followup.send(
            f"An unexpected error occurred before processing your `/delete` command. Error: `{e}`", ephemeral=True)
        return

    bot.log.debug(f"Calling bot.delete for '{target_path}' by user {interaction.user.id}.")
    await bot.delete(interaction, target_path, database_file, include_sub_folders)

@bot.tree.command(name="removedamaged", description="Removes damaged files from the database.")
@app_commands.describe(target_path="Path to the file or folder (e.g., 'MyFolder/MyFile.txt' or 'MyFolder').",
                       database_file="Database file (e.g., myfiles.db).",
                       include_sub_folders="Set to False to only check files directly in the specified folder.",
                       check_attachments_existance="Optional: If True, checks Discord attachments for file parts in addition to database entries. If False, only relies on database entries (defaults to False).")
async def removedamaged_command(interaction: discord.Interaction, target_path: str, database_file: str,
                                include_sub_folders: bool = True, check_attachments_existance: bool = False):
    bot.log.debug(f"Before defer for /removedamaged by {interaction.user.id}")
    try:
        # noinspection PyUnresolvedReferences
        await interaction.response.defer(ephemeral=False)
        bot.log.debug(f"After successful defer for /removedamaged by {interaction.user.id}")
    except discord.errors.NotFound as e:
        bot.log.debug(f"Defer failed for /removedamaged by {interaction.user.id}: {e}")
        await interaction.followup.send(
            f"An error occurred while trying to acknowledge your `/removedamaged` command. The interaction might have timed out. Please try again. Error: `{e}`",
            ephemeral=True)
        return
    except Exception as e:
        bot.log.debug(f"Unexpected error during defer for /removedamaged by {interaction.user.id}: {e}")
        await interaction.followup.send(
            f"An unexpected error occurred before processing your `/removedamaged` command. Error: `{e}`",
            ephemeral=True)
        return

    bot.log.debug(f"Calling bot.remove_damaged for '{target_path}' by user {interaction.user.id}.")
    await bot.remove_damaged(interaction, target_path, database_file, include_sub_folders,
                             check_attachments_existance)

@bot.tree.command(name="reindex",
                  description="Reindexes database to repair corrupted item indexes. Rarely used, mostly for emergency repair.")
@app_commands.describe(database_file="Database file (e.g., myfiles.db).")
async def reindex_command(interaction: discord.Interaction, database_file: str):
    bot.log.debug(f"Before defer for /reindex by {interaction.user.id}")
    try:
        # noinspection PyUnresolvedReferences
        await interaction.response.defer(ephemeral=False)
        bot.log.debug(f"After successful defer for /reindex by {interaction.user.id}")
    except discord.errors.NotFound as e:
        bot.log.debug(f"Defer failed for /reindex by {interaction.user.id}: {e}")
        await interaction.followup.send(
            f"An error occurred while trying to acknowledge your `/reindex` command. The interaction might have timed out. Please try again. Error: `{e}`",
            ephemeral=True)
        return
    except Exception as e:
        bot.log.debug(f"Unexpected error during defer for /reindex by {interaction.user.id}: {e}")
        await interaction.followup.send(
            f"An unexpected error occurred before processing your `/reindex` command. Error: `{e}`", ephemeral=True)
        return

    bot.log.debug(f"Calling bot.reindex_database for '{database_file}' by user {interaction.user.id}.")
    await bot.reindex_database(interaction, database_file)

bot.run('TOKEN')
