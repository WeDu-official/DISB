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
        # Fixed salt and info for HKDF (should be constant and unique to your bot's encryption)
        self._HKDF_SALT = b"my_bot_encryption_salt_12345"
        self._HKDF_INFO = b"discord_file_bot_encryption_info"

    async def get_prefix(self, message: discord.Message) -> List[str]:
        return ["/"]

    async def setup_hook(self):
        self.http_session = aiohttp.ClientSession()
        try:
            synced = await self.tree.sync()
            self.log.info(f"Synced {len(synced)} slash commands for {self.user}")
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
    def _sync_create_table_if_not_exists(self, conn: sqlite3.Connection):
        column_definitions = []
        for col_name in self.file_table_columns:
            if col_name == "fileid":
                column_definitions.append(f"{col_name} TEXT PRIMARY KEY")
            elif col_name in ["part_number", "total_parts", "message_id", "channel_id"]:
                column_definitions.append(f"{col_name} INTEGER")
            elif col_name in ["is_nicknamed", "is_base_filename_nicknamed", "store_hash_flag"]:
                column_definitions.append(f"{col_name} INTEGER")  # Store as INTEGER 0 or 1 for boolean
            elif col_name == "encryption_key_auto":
                column_definitions.append(f"{col_name} BLOB")  # Store as BLOB for bytes
            else:
                column_definitions.append(f"{col_name} TEXT")
        schema = ", ".join(column_definitions)
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS file_metadata_table (
            {schema}
        );
        """
        conn.execute(create_table_sql)
        conn.commit()
        self.log.info(f"[SQLite] Ensured table 'file_metadata_table' exists synchronously.")
    
    def _sync_insert_operation(self, database_file: str, entry: Dict[str, Any]):
        db_path_abs = os.path.abspath(os.path.normpath(database_file))
        os.makedirs(os.path.dirname(db_path_abs), exist_ok=True)
        conn = None
        try:
            print(db_path_abs + ": INSERT")
            conn = sqlite3.connect(db_path_abs)
            conn.row_factory = sqlite3.Row
            self._sync_create_table_if_not_exists(conn)
            columns = []
            placeholders = []
            values = []
            for col_name in self.file_table_columns:
                value_to_store = entry.get(col_name)
                # Handle default values for None
                if value_to_store is None:
                    if col_name in ["part_number", "total_parts", "message_id", "channel_id"]:
                        value_to_store = 0  # Store as integer 0
                    elif col_name in ["is_nicknamed", "is_base_filename_nicknamed", "store_hash_flag"]:
                        value_to_store = 0  # Store as integer 0 for False
                    elif col_name == "encryption_key_auto":
                        value_to_store = b""  # Store as empty bytes
                    else:
                        value_to_store = ""  # Default for other TEXT columns

                # Type conversion for database storage
                if col_name in ["is_nicknamed", "is_base_filename_nicknamed", "store_hash_flag"]:
                    value_to_store = 1 if value_to_store else 0  # Convert bool to int (1/0)
                elif col_name == "encryption_key_auto":
                    # Ensure it's bytes for BLOB, convert from str if necessary (e.g., if loaded from DB as str)
                    if isinstance(value_to_store, str):
                        value_to_store = value_to_store.encode('utf-8')
                elif col_name in ["part_number", "total_parts", "message_id", "channel_id"]:
                    value_to_store = int(value_to_store)  # Ensure it's int
                else:
                    value_to_store = str(value_to_store)  # Store everything else as TEXT

                columns.append(col_name)
                placeholders.append("?")
                values.append(value_to_store)
            insert_sql = f"""
            INSERT OR REPLACE INTO file_metadata_table ({", ".join(columns)})
            VALUES ({", ".join(placeholders)});
            """
            conn.execute(insert_sql, tuple(values))
            conn.commit()
            self.log.info(
                f"[SQLite] Successfully inserted new entry with fileid '{entry.get('fileid')}' into '{database_file}'.")
        except Exception as e:
            if conn:
                conn.rollback()
            self.log.error(f"[SQLite] ERROR inserting into DB '{database_file}': {e}")
            self.log.error(traceback.format_exc())
            raise
        finally:
            if conn:
                conn.close()

    def _sync_read_operation(self, database_file: str, query_conditions: Dict[str, Any]) -> List[Dict[str, Any]]:
        db_path_abs = os.path.abspath(os.path.normpath(database_file))
        if not os.path.exists(db_path_abs):
            self.log.warning(f"[SQLite] Database file not found: {db_path_abs}. Returning empty list.")
            return []
        conn = None
        results = []
        self.log.debug(f"{db_path_abs}: READ with conditions: {query_conditions}")
        try:
            conn = sqlite3.connect(db_path_abs)
            conn.row_factory = sqlite3.Row
            self._sync_create_table_if_not_exists(conn)
            where_clauses = []
            where_values = []
            for col_name, value in query_conditions.items():
                if col_name in self.file_table_columns:
                    where_clauses.append(f"{col_name} = ?")
                    # Convert query values for boolean columns to int
                    if col_name in ["is_nicknamed", "is_base_filename_nicknamed", "store_hash_flag"]:
                        where_values.append(1 if value else 0)
                    # Convert query values for BLOB columns to bytes
                    elif col_name == "encryption_key_auto" and isinstance(value, str):
                        where_values.append(value.encode('utf-8'))  # Assume it was originally stored as bytes
                    else:
                        where_values.append(str(value))  # Convert all other query values to string
                else:
                    self.log.warning(f"[SQLite] WARNING: Query condition for unknown column '{col_name}' ignored.")
            sql_query = "SELECT * FROM file_metadata_table"
            if where_clauses:
                sql_query += " WHERE " + " AND ".join(where_clauses)
            self.log.debug(f"SQL Query: {sql_query}, Values: {where_values}")
            cursor = conn.execute(sql_query, tuple(where_values))
            rows = cursor.fetchall()
            for row in rows:
                entry_data = {}
                for col_name in self.file_table_columns:
                    value = row[col_name]
                    entry_data[col_name] = value  # Get raw value from DB

                # Convert values back to Python types
                converted_entry = {}
                for k, v in entry_data.items():
                    if k in ["part_number", "total_parts", "message_id", "channel_id"]:
                        converted_entry[k] = int(v) if v is not None else 0
                    elif k in ["is_nicknamed", "is_base_filename_nicknamed", "store_hash_flag"]:
                        converted_entry[k] = bool(v) if v is not None else False  # Convert int (1/0) to bool
                    elif k == "encryption_key_auto":
                        converted_entry[k] = v if v is not None else b''  # Keep as bytes
                    elif k == "password_seed_hash":
                        converted_entry[k] = v if v is not None else ""
                    elif k == "encryption_mode":
                        converted_entry[k] = v if v is not None else "off"
                    elif k == "original_root_name":
                        converted_entry[k] = v if v is not None else ""
                    elif k == "original_base_filename":
                        converted_entry[k] = v if v is not None else ""
                    elif k == "version":  # NEW: Handle version column
                        converted_entry[k] = v if v is not None else ""
                    else:
                        converted_entry[k] = v
                results.append(converted_entry)
            self.log.info(
                f"[SQLite] Successfully read {len(results)} entries from '{database_file}' matching conditions.")
            return results
        except Exception as e:
            self.log.error(f"[SQLite] ERROR reading from DB '{database_file}': {e}")
            self.log.error(traceback.format_exc())
            return []
        finally:
            if conn:
                conn.close()

    def _sync_delete_operation(self, database_file: str, deletion_targets: List[Dict[str, Any]]) -> int:
        db_path_abs = os.path.abspath(os.path.normpath(database_file))
        if not os.path.exists(db_path_abs):
            self.log.warning(f"[SQLite] Database file not found: {db_path_abs}. No rows to delete.")
            return 0
        conn = None
        deleted_count = 0
        try:
            print(db_path_abs + ": DELETE")
            conn = sqlite3.connect(db_path_abs)
            conn.row_factory = sqlite3.Row
            self._sync_create_table_if_not_exists(conn)
            delete_conditions = []
            for target_cond in deletion_targets:
                root_upload_name = target_cond.get("root_upload_name")
                relative_path_in_archive = (target_cond.get("relative_path_in_archive") or "").replace(os.path.sep,
                                                                                                       '/').strip('/')
                base_filename = target_cond.get("base_filename")
                version = target_cond.get("version")  # NEW: Get version for deletion

                if version is not None:
                    delete_conditions.append((root_upload_name, relative_path_in_archive, base_filename, version))
                else:
                    delete_conditions.append((root_upload_name, relative_path_in_archive, base_filename))

            if not delete_conditions:
                self.log.warning(f"[SQLite] No valid deletion targets provided for '{database_file}'.")
                return 0

            delete_sql_parts = []
            delete_values = []
            for target in delete_conditions:
                if len(target) == 4:  # If version is included
                    delete_sql_parts.append(
                        "(root_upload_name = ? AND relative_path_in_archive = ? AND base_filename = ? AND version = ?)"
                    )
                else:  # If version is not included (e.g., deleting all versions of an item)
                    delete_sql_parts.append(
                        "(root_upload_name = ? AND relative_path_in_archive = ? AND base_filename = ?)"
                    )
                delete_values.extend(target)

            delete_sql = f"""
            DELETE FROM file_metadata_table
            WHERE {" OR ".join(delete_sql_parts)};
            """
            cursor = conn.execute(delete_sql, tuple(delete_values))
            deleted_count = cursor.rowcount
            conn.commit()
            self.log.info(f"[SQLite] Deleted {deleted_count} entries from '{database_file}'.")
            return deleted_count
        except Exception as e:
            if conn:
                conn.rollback()
            self.log.error(f"[SQLite] ERROR deleting from DB '{database_file}': {e}")
            self.log.error(traceback.format_exc())
            raise
        finally:
            if conn:
                conn.close()

    def _sync_vacuum_operation(self, database_file: str):
        """
        Synchronously performs a VACUUM operation on the SQLite database.
        This compacts the database file and reclaims unused space.
        """
        db_path_abs = os.path.abspath(os.path.normpath(database_file))
        if not os.path.exists(db_path_abs):
            self.log.warning(f"[SQLite] Database file not found: {db_path_abs}. Cannot VACUUM.")
            return
        conn = None
        try:
            conn = sqlite3.connect(db_path_abs)
            self.log.info(f"[SQLite] Attempting VACUUM on '{database_file}'...")
            conn.execute("VACUUM;")
            conn.commit()
            self.log.info(f"[SQLite] Successfully VACUUMed database: '{database_file}'.")
        except Exception as e:
            if conn:
                conn.rollback()
            self.log.error(f"[SQLite] ERROR during VACUUM on '{database_file}': {e}")
            self.log.error(traceback.format_exc())
            raise
        finally:
            if conn:
                conn.close()

    async def _db_insert_sync(self, database_file: str, entry: Dict[str, Any]):
        await asyncio.to_thread(self._sync_insert_operation, database_file, entry)

    async def _db_read_sync(self, database_file: str, query_conditions: Dict[str, Any]) -> List[Dict[str, Any]]:
        return await asyncio.to_thread(self._sync_read_operation, database_file, query_conditions)

    async def _db_delete_sync(self, database_file: str, deletion_targets: List[Dict[str, Any]]) -> int:
        return await asyncio.to_thread(self._sync_delete_operation, database_file, deletion_targets)

    async def _db_vacuum_sync(self, database_file: str):
        await asyncio.to_thread(self._sync_vacuum_operation, database_file)

    async def is_folder_entry_in_db(self, db_file: str, root_name: str, relative_path: str, version: str) -> bool:
        """Checks if a specific folder entry exists in the database for a given version."""
        query = {
            "root_upload_name": root_name,
            "relative_path_in_archive": relative_path,
            "base_filename": "_DIR_",
            "version": version  # Include version in query
        }
        entries = await self._db_read_sync(db_file, query)
        return len(entries) > 0

    async def _check_duplicate_root_upload_name(self, db_file: str, root_upload_name: str, version: str) -> bool:
        """
        Checks if a root_upload_name (nickname) with the SAME version already exists as a top-level file or folder.
        This ensures we don't overwrite/duplicate the exact same version of a root item.
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
        existing_folders = await self._db_read_sync(db_file, folder_query)
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
        existing_files = await self._db_read_sync(db_file, file_query)
        if existing_files:
            self.log.debug(f"Duplicate check: Top-level file '{root_upload_name}' version '{version}' already exists.")
            return True

        self.log.debug(f"Duplicate check: No existing item found for '{root_upload_name}'.")
        return False

    def parse_version(self, version_str: str) -> tuple:
        """Parses a version string (e.g., '1.0.0.1', '2.1-beta', 'alpha') into a comparable tuple."""
        # Split by non-digit characters to handle '1.8g2', '1.8-beta'
        parts = re.split(r'[^0-9a-zA-Z]+', version_str)
        parsed_parts = []
        for part in parts:
            if part.isdigit():
                parsed_parts.append(int(part))
            else:
                parsed_parts.append(part)  # Keep non-numeric parts as strings
        return tuple(parsed_parts)

    async def _get_item_metadata_for_versioning(self, database_file: str, root_upload_name: str,
                                                relative_path_in_archive: str, base_filename: str) -> Optional[
        Dict[str, Any]]:
        """
        Fetches the latest metadata for a specific item (file or folder) for versioning purposes.
        Returns the entry for the highest version.
        """
        query = {
            "root_upload_name": root_upload_name,
            "relative_path_in_archive": relative_path_in_archive,
            "base_filename": base_filename  # This covers both _DIR_ for folders and actual filenames for files
        }
        all_versions = await self._db_read_sync(database_file, query)

        if not all_versions:
            return None  # Item not found

        latest_version_entry = max(all_versions, key=lambda x: self.parse_version(x.get('version', '0.0.0.0')))
        self.log.debug(
            f"Found latest version for item '{root_upload_name}/{relative_path_in_archive}/{base_filename}': {latest_version_entry.get('version')}")
        return latest_version_entry

    async def _generate_next_version_string(self, database_file: str, root_upload_name: str,
                                            relative_path_in_archive: str, base_filename: str) -> str:
        """
        Generates the next sequential version string (e.g., "1.0.0.1" -> "1.0.0.2").
        If no previous version, returns "0.0.0.1".
        """
        latest_entry = await self._get_item_metadata_for_versioning(database_file, root_upload_name,
                                                                    relative_path_in_archive, base_filename)

        if not latest_entry or not latest_entry.get('version'):
            return "0.0.0.1"  # First version

        current_version = latest_entry['version']
        try:
            # Split version by '.' and try to increment the last numeric part
            parts = current_version.split('.')
            last_part = parts[-1]
            if last_part.isdigit():
                next_last_part = str(int(last_part) + 1)
                return ".".join(parts[:-1] + [next_last_part])
            else:
                # If last part is not numeric (e.g., "beta-1"), append ".1" or similar
                return f"{current_version}.1"
        except Exception:
            # Fallback for complex or unexpected version strings
            return f"{current_version}_new"

    async def _get_relevant_item_versions(self, db_file: str, root_upload_name: str, relative_path_in_archive: str,
                                          base_filename: str, version_param: Optional[str],
                                          start_version_param: Optional[str], end_version_param: Optional[str],
                                          all_versions_param: bool) -> List[Dict[str, Any]]:
        """
        Fetches relevant item versions from the database based on the provided versioning parameters.
        Filters and sorts the versions according to the precedence rules.
        """
        query = {
            "root_upload_name": root_upload_name,
            "relative_path_in_archive": relative_path_in_archive,
            "base_filename": base_filename
        }
        all_item_versions = await self._db_read_sync(db_file, query)

        if not all_item_versions:
            return []

        # Sort all versions first
        sorted_versions = sorted(all_item_versions, key=lambda x: self.parse_version(x.get('version', '0.0.0.0')))

        if all_versions_param:
            self.log.debug(f"Fetching ALL versions for '{root_upload_name}/{relative_path_in_archive}/{base_filename}'")
            return sorted_versions
        elif version_param:
            self.log.debug(
                f"Fetching specific version '{version_param}' for '{root_upload_name}/{relative_path_in_archive}/{base_filename}'")
            return [entry for entry in sorted_versions if entry.get('version') == version_param]
        elif start_version_param and end_version_param:
            self.log.debug(
                f"Fetching versions from '{start_version_param}' to '{end_version_param}' for '{root_upload_name}/{relative_path_in_archive}/{base_filename}'")
            parsed_start = self.parse_version(start_version_param)
            parsed_end = self.parse_version(end_version_param)
            # Ensure start is not greater than end, and if they are equal, it's a single point not a range
            if parsed_start > parsed_end:
                return []  # Invalid range

            # If start and end are the same, treat as requesting a single specific version
            if parsed_start == parsed_end:
                return [entry for entry in sorted_versions if
                        self.parse_version(entry.get('version', '0.0.0.0')) == parsed_start]

            return [entry for entry in sorted_versions if
                    parsed_start <= self.parse_version(entry.get('version', '0.0.0.0')) <= parsed_end]
        else:
            # Default: Fetch only the newest version
            self.log.debug(
                f"Fetching NEWEST version for '{root_upload_name}/{relative_path_in_archive}/{base_filename}'")
            if sorted_versions:
                return [sorted_versions[-1]]  # The last one after sorting is the newest
            return []

    async def _resolve_path_to_db_entry_keys(self, target_path: str, all_db_entries: List[Dict[str, Any]]) -> Optional[
        Tuple[str, str, str, bool]]:
        """
        Resolves a user-provided conceptual path (e.g., 'MyRoot/Subfolder/File.txt' or 'MyRoot/Subfolder')
        to its corresponding database root_upload_name, relative_path_in_archive, base_filename, and whether it's a folder.
        This function does NOT consider versions; it's purely for path matching.

        Returns: (root_upload_name, relative_path_in_archive, base_filename, is_folder_target) or None
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

    async def _check_item_version_exists(self, database_file: str, root_upload_name: str, relative_path_in_archive: str,
                                         base_filename: str, version: str) -> bool:
        """Checks if a specific version of an item already exists in the database."""
        query = {
            "root_upload_name": root_upload_name,
            "relative_path_in_archive": relative_path_in_archive,
            "base_filename": base_filename,
            "version": version
        }
        entries = await self._db_read_sync(database_file, query)
        return len(entries) > 0

    async def _get_items_for_list_display(self, database_file: str, target_display_path: Optional[str],
                                          version_param: Optional[str], start_version_param: Optional[str],
                                          end_version_param: Optional[str], all_versions_param: bool,
                                          check_all_versions_param: bool) -> List[Dict[str, Any]]:
        """
        Helper function to determine and aggregate which items (and which versions) should be displayed
        and potentially checked for existence based on list_files command parameters.
        Returns a list of aggregated item dictionaries.
        """
        all_db_entries = await self._db_read_sync(database_file, {})
        if not all_db_entries:
            return []

        normalized_target_path = os.path.normpath(target_display_path).replace(os.path.sep, '/').strip(
            '/') if target_display_path else ""
        is_global_scan = (normalized_target_path == '')

        resolved_target_info = None
        if not is_global_scan:
            resolved_target_info = await self._resolve_path_to_db_entry_keys(normalized_target_path, all_db_entries)
            if not resolved_target_info:
                self.log.warning(f"Target path '{target_display_path}' not resolved for listing.")
                return []  # Path not found

        # Determine if version filters from command line apply to the *target path itself*
        can_apply_version_filters_to_target = False
        is_target_a_folder_in_db = False

        if resolved_target_info:
            _, resolved_relative_path_in_archive, resolved_base_filename, is_target_a_folder_in_db = resolved_target_info

            # Version filters for listfiles apply only if it's a specific folder or file
            target_is_specific_file = (resolved_base_filename != "_DIR_" and any(e for e in all_db_entries if
                                                                                 e['root_upload_name'] ==
                                                                                 resolved_target_info[0] and e[
                                                                                     'relative_path_in_archive'] == resolved_relative_path_in_archive and
                                                                                 e[
                                                                                     'base_filename'] == resolved_base_filename))
            target_is_specific_subfolder = (
                    resolved_base_filename == "_DIR_" and resolved_relative_path_in_archive != "" and any(
                e for e in all_db_entries if e['root_upload_name'] == resolved_target_info[0] and e[
                    'relative_path_in_archive'] == resolved_relative_path_in_archive and e[
                    'base_filename'] == "_DIR_"))

            if target_is_specific_file or target_is_specific_subfolder:
                can_apply_version_filters_to_target = True

        items_to_process_for_display: List[Dict[str, Any]] = []

        if is_global_scan:  # Listing all top-level root uploads
            unique_root_names = sorted(
                list(set(e.get('root_upload_name') for e in all_db_entries if e.get('root_upload_name'))))
            for root_name in unique_root_names:
                # Find the latest version of the root folder entry if it exists
                root_folder_entry = await self._get_relevant_item_versions(
                    database_file, root_name, "", "_DIR_",
                    version_param=None, start_version_param=None, end_version_param=None, all_versions_param=False
                )
                if root_folder_entry:  # It's a folder, add its latest _DIR_ entry
                    items_to_process_for_display.append(root_folder_entry[0])  # take the single latest version

                # Now, find all top-level files directly under this root (where relative_path_in_archive is empty)
                top_level_files = [e for e in all_db_entries if e.get('root_upload_name') == root_name and (
                        e.get('relative_path_in_archive') == "" or e.get(
                    'relative_path_in_archive') is None) and e.get('base_filename') != '_DIR_']

                # For each such top-level file, get its newest version
                unique_top_level_file_names = set(f.get('base_filename') for f in top_level_files)
                for file_name in unique_top_level_file_names:
                    latest_file_entry = await self._get_relevant_item_versions(
                        database_file, root_name, "", file_name,
                        version_param=None, start_version_param=None, end_version_param=None, all_versions_param=False
                    )
                    if latest_file_entry:
                        items_to_process_for_display.append(latest_file_entry[0])  # Add the single latest version


        elif is_target_a_folder_in_db and version_param and can_apply_version_filters_to_target:
            # Special case: listing contents of a SPECIFIC VERSION of a folder
            target_r_name, target_r_path, target_b_name, _ = resolved_target_info

            # Ensure the specific version of the folder itself exists
            specific_folder_version_entry = await self._get_relevant_item_versions(
                database_file, target_r_name, target_r_path, "_DIR_",
                version_param, None, None, False
            )
            if not specific_folder_version_entry:
                self.log.warning(f"Specified folder '{normalized_target_path}' version '{version_param}' not found.")
                return []  # Folder version not found

            # Now find all direct children (files and folders) within that *specific version scope*
            # This means children must have matching `version` AND be direct children.
            scoped_children_files = []
            scoped_children_folders = []

            for entry in all_db_entries:
                e_r_name = entry.get('root_upload_name')
                e_r_path = (entry.get('relative_path_in_archive') or '').strip('/')
                e_b_name = entry.get('base_filename')
                e_version = entry.get('version')

                # Check if it's within the target root and version
                if e_r_name == target_r_name and e_version == version_param:
                    # Check if it's a direct child of the target folder
                    if e_r_path == target_r_path:  # Item is directly in the targeted folder
                        if e_b_name != "_DIR_":  # It's a file
                            scoped_children_files.append(entry)
                        else:  # It's a folder (the folder entry itself)
                            if (e_r_name, e_r_path, e_b_name) != (
                                    target_r_name, target_r_path, target_b_name):  # Exclude the target folder itself
                                scoped_children_folders.append(entry)

                    elif e_r_path.startswith(target_r_path + '/'):  # It's a nested item
                        # Check if it's a direct sub-folder
                        # E.g., target_r_path="A/B", e_r_path="A/B/C" -> "C" is a direct subfolder
                        # E.g., target_r_path="A/B", e_r_path="A/B/C/D" -> "C" is a direct subfolder, "D" is not
                        path_after_target = e_r_path[len(target_r_path):].strip('/')
                        if '/' not in path_after_target and e_b_name == "_DIR_":  # direct subfolder
                            scoped_children_folders.append(entry)
                        elif '/' not in path_after_target and e_b_name != "_DIR_":  # direct file
                            scoped_children_files.append(entry)

            # For files, just add them. For folders, add their specific version _DIR_ entries.
            # We already have specific_folder_version_entry, so add it first.
            items_to_process_for_display.append(specific_folder_version_entry[0])
            items_to_process_for_display.extend(scoped_children_files)
            items_to_process_for_display.extend(scoped_children_folders)

        else:  # Standard listing: show newest version of items in scope, whether explicit target or global.
            # This covers: Global scan, specific file target, specific folder target (not a specific version).

            # Identify all unique conceptual items (files and folders) within the target path's scope
            unique_conceptual_items = set()  # Stores (root_name, relative_path, base_filename) tuples

            if is_global_scan:
                for entry in all_db_entries:
                    unique_conceptual_items.add((entry.get('root_upload_name'),
                                                 (entry.get('relative_path_in_archive') or '').strip('/'),
                                                 entry.get('base_filename')))
            else:
                target_r_name, target_r_path, target_b_name, is_target_a_folder = resolved_target_info

                for entry in all_db_entries:
                    e_r_name = entry.get('root_upload_name')
                    e_r_path = (entry.get('relative_path_in_archive') or '').strip('/')
                    e_b_name = entry.get('base_filename')

                    if e_r_name != target_r_name: continue  # Must be under the same root

                    if is_target_a_folder:
                        # Item must be a direct child of the target folder OR the target folder itself
                        # This logic needs to correctly differentiate between 'MyFolder' (target) and 'MyFolder/SubFile.txt' (child)
                        # And also 'MyFolder/SubFolder' (child)
                        target_full_path_for_scope_check = f"{target_r_name}/{target_r_path}"
                        current_item_full_path = f"{e_r_name}/{e_r_path}"
                        if e_b_name != "_DIR_":  # It's a file
                            current_item_full_path = f"{current_item_full_path}/{e_b_name}"

                        current_item_full_path = re.sub(r'/{2,}', '/', current_item_full_path).strip('/')
                        target_full_path_for_scope_check = re.sub(r'/{2,}', '/',
                                                                  target_full_path_for_scope_check).strip('/')

                        if current_item_full_path == target_full_path_for_scope_check:  # It's the target folder itself
                            unique_conceptual_items.add((e_r_name, e_r_path, e_b_name))
                        elif current_item_full_path.startswith(target_full_path_for_scope_check + '/'):  # It's a child
                            # Only direct children are displayed, not grandchildren
                            path_after_target = current_item_full_path[len(target_full_path_for_scope_check):].strip(
                                '/')
                            if '/' not in path_after_target:  # This is a direct child file or folder
                                unique_conceptual_items.add((e_r_name, e_r_path, e_b_name))  # Add the conceptual item

                    else:  # Target is a specific file
                        if e_r_name == target_r_name and e_r_path == target_r_path and e_b_name == target_b_name:
                            unique_conceptual_items.add((e_r_name, e_r_path, e_b_name))

            # For each unique conceptual item, get its NEWEST version to display
            for r_name, r_path, b_name in unique_conceptual_items:
                latest_entry = await self._get_relevant_item_versions(
                    database_file, r_name, r_path, b_name,
                    version_param=None, start_version_param=None, end_version_param=None, all_versions_param=False
                )
                if latest_entry:
                    items_to_process_for_display.append(latest_entry[0])  # Add the single latest version

        # Now, aggregate the data for the items actually being displayed
        aggregated_display_items = {}  # Key: (root_name, relative_path, base_filename), Value: Aggregated display data

        for item_data in items_to_process_for_display:
            root_name = item_data.get('root_upload_name')
            relative_path = (item_data.get('relative_path_in_archive') or '').strip('/')
            base_filename = item_data.get('base_filename')
            version = item_data.get('version')

            # --- NEW: Get total number of versions for this conceptual item ---
            # Call _get_relevant_item_versions with all_versions_param=True to get all versions for this item
            all_versions_for_conceptual_item = await self._get_relevant_item_versions(
                database_file, root_name, relative_path, base_filename,
                version_param=None, start_version_param=None, end_version_param=None, all_versions_param=True
            )
            total_versions_count = len(all_versions_for_conceptual_item)
            # --- END NEW ---

            # The key for aggregation should be unique for what's displayed.
            # If we're showing specific folder version, keep version in key.
            # Otherwise, it's always the newest item that's being displayed so key is just conceptual path.
            if is_target_a_folder_in_db and version_param and can_apply_version_filters_to_target:
                # Key must include version to distinguish folder contents from other versions
                aggregation_key = (root_name, relative_path, base_filename, version)
            else:
                aggregation_key = (root_name, relative_path, base_filename)  # Only show newest for general lists

            if aggregation_key not in aggregated_display_items:
                # Initialize base structure for item
                display_item_entry = {
                    'is_folder': (base_filename == "_DIR_"),
                    'root_name': root_name,
                    'relative_path': relative_path,
                    'base_filename': base_filename,
                    'version': version,  # Include version for display
                    'original_root_name': item_data.get("original_root_name", ""),
                    'is_nicknamed': item_data.get("is_nicknamed", False),
                    'original_base_filename': item_data.get("original_base_filename", ""),
                    'is_base_filename_nicknamed': item_data.get("is_base_filename_nicknamed", False),
                    'total_expected_parts': item_data.get('total_parts', 1) if base_filename != "_DIR_" else 0,
                    'current_parts_found_set': set() if base_filename != "_DIR_" else set(),
                    'part_discord_info': {} if base_filename != "_DIR_" else {},
                    'actual_found_parts_for_display': 0,
                    'is_damaged_by_discord_absence': False,
                    'total_versions': total_versions_count  # NEW: Add total_versions count
                }
                aggregated_display_items[aggregation_key] = display_item_entry

            # For files, collect part information
            if base_filename != "_DIR_":
                part_number = int(item_data.get("part_number", "0"))
                message_id = int(item_data.get("message_id", "0"))
                channel_id = int(item_data.get("channel_id", "0"))

                aggregated_display_items[aggregation_key]['current_parts_found_set'].add(part_number)
                if message_id != 0 and channel_id != 0:
                    aggregated_display_items[aggregation_key]['part_discord_info'][part_number] = (
                        channel_id, message_id)

        # Convert to list and sort for display
        final_display_items = list(aggregated_display_items.values())
        final_display_items.sort(key=lambda x: (
            x['root_name'].lower(), x['relative_path'].lower(), x['base_filename'].lower(),
            self.parse_version(x.get('version', '0.0.0.0'))
        ))

        return final_display_items
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
        fileid = f"{root_upload_name}_{relative_path_in_archive}_{base_filename}_part{part_number}_v{version}"
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
        await self._db_insert_sync(database_file, entry)

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
            await self._db_insert_sync(database_file, entry)
            self.log.info(
                f"Stored metadata for folder: '{root_upload_name}/{relative_folder_path}' version '{version}'")
        else:
            self.log.warning(
                f"Skipped storing duplicate metadata for folder: '{root_upload_name}/{relative_folder_path}' version '{version}'")

    async def upload_single_file(self, interaction: discord.Interaction, file_path: str,
                                 DB_FILE: str,
                                 channel_id: int, root_upload_name: str, relative_path_in_archive: str = '',
                                 overall_parts_uploaded_ref: Optional[List[int]] = None, overall_total_parts: int = 0,
                                 is_top_level_single_file_upload: bool = False,
                                 original_root_name_for_db: str = "", is_nicknamed_flag_for_db: bool = False,
                                 encryption_mode: str = "off", encryption_key: Optional[bytes] = None,
                                 password_seed_hash: str = "",
                                 store_hash_flag: bool = True,
                                 current_chunk_size: int = 0,
                                 version: str = "0.0.0.1"):
        """
        Uploads a single file, splitting it into parts if necessary, with enhanced progress messaging.
        Returns the number of parts successfully uploaded for this file.
        Applies nicknaming to the file's basename if it exceeds 60 characters.
        """
        user_mention = interaction.user.mention
        CHUNK_SIZE = current_chunk_size
        current_file_original_name = os.path.basename(file_path)

        final_base_filename_for_db = current_file_original_name
        original_base_filename_for_db = ""
        is_base_filename_nicknamed_flag_for_db = False

        if is_top_level_single_file_upload and is_nicknamed_flag_for_db and original_root_name_for_db:
            final_base_filename_for_db = root_upload_name
            original_base_filename_for_db = current_file_original_name
            is_base_filename_nicknamed_flag_for_db = True
            self.log.info(
                f"Top-level single file '{current_file_original_name}' was user-nicknamed to '{root_upload_name}'.")
        elif len(current_file_original_name) > 60:
            final_base_filename_for_db = self._generate_random_nickname()
            original_base_filename_for_db = current_file_original_name
            is_base_filename_nicknamed_flag_for_db = True
            self.log.info(
                f"File name '{current_file_original_name}' >60 chars. Auto-nicknamed to: '{final_base_filename_for_db}'.")

        if not root_upload_name:
            root_upload_name = final_base_filename_for_db  # This seems like an old logic branch. In the new system, root_upload_name is always provided
            # as the conceptual top-level container. This line might need review depending on how `root_upload_name` is derived upstream.

        if relative_path_in_archive == '.' or relative_path_in_archive is None:
            relative_path_in_archive = ''
        relative_path_in_archive = relative_path_in_archive.replace(os.path.sep, '/')
        parts_uploaded_for_this_file = 0

        display_path_parts = []
        if is_nicknamed_flag_for_db and original_root_name_for_db:
            display_path_parts.append(f"{root_upload_name} (Original: {original_root_name_for_db})")
        elif is_top_level_single_file_upload:
            display_path_parts.append(root_upload_name)
        else:
            if relative_path_in_archive:
                display_path_parts.append(relative_path_in_archive)

            if is_base_filename_nicknamed_flag_for_db and original_base_filename_for_db:
                display_base_name_for_message = f"{final_base_filename_for_db} (Original: {original_base_filename_for_db})"
            else:
                display_base_name_for_message = final_base_filename_for_db

            display_path_parts.append(display_base_name_for_message)

        display_path = os.path.join(*display_path_parts).replace(os.path.sep, '/')

        # Append version to display path for messages
        display_path_with_version = f"{display_path} (v{version})"

        try:
            file_size = os.path.getsize(file_path)
            total_parts_for_this_file = (file_size + CHUNK_SIZE - 1) // CHUNK_SIZE
            if total_parts_for_this_file == 0:
                total_parts_for_this_file = 1
            self.log.info(
                f"Uploading '{file_path}' as part of '{root_upload_name}' version '{version}'. Storing as '{final_base_filename_for_db}'. "
                f"Total parts for this file: {total_parts_for_this_file} with CHUNK_SIZE: {CHUNK_SIZE} bytes.")
            await self.send_message_robustly(
                channel_id,
                content=f"{user_mention}, Starting upload of `{display_path_with_version}` (Total {total_parts_for_this_file} parts)."
            )
            with open(file_path, 'rb') as f:
                for i in range(total_parts_for_this_file):
                    part_data = f.read(CHUNK_SIZE)
                    if encryption_mode != "off" and encryption_key is not None:
                        try:
                            part_data = self._encrypt_data(part_data, encryption_key)
                            self.log.debug(f"Encrypted part {i + 1} for '{display_path_with_version}'.")
                        except Exception as e:
                            self.log.error(
                                f"ERROR: Encryption failed for part {i + 1} of '{display_path_with_version}': {e}")
                            await self.send_message_robustly(channel_id,
                                                                     f"{user_mention}, Encryption failed for `{display_path_with_version}`. Aborting upload.")
                            raise
                    part_filename = f"{final_base_filename_for_db}.part{i + 1:03d}"
                    await self.send_message_robustly(
                        channel_id,
                        content=f"{user_mention}, Uploading part {i + 1} of {total_parts_for_this_file} for `{display_path_with_version}`..."
                    )
                    file_part_message = await self._send_file_part_to_discord(
                        self.get_channel(channel_id), part_data,
                        part_filename)
                    if file_part_message:
                        await self._store_file_metadata(
                            root_upload_name=root_upload_name,
                            base_filename=final_base_filename_for_db,
                            part_number=i + 1,
                            total_parts=total_parts_for_this_file,
                            message_id=file_part_message.id,
                            channel_id=channel_id,
                            relative_path_in_archive=relative_path_in_archive,
                            database_file=DB_FILE,
                            original_root_name=original_root_name_for_db,
                            is_nicknamed=is_nicknamed_flag_for_db,
                            original_base_filename=original_base_filename_for_db,
                            is_base_filename_nicknamed=is_base_filename_nicknamed_flag_for_db,
                            encryption_mode=encryption_mode,
                            encryption_key_auto=encryption_key if encryption_mode == "automatic" else b"",
                            password_seed_hash=password_seed_hash,
                            store_hash_flag=store_hash_flag,
                            version=version
                        )
                        self.log.info(
                            f"Stored metadata for '{current_file_original_name}' (DB name: '{final_base_filename_for_db}') part {i + 1} version '{version}'.")
                        parts_uploaded_for_this_file += 1
                        if overall_parts_uploaded_ref is not None:
                            overall_parts_uploaded_ref[0] += 1
                            overall_percentage = (overall_parts_uploaded_ref[
                                                      0] / overall_total_parts) * 100 if overall_total_parts > 0 else 0
                            await self.send_message_robustly(
                                channel_id,
                                content=f"{user_mention}, Completed part {parts_uploaded_for_this_file}/{total_parts_for_this_file} of `{display_path_with_version}`. Overall: {overall_parts_uploaded_ref[0]}/{overall_total_parts} parts ({overall_percentage:.0f}%)."
                            )
                    else:
                        raise Exception(f"Failed to upload part {i + 1} of '{current_file_original_name}'.")
            self.log.info(
                f"Finished uploading '{file_path}' version '{version}'. Successfully uploaded {parts_uploaded_for_this_file} parts.")
        except FileNotFoundError:
            self.log.error(f"Error: File not found at '{file_path}'.")
            await self.send_message_robustly(channel_id,
                                                     f"{user_mention}, Error: File not found at '{file_path}'.")
            raise
        except Exception as e:
            self.log.error(f"Error uploading '{file_path}' version '{version}': {e}")
            self.log.error(traceback.format_exc())
            await self.send_message_robustly(channel_id,
                                                     f"{user_mention}, An error occurred during upload of '{display_path_with_version}': {e}")
            raise

    async def upload_folder_contents(self, interaction: discord.Interaction, local_folder_path: str,
                                     DB_FILE: str,
                                     channel_id: int, root_upload_name: str,
                                     overall_uploaded_parts_ref: List[int], overall_total_parts: int,
                                     original_root_name_for_db: str = "", is_nicknamed_flag_for_db: bool = False,
                                     encryption_mode: str = "off", encryption_key: Optional[bytes] = None,
                                     password_seed_hash: str = "",
                                     store_hash_flag: bool = True,
                                     current_chunk_size: int = 0,
                                     version: str = "0.0.0.1"):
        user_mention = interaction.user.mention
        files_processed_count = 0
        total_files_in_folder = 0
        for r, d, f in os.walk(local_folder_path):
            total_files_in_folder += len(f)
        self.log.info(
            f"Starting upload of folder '{root_upload_name}' version '{version}'. Total files: {total_files_in_folder}. Overall parts: {overall_total_parts}")

        original_folder_basename_for_root_entry = ""
        is_folder_basename_nicknamed_for_root_entry = False
        if is_nicknamed_flag_for_db:
            original_folder_basename_for_root_entry = original_root_name_for_db
            is_folder_basename_nicknamed_for_root_entry = True

        await self._store_folder_metadata(
            root_upload_name, "", DB_FILE,
            original_root_name=original_root_name_for_db,
            is_nicknamed=is_nicknamed_flag_for_db,
            original_base_filename=original_folder_basename_for_root_entry,
            is_base_filename_nicknamed=is_folder_basename_nicknamed_for_root_entry,
            encryption_mode=encryption_mode,
            encryption_key_auto=encryption_key if encryption_mode == "automatic" else b"",
            password_seed_hash=password_seed_hash,
            store_hash_flag=store_hash_flag,
            version=version
        )
        self.log.info(f"Stored metadata for root folder: '{root_upload_name}' version '{version}'")

        nicknamed_path_map = {
            os.path.abspath(local_folder_path): ""
        }

        for root, dirs, files in os.walk(local_folder_path):
            relative_path_from_base = os.path.relpath(root, local_folder_path)
            if relative_path_from_base == '.':
                relative_path_from_base = ''

            parent_local_path = os.path.dirname(os.path.abspath(root))
            nicknamed_parent_relative_path = nicknamed_path_map.get(parent_local_path, "")

            for dir_name in dirs:
                full_subfolder_local_path = os.path.join(root, dir_name)

                current_folder_original_name = os.path.basename(dir_name)
                name_for_relative_path_segment = current_folder_original_name

                original_folder_basename_for_db = ""
                is_folder_basename_nicknamed_flag_for_db = False

                # This block for nicknaming subfolders is currently based on `is_nicknamed_flag_for_db`
                # which refers to the top-level root. If the intention is for subfolders to also be nicknamed
                # if *their own names* are too long, the logic for `is_folder_basename_nicknamed_flag_for_db`
                # needs to be based on `len(current_folder_original_name) > 60` directly.
                # I'm adjusting it to reflect common behavior: long names are always nicknamed.
                if len(current_folder_original_name) > 60:
                    name_for_relative_path_segment = self._generate_random_nickname()
                    is_folder_basename_nicknamed_flag_for_db = True
                    original_folder_basename_for_db = current_folder_original_name
                    self.log.debug(
                        f"Sub-folder name '{current_folder_original_name}' >60 chars. Auto-nicknamed its path segment to: '{name_for_relative_path_segment}'.")
                elif is_nicknamed_flag_for_db:  # If the root itself was nicknamed, this sub-folder is also part of that
                    is_folder_basename_nicknamed_flag_for_db = True
                    original_folder_basename_for_db = current_folder_original_name

                relative_path_for_new_folder = os.path.join(nicknamed_parent_relative_path,
                                                            name_for_relative_path_segment)
                relative_path_for_new_folder = relative_path_for_new_folder.replace(os.path.sep, '/')

                nicknamed_path_map[os.path.abspath(full_subfolder_local_path)] = relative_path_for_new_folder

                await self._store_folder_metadata(
                    root_upload_name,
                    relative_path_for_new_folder,
                    DB_FILE,
                    original_root_name=original_root_name_for_db,
                    is_nicknamed=is_nicknamed_flag_for_db,
                    original_base_filename=original_folder_basename_for_db,
                    is_base_filename_nicknamed=is_folder_basename_nicknamed_flag_for_db,
                    encryption_mode=encryption_mode,
                    encryption_key_auto=encryption_key if encryption_mode == "automatic" else b"",
                    password_seed_hash=password_seed_hash,
                    store_hash_flag=store_hash_flag,
                    version=version
                )

            relative_path_for_files_in_current_dir = nicknamed_path_map.get(os.path.abspath(root), "")

            for file_name in files:
                full_file_path = os.path.join(root, file_name)
                files_processed_count += 1
                try:
                    await self.upload_single_file(
                        interaction, full_file_path, DB_FILE, channel_id, root_upload_name,
                        relative_path_in_archive=relative_path_for_files_in_current_dir,
                        overall_parts_uploaded_ref=overall_uploaded_parts_ref,
                        overall_total_parts=overall_total_parts,
                        is_top_level_single_file_upload=False,
                        original_root_name_for_db=original_root_name_for_db,
                        is_nicknamed_flag_for_db=is_nicknamed_flag_for_db,
                        encryption_mode=encryption_mode, encryption_key=encryption_key,
                        password_seed_hash=password_seed_hash,
                        store_hash_flag=store_hash_flag,
                        current_chunk_size=current_chunk_size,
                        version=version
                    )
                    self.log.info(f"Successfully processed file '{full_file_path}' version '{version}'.")
                except Exception as e:
                    self.log.error(f"Error processing file '{full_file_path}' version '{version}': {e}")
                    self.log.error(traceback.format_exc())
                    pass
        await self.send_message_robustly(channel_id,
                                                 content=f"{user_mention}, Completed processing all files and folders in '{root_upload_name}' version '{version}'. Finalizing upload...")
        self.log.info(f"Finished uploading all contents of folder '{root_upload_name}' version '{version}'.")

    async def _start_upload_process(self, interaction: discord.Interaction, local_path: str,
                                    DB_FILE: str,
                                    channel_id: int, custom_root_name: Optional[str] = None,
                                    encryption_mode: str = "off", user_seed: Optional[str] = None,
                                    random_seed: bool = False, save_hash: bool = True,
                                    upload_mode: str = "new_upload",
                                    target_item_path: Optional[str] = None,
                                    new_version_string: Optional[str] = None):
        user_id = interaction.user.id
        user_mention = interaction.user.mention
        is_folder = os.path.isdir(local_path)

        original_root_name_for_db = ""
        is_nicknamed_flag_for_db = False
        root_upload_name = ""

        # --- Determine root_upload_name (and original name/nicknaming flags for DB) ---
        effective_root_name_from_path = os.path.basename(local_path)

        if custom_root_name is not None:
            if len(custom_root_name) > 60:
                await interaction.followup.send(
                    content=f"{user_mention}, Error: Your provided 'upload_name' ('{custom_root_name}') is too long. Please keep it 60 characters or less.",
                    ephemeral=False)
                self.log.info(
                    f">>> [UPLOAD] User {user_id} provided a custom_root_name > 60 chars. Upload aborted.")
                return
            else:
                root_upload_name = custom_root_name
                if custom_root_name != effective_root_name_from_path:
                    original_root_name_for_db = effective_root_name_from_path
                    is_nicknamed_flag_for_db = True
                    self.log.info(
                        f"Using user-provided custom root name: '{root_upload_name}'. Original name was '{original_root_name_for_db}'. Marked as nicknamed.")
                else:
                    original_root_name_for_db = ""
                    is_nicknamed_flag_for_db = False
                    self.log.info(
                        f"Using user-provided root name: '{root_upload_name}' (same as original local name).")
        else:
            if len(effective_root_name_from_path) > 60:
                original_root_name_for_db = effective_root_name_from_path
                root_upload_name = self._generate_random_nickname()
                is_nicknamed_flag_for_db = True
                self.log.info(
                    f"Original local path name '{original_root_name_for_db}' is >60 chars. Auto-nicknamed to: '{root_upload_name}'")
            else:
                root_upload_name = effective_root_name_from_path
                original_root_name_for_db = ""
                is_nicknamed_flag_for_db = False
                self.log.info(
                    f"Using local path basename as root name: '{root_upload_name}' (length <= 60 chars).")

        # --- Versioning Logic ---
        current_version_for_upload = "0.0.0.1"  # Default for new uploads
        inherited_encryption_key: Optional[bytes] = None
        inherited_password_seed_hash: str = ""
        inherited_encryption_mode: str = encryption_mode
        inherited_store_hash_flag: bool = save_hash

        if upload_mode == "new_version":
            if not target_item_path:
                await interaction.followup.send(
                    f"{user_mention}, Error: `target_item_path` is required when `upload_mode` is `new_version`.",
                    ephemeral=False)
                return

            # Resolve target_item_path into its DB components
            all_db_entries = await self._db_read_sync(DB_FILE, {})  # Need all entries for resolution
            resolved_target_info = await self._resolve_path_to_db_entry_keys(target_item_path, all_db_entries)

            if not resolved_target_info:
                await interaction.followup.send(
                    f"{user_mention}, Error: Target item '{target_item_path}' not found in the database for versioning. Please ensure the path is correct.",
                    ephemeral=False)
                return

            target_root_name, target_relative_path, target_base_filename, _ = resolved_target_info

            # Check if the type of local_path (file/folder) matches the type of target_item_path in DB
            is_target_folder_in_db = (target_base_filename == "_DIR_")
            if is_folder != is_target_folder_in_db:
                await interaction.followup.send(
                    f"{user_mention}, Error: The type of the local path ({'folder' if is_folder else 'file'}) does not match the type of the target item '{target_item_path}' in the database ({'folder' if is_target_folder_in_db else 'file'}). Cannot create new version.",
                    ephemeral=False
                )
                return

            # Fetch the latest existing item's metadata
            latest_existing_item = await self._get_item_metadata_for_versioning(
                DB_FILE, target_root_name, target_relative_path, target_base_filename
            )

            if not latest_existing_item:
                # This should ideally not be hit if _resolve_path_to_db_entry_keys worked, but defensive
                await interaction.followup.send(
                    f"{user_mention}, Error: Could not retrieve existing metadata for '{target_item_path}'. Cannot create new version.",
                    ephemeral=False)
                return

            # Inherit encryption mode/key/hash from the latest existing version
            inherited_encryption_mode = latest_existing_item.get('encryption_mode', 'off')
            inherited_store_hash_flag = latest_existing_item.get('store_hash_flag', True)

            if inherited_encryption_mode == "automatic":
                inherited_encryption_key = latest_existing_item.get('encryption_key_auto')
                if not inherited_encryption_key:
                    inherited_encryption_key = Fernet.generate_key()  # Generate new if missing
                    self.log.warning(
                        f"No existing automatic key found for '{target_item_path}', generating new one for new version.")
                else:
                    self.log.info(
                        f"Inheriting automatic encryption key for '{target_item_path}' from previous version.")
            elif inherited_encryption_mode == "not_automatic":
                if random_seed:
                    user_seed = self._generate_random_nickname(length=32)
                    await interaction.followup.send(
                        f"{user_mention}, sending a random password seed for new version of '{target_item_path}' to your DMs...",
                        ephemeral=True
                    )
                    await interaction.user.send(
                        f"Your random password seed for new version of '{target_item_path}' is: ||`{user_seed}`||. Please save this securely!"
                    )
                    self.log.info(
                        f"Generated random seed for new version of '{target_item_path}' for user {user_id}. Sent via DM.")
                elif not user_seed:
                    await interaction.followup.send(
                        content=f"{user_mention}, Error: For 'Not Automatic' encryption, you must provide a `password_seed` for the new version.",
                        ephemeral=False)
                    return

                derived_key = self._derive_key_from_seed(user_seed)
                derived_hash = hashlib.sha256(user_seed.encode('utf-8')).hexdigest()

                # Zero-knowledge check (if hash was stored)
                if inherited_store_hash_flag and latest_existing_item.get(
                        'password_seed_hash') and derived_hash != latest_existing_item.get('password_seed_hash'):
                    await interaction.followup.send(
                        f"{user_mention}, Error: Provided password seed does not match the hash of the previous version's key for '{target_item_path}'. Upload aborted.",
                        ephemeral=False
                    )
                    return

                inherited_encryption_key = derived_key
                inherited_password_seed_hash = derived_hash if inherited_store_hash_flag else ""
                self.log.info(
                    f"Using provided password seed for new version of '{target_item_path}'. Hash check {'passed' if inherited_store_hash_flag else 'skipped (zero-knowledge)'}.")

            # Determine the version string for the NEW version
            if new_version_string:
                current_version_for_upload = new_version_string
            else:
                current_version_for_upload = await self._generate_next_version_string(
                    DB_FILE, target_root_name, target_relative_path, target_base_filename
                )

            # Check if this specific version string already exists for the item
            if await self._check_item_version_exists(
                    DB_FILE, target_root_name, target_relative_path, target_base_filename, current_version_for_upload
            ):
                await interaction.followup.send(
                    f"{user_mention}, Error: Version '{current_version_for_upload}' already exists for item '{target_item_path}'. Please choose a different version string.",
                    ephemeral=False
                )
                return

            # Override root_upload_name and related flags for the new version based on the *target item's* existing root
            # We need the conceptual top-level root that `target_item_path` falls under.
            # If target_item_path is "MyProject/SubFolder/file.txt", then `root_upload_name` should be "MyProject"
            # and `original_root_name_for_db` and `is_nicknamed_flag_for_db` should come from `MyProject`'s DB entry.

            # Find the actual top-level item (RootName in "RootName/SubPath/File.ext")
            path_parts = target_item_path.strip('/').split('/')
            conceptual_top_level_name = path_parts[0]

            top_level_item_entry = None
            # Try to find it as a top-level folder
            top_level_folder_query = {
                "root_upload_name": conceptual_top_level_name,
                "relative_path_in_archive": "",
                "base_filename": "_DIR_"
            }
            folder_entries = await self._db_read_sync(DB_FILE, top_level_folder_query)
            if folder_entries:
                top_level_item_entry = folder_entries[0]
            else:  # Try to find it as a top-level file
                top_level_file_query = {
                    "root_upload_name": conceptual_top_level_name,
                    "relative_path_in_archive": "",
                    "base_filename": conceptual_top_level_name
                }
                file_entries = await self._db_read_sync(DB_FILE, top_level_file_query)
                if file_entries:
                    top_level_item_entry = file_entries[0]

            if top_level_item_entry:
                root_upload_name = top_level_item_entry.get('root_upload_name')
                original_root_name_for_db = top_level_item_entry.get('original_root_name')
                is_nicknamed_flag_for_db = top_level_item_entry.get('is_nicknamed')
            else:
                # Fallback (should not be reached if path resolution and previous checks are solid)
                self.log.warning(
                    f"Could not find top-level entry for '{conceptual_top_level_name}' during new version upload. Using derived names.")
                root_upload_name = conceptual_top_level_name
                original_root_name_for_db = ""
                is_nicknamed_flag_for_db = False

            self.log.info(
                f"Preparing to upload new version '{current_version_for_upload}' for existing item '{target_item_path}'.")

        else:  # upload_mode == "new_upload"
            if new_version_string:
                current_version_for_upload = new_version_string
            else:
                current_version_for_upload = "0.0.0.1"

            if (await self.is_folder_entry_in_db(DB_FILE, root_upload_name,"", current_version_for_upload) or
                    await self._check_item_version_exists(DB_FILE, root_upload_name, "", root_upload_name,
                                                                  current_version_for_upload)):
                await interaction.followup.send(
                    f"{user_mention}, Error: An item with the name '{root_upload_name}' and version '{current_version_for_upload}' already exists. Please choose a different name for this new upload or use `upload_mode: new_version` to add a version to it.",
                    ephemeral=False
                )
                return

            # Encryption for new uploads: Generate new keys/hashes as before
            if inherited_encryption_mode == "automatic":
                inherited_encryption_key = Fernet.generate_key()
                self.log.info(f"Generated new automatic encryption key for new upload '{root_upload_name}'.")
            elif inherited_encryption_mode == "not_automatic":
                if random_seed:
                    user_seed = self._generate_random_nickname(length=32)
                    await interaction.followup.send(
                        f"{user_mention}, sending a random password seed for upload '{root_upload_name}' to your DMs...",
                        ephemeral=True
                    )
                    await interaction.user.send(
                        f"Your random password seed for upload '{root_upload_name}' is: ||`{user_seed}`||. Please save this securely! You will need it to download your files."
                    )
                    self.log.info(
                        f"Generated random seed for '{root_upload_name}' for user {user_id}. Sent via DM.")
                elif not user_seed:
                    await interaction.followup.send(
                        content=f"{user_mention}, For 'Not Automatic' encryption, either a password seed must be provided or `random_seed` must be set to `True`.",
                        ephemeral=False)
                    return
                try:
                    inherited_encryption_key = self._derive_key_from_seed(user_seed)
                    if inherited_store_hash_flag:
                        inherited_password_seed_hash = hashlib.sha256(user_seed.encode('utf-8')).hexdigest()
                        self.log.info(
                            f"Derived encryption key and hash from user seed for '{root_upload_name}'. Hash saved.")
                    else:
                        inherited_password_seed_hash = ""
                        self.log.info(
                            f"Derived encryption key from user seed for '{root_upload_name}'. Hash NOT saved (zero-knowledge mode).")
                except ValueError as e:
                    await interaction.followup.send(
                        content=f"{user_mention}, Error deriving encryption key: {e}. Aborting upload.",
                        ephemeral=False)
                    return
                except Exception as e:
                    self.log.error(f"ERROR: Unexpected error deriving key: {e}")
                    self.log.error(traceback.format_exc())
                    await interaction.followup.send(
                        f"{user_mention}, An unexpected error occurred during key derivation. Please try again.",
                        ephemeral=False)
                    self.user_uploading.pop(interaction.user.id, None)
                    return

        # --- Normalize DB_FILE path ---
        if not DB_FILE.lower().endswith(('.db')):
            DB_FILE += '.db'
        DATABASE_FILE = os.path.abspath(os.path.normpath(DB_FILE))

        # --- Duplicate Root Upload Name Check (ONLY for new_upload mode, across ALL versions) ---
        if upload_mode == "new_upload":
            await interaction.followup.send(
                content=f"{user_mention}, Initiating upload process for '{root_upload_name}' version '{current_version_for_upload}'. Checking for root-level duplicates...")

            # Check if an item with `root_upload_name` exists at the top-level AT ALL (any version)
            # This prevents creating a completely new `root_upload_name` if one already exists.
            # This check is separate from `_check_item_version_exists` which checks for a specific name+version.
            # This `_check_duplicate_root_upload_name` ensures `MyProject` doesn't exist if user wants a *new* one.
            if await self._check_duplicate_root_upload_name(DATABASE_FILE, root_upload_name, new_version_string):
                await interaction.followup.send(
                    content=f"{user_mention}, An item with the name '{root_upload_name}' already exists in your database (it might be an older version or a different type). Please choose a different name for this new upload or use `upload_mode: new_version` to add a new version to it.",
                    ephemeral=False)
                self.log.info(
                    f">>> [UPLOAD] User {user_id} attempted to upload '{root_upload_name}' but a duplicate (of any version/type) was found at the top-level. Upload aborted.")
                return

        # --- Concurrency Control ---
        if user_id not in self.user_uploading:
            self.user_uploading[user_id] = []
        if root_upload_name in self.user_uploading[user_id]:
            await interaction.followup.send(
                content=f"{user_mention}, You are already uploading an item named '{root_upload_name}'. Please wait for it to complete or choose a different name if you wish to upload again.",
                ephemeral=False)
            self.log.info(
                f">>> [UPLOAD] User {user_id} attempted to upload '{root_upload_name}' but an upload with that name is already in progress.")
            return
        self.user_uploading[user_id].append(root_upload_name)
        self.log.info(
            f">>> [UPLOAD] User {user_id} started upload of '{root_upload_name}' version '{current_version_for_upload}'. Currently uploading: {self.user_uploading[user_id]}")

        # Initial message after checks pass
        await interaction.followup.send(
            content=f"{user_mention}, No conflicts found. Starting upload of '{root_upload_name}' (Version: {current_version_for_upload}). You will receive further updates as new messages.")

        # --- Determine Chunk Size ---
        current_chunk_size = 10 * 1024 * 1024 if inherited_encryption_mode == "off" else 7 * 1024 * 1024
        self.log.info(
            f"Using {current_chunk_size / (1024 * 1024):.0f} MB chunk size for '{inherited_encryption_mode}' upload of '{root_upload_name}'.")

        overall_uploaded_parts_ref = [0]
        overall_total_parts = 0
        upload_successful = True

        try:
            # --- Pre-calculation of total parts ---
            try:
                if not os.path.exists(local_path):
                    raise FileNotFoundError(f"Local path not found: {local_path}")

                if os.path.isdir(local_path):
                    for root, _, files in os.walk(local_path):
                        for file_name in files:
                            full_file_path = os.path.join(root, file_name)
                            try:
                                file_size = os.path.getsize(full_file_path)
                                parts_for_file = (file_size + current_chunk_size - 1) // current_chunk_size
                                if parts_for_file == 0: parts_for_file = 1
                                overall_total_parts += parts_for_file
                            except FileNotFoundError:
                                self.log.warning(
                                    f"Warning: File not found during pre-calculation: {full_file_path}. It will be skipped.")
                                pass
                else:
                    file_size = os.path.getsize(local_path)
                    overall_total_parts = (file_size + current_chunk_size - 1) // current_chunk_size
                    if overall_total_parts == 0: overall_total_parts = 1
            except FileNotFoundError:
                await self.send_message_robustly(channel_id,
                                                         content=f"{user_mention}, Error: The item '{local_path}' was not found for upload during pre-calculation.")
                self.log.error(
                    f">>> [UPLOAD] ERROR: Item '{local_path}' not found during pre-calculation for user {user_id}.")
                upload_successful = False
                return
            except Exception as e:
                await self.send_message_robustly(channel_id,
                                                         content=f"{user_mention}, An unexpected error occurred during pre-calculation for '{local_path}': {e}.")
                self.log.error(
                    f">>> [UPLOAD] ERROR: Unexpected error during pre-calculation for '{local_path}': {e}")
                self.log.error(traceback.format_exc())
                upload_successful = False
                return

            self.log.debug(
                f">>> [UPLOAD] Overall total parts calculated: {overall_total_parts}. overall_uploaded_parts_ref: {overall_uploaded_parts_ref[0]}")

            await self.upload_semaphore.acquire()
            self.log.info(
                f">>> [UPLOAD] Acquired upload semaphore for user {user_id} ('{root_upload_name}'). Available permits: {self.upload_semaphore._value}")

            if os.path.isdir(local_path):
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
                    version=current_version_for_upload)
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
                    version=current_version_for_upload)

            if overall_uploaded_parts_ref[0] != overall_total_parts:
                upload_successful = False
                self.log.warning(
                    f"Upload of '{root_upload_name}' version '{current_version_for_upload}' completed but is incomplete. Uploaded {overall_uploaded_parts_ref[0]} of {overall_total_parts} parts.")
            else:
                await self.send_message_robustly(channel_id,
                                                         content=f"{user_mention}, All parts of '{root_upload_name}' (Version: {current_version_for_upload}) have been uploaded successfully!")
                self.log.info(
                    f"Successfully completed upload process for '{root_upload_name}' version '{current_version_for_upload}'.")
        except Exception as e:
            self.log.critical(
                f"Critical error during upload process for '{root_upload_name}' version '{current_version_for_upload}': {e}")
            self.log.critical(traceback.format_exc())
            await interaction.followup.send(
                content=f"{user_mention}, A critical and unexpected error occurred during the upload of '{root_upload_name}' (Version: {current_version_for_upload}): {e}. Please report this and try again."
            )
            upload_successful = False
        finally:
            if not upload_successful:
                problem_message_prefix = f"{user_mention}, **Upload Incomplete!** "
                problem_message_detail = (
                    f"The upload process for `{root_upload_name}` (Version: {current_version_for_upload}) did not complete 100%. "
                    f"Only {overall_uploaded_parts_ref[0]} of {overall_total_parts} parts were successfully uploaded. "
                    f"This could be due to network issues, Discord API limits, or an unexpected error.\n\n"
                )
                problem_message_suffix = f"The incomplete metadata for `{root_upload_name}` (Version: {current_version_for_upload}) is still in your database (`{DATABASE_FILE}`)."
                problem_message = problem_message_prefix + problem_message_detail + problem_message_suffix
                view = discord.ui.View(timeout=180)
                remove_button = discord.ui.Button(
                    label="Remove Incomplete Upload", style=discord.ButtonStyle.red, emoji="🗑️",
                    custom_id="remove_incomplete_upload"
                )

                async def remove_upload_callback(button_interaction: discord.Interaction):
                    await button_interaction.response.defer(ephemeral=True)
                    # Call the self's method for deletion
                    # This needs to be available on the self, likely from deletesystems.py
                    await self.delete_specific_item_version_entry(
                        button_interaction,
                        root_upload_name,
                        DATABASE_FILE,
                        current_version_for_upload,
                        is_folder  # Pass if it's a folder or file
                    )
                    for item in view.children:
                        item.disabled = True
                    await button_interaction.message.edit(view=view)
                    await button_interaction.followup.send(f"Operation completed.", ephemeral=True)

                remove_button.callback = remove_upload_callback
                view.add_item(remove_button)
                cancel_button = discord.ui.Button(
                    label="Keep Files (Cancel)", style=discord.ButtonStyle.grey, emoji="❌",
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
                max_button_msg_attempts = 15
                initial_button_msg_delay = 5
                max_button_msg_delay = 60
                button_msg_sent = False
                button_msg_start_time = time.time()
                for attempt in range(1, max_button_msg_attempts + 1):
                    try:
                        await interaction.followup.send(content=problem_message, view=view)
                        self.log.info(
                            f"Successfully sent incomplete upload message with buttons on attempt {attempt}/{max_button_msg_attempts}.")
                        button_msg_sent = True
                        break
                    except (discord.HTTPException, aiohttp.ClientError, asyncio.TimeoutError) as e:
                        self.log.warning(
                            f"Failed to send incomplete upload message with buttons (Attempt {attempt}/{max_button_msg_attempts}): {type(e).__name__}: {e!r}")
                        if attempt < max_button_msg_attempts:
                            current_delay = min(max_button_msg_delay, initial_button_msg_delay * (2 ** (attempt - 1)))
                            jitter = random.uniform(0, current_delay * 0.25)
                            wait_time = current_delay + jitter
                            self.log.debug(f"Retrying button message in {wait_time:.2f} seconds...")
                            await asyncio.sleep(wait_time)
                        else:
                            self.log.error(
                                f"Max attempts ({max_button_msg_attempts}) reached for sending incomplete upload message. Giving up.")
                    except Exception as e:
                        self.log.error(
                            f"Unexpected error sending incomplete upload message with buttons (Attempt {attempt}/{max_button_msg_attempts}): {type(e).__name__}: {e!r}")
                        self.log.error(traceback.format_exc())
                        break
                if not button_msg_sent:
                    self.log.warning(
                        f"WARNING: Incomplete upload message with buttons was NOT sent after all attempts for '{root_upload_name}'.")
                    await interaction.followup.send(
                        content=f"{user_mention}, **Upload Incomplete!** (Failed to send interactive cleanup options). "
                                f"The upload for `{root_upload_name}` (Version: {current_version_for_upload}) did not complete. "
                                f"Its incomplete metadata is still in your database (`{DATABASE_FILE}`). "
                                f"You can use `/delete` to remove it manually if needed."
                    )
            if self.upload_semaphore._value < self._upload_semaphore_initial_capacity:
                self.upload_semaphore.release()
                self.log.info(
                    f">>> [UPLOAD] Released upload semaphore for user {user_id} ('{root_upload_name}'). Available permits: {self.upload_semaphore._value}")
            else:
                self.log.warning(
                    f">>> [UPLOAD] Semaphore not released for user {user_id} ('{root_upload_name}') as it was not acquired or already released.")
            if user_id in self.user_uploading and root_upload_name in self.user_uploading[user_id]:
                self.user_uploading[user_id].remove(root_upload_name)
            if user_id in self.user_uploading and not self.user_uploading[user_id]:
                del self.user_uploading[user_id]
            self.log.info(
                f">>> [UPLOAD] User {user_id} finished upload of '{root_upload_name}' version '{current_version_for_upload}'. user_uploading state cleared.")

    async def _is_root_upload_a_file(self, all_db_entries: List[Dict[str, Any]], root_upload_name: str) -> bool:
        """Checks if the given root_upload_name corresponds to a top-level file."""
        for entry in all_db_entries:
            if entry.get('root_upload_name') == root_upload_name and \
                    entry.get('relative_path_in_archive') == '' and \
                    entry.get('base_filename') != '_DIR_':
                return True
        return False

    def _calculate_retry_delay(self, attempt: int, base_delay: float = 1.0, max_delay: float = 60.0) -> float:
        """
        Calculates the exponential backoff delay with added jitter.

        Args:
            attempt (int): The current retry attempt (0-indexed).
            base_delay (float): The base delay in seconds for the first retry.
            max_delay (float): The maximum allowed delay in seconds.

        Returns:
            float: The calculated delay in seconds.
        """
        # Exponential backoff: base_delay * (2 ** attempt)
        delay = base_delay * (2 ** attempt)
        # Add jitter (randomness) to prevent thundering herd problem
        jitter = random.uniform(0, base_delay / 2)  # Jitter up to half of the base delay
        calculated_delay = min(delay + jitter, max_delay)
        return calculated_delay


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