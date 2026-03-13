import asyncio
import logging
import os
import traceback
from typing import Dict, Optional, Tuple
import discord
from database import DatabaseManager
from versioning import VersioningManager
from downloadtools.message_related import MRD
from downloadtools.download_database import DDB
from downloadtools.utils import _is_root_upload_a_file
from baseapi import BASEapi
from downloadtools.files import files
import downloadtools.utils as utils
from downloadtools.FS_manager import FS
class DecryptionCancelledError(Exception): pass
class DownloadContext:
    def __init__(self,bot,log,intents,interaction: discord.Interaction,enc=True):
        self.interaction = interaction
        self.user_id = interaction.user.id
        self.bot = bot
        self.user_mention = interaction.user.mention
        self.log_prefix = "[DOWNLOADA]"
        self.log = log
        self.file_table_columns = [
            'fileid', 'base_filename', 'part_number', 'total_parts',
            'message_id', 'channel_id', 'relative_path_in_archive', 'root_upload_name', 'upload_timestamp',
            'original_root_name', 'is_nicknamed',
            'original_base_filename', 'is_base_filename_nicknamed',
            'encryption_mode', 'encryption_key_auto', 'password_seed_hash',
            'store_hash_flag',
            'version'  # Added new column for versioning
        ]
        self.baseapi=BASEapi(self.log,intents=intents) #baseapi.py
        self.MRD=MRD(self.baseapi) #message_related.py
        self.DDB=DDB(interaction) #download_database.py
        self.db = DatabaseManager(file_table_columns=self.file_table_columns, log=self.log) #database.py
        self.version_manager = VersioningManager(db_read_func=self.db._db_read_sync, log=self.log) #versioning.py
        if enc:
            from downloadtools.encrytion import denc
            self.denc=denc(log=self.log,ddb=self.DDB,version_manager=self.version_manager) #encrytion.py
        self.files = files(self.log,self.version_manager,self.DDB,self.baseapi,self.bot) #files.py
        self.utils = utils #utils.py
        self.FS = FS(self.log,self.baseapi)
        self._get_file_parts_from_discord = self.MRD._get_file_parts_from_discord
        self._resolve_target_and_version_mode = self.DDB._resolve_target_and_version_mode
        self._get_original_path_components = self.DDB._get_original_path_components
        self._is_root_upload_a_file = _is_root_upload_a_file
    async def download_filea(
        self,
        target_path: str,
        DB_FILE: str,
        download_folder: str,
        decryption_password_seed: Optional[Dict[Tuple[str, str, str, str], bytes]] = None,
        version_param: Optional[str] = None,
        start_version_param: Optional[str] = None,
        end_version_param: Optional[str] = None,
        all_versions_param: bool = False,
        can_apply_version_filters: bool = False
    ):
        """Orchestrator for downloading files/folders with versioning and encryption."""
        # --- Original initialisation ---
        decryption_password_seed = decryption_password_seed or {}
        acquired_semaphore = False
        self.download_queue = asyncio.Queue()
        self.download_semaphore = asyncio.Semaphore(3)
        self.user_downloading: Dict[int, str] = {}  # Track root_upload_names per user
        self.http_session = None
        self.total_parts_cache: Dict[
            str, int] = {}  # Stores total parts for a given lnaocal file path (e.g., 'path/to/file.txt')
        self._db_table_init_status: Dict[str, bool] = {}
        self.discord_api_delay = 0.05
        self.batch_size_discord_checks = 50
        self.batch_delay_discord_checks = 2.0
        self._HKDF_SALT = b"my_bot_encryption_salt_12345"
        self._HKDF_INFO = b"discord_file_bot_encryption_info"
        download_successful = True
        overall_parts_downloaded = 0
        overall_total_parts = 0
        local_cleanup_path = None
        multiple_versions = False
        should_create_target_folder = False
        db_path = None


        # Normalise target path
        normalized_target_path = os.path.normpath(target_path).replace(os.path.sep, '/').strip('/')
        if normalized_target_path == ".":
            normalized_target_path = ""  # treat '.' as global download

        # Set user downloading state immediately (original order)
        self.user_downloading[self.user_id] = target_path
        self.log.info(f">>> [DOWNLOAD] User {self.user_id} started download of '{target_path}'.")

        try:
            # --- Acquire semaphore ---
            await self.download_semaphore.acquire()
            acquired_semaphore = True
            self.log.info(
                f">>> [DOWNLOAD] Acquired download semaphore for user {self.user_id} ('{target_path}'). "
                f"Available permits: {self.download_semaphore._value}"
            )

            # --- Step 2: Validate inputs (original order) ---
            # Database file
            if not DB_FILE.lower().endswith('.db'):
                DB_FILE += '.db'
            db_path = os.path.abspath(os.path.normpath(DB_FILE))
            if not os.path.exists(db_path):
                await self.interaction.followup.send(
                    content=f"{self.user_mention}, the database file '{DB_FILE}' was not found.",
                    ephemeral=False
                )
                self.log.error(f">>> [DOWNLOAD] ERROR: Database file not found at '{db_path}'.")
                download_successful = False
                return

            all_entries = await self.db._db_read_sync(db_path, {})
            if not all_entries:
                await self.interaction.followup.send(
                    content=f"{self.user_mention}, No items found in database '{DB_FILE}'.",
                    ephemeral=False
                )
                self.log.info(f"No entries found in database: '{DB_FILE}'.")
                download_successful = False
                return

            # Download folder
            base_download_dir = os.path.abspath(os.path.normpath(download_folder))
            os.makedirs(base_download_dir, exist_ok=True)
            self.log.info(f"Ensured base download directory exists: '{base_download_dir}'.")

            # --- Step 3: Resolve target path and determine download mode ---
            resolved_info = await self._resolve_target_and_version_mode(
                normalized_target_path,
                all_entries,
                version_param,
                start_version_param,
                end_version_param,
                all_versions_param,
                can_apply_version_filters,target_path)
            if resolved_info is None:
                await self.interaction.followup.send(
                    f"{self.user_mention}, Could not find any item matching '{target_path}' in `{db_path}`."
                )
                self.log.info(f">>> [DOWNLOAD] No item found for '{target_path}' in '{db_path}' after resolution.")
                download_successful = False
                return

            # Extract flags from resolved_info
            is_global = resolved_info['is_global']
            resolved_root = resolved_info.get('root')
            resolved_rel_path = resolved_info.get('rel_path')
            resolved_base = resolved_info.get('base_filename')
            is_target_folder = resolved_info.get('is_folder', False)
            multiple_versions = resolved_info['multiple_versions']
            should_create_target_folder = resolved_info.get('should_create_target_folder', False)

            # --- Step 4: Collect relevant file entries ---
            files_grouped, total_parts = await self.files._collect_relevant_files(
                normalized_target_path,
                all_entries,
                resolved_info,
                version_param,
                start_version_param,
                end_version_param,
                all_versions_param,
                can_apply_version_filters,
                db_path
            )
            print(f'files groups: {files_grouped}')
            print(f'total parts: {total_parts}')
            if not files_grouped:
                await self.interaction.followup.send(
                    f"{self.user_mention}, No downloadable file parts found for '{target_path}' with the specified version criteria.",
                    ephemeral=False
                )
                self.log.info(f"No downloadable items found for '{target_path}' with version criteria.")
                download_successful = False
                return


            overall_total_parts = total_parts
            if overall_total_parts == 0:
                await self.interaction.followup.send(
                    content=f"{self.user_mention}, No actual file parts found to download within '{target_path}' with the specified version criteria.",
                    ephemeral=False
                )
                self.log.info(f"No file parts to download for target: '{target_path}' with version criteria.")
                download_successful = False
                return

            # --- Step 5: Prepare output directories and cleanup path ---
            local_cleanup_path = await self.files._prepare_output_directories(
                self.interaction,download_folder,
                base_download_dir,
                all_entries,
                resolved_info,
                files_grouped,
                multiple_versions,
                should_create_target_folder,
                is_global
            )

            # --- Step 6: Download each file sequentially ---
            sorted_files = sorted(
                files_grouped.items(),
                key=lambda item: (item[0][0], item[0][1], self.version_manager.parse_version(item[0][3]))
            )
            print(f'sorted files {sorted_files}')

            for (root_name, rel_path, base_name, version), file_data in sorted_files:
                self.log.debug(f"DEBUG: Processing file_data for version {version}: {file_data}")

                # Determine encryption key (original logic)
                encryption_key = self.denc._get_file_encryption_key(
                    file_data,
                    decryption_password_seed,
                    (root_name, rel_path, base_name, version)
                )
                if encryption_key is None:
                    self.log.warning(
                        f"File '{base_name}' version '{version}' requires 'not_automatic' decryption but no key found. Skipping this file."
                    )
                    download_successful = False
                    await self.interaction.followup.send(
                        content=f"{self.user_mention}, Skipping `{file_data.get('original_file_name', base_name)}` version `{version}` because no decryption key was provided or it was incorrect."
                    )
                    overall_parts_downloaded += file_data['total_expected_parts']  # count as skipped (original behaviour)
                    continue

                # Compute output and display paths (original complex logic)
                output_path, display_path = await self.utils._compute_file_paths(
                    all_entries,
                    resolved_info,
                    file_data,
                    root_name,
                    rel_path,
                    base_name,
                    version,
                    base_download_dir,
                    local_cleanup_path,
                    multiple_versions,
                    should_create_target_folder,
                    is_global,
                    is_target_folder
                )

                # Download the file
                file_success, parts_downloaded = await self.files._download_single_file(
                    self.interaction,
                    file_data,
                    output_path,
                    display_path,
                    encryption_key,
                    version,
                    self.user_mention,
                    local_cleanup_path,
                    overall_parts_downloaded,
                    overall_total_parts
                )

                overall_parts_downloaded += parts_downloaded
                if not file_success:
                    download_successful = False

                # Send overall progress (original)
                if overall_total_parts > 0:
                    percent = (overall_parts_downloaded / overall_total_parts) * 100
                    await self.interaction.followup.send(
                        content=f"{self.user_mention}, Overall Download Progress: {overall_parts_downloaded}/{overall_total_parts} parts ({percent:.0f}%)"
                    )

            # --- Step 7: Finalise download ---
            await self.FS._finalize_download(
                self.interaction,
                self.user_mention,
                target_path,
                download_folder,
                base_download_dir,
                local_cleanup_path,
                download_successful and overall_parts_downloaded == overall_total_parts,
                multiple_versions,
                should_create_target_folder,
                is_global,
                overall_parts_downloaded,
                overall_total_parts
            )

        except DecryptionCancelledError:
            self.log.info("Download cancelled by user due to decryption failure.")
            await self.interaction.followup.send(f"{self.user_mention}, Download cancelled.")
            download_successful = False

        except Exception as e:
            self.log.critical(f"Critical error during download process for '{target_path}': {e}")
            self.log.critical(traceback.format_exc())
            await self.FS._handle_critical_error(
                self.interaction, self.user_mention, target_path, local_cleanup_path, e
            )

        finally:
            if acquired_semaphore:
                self.download_semaphore.release()
                self.log.info(
                    f">>> [DOWNLOAD] Released download semaphore for user {self.user_id} ('{target_path}'). "
                    f"Available permits: {self.download_semaphore._value}"
                )
            if self.user_id in self.user_downloading:
                del self.user_downloading[self.user_id]
                self.log.info(
                    f">>> [DOWNLOAD] User {self.user_id} finished download of '{target_path}'. user_downloading state cleared."
                )