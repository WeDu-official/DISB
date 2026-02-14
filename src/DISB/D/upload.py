import discord
from typing import Optional
import os
import traceback
class UPLOAD:
    def __init__(self, db, upload_manager,utils,eup, log,baseapi,semaup):
        self.db = db
        self.upmang = upload_manager
        self.utils = utils
        self.encryption = eup
        self.baseapi = baseapi
        self.log = log
        self.upload_semaphore = semaup
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
        DATABASE_FILE = self.db._normalize_db_file_path(DB_FILE)

        # --- Step 1: Determine root_upload_name and nicknaming ---
        root_upload_name, original_root_name_for_db, is_nicknamed_flag_for_db = \
            await self.upmang._determine_root_name(local_path, custom_root_name, interaction)

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
        ) = await self.encryption._prepare_encryption_and_version(
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
            if await self.db._check_duplicate_root_upload_name(DATABASE_FILE, root_upload_name, new_version_string):
                await interaction.followup.send(
                    f"{user_mention}, An item with the name '{root_upload_name}' already exists. Choose a different name or use `upload_mode: new_version`.",
                    ephemeral=False
                )
                self.log.info(f">>> [UPLOAD] Duplicate root '{root_upload_name}' found. Aborting.")
                return

        # --- Step 4: Concurrency control ---
        if not await self.upmang._acquire_user_upload_slot(user_id, root_upload_name, interaction):
            return

        # --- Step 5: Determine chunk size ---
        current_chunk_size = self.utils._get_chunk_size(inherited_encryption_mode)
        self.log.info(
            f"Using {current_chunk_size / (1024 * 1024):.0f} MB chunk size for '{inherited_encryption_mode}' upload of '{root_upload_name}'.")

        # --- Step 6: Pre-calculate total parts ---
        overall_uploaded_parts_ref = [0]
        overall_total_parts = await self.utils._precalculate_total_parts(local_path, current_chunk_size)

        if overall_total_parts is None:
            await self.upmang._release_upload_slot(user_id, root_upload_name)
            return

        # --- Step 7: Acquire upload semaphore and start actual upload ---
        upload_successful = True
        try:
            await self.upload_semaphore.acquire()
            self.log.info(
                f">>> [UPLOAD] Acquired upload semaphore for user {user_id} ('{root_upload_name}'). Available permits: {self.upload_semaphore._value}")

            if is_folder:
                await self.upmang.upload_folder_contents(
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
                await self.upmang.upload_single_file(
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
                await self.baseapi.send_message_robustly(
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
                await self.upmang._handle_incomplete_upload(
                    interaction,
                    root_upload_name,
                    DATABASE_FILE,
                    current_version_for_upload,
                    is_folder,
                    uploaded_parts=overall_uploaded_parts_ref[0],
                    total_parts=overall_total_parts
                )

            # --- Step 10: Release semaphore and clean up user_uploading state ---
            await self.upmang._release_upload_slot(user_id, root_upload_name)