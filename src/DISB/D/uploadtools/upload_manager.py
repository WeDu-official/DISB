import random
import aiohttp
import discord
import asyncio
import os
import traceback
from typing import List, Optional,Tuple
class UploadManager:
    def __init__(self, bot, metadata_handler,user_uploading, utils_handler,eup,ebase, log,get_channel,baseapi,semaup):
        self.bot = bot
        self.db = metadata_handler
        self.utils = utils_handler
        self.log = log
        self.get_channel = get_channel
        self.baseapi = baseapi
        self.encryption = eup
        self.encry = ebase
        self.user_uploading = user_uploading
        self.upload_semaphore = semaup
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
        file_part_message = await self.baseapi._send_file_part_to_discord(channel, part_data, part_filename)
        if not file_part_message:
            raise Exception(f"Failed to upload part {part_number} of '{base_filename}'.")

        await self.db._store_file_metadata(
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
            await self.baseapi.send_message_robustly(
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
        final_base, original_base, is_base_nicknamed = self.utils._resolve_file_nickname(
            file_path, root_upload_name, is_top_level_single_file_upload,
            is_nicknamed_flag_for_db, original_root_name_for_db
        )

        # 2️⃣ Compute display path
        display_path = self.utils._compute_display_path(
            root_upload_name, relative_path_in_archive, final_base,
            is_base_nicknamed, original_base, is_nicknamed_flag_for_db,
            original_root_name_for_db, is_top_level_single_file_upload, version
        )

        try:
            # 3️⃣ Compute file size and total parts
            file_size, total_parts = self.utils._compute_file_parts(file_path, current_chunk_size)

            self.log.info(
                f"Uploading '{file_path}' as '{final_base}' with {total_parts} parts (chunk size {current_chunk_size}).")
            await self.baseapi.send_message_robustly(channel_id,
                                             content=f"{user_mention}, Starting upload of `{display_path}` ({total_parts} parts).")

            # 4️⃣ Read chunks, encrypt, upload
            async for part_number, chunk in self.utils._read_file_chunks(file_path, current_chunk_size):
                chunk = self.utils._encrypt_chunk_if_needed(chunk, encryption_mode, encryption_key)
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
            await self.baseapi.send_message_robustly(channel_id, f"{user_mention}, Error uploading `{display_path}`: {e}")
            raise

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
                nicknamed_name = await self.utils._process_subfolder(
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
        await self.db._store_root_folder_metadata(
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

        await self.baseapi.send_message_robustly(
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
            root_upload_name = self.encry._generate_random_nickname()
            original_root_name_for_db = effective_root_name_from_path
            is_nicknamed_flag_for_db = True
        else:
            root_upload_name = effective_root_name_from_path

        return root_upload_name, original_root_name_for_db, is_nicknamed_flag_for_db
    async def _check_duplicate_root(self, DATABASE_FILE: str, root_upload_name: str,
                                    new_version_string: Optional[str], interaction: discord.Interaction,
                                    user_mention: str, user_id: int) -> bool:
        """Return True if a top-level root upload name already exists in DB."""
        if await self.db._check_duplicate_root_upload_name(DATABASE_FILE, root_upload_name, new_version_string):
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