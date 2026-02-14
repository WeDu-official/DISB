from typing import Optional, Tuple
import hashlib
from cryptography.fernet import Fernet
import discord
from encryption_base import encrybase
class EncryptionManager:
    def __init__(self, db, version_manager,utils, log,salt,info):
        self.db = db
        self.utils = utils
        self.version_manager = version_manager
        self.log = log
        self.encryption = encrybase(salt,info,self.log)
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
        resolved_target_info = await self.utils._resolve_path_to_db_entry_keys(target_item_path, all_db_entries)

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
            user_seed = self.encryption._generate_random_nickname(length=32)
            await interaction.followup.send(
                f"{user_mention}, sending a random password seed for upload '{target_item_path}' to your DMs...",
                ephemeral=True
            )
            await interaction.user.send(
                f"Your random password seed for '{target_item_path}' is: ||`{user_seed}`||. Save it securely!"
            )
            self.log.info(f"Generated random seed for '{target_item_path}' for user {user_id}. Sent via DM.")

        derived_key = self.encryption._derive_key_from_seed(user_seed)
        derived_hash = hashlib.sha256(user_seed.encode('utf-8')).hexdigest() if inherited_store_hash_flag else ""
        return derived_key, derived_hash
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
                    user_seed_final = self.encryption._generate_random_nickname(length=32)
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
                    inherited_encryption_key = self.encryption._derive_key_from_seed(user_seed_final)
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