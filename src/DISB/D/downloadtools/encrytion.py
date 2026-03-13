import os
from typing import Dict, Tuple, Optional, Any, List

import discord

from database import DatabaseManager

class denc:
    def __init__(self,log,ddb,version_manager):
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
        self.db = DatabaseManager(file_table_columns=self.file_table_columns, log=self.log)
        self.ddb=ddb
        self.version_manager = version_manager
    def _get_file_encryption_key(
            self,
            file_data: dict,
            password_seed: Dict[Tuple[str, str, str, str], bytes],
            key_tuple: tuple
    ) -> Optional[bytes]:
        """Return encryption key, or None if not available (skip file)."""
        mode = file_data.get('encryption_mode', 'off')
        if mode == 'off':
            return None  # Important: None, not b'' (original passes None)
        if mode == 'automatic':
            key = file_data.get('encryption_key_auto')
            if isinstance(key, str):
                key = key.encode('utf-8')
            return key if isinstance(key, bytes) else b''
        if mode == 'not_automatic':
            return password_seed.get(key_tuple)  # None if missing
        return None


    async def _handle_decryption_failure(
            self,
            interaction: discord.Interaction,
            display_path: str,
            version: str,
            total_parts: int,
            user_mention: str,
            local_cleanup_path: Optional[str],
            overall_parts_downloaded: int,
            overall_total_parts: int
    ) -> str:
        """Present options when decryption fails. Returns 'cancel_all', 'cancel_keep', or 'continue'."""
        from downloadtools.innerclasses import ZeroKnowledgeDecryptionFailureView
        view = ZeroKnowledgeDecryptionFailureView(
            self, interaction, local_cleanup_path,
            overall_parts_downloaded, overall_total_parts,
            total_parts
        )
        await interaction.followup.send(
            content=f"{user_mention}, Decryption failed for `{display_path} (v{version})`. What would you like to do?",
            view=view, ephemeral=False
        )
        await view.wait()
        choice = view.choice if view.choice else 'cancel_all'  # timeout → cancel_all
        self.log.info(f"User chose: {choice}")
        return choice
    async def _get_items_requiring_password_for_download(
            self,
            database_file: str,
            target_path: str,
            version_param: Optional[str],
            start_version_param: Optional[str],
            end_version_param: Optional[str],
            all_versions_param: bool,
            can_apply_version_filters: bool
    ) -> List[Dict[str, Any]]:
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
        scoped_items_raw = []
        if is_global_download:
            # For global downloads, check all file entries (not folder markers)
            scoped_items_raw = [e for e in all_db_entries if e.get('base_filename') != '_DIR_']
        else:
            resolved_target_info = await self.db._resolve_path_to_db_entry_keys(normalized_target_path, all_db_entries)
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
        final_items_to_check = []
        if can_apply_version_filters:
            # For a single targeted file/folder, we use the specific version parameters
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
                original_root, original_rel_path_segments, original_base_file = self.ddb._get_original_path_components(
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