import os
import re
from typing import List, Dict, Any, Optional, Tuple

class UploadUtils:
    def __init__(self, log,metadata_handler,ebase):
        self.log = log
        self.db = metadata_handler
        self.encry = ebase

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
            final_name = self.encry._generate_random_nickname()
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
            return self.encry._encrypt_data(chunk_data, encryption_key)
        return chunk_data
    async def _is_root_upload_a_file(self, all_db_entries: List[Dict[str, Any]], root_upload_name: str) -> bool:
        """Checks if the given root_upload_name corresponds to a top-level file."""
        for entry in all_db_entries:
            if entry.get('root_upload_name') == root_upload_name and \
                    entry.get('relative_path_in_archive') == '' and \
                    entry.get('base_filename') != '_DIR_':
                return True
        return False
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
            nicknamed_name = self.encry._generate_random_nickname()
            is_dir_nicknamed = True
            original_name_for_db = dir_name
            self.log.debug(f"Sub-folder '{dir_name}' >60 chars. Auto-nicknamed to '{nicknamed_name}'.")

        folder_relative_path = os.path.join(parent_relative_path, nicknamed_name).replace(os.path.sep, '/')

        await self.db._store_folder_metadata(
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