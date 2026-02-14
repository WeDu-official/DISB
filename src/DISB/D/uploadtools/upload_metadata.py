import os
import datetime
import hashlib
from typing import Optional,List
from database import DatabaseManager
class UploadMetadata(DatabaseManager):
    def __init__(self, db, log, file_table_columns: List[str]):
        super().__init__(file_table_columns, log)
        self.db = db
        self.log = log

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
