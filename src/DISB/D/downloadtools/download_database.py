import logging
import os
from typing import List, Dict, Any, Tuple
import discord
from database import DatabaseManager
class DDB:
    def __init__(self,interaction: discord.Interaction = None):
        if interaction is not None:
            self.interaction = interaction
            self.user_mention = interaction.user.mention
        self.log = logging.getLogger('downloada')
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
        self.multiple_versions = False
        self.should_create_target_folder = False
    async def _resolve_target_and_version_mode(self, normalized_target_path: str, all_entries: list,
                                               version_param, start_version_param, end_version_param,
                                               all_versions_param, can_apply_version_filters, target_path):
        """
        Resolves the target path to database keys and determines download mode flags.
        Returns a dict with resolved info or None if resolution fails.
        """
        if normalized_target_path == "":
            # Global download
            self.should_create_target_folder = False
            self.multiple_versions = False
            return {
                'is_global': True,
                'root': None,
                'rel_path': None,
                'base_filename': None,
                'is_folder': True,  # treat as folder for some logic
                'multiple_versions': False
            }

        resolved = await self.db._resolve_path_to_db_entry_keys(normalized_target_path, all_entries)
        if not resolved:
            await self.interaction.followup.send(
                f"{self.user_mention}, Could not find any item matching '{target_path}'."
            )
            return None

        root, rel_path, base_filename, is_folder = resolved
        self.should_create_target_folder = is_folder  # folders get a named folder

        # Determine if this is a multi-version download of a single item
        self.multiple_versions = (
                can_apply_version_filters and
                (version_param is None and not (start_version_param and end_version_param) and all_versions_param) or
                (start_version_param and end_version_param and not version_param)
        )

        # Get original path components for later use
        orig_root, orig_rel_segments, orig_base = self._get_original_path_components(
            all_entries, root, rel_path, base_filename, not is_folder
        )

        return {
            'is_global': False,
            'root': root,
            'rel_path': rel_path,
            'base_filename': base_filename,
            'is_folder': is_folder,
            'original_root': orig_root,
            'original_rel_segments': orig_rel_segments,
            'original_base': orig_base,
            'multiple_versions': self.multiple_versions
        }


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
                    db_path_for_segment_lookup = os.path.join(root_upload_name_db, current_db_path_segment_for_lookup,
                                                              segment_db).replace(os.path.sep, '/')

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