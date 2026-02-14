import re
from typing import Dict, Any, List, Optional

class VersioningManager:
    def __init__(self, db_read_func, log):
        """
        db_read_func: async function to read from DB
        log: logger instance
        """
        self._db_read_sync = db_read_func
        self.log = log
    def parse_version(self, version_str: str) -> tuple:
        parts = re.split(r'[^0-9a-zA-Z]+', version_str.lower())
        parsed = []
        for part in parts:
            if part.isdigit():
                parsed.append((0, int(part)))  # numeric parts first
            else:
                parsed.append((1, part))  # string parts second
        return tuple(parsed)


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


    def _generate_next_version_string(current_version: str) -> str:
        numbers = [int(p) for p in current_version.split('.') if p.isdigit()]
        if not numbers:
            return "0.0.0.1"
        numbers[-1] += 1
        return ".".join(map(str, numbers))


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
    async def _determine_version_string(
            self, DB_FILE: str, target_root_name: str, target_relative_path: str, target_base_filename: str,
            new_version_string: Optional[str] = None
    ) -> str:
        if new_version_string:
            return new_version_string
        return self._generate_next_version_string(
            DB_FILE, target_root_name, target_relative_path, target_base_filename
        )
