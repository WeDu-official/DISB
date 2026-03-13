import os
from typing import Optional, Tuple, Dict

import discord
from cryptography.fernet import InvalidToken
try:
    from utils import _compute_file_paths
    from encrytion import denc
    from message_related import MRD
except Exception:
    from downloadtools.utils import _compute_file_paths
    from downloadtools.encrytion import denc
    from downloadtools.message_related import MRD
class DecryptionCancelledError(Exception):
    """Raised when user cancels all downloads due to decryption failure."""
    pass
class files:
    def __init__(self,log,version_manager,DDB,baseapi,bot):
        self.log = log
        self.version_manager = version_manager
        self.ba = baseapi
        self.MRD = MRD(self.ba)
        self.denc = denc(log=self.log, ddb=DDB, version_manager=self.version_manager)
        self.bot = bot
    def get_channel(self, channel_id):
        """Wrapper for bot.get_channel"""
        return self.bot.get_channel(channel_id)

    async def fetch_channel(self, channel_id):
        """Wrapper for bot.fetch_channel"""
        return await self.bot.fetch_channel(channel_id)
    async def _collect_relevant_files(
            self,
            normalized_target_path: str,
            all_entries: list,
            resolved_info: dict,
            version_param: Optional[str],
            start_version_param: Optional[str],
            end_version_param: Optional[str],
            all_versions_param: bool,
            can_apply_version_filters: bool,
            db_path: str
    ) -> Tuple[Dict, int]:
        """Build files_to_download_grouped dict and compute total parts."""
        files_grouped = {}
        relevant_raw = []

        if resolved_info['is_global']:
            # Global download: newest version of every file (exclude _DIR_)
            unique_keys = set()
            for entry in all_entries:
                key = (entry.get('root_upload_name'), entry.get('relative_path_in_archive'), entry.get('base_filename'))
                if key not in unique_keys:
                    unique_keys.add(key)
                    latest = await self.version_manager._get_relevant_item_versions(
                        db_path, key[0], key[1], key[2],
                        None, None, None, False
                    )
                    relevant_raw.extend(latest)
            relevant_raw = [e for e in relevant_raw if e.get('base_filename') != '_DIR_']

        elif resolved_info['is_folder']:
            root = resolved_info['root']
            rel_path = resolved_info['rel_path']
            self.log.info(
                f"DEBUG: Collecting relevant files for folder download. "
                f"Resolved root: '{root}', relative path: '{rel_path}'"
            )

            if can_apply_version_filters and (
                    version_param or start_version_param or end_version_param or all_versions_param):
                # Get folder versions, then all files in those versions
                folder_versions = await self.version_manager._get_relevant_item_versions(
                    db_path, root, rel_path, "_DIR_",
                    version_param, start_version_param, end_version_param, all_versions_param
                )
                for fv in folder_versions:
                    ver = fv.get('version')
                    files_in_ver = [
                        e for e in all_entries
                        if e.get('root_upload_name') == root
                           and e.get('base_filename') != '_DIR_'
                           and e.get('version') == ver
                           and (e.get('relative_path_in_archive') == rel_path or
                                e.get('relative_path_in_archive', '').startswith(rel_path + '/'))
                    ]
                    relevant_raw.extend(files_in_ver)
            else:
                # No version filters: newest version of each file under this folder
                files_in_folder = [
                    e for e in all_entries
                    if e.get('root_upload_name') == root
                       and e.get('base_filename') != '_DIR_'
                       and (e.get('relative_path_in_archive') == rel_path or
                            e.get('relative_path_in_archive', '').startswith(rel_path + '/'))
                ]
                unique = set()
                for e in files_in_folder:
                    key = (e.get('root_upload_name'), e.get('relative_path_in_archive'), e.get('base_filename'))
                    if key not in unique:
                        unique.add(key)
                        latest = await self.version_manager._get_relevant_item_versions(
                            db_path, key[0], key[1], key[2],
                            None, None, None, False
                        )
                        relevant_raw.extend(latest)

        else:
            # Single file download
            root = resolved_info['root']
            rel_path = resolved_info['rel_path']
            base = resolved_info['base_filename']
            self.log.info(
                f"DEBUG: Collecting relevant file parts for specific file download. "
                f"Resolved root: '{root}', relative path: '{rel_path}', base filename: '{base}'"
            )
            # First, let's see all entries for this root to debug
            all_for_root = [e for e in all_entries if e.get('root_upload_name') == root]
            self.log.debug(f"Found {len(all_for_root)} total entries for root '{root}'")

            # Then filter by base_filename
            all_for_file = [e for e in all_for_root if e.get('base_filename') == base]
            self.log.debug(f"Found {len(all_for_file)} entries for base_filename '{base}'")

            # Then check relative_path
            all_for_path = [e for e in all_for_file if e.get('relative_path_in_archive') == rel_path]
            self.log.debug(f"Found {len(all_for_path)} entries with rel_path '{rel_path}'")

            # Now get versions
            relevant_raw = await self.version_manager._get_relevant_item_versions(
                db_path, root, rel_path, base,
                version_param, start_version_param, end_version_param, all_versions_param
            )
            self.log.info(f"DEBUG: Found {len(relevant_raw)} entries for target file with version filters.")

            # If we got 0 entries but should have 7, try a broader query
            if len(relevant_raw) == 0 and len(all_for_path) > 0:
                self.log.warning(
                    "Version query returned 0 but manual filtering found entries. Using manual filtered results.")
                relevant_raw = all_for_path
        # Group by (root, rel_path, base, version)
        for entry in relevant_raw:
            if entry.get('base_filename') == '_DIR_':
                continue
            key = (entry['root_upload_name'], entry['relative_path_in_archive'],
                   entry['base_filename'], entry['version'])

            if key not in files_grouped:
                files_grouped[key] = {
                    'parts': {},
                    'total_expected_parts': int(entry.get('total_parts', 1)),
                    'channel_id': int(entry.get('channel_id', 0)),
                    'root_upload_name': entry['root_upload_name'],
                    'relative_path_in_archive': entry['relative_path_in_archive'],
                    'original_root_name': entry.get('original_root_name', ''),
                    'original_file_name': entry.get('original_base_filename', ''),
                    'encryption_mode': entry.get('encryption_mode', 'off'),
                    'encryption_key_auto': entry.get('encryption_key_auto', b''),
                    'password_seed_hash': entry.get('password_seed_hash', ''),
                    'store_hash_flag': entry.get('store_hash_flag', False),
                    'version': entry['version']
                }

            files_grouped[key]['parts'][int(entry.get('part_number', 0))] = int(entry.get('message_id', 0))

        # 🧠 sanity check: verify all parts exist
        for key, file_data in files_grouped.items():
            print('---------SCP-----------')
            if len(file_data['parts']) != file_data['total_expected_parts']:
                self.log.error(
                    f"File {key} missing parts: "
                    f"{len(file_data['parts'])}/{file_data['total_expected_parts']}"
                )
        total_parts = sum(fd['total_expected_parts'] for fd in files_grouped.values())
        self.log.debug(f"DEBUG: files_grouped count: {len(files_grouped)}")
        return files_grouped, total_parts
    async def _prepare_output_directories(
        self,
        interaction: discord.Interaction,download_folder,
        base_download_dir: str,
        all_entries: list,
        resolved_info: dict,
        files_grouped: dict,
        multiple_versions: bool,
        should_create_target_folder: bool,
        is_global: bool,
    ) -> Optional[str]:
        """Create base folders and return local_cleanup_path."""
        user_mention = interaction.user.mention
        local_cleanup_path = None

        if is_global:
            local_cleanup_path = base_download_dir
            await interaction.followup.send(
                content=f"{user_mention}, Preparing to download entire database (newest versions)."
            )

        elif multiple_versions:
            # Single item with multiple versions: create parent folder with "_Versions"
            if resolved_info['is_folder']:
                item_name = (resolved_info['original_rel_segments'][-1]
                             if resolved_info['original_rel_segments'] else resolved_info['original_root'])
            else:
                item_name = resolved_info['original_base']
            version_parent = os.path.join(base_download_dir, f"{item_name}_Versions")
            os.makedirs(version_parent, exist_ok=True)
            local_cleanup_path = version_parent
            self.log.info(f"Created versioned output parent folder: '{version_parent}'.")
            await interaction.followup.send(
                content=f"{user_mention}, Preparing to download multiple versions of `{item_name}` to `{os.path.basename(version_parent)}/` in `{download_folder}`."
            )

        elif should_create_target_folder:
            # Single folder download
            folder_name = (resolved_info['original_rel_segments'][-1]
                           if resolved_info['original_rel_segments'] else resolved_info['original_root'])
            folder_path = os.path.join(base_download_dir, folder_name)
            os.makedirs(folder_path, exist_ok=True)
            local_cleanup_path = folder_path
            self.log.info(f"Created top-level download folder for target: '{folder_path}'.")
            await interaction.followup.send(
                content=f"{user_mention}, Creating download folder: `{os.path.basename(folder_path)}/` in `{download_folder}`."
            )

        else:
            # Single file download: compute full path and set cleanup path to that file
            # Use the first (and only) file in files_grouped
            first_key = next(iter(files_grouped))
            first_data = files_grouped[first_key]
            root_name, rel_path, base_name, version = first_key
            output_path, _ = await _compute_file_paths(
                all_entries, resolved_info, first_data,
                root_name, rel_path, base_name, version,
                base_download_dir, None, multiple_versions,
                should_create_target_folder, is_global, resolved_info['is_folder']
            )
            local_cleanup_path = output_path
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            self.log.info(f"Calculated cleanup path for single file: '{output_path}'.")

        return local_cleanup_path

    async def _download_single_file(
            self,
            interaction: discord.Interaction,
            file_data: dict,
            output_path: str,
            display_path: str,
            encryption_key: Optional[bytes],
            version: str,
            user_mention: str,
            local_cleanup_path: Optional[str],
            overall_parts_downloaded: int,
            overall_total_parts: int
    ) -> Tuple[bool, int]:

        """
        Download all parts of one file. Returns (success, parts_downloaded).
        Raises DecryptionCancelledError if user chooses to cancel all.
        """
        total_parts = file_data['total_expected_parts']
        channel_id = file_data['channel_id']
        parts_map = file_data['parts']
        print(f'FILE DATA: {file_data}')

        # Send initial message (original)
        await interaction.followup.send(
            content=f"{user_mention}, Downloading file: `{display_path} (v{version})` (Total {total_parts} parts)..."
        )

        # Get channel
        channel = self.get_channel(channel_id)
        if not channel:
            try:
                channel = await self.fetch_channel(channel_id)
            except Exception as e:
                self.log.error(f"Error fetching channel {channel_id}: {e}. Cannot download parts.")
                await interaction.followup.send(
                    content=f"{user_mention}, Error accessing channel for `{display_path} (v{version})`. Skipping this file.",
                    ephemeral=False
                )
                return False, 0

        if not isinstance(channel, (discord.TextChannel, discord.DMChannel, discord.Thread)):
            self.log.warning(f"Channel {channel_id} is not text-based.")
            await interaction.followup.send(
                content=f"{user_mention}, Channel for `{display_path} (v{version})` is not text-based. Skipping this file.",
                ephemeral=False
            )
            return False, 0

        downloaded_parts = {}
        file_success = True
        print(total_parts,parts_map)
        for p_num in range(1, total_parts + 1):
            msg_id = parts_map.get(p_num)
            if not msg_id:
                self.log.warning(f"Missing message ID for part {p_num} of {display_path}")
                file_success = False
                break

            try:
                # Progress update (using robust sender, like original)
                await self.ba.send_message_robustly(
                    interaction.channel_id,
                    content=f"{user_mention}, Fetching part {p_num}/{total_parts} for `{display_path} (v{version})`..."
                )

                fetched = await self.MRD._get_file_parts_from_discord(
                    channel_id, [msg_id], encryption_key=encryption_key
                )
                if p_num not in fetched:
                    self.log.warning(f"Part {p_num} not fetched for {display_path}")
                    file_success = False
                    break
                downloaded_parts[p_num] = fetched[p_num]

            except InvalidToken:
                # Decryption failure → offer options (original behaviour)
                choice = await self.denc._handle_decryption_failure(
                    interaction, display_path, version, total_parts,
                    user_mention, local_cleanup_path,
                    overall_parts_downloaded, overall_total_parts
                )
                if choice == 'cancel_all':
                    raise DecryptionCancelledError()
                elif choice == 'cancel_keep':
                    # Original raises an exception to abort entire download
                    raise DecryptionCancelledError("User cancelled current download but kept files.")
                elif choice == 'continue':
                    # Skip this file, mark overall as incomplete, count parts as skipped
                    file_success = False
                    await interaction.followup.send(
                        content=f"{user_mention}, Skipping `{display_path} (v{version})` due to decryption failure."
                    )
                    return False, total_parts  # count all parts as "skipped"
                else:
                    # Timeout or unexpected → treat as cancel_all
                    raise DecryptionCancelledError()

            except Exception as e:
                self.log.error(f"Error fetching part {p_num} for {display_path}: {e}", exc_info=True)
                file_success = False
                break

        if not file_success or len(downloaded_parts) != total_parts:
            missing = [p for p in range(1, total_parts + 1) if p not in downloaded_parts]
            self.log.warning(f"Could not reconstruct '{display_path}'. Missing parts: {missing}.")
            await interaction.followup.send(
                content=f"{user_mention}, Could not fully download and reconstruct `{display_path} (v{version})`. Missing parts: {', '.join(map(str, missing))}"
            )
            return False, len(downloaded_parts)

        # All parts fetched → write file
        try:
            with open(output_path, 'wb') as f:
                for p_num in sorted(downloaded_parts.keys()):
                    f.write(downloaded_parts[p_num])
            self.log.info(f"Successfully downloaded and reconstructed: {display_path}")
            await interaction.followup.send(
                content=f"{user_mention}, Successfully downloaded and reconstructed: `{display_path} (v{version})`"
            )
            return True, len(downloaded_parts)
        except Exception as e:
            self.log.error(f"Failed to write file {output_path}: {e}", exc_info=True)
            await interaction.followup.send(
                content=f"{user_mention}, Error reconstructing `{display_path} (v{version})`: {e}"
            )
            return False, len(downloaded_parts)