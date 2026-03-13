import os
import re
from typing import List, Dict, Any, Optional, Tuple
from downloadtools.download_database import DDB
DDBV = DDB()
async def _is_root_upload_a_file(all_db_entries: List[Dict[str, Any]], root_upload_name: str) -> bool:
    """Checks if the given root_upload_name corresponds to a top-level file."""
    for entry in all_db_entries:
        if entry.get('root_upload_name') == root_upload_name and \
                entry.get('relative_path_in_archive') == '' and \
                entry.get('base_filename') != '_DIR_':
            return True
    return False
async def _compute_file_paths(
    all_entries: list,
    resolved_info: dict,
    file_data: dict,
    root_name: str,
    rel_path: str,
    base_name: str,
    version: str,
    base_download_dir: str,
    local_cleanup_path: Optional[str],
    multiple_versions: bool,
    should_create_target_folder: bool,
    is_global: bool,
    is_target_folder: bool
) -> Tuple[str, str]:
    """Return (absolute_output_path, display_path_without_version)."""
    orig_root, orig_rel_segments, orig_base = DDBV._get_original_path_components(
        all_entries, root_name, rel_path, base_name, True
    )

    output_path = None
    display_path = None

    if multiple_versions:
        # Path: local_cleanup_path (the _Versions folder) / v{version} / (relative subpath) / orig_base
        base = local_cleanup_path
        path_parts = [base, f"v{version}"]

        if resolved_info['is_folder']:
            # Targeted item was a folder → compute relative path from it
            target_full = os.path.join(resolved_info['original_root'],
                                       *resolved_info['original_rel_segments']).replace('\\', '/')
            file_full = os.path.join(orig_root, *orig_rel_segments).replace('\\', '/')
            if file_full.startswith(target_full):
                remainder = file_full[len(target_full):].strip('/')
                if remainder:
                    path_parts.extend(remainder.split('/'))
        # else: targeted item was a file → no extra subfolders

        path_parts.append(orig_base)
        output_path = os.path.join(*path_parts)

        # Display path: version folder + relative part
        if resolved_info['is_folder'] and 'remainder' in locals() and remainder:
            display_path = f"v{version}/{remainder}/{orig_base}"
        else:
            display_path = f"v{version}/{orig_base}"

    elif is_global:
        # Path: base_download_dir / (original root if not a file) / original rel segments / orig_base
        path_parts = [base_download_dir]
        if not await _is_root_upload_a_file(all_entries, root_name):
            path_parts.append(orig_root)
        path_parts.extend(orig_rel_segments)
        path_parts.append(orig_base)
        output_path = os.path.join(*path_parts)
        display_path = os.path.join(orig_root, *orig_rel_segments, orig_base).replace('\\', '/')

    elif should_create_target_folder:
        # Targeted single folder download
        base = local_cleanup_path  # this is the created folder (e.g., download_folder/OriginalTargetFolderName)
        target_full = os.path.join(resolved_info['original_root'],
                                   *resolved_info['original_rel_segments']).replace('\\', '/')
        file_full = os.path.join(orig_root, *orig_rel_segments).replace('\\', '/')
        if file_full.startswith(target_full):
            remainder = file_full[len(target_full):].strip('/')
            if remainder:
                output_path = os.path.join(base, remainder, orig_base)
                display_path = os.path.join(remainder, orig_base).replace('\\', '/')
            else:
                output_path = os.path.join(base, orig_base)
                display_path = orig_base
        else:
            # fallback (should not happen)
            output_path = os.path.join(base, orig_base)
            display_path = orig_base

    else:
        # Targeted single file download (not multiple versions)
        path_parts = [base_download_dir]
        if not await _is_root_upload_a_file(all_entries, root_name):
            path_parts.append(orig_root)
        path_parts.extend(orig_rel_segments)
        path_parts.append(orig_base)
        output_path = os.path.join(*path_parts)
        display_path = os.path.join(orig_root, *orig_rel_segments, orig_base).replace('\\', '/')

    # Clean up double slashes and ensure directory exists
    output_path = os.path.normpath(output_path)
    display_path = re.sub(r'/{2,}', '/', display_path).strip('/')
    if not display_path:
        display_path = orig_base or "unknown_file"

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    return output_path, display_path