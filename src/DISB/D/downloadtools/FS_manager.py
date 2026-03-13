import os
import shutil
import traceback
from typing import Optional

import discord

class FS: #FS means Fails/successed
    def __init__(self,log,baseapi):
        self.log = log
        self.ba = baseapi
    async def _cleanup_incomplete_download_files(self, user_mention: str, channel_id: int,
                                                 folder_or_file_to_remove: str):
        """
        Removes the specified folder or file and its contents from the local file system.
        Sends confirmation/error messages to the user.
        """
        self.log.info(f">>> [CLEANUP] Attempting to remove: '{folder_or_file_to_remove}'")
        try:
            if os.path.isdir(folder_or_file_to_remove):
                shutil.rmtree(folder_or_file_to_remove)
                await self.ba.send_message_robustly(
                    channel_id,
                    content=f"{user_mention}, Successfully removed the incomplete download folder located at `{folder_or_file_to_remove}`."
                )
                self.log.info(f">>> [CLEANUP] Successfully removed folder '{folder_or_file_to_remove}'.")
            elif os.path.isfile(folder_or_file_to_remove):
                os.remove(folder_or_file_to_remove)
                await self.ba.send_message_robustly(
                    channel_id,
                    content=f"{user_mention}, Successfully removed the incomplete download file located at `{folder_or_file_to_remove}`."
                )
                self.log.info(f">>> [CLEANUP] Successfully removed file '{folder_or_file_to_remove}'.")
            else:
                await self.ba.send_message_robustly(
                    channel_id,
                    content=f"{user_mention}, The item `{folder_or_file_to_remove}` does not exist or has already been removed."
                )
                self.log.warning(f">>> [CLEANUP] Item '{folder_or_file_to_remove}' not found for removal.")
        except OSError as e:
            await self.ba.send_message_robustly(
                channel_id,
                content=f"{user_mention}, Error removing incomplete download at `{folder_or_file_to_remove}`: {e}. You may need to delete it manually."
            )
            self.log.error(f">>> [CLEANUP] Error removing '{folder_or_file_to_remove}': {e}")
            self.log.error(traceback.format_exc())
        except Exception as e:
            self.log.error(f">>> [CLEANUP] Unexpected error during cleanup of '{folder_or_file_to_remove}': {e}")
            self.log.error(traceback.format_exc())
            await self.ba.send_message_robustly(
                channel_id,
                content=f"{user_mention}, An unexpected error occurred during cleanup of `{folder_or_file_to_remove}`: {e}. You may need to delete it manually."
            )


    async def _finalize_download(
            self,
            interaction: discord.Interaction,
            user_mention: str,
            target_path: str,
            download_folder: str,
            base_download_dir: str,
            local_cleanup_path: Optional[str],
            fully_successful: bool,
            multiple_versions: bool,
            should_create_target_folder: bool,
            is_global: bool,
            overall_parts_downloaded: int,
            overall_total_parts: int
    ):
        """Send final success message or offer cleanup for incomplete download."""
        if fully_successful:
            msg = f"{user_mention}, Download process complete."
            if multiple_versions and local_cleanup_path:
                msg += f"\nYour versioned files have been downloaded to: `{local_cleanup_path}`"
                self.log.info(f"Download process complete. Versioned files saved to: '{local_cleanup_path}'")
            elif is_global:
                msg += f"\nYour files have been downloaded to: `{base_download_dir}`"
                self.log.info(f"Download process complete. Files saved to: '{base_download_dir}'")
            elif should_create_target_folder and local_cleanup_path:
                msg += f"\nYour files have been downloaded to: `{local_cleanup_path}`"
                self.log.info(f"Download process complete. Files saved to: '{local_cleanup_path}'")
            elif not should_create_target_folder and local_cleanup_path:
                parent_dir = os.path.dirname(local_cleanup_path)
                msg += f"\nYour file has been downloaded to: `{parent_dir}`"
                self.log.info(f"Download process complete. File saved to: '{parent_dir}'")
            else:
                msg += f"\nCheck your specified download folder: `{download_folder}`."
                self.log.info(f"Download process complete. Files saved to: '{download_folder}' (fallback).")
            await interaction.followup.send(content=msg)
        else:
            await self._offer_incomplete_cleanup(
                interaction, user_mention, target_path,
                local_cleanup_path, base_download_dir,
                overall_parts_downloaded, overall_total_parts
            )


    async def _offer_incomplete_cleanup(
            self,
            interaction: discord.Interaction,
            user_mention: str,
            target_path: str,
            local_cleanup_path: Optional[str],
            base_download_dir: str,
            overall_parts_downloaded: int,
            overall_total_parts: int
    ):
        """Send a message with buttons to remove or keep partial files (original)."""
        if overall_parts_downloaded != overall_total_parts:
            problem = (
                f"{user_mention}, **Download Incomplete!** "
                f"The download process for `{target_path}` did not complete 100%. "
                f"Only {overall_parts_downloaded} of {overall_total_parts} parts were successfully downloaded. "
                f"This could be due to corrupted file parts on Discord, internet issues, or an incorrect decryption key.\n\n"
            )
        else:
            problem = (
                f"{user_mention}, **Download Incomplete!** "
                f"An unexpected error occurred during the download of `{target_path}`. "
                f"The download might be incomplete or corrupted.\n\n"
            )

        cleanup_display = "the specified download location"
        if local_cleanup_path:
            cleanup_display = f"`{local_cleanup_path}`"
        else:
            cleanup_display = f"`{base_download_dir}`"
        problem += f"Your partially downloaded content might be located at: {cleanup_display}."

        view = discord.ui.View(timeout=180)
        remove_btn = discord.ui.Button(
            label="Remove Incomplete Download", style=discord.ButtonStyle.red, emoji="🗑️",
            custom_id="remove_incomplete_download"
        )

        async def remove_cb(btn_interaction: discord.Interaction):
            await btn_interaction.response.defer(ephemeral=True)
            path = local_cleanup_path or base_download_dir
            await self._cleanup_incomplete_download_files(
                user_mention, btn_interaction.channel_id, path
            )
            for item in view.children:
                item.disabled = True
            await btn_interaction.message.edit(view=view)
            await btn_interaction.followup.send("Operation completed.", ephemeral=True)

        remove_btn.callback = remove_cb

        keep_btn = discord.ui.Button(
            label="Keep Files (Cancel)", style=discord.ButtonStyle.grey, emoji="❌",
            custom_id="cancel_incomplete_download"
        )

        async def keep_cb(btn_interaction: discord.Interaction):
            await btn_interaction.response.send_message(
                f"{user_mention}, Okay, the partially downloaded files will remain on your machine.",
                ephemeral=True
            )
            for item in view.children:
                item.disabled = True
            await btn_interaction.message.edit(view=view)

        keep_btn.callback = keep_cb

        view.add_item(remove_btn)
        view.add_item(keep_btn)
        await interaction.followup.send(content=problem, view=view)
        self.log.warning(
            f"Download incomplete for '{target_path}'. {overall_parts_downloaded}/{overall_total_parts} parts downloaded.")


    async def _handle_critical_error(
            self,
            interaction: discord.Interaction,
            user_mention: str,
            target_path: str,
            local_cleanup_path: Optional[str],
            error: Exception
    ):
        """Handle unexpected exceptions (original)."""
        await interaction.followup.send(
            content=f"{user_mention}, A critical and unexpected error occurred during the download of '{target_path}': {error}. Please report this and try again."
        )
        if local_cleanup_path and os.path.exists(local_cleanup_path):
            problem = (
                f"{user_mention}, **Download Failed Due to Critical Error!** "
                f"A serious error occurred during the download of `{target_path}`: {error}.\n\n"
                f"Your partially downloaded content might be located at: `{local_cleanup_path}`."
            )
            view = discord.ui.View(timeout=180)
            remove_btn = discord.ui.Button(
                label="Remove Downloaded Files", style=discord.ButtonStyle.red, emoji="🗑️",
                custom_id="remove_incomplete_download_critical"
            )

            async def remove_cb(btn_interaction: discord.Interaction):
                await btn_interaction.response.defer(ephemeral=True)
                await self._cleanup_incomplete_download_files(
                    user_mention, btn_interaction.channel_id, local_cleanup_path
                )
                for item in view.children:
                    item.disabled = True
                await btn_interaction.message.edit(view=view)
                await btn_interaction.followup.send("Operation completed.", ephemeral=True)

            remove_btn.callback = remove_cb

            keep_btn = discord.ui.Button(
                label="Keep Files (Cancel)", style=discord.ButtonStyle.grey, emoji="❌",
                custom_id="cancel_incomplete_download_critical"
            )

            async def keep_cb(btn_interaction: discord.Interaction):
                await btn_interaction.response.send_message(
                    f"{user_mention}, Okay, the partially downloaded files will remain on your machine.",
                    ephemeral=True
                )
                for item in view.children:
                    item.disabled = True
                await btn_interaction.message.edit(view=view)

            keep_btn.callback = keep_cb

            view.add_item(remove_btn)
            view.add_item(keep_btn)
            await interaction.followup.send(content=problem, view=view)
        else:
            await interaction.followup.send(
                content=f"{user_mention}, No partial download found to offer cleanup for, or an error occurred before creating local files."
            )