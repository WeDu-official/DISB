import hashlib
import traceback
from typing import Optional, Dict, List, Any, Tuple
import discord
from downloadtools.encryption_base import benc
import sys
import os
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)
class ZeroKnowledgeDecryptionFailureView(discord.ui.View):
    def __init__(self,salt,info,log, bot_instance: 'DOWNLOADA', original_interaction: discord.Interaction,
                 local_base_path_for_cleanup: str,
                 overall_parts_downloaded_counter_ref: int, overall_total_parts_to_download_ref: int,
                 current_file_total_parts: int):
        super().__init__(timeout=180)
        self.bot = bot_instance
        self.original_interaction = original_interaction
        self.local_base_path_for_cleanup = local_base_path_for_cleanup
        self.overall_parts_downloaded_counter_ref = overall_parts_downloaded_counter_ref
        self.overall_total_parts_to_download_ref = overall_total_parts_to_download_ref
        self.current_file_total_parts = current_file_total_parts
        self.choice = None # Will store user's choice: "cancel_all", "cancel_keep", "continue"
        self.benc = benc(salt=salt,info=info,log=log)

    async def _disable_all_buttons(self, interaction: discord.Interaction):
        for item in self.children:
            item.disabled = True
        await interaction.message.edit(view=self)

    @discord.ui.button(label="Cancel All & Remove Files", style=discord.ButtonStyle.red, emoji="🗑️", custom_id="cancel_all_remove")
    async def cancel_all_remove_button(self, button_interaction: discord.Interaction, button: discord.ui.Button):
        self.choice = "cancel_all"
        await self._disable_all_buttons(button_interaction)
        await button_interaction.response.send_message(
            f"{button_interaction.user.mention}, Cancelling download and removing partially downloaded files. This may take a moment...",
            ephemeral=True
        )
        # This will cause the download_filea to raise an exception, which will trigger cleanup.
        self.stop() # Stop the wait() in download_filea

    @discord.ui.button(label="Cancel & Keep Files", style=discord.ButtonStyle.grey, emoji="❌", custom_id="cancel_keep_files")
    async def cancel_keep_files_button(self, button_interaction: discord.Interaction, button: discord.ui.Button):
        self.choice = "cancel_keep"
        await self._disable_all_buttons(button_interaction)
        await button_interaction.response.send_message(
            f"{button_interaction.user.mention}, Cancelling download. Partially downloaded files will remain on your machine.",
            ephemeral=True
        )
        self.stop() # Stop the wait() in download_filea

    @discord.ui.button(label="Continue (Skip Item)", style=discord.ButtonStyle.green, emoji="➡️", custom_id="continue_skip_item")
    async def continue_skip_item_button(self, button_interaction: discord.Interaction, button: discord.ui.Button):
        self.choice = "continue"
        await self._disable_all_buttons(button_interaction)
        await button_interaction.response.send_message(
            f"{button_interaction.user.mention}, Skipping this item and continuing with the rest of the download.",
            ephemeral=True
        )
        self.stop() # Stop the wait() in download_file
class DecryptionPasswordModal(discord.ui.Modal, title="Enter Decryption Passwords"):
    def __init__(self, bot_instance: 'DOWNLOADA', original_interaction: discord.Interaction,intents,
                 required_passwords_info: List[Dict[str, Any]], database_file: str,
                 target_path: str, download_folder: str,
                 version_param: Optional[str] = None, # NEW
                 start_version_param: Optional[str] = None, # NEW
                 end_version_param: Optional[str] = None, # NEW
                 all_versions_param: bool = False, # NEW
                 can_apply_version_filters: bool = False, # NEW
                 initial_passwords: Optional[Dict[Tuple[str, str, str, str], str]] = None, # Key is tuple
                 error_messages: Optional[Dict[Tuple[str, str, str, str], str]] = None): # Key is tuple
        super().__init__(timeout=300)
        self.bot = bot_instance
        self.intents = intents
        self.original_interaction = original_interaction # Store the original command interaction
        self.required_passwords_info = required_passwords_info
        self.database_file = database_file
        self.target_path = target_path
        self.download_folder = download_folder
        # New versioning parameters for re-passing
        self.version_param = version_param
        self.start_version_param = start_version_param
        self.end_version_param = end_version_param
        self.all_versions_param = all_versions_param
        self.can_apply_version_filters = can_apply_version_filters

        self.entered_passwords: Dict[Tuple[str, str, str, str], str] = initial_passwords or {}
        self.error_messages: Dict[Tuple[str, str, str, str], str] = error_messages or {}
        self.decrypted_keys: Dict[Tuple[str, str, str, str], bytes] = {} # Store derived keys per unique item+version

        self._add_password_inputs()

    def _add_password_inputs(self):
        self.clear_items()

        for item_info in self.required_passwords_info:
            root_upload_name = item_info['root_upload_name']
            relative_path_in_archive = item_info['relative_path_in_archive']
            base_filename = item_info['base_filename']
            version = item_info['version']

            display_name = item_info['display_name'] # This now includes version
            store_hash = item_info.get('store_hash_flag', False)

            item_key = (root_upload_name, relative_path_in_archive, base_filename, version) # Full key
            error_msg = self.error_messages.get(item_key, "")

            # --- MODIFIED: Ensure label is 45 characters or fewer for display_name ---
            label_prefix = "Password for: "
            max_label_length = 45 # Discord limit

            # Truncate display_name if too long, considering the prefix
            if len(label_prefix) + len(display_name) > max_label_length:
                # Calculate how much of display_name can fit
                available_space = max_label_length - len(label_prefix) - 3 # -3 for ellipsis
                if available_space < 0: available_space = 0 # Should not happen, but safety
                truncated_display_name = display_name[:available_space] + "..."
            else:
                truncated_display_name = display_name

            final_label = f"{label_prefix}{truncated_display_name}"

            placeholder_text = f"Password for {display_name}"
            if not store_hash:
                placeholder_text += " (Zero-Knowledge)"
            if error_msg:
                placeholder_text = f"INCORRECT: {error_msg}"

            # Generate a unique custom_id using a hash of the item_key to avoid exceeding 100 char limit
            # This is important if item_key parts (paths) become very long.
            unique_id_hash = hashlib.sha1(str(item_key).encode('utf-8')).hexdigest()
            custom_id = f"password_{unique_id_hash}" # Max 100 chars, so a hash is safe.

            text_input = discord.ui.TextInput(
                label=final_label,
                placeholder=placeholder_text,
                required=True,
                max_length=256,
                style=discord.TextStyle.short,
                custom_id=custom_id
            )
            if item_key in self.entered_passwords:
                text_input.default = self.entered_passwords[item_key]
            self.add_item(text_input)

    async def on_submit(self, interaction: discord.Interaction):
        self.bot.log.info("DecryptionPasswordModal submitted. Validating passwords...")
        # noinspection PyUnresolvedReferences
        await interaction.response.defer(ephemeral=True) # Defer the modal submission interaction

        new_entered_passwords: Dict[Tuple[str, str, str, str], str] = {}
        current_errors: Dict[Tuple[str, str, str, str], str] = {}
        temp_decrypted_keys: Dict[Tuple[str, str, str, str], bytes] = {}
        all_passwords_correct = True # This flag refers to successful *derivation/hash-check*, not final decryption

        for item_info in self.required_passwords_info:
            root_upload_name = item_info['root_upload_name']
            relative_path_in_archive = item_info['relative_path_in_archive']
            base_filename = item_info['base_filename']
            version = item_info['version']
            db_password_hash = item_info['password_seed_hash']
            store_hash = item_info.get('store_hash_flag', False)

            item_key = (root_upload_name, relative_path_in_archive, base_filename, version)

            unique_id_hash = hashlib.sha1(str(item_key).encode('utf-8')).hexdigest()
            custom_id = f"password_{unique_id_hash}"

            text_input: Optional[discord.ui.TextInput] = next(
                (c for c in self.children if
                 isinstance(c, discord.ui.TextInput) and c.custom_id == custom_id),
                None
            )

            if not text_input:
                self.bot.log.error(f"Could not find TextInput for {item_key} in modal with custom_id {custom_id}.")
                current_errors[item_key] = "Internal error: Input missing."
                all_passwords_correct = False
                continue

            entered_password = text_input.value
            new_entered_passwords[item_key] = entered_password


            if not entered_password:
                current_errors[item_key] = "Password cannot be empty."
                all_passwords_correct = False
                continue

            try:
                derived_key = self.benc._derive_key_from_seed(entered_password)
                provided_seed_hash = hashlib.sha256(entered_password.encode('utf-8')).hexdigest()

                if store_hash: # If hash is stored, validate against it
                    if provided_seed_hash == db_password_hash:
                        temp_decrypted_keys[item_key] = derived_key
                        self.bot.log.debug(f"Password for '{item_key}' is CORRECT (hash matched).")
                    else:
                        current_errors[item_key] = "Incorrect password."
                        all_passwords_correct = False
                        self.bot.log.warning(f"Password for '{item_key}' is INCORRECT (hash mismatch).")
                else: # Zero-knowledge: hash not stored, assume correct for now. Real test happens during download.
                    temp_decrypted_keys[item_key] = derived_key
                    self.bot.log.debug(f"Password for '{item_key}' accepted (zero-knowledge mode, no hash check).")

            except ValueError as e:
                current_errors[item_key] = f"Key derivation error: {e}"
                all_passwords_correct = False
                self.bot.log.error(f"Key derivation error for '{item_key}': {e}")
            except Exception as e:
                current_errors[item_key] = f"Unexpected error: {e}"
                all_passwords_correct = False
                self.bot.log.error(f"Unexpected error during password validation for '{item_key}': {e}")
                self.bot.log.error(traceback.format_exc())

        # Merge new_entered_passwords with existing self.entered_passwords
        # This ensures that if a user corrected one password but another was already correct,
        # the already correct one is retained.
        self.entered_passwords.update(new_entered_passwords)
        self.error_messages = current_errors
        self.decrypted_keys.update(temp_decrypted_keys)  # Update with successfully derived keys

        if all_passwords_correct:
            self.bot.log.info("All passwords correct. Initiating download.")
            # Delete the modal response (the original modal interaction response)
            await interaction.delete_original_response()
            # Re-call download_filea with the original interaction and all collected keys
            from download import DownloadContext
            ctx = DownloadContext(intents=self.bot.intents, interaction=self.original_interaction, enc=False)
            await ctx.download_filea(
                baseapi=self.bot,
                target_path=self.target_path,
                DB_FILE=self.database_file,
                download_folder=self.download_folder,
                decryption_password_seed=self.password_seed,
                version_param=None,
                start_version_param=None,
                end_version_param=None,
                all_versions_param=False,
                can_apply_version_filters=False
            )
        else:
            self.bot.log.warning("Some passwords were incorrect or derivation failed. Re-presenting modal via button.")
            new_modal = DecryptionPasswordModal(
                self.bot,
                self.original_interaction, # Still pass the original interaction
                self.required_passwords_info,  # Still need all required info for the new modal
                self.database_file,
                self.target_path,
                self.download_folder,
                version_param=self.version_param, # Re-pass versioning parameters
                start_version_param=self.start_version_param,
                end_version_param=self.end_version_param,
                all_versions_param=self.all_versions_param,
                can_apply_version_filters=self.can_apply_version_filters,
                initial_passwords=self.entered_passwords,
                error_messages=self.error_messages
            )

            error_message_content = (
                f"{interaction.user.mention}, Some passwords were incorrect. Please review and re-enter. "
                f"Incorrect items: {', '.join([item_info['display_name'] for item_info in self.required_passwords_info if (item_info['root_upload_name'], item_info['relative_path_in_archive'], item_info['base_filename'], item_info['version']) in current_errors])}"
            )

            retry_view = discord.ui.View(timeout=180)
            retry_button = discord.ui.Button(label="Try Again", style=discord.ButtonStyle.red, emoji="❌")

            async def retry_callback(button_interaction: discord.Interaction):
                # This is a new interaction from the retry button
                # noinspection PyUnresolvedReferences
                await button_interaction.response.send_modal(new_modal)
                retry_button.disabled = True
                await button_interaction.message.edit(view=retry_view)

            retry_button.callback = retry_callback
            retry_view.add_item(retry_button)

            # Edit the original response of the modal submission to show the error and retry button
            await interaction.edit_original_response(content=error_message_content, view=retry_view)

class DecryptionPasswordTriggerView(discord.ui.View):
    def __init__(self, bot_instance: 'DOWNLOADA', original_interaction: discord.Interaction,
                 required_passwords_info: List[Dict[str, Any]], database_file: str, # Changed Any
                 target_path: str, download_folder: str,
                 version_param: Optional[str] = None, # NEW
                 start_version_param: Optional[str] = None, # NEW
                 end_version_param: Optional[str] = None, # NEW
                 all_versions_param: bool = False, # NEW
                 can_apply_version_filters: bool = False): # NEW
        super().__init__(timeout=180)
        self.bot = bot_instance
        self.original_interaction = original_interaction
        self.required_passwords_info = required_passwords_info
        self.database_file = database_file
        self.target_path = target_path
        self.download_folder = download_folder
        # Store new versioning parameters
        self.version_param = version_param
        self.start_version_param = start_version_param
        self.end_versitarget_path_displayon_param = end_version_param
        self.all_versions_param = all_versions_param
        self.can_apply_version_filters = can_apply_version_filters

    @discord.ui.button(label="Enter Passwords", style=discord.ButtonStyle.primary, custom_id="trigger_password_modal")
    async def trigger_modal_button(self, button_interaction: discord.Interaction, button: discord.ui.Button):
        # noinspection PyUnresolvedReferences
        await button_interaction.response.send_modal(DecryptionPasswordModal(
            bot_instance=self.bot,
            original_interaction=self.original_interaction,
            required_passwords_info=self.required_passwords_info,
            database_file=self.database_file,
            target_path=self.target_path,
            download_folder=self.download_folder,
            version_param=self.version_param, # Pass through versioning parameters
            start_version_param=self.start_version_param,
            end_version_param=self.end_version_param,
            all_versions_param=self.all_versions_param,
            can_apply_version_filters=self.can_apply_version_filters
        ))
        button.disabled = True
        await button_interaction.message.edit(view=self)