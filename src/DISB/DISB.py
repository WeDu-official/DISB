import random
import aiohttp
import discord
from discord.ext import commands
from discord import app_commands
import asyncio
import datetime
import os
import io
import traceback
import PowerDB as pdb
from typing import List, Dict, Optional
intents = discord.Intents.default()
intents.message_content = True
class FileBotAPI(commands.Bot):
    def __init__(self, *, intents: discord.Intents):
        super().__init__(command_prefix=self.get_prefix, intents=intents)
        self.upload_semaphore = asyncio.Semaphore(3)
        self.download_queue = asyncio.Queue()
        self.download_semaphore = asyncio.Semaphore(3)
        self.delete_task_queue = asyncio.Queue()
        self.deletion_task = None
        self.user_uploading: Dict[int, List[str]] = {}
        self.user_downloading: Dict[int, str] = {}
        self.http_session = None
        self.log_prefix = "[FileBotAPI]"
        self.total_parts_cache: Dict[str, int] = {}
    async def get_prefix(bot: commands.Bot, message: discord.Message) -> List[str]:
        return ["/"]
    async def setup_hook(self):
        self.http_session = aiohttp.ClientSession()
        try:
            synced = await self.tree.sync()
            self.log(f"Synced {len(synced)} slash commands for {self.user}")
        except discord.DiscordException as e:
            self.log(f"Error syncing slash commands: {e}")
    def log(self, message):
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{timestamp}] {self.log_prefix} {message}")
    async def on_ready(self):
        self.log(f'Logged in as {self.user.name} (ID: {self.user.id})')
    async def on_message(self, message: discord.Message):
        if message.author == self.user:
            return
        if message.content.startswith("/"):
            await self.process_commands(message)
    async def download_filea(self, interaction: discord.Interaction, filename: str, database_file: str,
                             download_folder: str):
        user_mention = interaction.user.mention
        initial_response_message = await interaction.followup.send(
            f"{user_mention}, download for '{filename}' started. I'll notify you when done.")
        if not filename:
            await initial_response_message.edit(content=f"{user_mention}, please specify the filename to download.")
            return
        if not database_file:
            await initial_response_message.edit(
                content=f"{user_mention}, please specify the database file to search in.")
            return
        if not download_folder:
            await initial_response_message.edit(content=f"{user_mention}, please specify the download folder.")
            return
        filename = os.path.basename(filename)
        download_folder = os.path.abspath(os.path.normpath(download_folder))
        if interaction.user.id in self.user_downloading and self.user_downloading[interaction.user.id] == filename:
            await initial_response_message.edit(
                content=f"{user_mention}, you are already downloading '{filename}'. Please wait.")
            return
        if self.download_semaphore.locked():
            await initial_response_message.edit(
                content=f"{user_mention}, server is busy with 3 download requests going at the same time(maximum)")
            self.log(f"Download limit reached. User {interaction.user.id} waiting.")
            return
        self.user_downloading[interaction.user.id] = filename
        try:
            async with self.download_semaphore:
                self.log(f"Starting download processing for '{filename}' by user {interaction.user.id}.")
                await self.download(self, interaction, filename, database_file, download_folder,
                                    initial_response_message)
                if os.path.exists(os.path.join(download_folder, filename)):
                    await initial_response_message.edit(
                        content=f"{user_mention}, '{filename}' found in '{download_folder}'. Download complete!")
                else:
                    await initial_response_message.edit(
                        content=f"{user_mention}, '{filename}' not found or download failed.")
                self.log(f"Download request of '{filename}' by user {interaction.user.id} completed.")
        except Exception as e:
            self.log(f"Error during download of '{filename}' by user {interaction.user.id}: {e}")
            self.log(traceback.format_exc())
            await initial_response_message.edit(
                content=f"{user_mention}, error during download: {e}. See logs for details.")
        finally:
            if interaction.user.id in self.user_downloading and self.user_downloading[interaction.user.id] == filename:
                del self.user_downloading[interaction.user.id]
    async def list_files_commanda(self, interaction: discord.Interaction, database_file: str):
        user_mention = interaction.user.mention
        if not database_file:
            await interaction.followup.send(f"{user_mention}, please specify the database file.",
                                            ephemeral=True)
            return
        await self.list_files(interaction, database_file)
    async def delete_filea(self, interaction: discord.Interaction, filename: str, database_file: str):
        user_mention = interaction.user.mention
        if not filename:
            await interaction.followup.send(f"{user_mention}, please specify the filename to delete.", ephemeral=True)
            return
        if not database_file:
            await interaction.followup.send(f"{user_mention}, please specify the database file.", ephemeral=True)
            return
        await self.delete(interaction, filename,
                          database_file)
    async def _start_upload_process(self, interaction: discord.Interaction, local_file_path: str, database_file: str,
                                    channel_id: int):
        user_mention = interaction.user.mention
        if not local_file_path:
            await interaction.followup.send(f"{user_mention}, please specify the local file path to upload.",
                                            ephemeral=True)
            return
        if not database_file:
            await interaction.followup.send(f"{user_mention}, please specify the database file to use.", ephemeral=True)
            return
        local_file_path = os.path.abspath(local_file_path)
        filename = os.path.basename(local_file_path)
        self.log(f"Upload requested by {interaction.user.id} for '{filename}' to database '{database_file}'.")
        if interaction.user.id in self.user_uploading and filename in self.user_uploading[interaction.user.id]:
            await interaction.followup.send(f"{user_mention}, you are already uploading '{filename}'. Please wait.",
                                            ephemeral=True)
            return
        if not os.path.exists(local_file_path):
            await interaction.followup.send(f"{user_mention}, file '{filename}' not found at '{local_file_path}'.",
                                            ephemeral=True)
            self.log(f"Error: File not found at '{local_file_path}' for user {interaction.user.id}.")
            return
        if self.upload_semaphore.locked():
            await interaction.followup.send(
                f"{user_mention}, server is busy with 3 upload requests going at the same time(maximum)",
                ephemeral=True)
            self.log(f"Upload limit reached. User {interaction.user.id} waiting.")
            return
        if interaction.user.id not in self.user_uploading:
            self.user_uploading[interaction.user.id] = [filename]
        else:
            self.user_uploading[interaction.user.id].append(filename)
        self.log(f"after modification user_uploading is {self.user_uploading}")
        initial_response_message = await interaction.followup.send(
            f"{user_mention}, upload for '{filename}' started. Preparing...", ephemeral=False)
        try:
            async with self.upload_semaphore:
                self.log(
                    f"Starting upload processing for '{filename}' from '{local_file_path}' by user {interaction.user.id}. Channel ID: {channel_id}")
                await self._handle_upload_task(interaction, local_file_path, database_file, channel_id,
                                               initial_response_message)
        except Exception as e:
            self.log(f"Error during upload of '{filename}' by user {interaction.user.id}: {e}")
            self.log(traceback.format_exc())
            try:
                await initial_response_message.edit(
                    content=f"{user_mention}, error during upload: {e}. See logs for details.")
            except discord.HTTPException:
                await interaction.followup.send(
                    f"{user_mention}, an error occurred during upload: {e}. See logs for details.", ephemeral=True)
        finally:
            if interaction.user.id in self.user_uploading:
                if filename in self.user_uploading[interaction.user.id]:
                    self.user_uploading[interaction.user.id].remove(filename)
                if not self.user_uploading[interaction.user.id]:
                    del self.user_uploading[interaction.user.id]
    async def _handle_upload_task(self, interaction: discord.Interaction, local_file_path: str, DB_FILE: str,
                                  channel_id: int, initial_response_message: discord.Message):
        """Handles the actual file upload process with detailed logging and error reporting."""
        filename = os.path.basename(local_file_path)
        user_mention = interaction.user.mention
        try:
            await self.upload_local_file(interaction, local_file_path, DB_FILE, channel_id,
                                         initial_response_message)
            await initial_response_message.edit(
                content=f"{user_mention}, upload of '{filename}' to '{DB_FILE}' complete!")
            self.log(f"Upload request of '{filename}' by user {interaction.user.id} completed.")
        except FileNotFoundError:
            await initial_response_message.edit(
                content=f"{user_mention}, the file '{filename}' was not found during the upload task. Please ensure the file exists at the specified path.")
            self.log(f"Error in upload task for '{filename}': File not found at '{local_file_path}'.")
        except Exception as e:
            self.log(f"Error in upload task for '{filename}': {e}")
            self.log(traceback.format_exc())
            await initial_response_message.edit(
                content=f"{user_mention}, an error occurred during the upload of '{filename}': {e}. Please check the logs for more details.")
    async def close(self):
        if self.http_session:
            await self.http_session.close()
            self.log("aiohttp session closed.")
    async def _store_file_part(self, filename: str, part_number: int, total_parts: int, message_id: int,
                                 channel_id: int, DB_FILE: str):
        print('_store_file_part hit')
        if not DB_FILE.lower().endswith('.pdb'):
            DB_FILE += '.pdb'
        DATABASE_FILE = os.path.abspath(os.path.normpath(DB_FILE))
        try:
            if not os.path.exists(DATABASE_FILE):
                self.log(
                    f">>> [_store_file_part] ERROR: Database file not found at '{DATABASE_FILE}'. Attempting to create.")
                pdb.create.make_db(DATABASE_FILE)
                pdb.create.make_table(DATABASE_FILE, 'files')
            row_id = await asyncio.to_thread(pdb.table_data.totalrows, DATABASE_FILE, 0)
            fileid = await asyncio.to_thread(pdb.table_data.read, DATABASE_FILE, [0, 0, row_id - 1]) or 0
            values_to_insert = [str(int(fileid) + 1), filename, str(part_number), str(total_parts), str(message_id),
                                str(channel_id)]
            for i, val in enumerate(values_to_insert):
                await asyncio.to_thread(self._db_insert_sync, DATABASE_FILE, val, [0, i], row_id)
            self.log(
                f"Stored part {part_number}/{total_parts} of '{filename}' (Message ID: {message_id}, Channel ID: {channel_id}) in database.")
        except Exception as e:
            self.log(
                f">>> [_store_file_part] ERROR: An error occurred while storing part {part_number} of '{filename}': {e}")
            self.log(traceback.format_exc())
    def _db_insert_sync(self, db_file: str, value: str, coordinates: List[int], row: int):
        print('_db_insert_sync hit')
        address = None
        try:
            tableid = coordinates[0]
            columnid = coordinates[1]
            address = [tableid, columnid, row]
            pdb.table_data.insert(db_file, value, address)
        except Exception as e:
            self.log(
                f">>> [_db_insert_sync] ERROR: An error occurred while writing '{value}' to the database '{db_file}' at {address}: {e}")
            self.log(traceback.format_exc())
    def remove_rows_by_value(self, database_file, target_value):
        print('remove_rows_by_value hit')
        new_lines = []
        rows_to_delete = []
        total_rows = pdb.table_data.totalrows(database_file, 0)
        for r in range(total_rows):
            try:
                value = pdb.table_data.read(database_file, [0, 1, r])
                if value == target_value:
                    rows_to_delete.append(r)
            except IndexError:
                print(f"IndexError at row {r}.  This might indicate an issue with the database structure.")
                pass
        rows_to_delete = sorted(list(set(rows_to_delete)), reverse=True)
        with open(database_file, 'r', encoding='utf-8', errors='surrogateescape') as f:
            for i, line in enumerate(f):
                if i < 3 or not any(
                        f"~<[0;{row_col}?{row_num}]" in line for row_num in rows_to_delete for row_col in
                        [0, 1, 2, 3, 4, 5]):
                    new_lines.append(line)
        with open(database_file, 'w', encoding='utf-8', errors='surrogateescape') as f:
            f.write(f"#POWER_DB\n&<0^files>\n")
            f.writelines(new_lines[3:])
    async def safe_send(self, target_destination: discord.abc.Messageable, file_path: str, filename_to_send: str,
                        offset: int, part_size: int, attempt: int, retries: int, part_number: int) -> Optional[
        discord.Message]:
        print('safe_send hit')
        send_retries = 3
        send_delay = 2
        for send_attempt in range(send_retries):
            part_data = b''
            try:
                with open(file_path, 'rb') as f:
                    f.seek(offset)
                    part_data = f.read(part_size)
                file_size = len(part_data)
                self.log(
                    f"Send attempt {send_attempt + 1}/{send_retries} (overall attempt {attempt + 1}/{retries}), part {part_number}, file size: {file_size} bytes")
                if len(part_data) > 10:
                    self.log(
                        f"Send attempt {send_attempt + 1}/{send_retries} (overall attempt {attempt + 1}/{retries}), part {part_number}, first 10 bytes: {part_data[:10]!r}")
                else:
                    self.log(
                        f"Send attempt {send_attempt + 1}/{send_retries} (overall attempt {attempt + 1}/{retries}), part {part_number}, data: {part_data!r}")
                if not part_data and part_number < self.total_parts_cache.get(file_path, float('inf')):
                    self.log(
                        f"Send attempt {send_attempt + 1}/{send_retries} (overall attempt {attempt + 1}/{retries}), part {part_number}, WARNING: Empty read, retrying send.")
                    await asyncio.sleep(0.3)
                    continue
                discord_file = discord.File(io.BytesIO(part_data), filename=filename_to_send)
                message = await target_destination.send(file=discord_file)
                self.log(
                    f"Send attempt {send_attempt + 1}/{send_retries} (overall attempt {attempt + 1}/{retries}), part {part_number} successful, Message ID: {message.id if message else None}")
                return message
            except (aiohttp.ClientError, asyncio.TimeoutError, discord.errors.DiscordException) as e:
                self.log(
                    f"Send attempt {send_attempt + 1}/{send_retries} (overall attempt {attempt + 1}/{retries}), part {part_number} failed: {e!r}")
                if send_attempt < send_retries - 1:
                    wait_time = send_delay + random.uniform(0, 1)
                    self.log(f"Retrying send in {wait_time:.2f} seconds...")
                    await asyncio.sleep(wait_time)
                    send_delay *= 2
                else:
                    self.log("Max send retries reached within safe_send, returning None.")
                    return None
            except Exception as e:
                self.log(
                    f"Unexpected error during send attempt {send_attempt + 1}/{send_retries} (overall attempt {attempt + 1}/{retries}), part {part_number}: {e!r}")
                return None
        return None
    async def send_with_retries(self, target_channel: discord.abc.Messageable, file_path: str, part_number: int,
                                total_parts: int,
                                part_size: int, filename_to_send: str) -> Optional[discord.Message]:
        retries = 5
        delay = 4
        offset = (part_number - 1) * part_size
        self.total_parts_cache[file_path] = total_parts
        for attempt in range(retries):
            result = await self.safe_send(target_channel, file_path, filename_to_send, offset, part_size, attempt,
                                          retries, part_number)
            if result:
                self.log(f"[ATTEMPT {attempt + 1}/{retries}] Part {part_number} sent successfully.")
                return result
            else:
                self.log(f"[ATTEMPT {attempt + 1}/{retries}] Part {part_number} send failed on attempt {attempt + 1}.")
                if attempt < retries - 1:
                    wait_time = delay + random.uniform(0, 1)
                    self.log(f"Retrying part {part_number} send in {wait_time:.2f} seconds...")
                    await asyncio.sleep(wait_time)
                    delay *= 2
                else:
                    self.log(
                        f"[ATTEMPT {attempt + 1}/{retries}] Max retries reached for part {part_number}, giving up.")
                    return None
        return None
    async def is_filename_in_db(self, db_file: str, filename: str):
        DATABASE_FILE = os.path.abspath(os.path.normpath(db_file))
        try:
            total_rows = await asyncio.to_thread(pdb.table_data.totalrows, DATABASE_FILE, 0)
            for r in range(total_rows):
                stored_filename = await asyncio.to_thread(pdb.table_data.read, DATABASE_FILE, [0, 1, r])
                if stored_filename == filename:
                    return True
            return False
        except Exception as e:
            self.log(f">>> [is_filename_in_db] ERROR: {e}")
            return False
    async def upload_local_file(self, interaction: discord.Interaction, local_file_path: str, DB_FILE: str,
                                channel_id: int, initial_response_message: discord.Message):
        filename = os.path.basename(local_file_path)
        user_mention = interaction.user.mention
        if not DB_FILE.lower().endswith('.pdb'):
            DB_FILE += '.pdb'
        DATABASE_FILE = os.path.abspath(os.path.normpath(DB_FILE))
        file_size = os.path.getsize(local_file_path)
        if await self.is_filename_in_db(DATABASE_FILE, filename):
            await initial_response_message.edit(
                content=f"{user_mention}, the file '{filename}' is already present in the database '{DB_FILE}'. Upload aborted.")
            self.log(f">>> [upload_local_file] Aborted: '{filename}' already exists in '{DATABASE_FILE}'.")
            return
        self.log(f"Initiating upload of '{filename}' (size: {file_size} bytes).")
        await initial_response_message.edit(
            content=f"{user_mention}, preparing to upload '{filename}' (size: {file_size / (1024 * 1024):.2f} MB).")
        try:
            part_size = 10 * 1024 * 1024
            num_parts = (file_size + part_size - 1) // part_size
            await initial_response_message.edit(
                content=f"{user_mention}, this file will be uploaded in {num_parts} parts.")
            self.log(f"File '{filename}' will be uploaded in {num_parts} parts.")
            target_channel = self.get_channel(channel_id)
            if not target_channel:
                target_channel = await self.fetch_channel(channel_id)
            if not target_channel:
                await initial_response_message.edit(
                    content=f"{user_mention}, Failed to find the target channel for upload.")
                self.log(f"Error: Target channel (ID: {channel_id}) not found for upload.")
                return
            for i in range(1, num_parts + 1):
                part_filename = f'{filename}.part{i}'
                progress_percent = (i / num_parts) * 100
                await initial_response_message.edit(
                    content=f"{user_mention}, Uploading '{filename}'... Progress: {progress_percent:.1f}% (Part {i}/{num_parts})")
                message = await self.send_with_retries(target_channel, local_file_path, i, num_parts, part_size,
                                                       part_filename)
                if message:
                    await self._store_file_part(filename, i, num_parts, message.id, channel_id, DB_FILE)
                    self.log(
                        f"Part {i}/{num_parts} of '{filename}' sent and metadata stored (Message ID: {message.id}).")
                else:
                    await initial_response_message.edit(
                        content=f"{user_mention}, failed to send part {i}/{num_parts} of '{filename}'. Upload aborted. Cleaning up...")
                    self.log(f"Error: Failed to send part {i} of '{filename}'. Upload aborted.")
                    self.remove_rows_by_value(DATABASE_FILE, filename)
                    return
            await initial_response_message.edit(
                content=f"{user_mention}, all {num_parts} parts of '{filename}' uploaded successfully to '{DB_FILE}'.")
            self.log(f"All parts of '{filename}' uploaded and metadata stored successfully.")
        except FileNotFoundError:
            await initial_response_message.edit(
                content=f"{user_mention}, the file '{filename}' was not found during upload.")
            self.log(f"Error: File not found at '{local_file_path}' during upload.")
        except Exception as e:
            self.log(f"Error during upload of '{filename}': {e}")
            self.log(traceback.format_exc())
            await initial_response_message.edit(
                content=f"{user_mention}, an unexpected error occurred during the upload of '{filename}': {e}. Please check the logs for more details. Cleaning up...")
            self.remove_rows_by_value(DATABASE_FILE, filename)
        finally:
            pass
    async def download(self, bot: "FileBotAPI", interaction: discord.Interaction, filename: str, DB_FILE: str,
                       DOWNLOAD_FOLDER: str, initial_response_message: discord.Message):
        user_mention = interaction.user.mention
        message_retries = 5
        message_delay = 4
        attachment_retries = 5
        attachment_delay = 4
        download_successful = False
        try:
            if not DB_FILE.lower().endswith('.pdb'):
                DB_FILE += '.pdb'
            DATABASE_FILE = os.path.abspath(os.path.normpath(DB_FILE))
            if not os.path.exists(DATABASE_FILE):
                await initial_response_message.edit(
                    content=f"{user_mention}, the database file '{DB_FILE}' was not found.")
                self.log(f">>> [DOWNLOAD] ERROR: Database file not found at '{DATABASE_FILE}'.")
                pdb.create.make_db(DATABASE_FILE)
                pdb.create.make_table(DATABASE_FILE, 'files')
                return
            DOWNLOAD_FOLDER = os.path.abspath(os.path.normpath(DOWNLOAD_FOLDER))
            if not os.path.exists(DOWNLOAD_FOLDER):
                try:
                    os.makedirs(DOWNLOAD_FOLDER)
                    self.log(f">>> [DOWNLOAD] Created download directory: '{DOWNLOAD_FOLDER}'.")
                except OSError as e:
                    await initial_response_message.edit(
                        content=f"{user_mention}, could not create the download folder '{DOWNLOAD_FOLDER}'.")
                    self.log(f">>> [DOWNLOAD] ERROR: Could not create download directory: {e}")
                    return
            self.log(f"Downloading file: '{filename}' from '{DATABASE_FILE}' to '{DOWNLOAD_FOLDER}'.")
            await initial_response_message.edit(content=f"{user_mention}, Fetching file parts for '{filename}'...")
            parts_to_download = []
            try:
                all_rows = pdb.table_data.numberrows(DATABASE_FILE, [0, 0], False)
                for i in range(all_rows):
                    row_data = pdb.table_data.readcolumns(DATABASE_FILE, [0, i])
                    if row_data and len(row_data) > 1 and row_data[1] == filename:
                        try:
                            part_number = int(row_data[2])
                            total_parts = int(row_data[3])
                            message_id = int(row_data[4])
                            channel_id = int(row_data[5])
                            parts_to_download.append((part_number, total_parts, message_id, channel_id))
                        except (ValueError, IndexError) as e:
                            self.log(
                                f">>> [DOWNLOAD] WARNING: Invalid database data for '{filename}' at row {i}: {e}, data: {row_data}")
                parts_to_download.sort(key=lambda x: x[0])
                if not parts_to_download or len(parts_to_download) != parts_to_download[0][1]:
                    await initial_response_message.edit(
                        content=f"{user_mention}, file '{filename}' not found or incomplete metadata in the database.")
                    self.log(f">>> [DOWNLOAD] ERROR: File '{filename}' not found or incomplete metadata.")
                    return
                self.log(f">>> [DOWNLOAD] Found {len(parts_to_download)} parts to download: {parts_to_download}")
            except Exception as e:
                await initial_response_message.edit(
                    content=f"{user_mention}, error finding file parts for '{filename}': {e}.")
                self.log(f">>> [DOWNLOAD] ERROR: Exception finding file parts: {e}")
                self.log(traceback.format_exc())
                return
            channel_id = parts_to_download[0][3] if parts_to_download else None
            if not channel_id:
                await initial_response_message.edit(
                    content=f"{user_mention}, could not determine the channel for '{filename}'.")
                self.log(f">>> [DOWNLOAD] ERROR: Could not determine channel ID for '{filename}'.")
                return
            channel = bot.get_channel(channel_id)
            if not channel:
                try:
                    channel = await bot.fetch_channel(channel_id)
                except (discord.NotFound, discord.Forbidden, discord.HTTPException) as e:
                    await initial_response_message.edit(
                        content=f"{user_mention}, channel (ID: {channel_id}) not found or accessible for '{filename}'.")
                    self.log(f">>> [DOWNLOAD] ERROR: Channel with ID {channel_id} not found or accessible: {e}.")
                    return
            file_bytes = b""
            total_expected_parts = parts_to_download[0][1] if parts_to_download else 0
            all_parts_retrieved = True
            for part_number, _, message_id, _ in sorted(parts_to_download, key=lambda x: x[0]):
                progress_percent = (part_number / total_expected_parts) * 100
                await initial_response_message.edit(
                    content=f"{user_mention}, Downloading '{filename}'... Progress: {progress_percent:.1f}% (Part {part_number}/{total_expected_parts})")
                message = None
                for attempt in range(message_retries):
                    self.log(
                        f">>> [DOWNLOAD] [MESSAGE FETCH ATTEMPT {attempt + 1}/{message_retries}] Fetching message ID {message_id} for part {part_number}.")
                    try:
                        message = await channel.fetch_message(message_id)
                        self.log(
                            f">>> [DOWNLOAD] [MESSAGE FETCH ATTEMPT {attempt + 1}/{message_retries}] Successfully fetched message ID {message_id}.")
                        break
                    except discord.NotFound:
                        await initial_response_message.edit(
                            content=f"{user_mention}, part {part_number} (ID: {message_id}) not found. Download may be incomplete.")
                        self.log(f">>> [DOWNLOAD] ERROR: Part {part_number} (ID: {message_id}) not found.")
                        all_parts_retrieved = False
                        break
                    except discord.Forbidden:
                        await initial_response_message.edit(
                            content=f"{user_mention}, no permission to access message (ID: {message_id}).")
                        self.log(f">>> [DOWNLOAD] ERROR: Permission denied for message ID {message_id}.")
                        all_parts_retrieved = False
                        break
                    except discord.HTTPException as e:
                        self.log(
                            f">>> [DOWNLOAD] WARNING: [MESSAGE FETCH ATTEMPT {attempt + 1}/{message_retries}] HTTPException fetching message (ID: {message_id}): {e}")
                        if attempt < message_retries - 1:
                            wait_time = message_delay + random.uniform(0, 1)
                            self.log(
                                f">>> [DOWNLOAD] [MESSAGE FETCH ATTEMPT {attempt + 1}/{message_retries}] Retrying in {wait_time:.2f} seconds...")
                            await asyncio.sleep(wait_time)
                        else:
                            await initial_response_message.edit(
                                content=f"{user_mention}, failed to fetch part {part_number} after {message_retries} retries. Download aborted.")
                            self.log(
                                f">>> [DOWNLOAD] ERROR: Failed to fetch message ID {message_id} after {message_retries} retries.")
                            all_parts_retrieved = False
                            break
                    except Exception as e:
                        self.log(
                            f">>> [DOWNLOAD] ERROR: [MESSAGE FETCH ATTEMPT {attempt + 1}/{message_retries}] Unexpected error fetching message (ID: {message_id}): {e}")
                        if attempt < message_retries - 1:
                            wait_time = message_delay + random.uniform(0, 1)
                            self.log(
                                f">>> [DOWNLOAD] [MESSAGE FETCH ATTEMPT {attempt + 1}/{message_retries}] Retrying in {wait_time:.2f} seconds...")
                            await asyncio.sleep(wait_time)
                        else:
                            await initial_response_message.edit(
                                content=f"{user_mention}, critical error fetching part {part_number}. Download aborted.")
                            self.log(
                                f">>> [DOWNLOAD] CRITICAL ERROR: Error fetching message ID {message_id} after {message_retries} retries: {e}")
                            all_parts_retrieved = False
                            break
                if not all_parts_retrieved:
                    break
                if message and message.attachments:
                    attachment = message.attachments[0]
                    file_data = None
                    for attempt in range(attachment_retries):
                        self.log(
                            f">>> [DOWNLOAD] [ATTACHMENT READ ATTEMPT {attempt + 1}/{attachment_retries}] Reading attachment from message ID {message_id}.")
                        try:
                            file_data = await attachment.read()
                            self.log(
                                f">>> [DOWNLOAD] [ATTACHMENT READ ATTEMPT {attempt + 1}/{attachment_retries}] Successfully read attachment from message ID {message_id}.")
                            break
                        except aiohttp.client_exceptions.ClientPayloadError as e:
                            self.log(
                                f">>> [DOWNLOAD] WARNING: [ATTACHMENT READ ATTEMPT {attempt + 1}/{attachment_retries}] ClientPayloadError reading attachment: {e}")
                            if attempt < attachment_retries - 1:
                                await asyncio.sleep(attachment_delay + random.uniform(0, 1))
                            else:
                                await initial_response_message.edit(
                                    content=f"{user_mention}, failed to download part {part_number} after {attachment_retries} retries. Download aborted.")
                                self.log(
                                    f">>> [DOWNLOAD] ERROR: Failed to download attachment for message ID {message_id} after {attachment_retries} retries.")
                                all_parts_retrieved = False
                                break
                        except aiohttp.ClientError as e:
                            self.log(
                                f">>> [DOWNLOAD] WARNING: [ATTACHMENT READ ATTEMPT {attempt + 1}/{attachment_retries}] ClientError reading attachment: {e}")
                            if attempt < attachment_retries - 1:
                                await asyncio.sleep(attachment_delay + random.uniform(0, 1))
                            else:
                                await initial_response_message.edit(
                                    content=f"{user_mention}, network error downloading part {part_number}. Download aborted.")
                                self.log(
                                    f">>> [DOWNLOAD] ERROR: Network error downloading attachment for message ID {message_id}.")
                                all_parts_retrieved = False
                                break
                        except Exception as e:
                            self.log(
                                f">>> [DOWNLOAD] ERROR: [ATTACHMENT READ ATTEMPT {attempt + 1}/{attachment_retries}] Unexpected error reading attachment: {e}")
                            if attempt < attachment_retries - 1:
                                await asyncio.sleep(attachment_delay + random.uniform(0, 1))
                            else:
                                await initial_response_message.edit(
                                    content=f"{user_mention}, critical error downloading part {part_number}. Download aborted.")
                                self.log(
                                    f">>> [DOWNLOAD] CRITICAL ERROR: Error downloading attachment for message ID {message_id}: {e}")
                                all_parts_retrieved = False
                                break
                    if not all_parts_retrieved:
                        break
                    if file_data:
                        file_bytes += file_data
                        self.log(
                            f">>> [DOWNLOAD] Downloaded part {part_number} (Message ID: {message_id}) of '{filename}'.")
                    else:
                        await initial_response_message.edit(
                            content=f"{user_mention}, failed to retrieve data for part {part_number}. Download aborted.")
                        self.log(
                            f">>> [DOWNLOAD] ERROR: Failed to retrieve data for part {part_number} (ID: {message_id}).")
                        all_parts_retrieved = False
                        break
                else:
                    await initial_response_message.edit(
                        content=f"{user_mention}, part {part_number} (ID: {message_id}) has no attachment. Download aborted.")
                    self.log(
                        f">>> [DOWNLOAD] ERROR: Part {part_number} (ID: {message_id}) has no attachment.")
                    all_parts_retrieved = False
                    break
            if all_parts_retrieved:
                download_successful = True
            else:
                pass
        except Exception as e:
            self.log(f">>> [DOWNLOAD] ERROR: General exception during download: {e}")
            self.log(traceback.format_exc())
            await initial_response_message.edit(
                content=f"{user_mention}, a general error occurred during download: {e}.")
            download_successful = False
            return
        if download_successful:
            try:
                download_path = os.path.join(DOWNLOAD_FOLDER, filename)
                with open(download_path, 'wb') as f:
                    f.write(file_bytes)
                self.log(f">>> [DOWNLOAD] File '{filename}' successfully downloaded to '{download_path}'.")
                await initial_response_message.edit(
                    content=f'{user_mention}, File "{filename}" downloaded successfully to "{DOWNLOAD_FOLDER}".')
            except OSError as e:
                await initial_response_message.edit(
                    content=f"{user_mention}, error writing the downloaded file '{filename}': {e}.")
                self.log(f">>> [DOWNLOAD] ERROR: OSError writing file '{filename}': {e}")
                self.log(traceback.format_exc())
                return
            except Exception as e:
                await initial_response_message.edit(
                    content=f"{user_mention}, unexpected error during download of '{filename}': {e}.")
                self.log(f">>> [DOWNLOAD] ERROR: Unexpected error during download: {e}")
                self.log(traceback.format_exc())
                return
    async def delete(self, interaction: discord.Interaction, filename: str, DB_FILE: str):
        user_mention = interaction.user.mention
        await interaction.followup.send(f"{user_mention}, delete request started for '{filename}'.",
                                        ephemeral=False)
        self.log(f">>> [DELETE] Entering delete command for filename: '{filename}', DB_FILE: '{DB_FILE}'.")
        try:
            if not DB_FILE.lower().endswith('.pdb'):
                DB_FILE += '.pdb'
            DATABASE_FILE = os.path.abspath(os.path.normpath(DB_FILE))
            if not os.path.exists(DATABASE_FILE):
                await interaction.followup.send(
                    f"{user_mention}, the database file '{DB_FILE}' was not found. Cannot proceed with deletion.",
                    ephemeral=True)
                self.log(f">>> [DELETE] ERROR: Database file not found at '{DATABASE_FILE}'.")
                pdb.create.make_db(DATABASE_FILE)
                pdb.create.make_table(DATABASE_FILE, 'files')
                return
            rows_to_delete = []
            try:
                total_rows = await asyncio.to_thread(pdb.table_data.totalrows, DATABASE_FILE, 0)
                for r in range(total_rows):
                    value = await asyncio.to_thread(pdb.table_data.read, DATABASE_FILE, [0, 1, r])
                    if value == filename:
                        rows_to_delete.append(r)
            except Exception as e:
                await interaction.followup.send(
                    f"{user_mention}, error reading the database to find '{filename}' for deletion: {e}",
                    ephemeral=True)
                self.log(f">>> [DELETE] ERROR: Error reading database for '{filename}': {e}")
                self.log(traceback.format_exc())
                return
            if not rows_to_delete:
                await interaction.followup.send(f'{user_mention}, file "{filename}" not found in the database.',
                                                ephemeral=True)
                self.log(f">>> [DELETE] ERROR: File '{filename}' not found in database.")
                return
            rows_to_delete = sorted(list(set(rows_to_delete)), reverse=True)
            messages_to_delete_by_channel = {}
            deleted_message_count = 0
            for r in rows_to_delete:
                try:
                    parts = await asyncio.to_thread(pdb.table_data.readcolumns, DATABASE_FILE, [0, r])
                    if parts and len(parts) == 6:
                        try:
                            _, _, _, _, msg_id_str, channel_id_str = parts
                            msg_id = int(msg_id_str)
                            channel_id = int(channel_id_str)
                            channel = self.get_channel(channel_id) or await self.fetch_channel(
                                channel_id)
                            if channel:
                                if channel not in messages_to_delete_by_channel:
                                    messages_to_delete_by_channel[channel] = []
                                messages_to_delete_by_channel[channel].append(msg_id)
                                deleted_message_count += 1
                            else:
                                self.log(
                                    f">>> [DELETE] WARNING: Channel {channel_id} not found. Skipping message {msg_id}.")
                        except ValueError as ve:
                            self.log(
                                f">>> [DELETE] ERROR: ValueError during conversion of message/channel ID: {ve}, parts: {parts}")
                        except IndexError as ie:
                            self.log(f">>> [DELETE] ERROR: IndexError during unpacking of parts: {ie}, parts: {parts}")
                    else:
                        self.log(
                            f">>> [DELETE] WARNING: Skipping row {r} due to incorrect number of columns: {len(parts)}, data: {parts}")
                except Exception as e:
                    self.log(f">>> [DELETE] ERROR: Exception while processing row {r} for message deletion: {e}")
                    self.log(traceback.format_exc())
            for channel, message_ids in messages_to_delete_by_channel.items():
                unique_message_ids = list(set(message_ids))
                self.log(
                    f">>> [DELETE] Queued {len(unique_message_ids)} messages for deletion in channel {channel.id}.")
                await self.delete_task_queue.put((channel, unique_message_ids))
            self.start_deletion_task()
            try:
                new_lines = []
                with open(DATABASE_FILE, 'r', encoding='utf-8', errors='surrogateescape') as f:
                    for i, line in enumerate(f):
                        if i < 3 or not any(
                                f"~<[0;{row_col}?{row_num}]" in line for row_num in rows_to_delete for row_col in
                                [0, 1, 2, 3, 4, 5]):
                            new_lines.append(line)
                with open(DATABASE_FILE, 'w', encoding='utf-8', errors='surrogateescape') as f:
                    f.write(f"#POWER_DB\n&<0^files>\n")
                    f.writelines(new_lines[3:])
                    self.log(">>> [DELETE] Database file updated successfully.")
            except OSError as e:
                await interaction.followup.send(
                    f"{user_mention}, error updating database after deleting '{filename}': {e}",
                    ephemeral=True)
                self.log(f">>> [DELETE] ERROR: Error updating database: {e}")
                return
            except Exception as e:
                await interaction.followup.send(
                    f"{user_mention}, an unexpected error occurred while updating the database: {e}",
                    ephemeral=True)
                self.log(f">>> [DELETE] ERROR: Unexpected error during database update: {e}")
                self.log(traceback.format_exc())
                return
            await interaction.followup.send(
                f'{user_mention}, removed {len(rows_to_delete)} database entries for "{filename}" and queued {deleted_message_count} messages for deletion.'
            )
            self.log(">>> [DELETE] Sent confirmation message to user.")
        except discord.Forbidden:
            await interaction.followup.send(
                f"{user_mention}, I don't have permission to delete messages in the channel(s) where the file parts are located.",
                ephemeral=True)
            self.log(f">>> [DELETE] ERROR: Forbidden to delete messages.")
        except (discord.NotFound, discord.HTTPException) as e:
            await interaction.followup.send(f"{user_mention}, an error occurred during the deletion process: {e}",
                                            ephemeral=True)
            self.log(f">>> [DELETE] ERROR: Discord API error during deletion: {e}")
            self.log(traceback.format_exc())
        except Exception as e:
            await interaction.followup.send(
                f"{user_mention}, an unexpected error occurred during the deletion of '{filename}': {e}",
                ephemeral=True)
            self.log(f">>> [DELETE] ERROR: Unexpected error during deletion: {e}")
            self.log(traceback.format_exc())
        finally:
            self.log(">>> [DELETE] Exiting delete command.")
    async def _process_delete_queue(self):
        """Processes the queue of messages to be deleted."""
        self.log(">>> [_PDQ] Entering _process_delete_queue.")
        try:
            while True:
                channel, message_ids = await self.delete_task_queue.get()
                self.log(f">>> [_PDQ] Processing {len(message_ids)} messages in channel {channel.id}.")
                if channel:
                    await self._delete_messages(channel, message_ids)
                else:
                    self.log(f">>> [_PDQ] WARNING: Channel not found. Skipping {len(message_ids)} messages.")
                self.delete_task_queue.task_done()
        except asyncio.CancelledError:
            self.log(">>> [_PDQ] Deletion task cancelled.")
        except Exception as e:
            self.log(f">>> [_PDQ] ERROR: Exception in _process_delete_queue: {e}")
            self.log(traceback.format_exc())
        finally:
            self.log(">>> [_PDQ] Exiting _process_delete_queue.")
    def start_deletion_task(self):
        """Starts the background task to process the delete queue."""
        self.log(">>> [START] Entering start_deletion_task.")
        try:
            if self.deletion_task is None or self.deletion_task.done():
                self.deletion_task = asyncio.create_task(self._process_delete_queue())
                self.log(">>> [START] Started deletion task.")
            else:
                self.log(">>> [START] Deletion task already running.")
        except Exception as e:
            self.log(f">>> [START] ERROR: Exception in start_deletion_task: {e}")
            self.log(traceback.format_exc())
        finally:
            self.log(">>> [START] Exiting start_deletion_task.")
    def stop_deletion_task(self):
        """Stops the background task for deleting messages."""
        self.log(">>> [STOP] Entering stop_deletion_task.")
        try:
            if self.deletion_task:
                self.log(">>> [STOP] Stopping deletion task.")
                self.deletion_task.cancel()
                self.deletion_task = None
            else:
                self.log(">>> [STOP] No deletion task to stop.")
        except Exception as e:
            self.log(f">>> [STOP] ERROR: Exception in stop_deletion_task: {e}")
            self.log(traceback.format_exc())
        finally:
            self.log(">>> [STOP] Exiting stop_deletion_task.")
    async def _delete_messages(self, channel, message_ids):
        """Deletes a list of messages from a given channel, handling potential errors."""
        self.log(f">>> [_DELM] Entering _delete_messages for channel {channel.id} with {len(message_ids)} messages.")
        if not message_ids:
            self.log(">>> [_DELM] No messages to delete.")
            return
        unique_message_ids = list(set(message_ids))
        messages_to_delete = [discord.Object(id=mid) for mid in unique_message_ids]
        self.log(
            f">>> [_DELM] Attempting to delete {len(messages_to_delete)} unique messages from channel {channel.id}.")
        BATCH_SIZE = 100
        for i in range(0, len(messages_to_delete), BATCH_SIZE):
            batch = messages_to_delete[i:i + BATCH_SIZE]
            try:
                if len(batch) == 1:
                    await channel.delete_messages(batch[0])
                    self.log(f">>> [_DELM] Deleted single message {batch[0].id} from channel {channel.id}.")
                else:
                    await channel.delete_messages(batch)
                    self.log(f">>> [_DELM] Deleted {len(batch)} messages from channel {channel.id}.")
            except discord.errors.Forbidden as e:
                self.log(
                    f">>> [_DELM] Forbidden: {e}. Bot likely lacks manage_messages permission in channel {channel.id}. Falling back to individual deletes.")
                await self._delete_messages_individually(channel, [m.id for m in batch], channel)
            except discord.errors.NotFound as e:
                self.log(
                    f">>> [_DELM] NotFound: {e}. Some messages in the batch were not found in channel {channel.id}.")
            except discord.errors.HTTPException as e:
                if e.status == 429:
                    retry_after = getattr(e, 'retry_after', 5)
                    self.log(
                        f">>> [_DELM] Rate limit hit. Retrying after {retry_after:.2f} seconds for channel {channel.id}.")
                    await asyncio.sleep(retry_after + 1)
                    messages_to_retry = messages_to_delete[i:i + BATCH_SIZE]
                    if messages_to_retry:
                        await self._delete_messages(channel, [m.id for m in messages_to_retry])
                    return
                else:
                    self.log(
                        f">>> [_DELM] HTTPException: {e}. Falling back to individual deletes for batch in channel {channel.id}.")
                    await self._delete_messages_individually(channel, [m.id for m in batch], channel)
            except discord.errors.ClientException as e:
                self.log(
                    f">>> [_DELM] ClientException: {e}. Falling back to individual deletes for batch in channel {channel.id}.")
                await self._delete_messages_individually(channel, [m.id for m in batch], channel)
            except Exception as e:
                self.log(f">>> [_DELM] ERROR: Exception in _delete_messages for channel {channel.id}: {e}")
                self.log(traceback.format_exc())
        self.log(f">>> [_DELM] Finished processing delete requests for channel {channel.id}.")
    async def _delete_messages_individually(self, channel, message_ids, original_channel):
        """Attempts to delete messages one by one, used as a fallback."""
        self.log(
            f">>> [_DELMI] Entering _delete_messages_individually for channel {original_channel.id} with {len(message_ids)} messages.")
        for message_id in message_ids:
            self.log(f">>> [_DELMI] Attempting to delete message {message_id} in channel {channel.id}.")
            try:
                await channel.delete_messages([discord.Object(id=message_id)])
                await asyncio.sleep(0.4)
                self.log(f">>> [_DELMI] Deleted message {message_id} in channel {channel.id}.")
            except discord.NotFound:
                self.log(f">>> [_DELMI] Message {message_id} not found in {original_channel.id}.")
            except discord.errors.Forbidden as e:
                self.log(f">>> [_DELMI] Forbidden to delete {message_id} in {original_channel.id}: {e}")
            except discord.errors.HTTPException as e:
                self.log(f">>> [_DELMI] HTTPException deleting {message_id} in {original_channel.id}: {e}")
            except Exception as e:
                self.log(f">>> [_DELMI] ERROR: Exception in _delete_messages_individually: {e}")
                self.log(traceback.format_exc())
        self.log(f">>> [_DELMI] Finished individual delete attempts for channel {original_channel.id}.")
    async def list_files(self, interaction: discord.Interaction, DB_FILE):
        user_mention = interaction.user.mention
        if self.upload_semaphore._value < 3:
            await interaction.followup.send(
                f"{user_mention}, System is currently handling uploads. Please try listing files again later.",
                ephemeral=True)
            self.log(f">>> [LIST FILES] , NOT ALLOWED TO LIST FILES WHILE SYSTEM IS UPLOADING FILES.")
            return
        await interaction.followup.send(f"{user_mention}, Fetching file list...", ephemeral=False)
        self.log(f">>> [LIST FILES] Entering list_files command for DB_FILE: '{DB_FILE}'.")
        try:
            if not DB_FILE.lower().endswith('.pdb'):
                DB_FILE += '.pdb'
            DATABASE_FILE = os.path.abspath(os.path.normpath(DB_FILE))
            if not os.path.exists(DATABASE_FILE):
                await interaction.followup.send(f"{user_mention}, the database file '{DB_FILE}' was not found.",
                                                ephemeral=True)
                self.log(f">>> [LIST FILES] ERROR: Database file not found at '{DATABASE_FILE}'.")
                pdb.create.make_db(DATABASE_FILE)
                pdb.create.make_table(DATABASE_FILE, 'files')
                return
            filenames = set()
            try:
                total_rows = await asyncio.to_thread(pdb.table_data.totalrows, DATABASE_FILE, 0)
                for r in range(total_rows):
                    filename = await asyncio.to_thread(pdb.table_data.read, DATABASE_FILE, [0, 1, r])
                    if filename:
                        filenames.add(filename)
                if not filenames:
                    await interaction.followup.send(f'{user_mention}, no files have been uploaded yet.',
                                                    ephemeral=False)
                    self.log(f">>> [LIST FILES] INFO: No files found in database: '{DATABASE_FILE}'.")
                    return
                await interaction.followup.send(
                    f'{user_mention}, uploaded files:\n{", ".join(sorted(list(filenames)))}',
                    ephemeral=False)
                self.log(f">>> [LIST FILES] INFO: Listed {len(filenames)} files from database: '{DATABASE_FILE}'.")
            except Exception as e:
                await interaction.followup.send(
                    f"{user_mention}, an error occurred while reading the list of files: {e}",
                    ephemeral=True)
                self.log(f">>> [LIST FILES] ERROR: An error occurred while reading database: {e}")
                self.log(traceback.format_exc())
                return
        except Exception as e:
            self.log(f">>> [LIST FILES] ERROR: General exception in list_files: {e}")
            self.log(traceback.format_exc())
            await interaction.followup.send(f"{user_mention}, an unexpected error occurred while listing files: {e}",
                                            ephemeral=True)
def startbot(BOT_TOKEN):
    intents = discord.Intents.default()
    intents.message_content = True
    intents.members = True
    bot = FileBotAPI(intents=intents)
    @bot.tree.command(name="upload", description="Upload a file.")
    @app_commands.describe(local_file_path="Path of the local file to "
                                           "upload.", database_file="Database file.")
    async def upload_command(interaction: discord.Interaction, local_file_path: str, database_file: str):
        await interaction.response.defer(ephemeral=True)
        channel_id = interaction.channel_id
        bot.loop.create_task(bot._start_upload_process(interaction, local_file_path, database_file, channel_id))
    @bot.tree.command(name="download", description="Download a file.")
    @app_commands.describe(filename="Name of the file to download.", database_file="Database file.",
                           download_folder="Folder to download the file to.")
    async def download_command(interaction: discord.Interaction, filename: str, database_file: str,
                                 download_folder: str):
        await interaction.response.defer(ephemeral=True)
        await bot.download_filea(interaction, filename, database_file, download_folder)
    @bot.tree.command(name="listfiles", description="Lists files.")
    @app_commands.describe(database_file="Database file.")
    async def listfiles_command(interaction: discord.Interaction, database_file: str):
        await interaction.response.defer(ephemeral=True)
        await bot.list_files(interaction, database_file)
    @bot.tree.command(name="delete", description="Deletes a file.")
    @app_commands.describe(filename="File name.", database_file="Database file.")
    async def delete_command(interaction: discord.Interaction, filename: str, database_file: str):
        await interaction.response.defer(ephemeral=True)
        await bot.delete(interaction, filename, database_file)
    bot.run(BOT_TOKEN)