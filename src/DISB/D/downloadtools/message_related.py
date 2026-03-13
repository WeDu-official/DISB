import asyncio
import logging
import re
import traceback
from typing import Optional, Dict, List
import aiohttp
import discord
from cryptography.fernet import InvalidToken
from downloadtools.encryption_base import benc
class MRD:
    def __init__(self,baseapi):
        self._HKDF_SALT = b"my_bot_encryption_salt_12345"
        self._HKDF_INFO = b"discord_file_bot_encryption_info"
        self.log = logging.getLogger('MSG')
        self.ba=baseapi
        self.e = benc(self._HKDF_SALT,self._HKDF_INFO,self.log)
    async def _get_file_parts_from_discord(self, channel_id: int, message_ids: List[int],
                                           encryption_key: Optional[bytes] = None) -> Dict[int, bytes]:
        """
        Fetches and optionally decrypts file parts from Discord messages with robust retry logic.
        - Implements exponential backoff with jitter for retries.
        - Attempts to fetch each message part multiple times.
        """
        part_data = {}
        max_download_attempts = 15
        initial_download_delay = 1.0  # seconds, for the first retry
        max_download_delay = 60.0  # seconds, cap the exponential backoff delay

        for msg_id in message_ids:
            filename = ""
            for attempt in range(max_download_attempts):
                try:
                    channel = self.get_channel(channel_id) or await self.fetch_channel(channel_id)
                    if not isinstance(channel, (discord.TextChannel, discord.DMChannel, discord.Thread)):
                        self.log.warning(
                            f"Channel {channel_id} is not a text-based channel. Cannot fetch message {msg_id}. Skipping part.")
                        break  # No point retrying if channel type is wrong

                    message = await channel.fetch_message(msg_id)
                    if not message.attachments:
                        self.log.warning(f"Message {msg_id} has no attachments. Skipping this part.")
                        break  # No attachments, no point retrying

                    attachment = message.attachments[0]
                    downloaded_bytes = await attachment.read()

                    # --- Decryption ---
                    if encryption_key is not None:
                        try:
                            self.log.debug(
                                f"DEBUG: Key being passed to _decrypt_data: '{encryption_key}' (Type: {type(encryption_key)})")
                            downloaded_bytes = self._decrypt_data(downloaded_bytes, encryption_key)
                        except (InvalidToken, ValueError) as e:  # Catch specific decryption errors
                            self.log.warning(f"Decryption failed for message {msg_id}: {e}. Skipping this part.")
                            break  # Decryption failed, no point retrying this part
                        except Exception as e:
                            self.log.error(
                                f"Unexpected error during decryption for message {msg_id}: {e}. Skipping this part.")
                            self.log.error(traceback.format_exc())
                            break  # Unexpected decryption error, stop retrying

                    # Extract part number from filename (e.g., 'filename.part001')
                    part_num = 0  # Default to 0 if not found
                    try:
                        filename = attachment.filename
                        match = re.search(r'\.part(\d+)', filename)
                        if match:
                            part_num = int(match.group(1))
                        else:
                            self.log.warning(
                                f"WARNING: Could not extract part number from filename '{filename}' for message {msg_id}. Defaulting to 0.")
                    except ValueError:
                        self.log.warning(
                            f"WARNING: Could not parse part number from filename '{filename}' for message {msg_id}. Defaulting to 0.")

                    part_data[part_num] = downloaded_bytes
                    self.log.debug(f"Fetched part {part_num} from message {msg_id} on attempt {attempt + 1}.")
                    break  # Successfully downloaded, move to next message_id

                except (discord.NotFound, discord.Forbidden) as e:
                    self.log.warning(
                        f"Message {msg_id} in channel {channel_id} not found or forbidden on attempt {attempt + 1}/{max_download_attempts}: {e}. Skipping this part permanently.")
                    break  # These are typically non-retryable errors
                except (discord.HTTPException, asyncio.TimeoutError, aiohttp.ClientError) as e:
                    # Catch Discord API errors, network timeouts, and aiohttp client errors
                    self.log.warning(
                        f"Failed to download part (Message ID: {msg_id}) "
                        f"on attempt {attempt + 1}/{max_download_attempts} due to: {e}. "
                        f"Retrying..."
                    )
                    if attempt < max_download_attempts - 1:  # Only calculate delay if more attempts are left
                        retry_delay = self.ba._calculate_retry_delay(attempt, initial_download_delay, max_download_delay)
                        self.log.debug(f"Retrying download in {retry_delay:.2f} seconds...")
                        await asyncio.sleep(retry_delay)
                    else:
                        self.log.error(
                            f"Failed to download part (Message ID: {msg_id}) "
                            f"after {max_download_attempts} attempts. This part will be missing."
                        )
                        break  # Max attempts reached for this part
                except Exception as e:
                    self.log.error(
                        f"An unexpected error occurred while fetching message {msg_id} on attempt {attempt + 1}/{max_download_attempts}: {type(e).__name__}: {e!r}. No further retries for this part due to unexpected error.")
                    self.log.error(traceback.format_exc())
                    break  # Break out of retry loop for unexpected errors
            else:  # This else block executes if the inner loop completes without a 'break'
                self.log.error(
                    f"Failed to download part (Message ID: {msg_id}) after {max_download_attempts} attempts. This part will be missing (loop completed).")
        return part_data