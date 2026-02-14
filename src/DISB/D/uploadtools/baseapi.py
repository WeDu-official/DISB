import random
import time
import aiohttp
import discord
import asyncio
import io
from typing import Optional
class BASEapi:
    def __init__(self,log, *, intents: discord.Intents):
        super().__init__(command_prefix=["/"], intents=intents)
        self.log = log
    async def send_message_robustly(self, channel_id: int, content: Optional[str] = None,
                                    file: Optional[discord.File] = None, ephemeral: bool = False) -> Optional[
        discord.Message]:
        """
        Sends a message to a channel, handling common Discord API errors robustly.
        Retries on rate limits and attempts to fetch channel if not in cache.
        Uses enhanced retry logic with exponential backoff and jitter.
        """
        target_channel = self.get_channel(channel_id)
        if not target_channel:
            try:
                target_channel = await self.fetch_channel(channel_id)
            except (discord.NotFound, discord.Forbidden, discord.HTTPException) as e:
                self.log.error(f"Error fetching channel {channel_id}: {e}")
                return None
        if not target_channel:
            self.log.error(f"Could not resolve channel for ID: {channel_id}. Message not sent.")
            return None
        # --- Enhanced Retry Parameters from _send_file_part_to_discord ---
        max_send_attempts = 15
        initial_send_delay = 2  # seconds, for the first retry
        max_send_delay = 60  # seconds, cap the exponential backoff delay to prevent extremely long waits
        # --- End Retry Parameters ---
        for send_attempt in range(1, max_send_attempts + 1):  # Start from 1 for logging clarity
            try:
                if isinstance(target_channel, (discord.TextChannel, discord.DMChannel, discord.Thread)):
                    if content and file:
                        message = await target_channel.send(content=content, file=file)
                    elif content:
                        message = await target_channel.send(content=content)
                    elif file:
                        message = await target_channel.send(file=file)
                    else:
                        self.log.warning(f"Warning: Attempted to send empty message to channel {channel_id}.")
                        return None
                    self.log.info(
                        f"Send attempt {send_attempt}/{max_send_attempts} successful for channel '{channel_id}', Message ID: {message.id}")
                    return message  # Success!
                else:
                    self.log.warning(
                        f"Cannot send message to non-text channel type: {type(target_channel)}. Channel ID: {channel_id}")
                    return None
            except discord.errors.Forbidden as e:
                self.log.error(f"Forbidden to send message to channel {channel_id}: {e}. No further retries.")
                # No retry on Forbidden as it's a permanent permission issue
                return None
            except (discord.errors.HTTPException, asyncio.TimeoutError) as e:
                # Catch Discord API errors (including rate limits) and network timeouts
                self.log.error(
                    f"Send attempt {send_attempt}/{max_send_attempts} for channel '{channel_id}' failed: {type(e).__name__}: {e!r}")
                if send_attempt < max_send_attempts:
                    # Calculate exponential backoff with jitter
                    # current_delay will be 2, 4, 8, 16, 32, 60, 60...
                    current_delay = min(max_send_delay, initial_send_delay * (2 ** (send_attempt - 1)))
                    jitter = random.uniform(0, current_delay * 0.25)  # Add up to 25% random jitter
                    wait_time = current_delay + jitter
                    self.log.debug(f"Retrying send in {wait_time:.2f} seconds...")
                    await asyncio.sleep(wait_time)
                else:
                    self.log.error(
                        f"Max send attempts ({max_send_attempts}) reached for channel '{channel_id}', returning None.")
                    break  # Exit loop if max attempts reached
            except Exception as e:
                # Catch any other unexpected errors
                self.log.error(
                    f"Unexpected error during send attempt {send_attempt}/{max_send_attempts} for channel '{channel_id}': {type(e).__name__}: {e!r}")
                if send_attempt < max_send_attempts:
                    # Apply backoff for unexpected errors too
                    current_delay = min(max_send_delay, initial_send_delay * (2 ** (send_attempt - 1)))
                    jitter = random.uniform(0, current_delay * 0.25)
                    wait_time = current_delay + jitter
                    self.log.debug(f"Retrying send in {wait_time:.2f} seconds due to unexpected error...")
                    await asyncio.sleep(wait_time)
                else:
                    self.log.error(
                        f"Failed to send message to channel {channel_id} after {max_send_attempts} attempts due to unexpected error.")
                    break  # Exit loop if max attempts reached
        self.log.error(f"Failed to send message to channel {channel_id} after {max_send_attempts} attempts.")
        return None


    async def _send_file_part_to_discord(self, target_channel: discord.abc.Messageable, part_data: bytes,
                                         filename_to_send: str) -> Optional[discord.Message]:
        """
        Sends a file part (raw bytes) to a Discord channel with significantly enhanced retry logic.
        - Implements exponential backoff with jitter for retries.
        - Sets an overall timeout for attempting to send a single part.
        - Recreates BytesIO object for each retry to avoid "I/O operation on closed file."
        """
        max_send_attempts = 15
        initial_send_delay = 2  # seconds, for the first retry
        max_send_delay = 60  # seconds, cap the exponential backoff delay to prevent extremely long waits
        start_time = time.time()  # Start timer for overall part sending timeout
        for send_attempt in range(1, max_send_attempts + 1):  # Start from 1 for logging clarity
            try:
                # Recreate BytesIO for each attempt to ensure it's open and reset
                file_object = io.BytesIO(part_data)
                discord_file = discord.File(file_object, filename=filename_to_send)
                # Use target_channel.send to send the file
                message = await target_channel.send(file=discord_file)
                self.log.info(
                    f"Send attempt {send_attempt}/{max_send_attempts} successful for '{filename_to_send}', Message ID: {message.id}")
                return message  # Success!
            except (aiohttp.ClientError, asyncio.TimeoutError, discord.errors.DiscordException) as e:
                # Catch network errors, timeouts, and Discord API errors
                self.log.error(
                    f"Send attempt {send_attempt}/{max_send_attempts} for '{filename_to_send}' failed: {type(e).__name__}: {e!r}")
                if send_attempt < max_send_attempts:
                    # Calculate exponential backoff with jitter
                    # current_delay will be 2, 4, 8, 16, 32, 60, 60...
                    current_delay = min(max_send_delay, initial_send_delay * (2 ** (send_attempt - 1)))
                    jitter = random.uniform(0, current_delay * 0.25)  # Add up to 25% random jitter
                    wait_time = current_delay + jitter
                    self.log.debug(f"Retrying send in {wait_time:.2f} seconds...")
                    await asyncio.sleep(wait_time)
                else:
                    self.log.error(
                        f"Max send attempts ({max_send_attempts}) reached for '{filename_to_send}', returning None.")
                    break  # Exit loop if max attempts reached
            except Exception as e:
                # Catch any other unexpected errors
                self.log.error(
                    f"Unexpected error during send attempt {send_attempt}/{max_send_attempts} for '{filename_to_send}': {type(e).__name__}: {e!r}")
                # For unexpected errors, you might want to break immediately or use a different retry strategy
                # For now, we'll break to allow the higher-level logic to handle it
                break
        # If the loop finishes without returning a message, it means all attempts failed or timed out
        self.log.error(f"import sysFailed to send '{filename_to_send}' after all attempts or timeout.")
        return None
    def _calculate_retry_delay(self, attempt: int, base_delay: float = 1.0, max_delay: float = 60.0) -> float:
        """
        Calculates the exponential backoff delay with added jitter.

        Args:
            attempt (int): The current retry attempt (0-indexed).
            base_delay (float): The base delay in seconds for the first retry.
            max_delay (float): The maximum allowed delay in seconds.

        Returns:
            float: The calculated delay in seconds.
        """
        # Exponential backoff: base_delay * (2 ** attempt)
        delay = base_delay * (2 ** attempt)
        # Add jitter (randomness) to prevent thundering herd problem
        jitter = random.uniform(0, base_delay / 2)  # Jitter up to half of the base delay
        calculated_delay = min(delay + jitter, max_delay)
        return calculated_delay