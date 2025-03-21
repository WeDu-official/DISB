import discord
from discord.ext import commands
import os
import PowerDB as PDB
import io
import shutil
def runbot(TOKEN,DATABASE_FILE,DOWNLOAD_FOLDER):
    def create_database():
        if not os.path.exists(DATABASE_FILE):
            PDB.create.makeDB(DATABASE_FILE)
        PDB.create.maketable(DATABASE_FILE,'files')
    create_database()

    if not os.path.exists(DOWNLOAD_FOLDER):
        os.makedirs(DOWNLOAD_FOLDER)

    intents = discord.Intents.default()
    intents.message_content = True
    bot = commands.Bot(command_prefix='!', intents=intents)

    @bot.event
    async def on_ready():
        print(f'Logged in as {bot.user.name}')

    async def upload_local_file(ctx, local_file_path):
        if not os.path.exists(local_file_path):
            await ctx.send("File not found.")
            return

        filename = os.path.basename(local_file_path)
        file_size = os.path.getsize(local_file_path)
        max_file_size = 9 * 1024 * 1024  # 9 MB

        if file_size <= max_file_size:
            try:
                with open(local_file_path, 'rb') as f:
                    file_bytes = f.read()
                    file_data = io.BytesIO(file_bytes)
                    message = await ctx.send(file=discord.File(file_data, filename=filename))
                    store_file_part(filename, 1, 1, message.id)
                    print(f'File "{filename}" uploaded.')
                    await ctx.send(f'File "{filename}" uploaded.')
            except FileNotFoundError:
                await ctx.send("Error: File not found during upload.")
            except Exception as e:
                await ctx.send(f"An error occurred during upload: {e}")
        else:
            num_parts = (file_size + max_file_size - 1) // max_file_size
            try:
                with open(local_file_path, 'rb') as f:
                    upload_messages = []
                    for i in range(num_parts):
                        part_data = f.read(max_file_size)
                        part_filename = f'{filename}.part{i + 1}'
                        part_file_data = io.BytesIO(part_data)
                        message = await ctx.send(file=discord.File(part_file_data, filename=part_filename))
                        store_file_part(filename, i + 1, num_parts, message.id)
                        upload_messages.append(f'Part {i + 1} of {num_parts} uploaded.')
                        print(f'Part {i + 1} of {num_parts} for file "{filename}" uploaded.')

                    await ctx.send('\n'.join(upload_messages))

            except FileNotFoundError:
                await ctx.send("Error: File not found during upload.")
            except Exception as e:
                await ctx.send(f"An error occurred during upload: {e}")

    @bot.command(name='upload')
    async def upload(ctx, local_file_path: str):
        await upload_local_file(ctx, local_file_path)

    downloading_files = set()
    processed_message_ids = set()
    @bot.command(name='download')
    async def download(ctx, filename: str):
        global processed_message_ids
        message_id = ctx.message.id
        print(f"Received download command for '{filename}' with message ID: {message_id}")

        if message_id in processed_message_ids:
            print(f"Ignoring duplicate message ID: {message_id}")
            return
        else:
            print(f"First time seeing message ID: {message_id}")

        processed_message_ids.add(message_id)

        global downloading_files
        if filename in downloading_files:
            await ctx.send(f'Download for "{filename}" is already in progress.')
            processed_message_ids.remove(message_id)
            return

        downloading_files.add(filename)

        parts_to_download = sorted(((part[2], part[3], part[4])
    for part in (PDB.table_data.readcolumns(DATABASE_FILE, [0, i])
    for i in range(PDB.table_data.numberrows(DATABASE_FILE, [0, 0], False)))
    if part[1] == filename), key=lambda x: x[0], )
        print(parts_to_download)
        if not parts_to_download:
            await ctx.send('File not found in database.')
            downloading_files.remove(filename)
            processed_message_ids.remove(message_id)
            return

        total_parts_value = int(parts_to_download[0][1]) if parts_to_download else 0 # Ensure total_parts_value is an integer
        await ctx.send(f'Attempting to download and reassemble "{filename}" locally (expecting {total_parts_value} parts)...')

        temp_dir = f'temp_download_{filename}'
        os.makedirs(temp_dir, exist_ok=True)
        reassembled = False
        parts_dict = {int(part[0]): part[2] for part in parts_to_download} # Ensure keys in parts_dict are integers

        if len(parts_dict) != total_parts_value or any(i + 1 not in parts_dict for i in range(total_parts_value)):
            await ctx.send(f"Error: Incomplete set of {total_parts_value} parts found.")
        else:
            await ctx.send(f'Found a set of {total_parts_value} parts, attempting download...')

            part_files = {}
            successful_download = True

            # Deduplicate parts_to_download
            unique_parts = []
            seen = set()
            for part_number, total_parts, message_id_to_fetch in parts_to_download:
                if (part_number, message_id_to_fetch) not in seen:
                    unique_parts.append((part_number, total_parts, message_id_to_fetch))
                    seen.add((part_number, message_id_to_fetch))

            processed_parts = set() #Track processed parts

            # Iterate through the unique parts
            for part_number, _, message_id_to_fetch in unique_parts:
                print(f"Attempting to download part {part_number} of {total_parts_value}")
                if message_id_to_fetch:
                    try:
                        print(f"Fetching message with ID: {message_id_to_fetch}")
                        message = await ctx.fetch_message(message_id_to_fetch)
                        if message.attachments:
                            attachment = message.attachments[0]
                            part_bytes = await attachment.read()
                            part_filename = os.path.join(temp_dir, f'{filename}.part_{part_number}')
                            with open(part_filename, 'wb') as f:
                                f.write(part_bytes)
                            part_files[part_number] = part_filename
                            if part_number not in processed_parts: #only send message if not processed
                                await ctx.send(f'Downloaded part {part_number} of {total_parts_value}.')
                                processed_parts.add(part_number) #add to processed parts
                        else:
                            await ctx.send(f"Error: part {part_number} (message ID: {message_id_to_fetch}) does not exist on Discord.")
                            successful_download = False
                            break
                    except discord.NotFound:
                        await ctx.send(f"Error: part {part_number} (message ID: {message_id_to_fetch}) does not exist on Discord.")
                        successful_download = False
                        break
                    except discord.HTTPException as e:
                        await ctx.send(f"Error downloading part {part_number}: {e}")
                        successful_download = False
                        break
                    except Exception as e:
                        await ctx.send(f"An error occurred while downloading part {part_number}: {e}")
                        successful_download = False
                        break
                else:
                    await ctx.send(f"Error: part {part_number} information missing in database.")
                    successful_download = False
                    break

            if successful_download and len(part_files) == total_parts_value:
                output_filepath = os.path.join(DOWNLOAD_FOLDER, filename)
                try:
                    with open(output_filepath, 'wb') as outfile:
                        for i in range(1, total_parts_value + 1):
                            part_filepath = part_files.get(str(i))
                            if part_filepath and os.path.exists(part_filepath):
                                with open(part_filepath, 'rb') as infile:
                                    outfile.write(infile.read())
                            else:
                                await ctx.send(f"Error: Could not find part {i} locally.")
                                break
                        else:
                            await ctx.send(f'File "{filename}" successfully reassembled locally in "{DOWNLOAD_FOLDER}" from {total_parts_value} parts.')
                            reassembled = True
                except Exception as e:
                    await ctx.send(f"An error occurred while combining the parts: {e}")

            # Clean up temporary part files
            for part_file in part_files.values():
                if os.path.exists(part_file):
                    os.remove(part_file)

        try:
            shutil.rmtree(temp_dir)
        except Exception as e:
            await ctx.send(f"Error removing temporary directory: {e}")

        downloading_files.remove(filename)
        processed_message_ids.remove(message_id)

    def store_file_part(filename, part_number, total_parts, message_id):
        row_id = PDB.table_data.totalrows(DATABASE_FILE, 0)
        fileid = 0 if row_id == 0 else int(PDB.table_data.read(DATABASE_FILE, [0, 0, row_id - 1]) or 0)
        for i, val in enumerate([fileid + 1, filename, part_number, total_parts, message_id]):
            PDB.table_data.insert(DATABASE_FILE, val, [0, i, row_id])

    @bot.command(name='listfiles')
    async def listfiles(ctx):
        filenames = list(dict.fromkeys(f for f in PDB.table_data.readrows(DATABASE_FILE, [0, 1]) if f))
        if not filenames:
            await ctx.send('No files uploaded.')
            return
        await ctx.send(f'Uploaded files:\n{", ".join(filenames)}')

    @bot.command(name='delete')
    async def delete(ctx, filename: str):
        parts = sorted(((part[2], part[3], part[4])
    for part in (PDB.table_data.readcolumns(DATABASE_FILE, [0, i])
    for i in range(PDB.table_data.numberrows(DATABASE_FILE, [0, 0], False)))
    if part[1] == filename), key=lambda x: x[0], )

        if not parts:
            await ctx.send('File not found.')
            return

        for part_number, total_parts, message_id in parts:
            try:
                message = await ctx.fetch_message(message_id)
                await message.delete()
            except discord.NotFound:
                pass  # message already deleted

        rows_to_delete = sorted([r for r in range(PDB.table_data.totalrows(DATABASE_FILE, 0)) if
                                 PDB.table_data.read(DATABASE_FILE, [0, 1, r]) == filename], reverse=True)
        [PDB.table_data.delete(DATABASE_FILE, [0, c, r]) for r in rows_to_delete for c in
         range(PDB.table_data.totalcolumns(DATABASE_FILE, 0))]

        await ctx.send(f'File "{filename}" and its parts deleted.')

    bot.run(TOKEN)