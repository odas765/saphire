import os
import re
import shutil
import subprocess
from urllib.parse import urlparse
from telethon import TelegramClient, events, Button
from mutagen import File

# Set up your MTProto API credentials (API ID and hash from Telegram's Developer Portal)
api_id = '10074048'
api_hash = 'a08b1ed3365fa3b04bcf2bcbf71aff4d'
session_name = 'beatport_downloader'

# Regular expressions for Beatport and Crates.co URLs
beatport_pattern = '^https:\/\/www\.beatport\.com\/track\/[\w -]+\/\d+$'
crates_pattern = '^https:\/\/crates\.co\/track\/[\w -]+\/\d+$'

# Dictionary to store temporary states for each user
state = {}

# Initialize the client
client = TelegramClient(session_name, api_id, api_hash)

# Start the client and listen for new messages
@client.on(events.NewMessage(pattern='/start'))
async def start_handler(event):
    await event.reply("Hi! I'm Beatport Track Downloader using MTProto API.\n\n"
                      "Commands:\n"
                      "/download <track_url> - Download a track from Beatport or Crates.co.\n\n"
                      "Example:\n"
                      "/download https://www.beatport.com/track/take-me/17038421\n"
                      "/download https://crates.co/track/take-me/17038421")

@client.on(events.NewMessage(pattern='/download'))
async def download_handler(event):
    try:
        input_text = event.message.text.split(maxsplit=1)[1]
        
        # Validate the track URL against Beatport and Crates.co patterns
        is_beatport = re.match(rf'{beatport_pattern}', input_text)
        is_crates = re.match(rf'{crates_pattern}', input_text)

        if is_beatport or is_crates:
            # Convert Crates.co link to Beatport link if necessary
            if is_crates:
                input_text = input_text.replace('crates.co', 'www.beatport.com')

            # Save the input URL in the state dictionary for this user
            state[event.chat_id] = input_text

            await event.reply("Please choose the format:", buttons=[
                [Button.inline("FLAC (16 Bit)", b"flac"), Button.inline("MP3 (320K)", b"mp3")]
            ])
        else:
            await event.reply('Invalid track link.\nPlease enter a valid track link.')
    except Exception as e:
        await event.reply(f"An error occurred: {e}")

@client.on(events.CallbackQuery)
async def callback_query_handler(event):
    try:
        # Get the selected format
        format_choice = event.data.decode('utf-8')

        # Retrieve the input URL from the state dictionary
        input_text = state.get(event.chat_id)
        if not input_text:
            await event.edit("No URL found. Please start the process again using /download.")
            return

        # Confirm the user's choice and remove the buttons
        await event.edit(f"You selected {format_choice.upper()}. Downloading the file...")

        url = urlparse(input_text)
        components = url.path.split('/')

        # Run the orpheus script to download the track
        os.system(f'python orpheus.py {input_text}')

        # Get the downloaded filename
        download_dir = f'downloads/{components[-1]}'
        filename = os.listdir(download_dir)[0]
        filepath = f'{download_dir}/{filename}'

        # Convert the downloaded file to the chosen format
        converted_filepath = f'{download_dir}/{filename}.{format_choice}'
        if format_choice == 'flac':
            subprocess.run(['ffmpeg', '-i', filepath, converted_filepath])
        elif format_choice == 'mp3':
            subprocess.run(['ffmpeg', '-i', filepath, '-b:a', '320k', converted_filepath])

        # Extract metadata using mutagen
        audio = File(converted_filepath, easy=True)
        artist = audio.get('artist', ['Unknown Artist'])[0]
        title = audio.get('title', ['Unknown Title'])[0]

        # Clean semicolons in metadata fields
        for field in ['artist', 'title', 'album', 'genre']:
            if field in audio:
                audio[field] = [value.replace(";", ", ") for value in audio[field]]
        audio.save()

        # Create the new filename based on artist and title
        new_filename = f"{artist} - {title}.{format_choice}"

        # Replace ";" with ", " in the filename
        new_filename = new_filename.replace(";", ", ")

        new_filepath = f'{download_dir}/{new_filename}'

        # Rename the converted file
        os.rename(converted_filepath, new_filepath)

        # Send the renamed file to the user
        await client.send_file(event.chat_id, new_filepath)

        # Clean up the downloaded files
        shutil.rmtree(download_dir)

        # Clear the state for the user
        del state[event.chat_id]
    except Exception as e:
        await event.reply(f"An error occurred during conversion: {e}")

async def main():
    # Start the Telegram client
    async with client:
        print("Client is running...")
        await client.run_until_disconnected()

if __name__ == '__main__':
    client.loop.run_until_complete(main())
