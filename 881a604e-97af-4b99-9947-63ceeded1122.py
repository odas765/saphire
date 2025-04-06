import os
import re
import shutil
import subprocess
import json
import datetime
from urllib.parse import urlparse
from telethon import TelegramClient, events, Button
from mutagen import File

# Set up your MTProto API credentials (API ID and hash from Telegram's Developer Portal)
api_id = '10074048'
api_hash = 'a08b1ed3365fa3b04bcf2bcbf71aff4d'
session_name = 'beatport_downloader'

# Replace this with your own Telegram user ID
YOUR_USER_ID = 616584208

# Regular expressions for Beatport and Crates.co URLs
beatport_pattern = '^https:\/\/www\.beatport\.com\/track\/[\w -]+\/\d+$'
crates_pattern = '^https:\/\/crates\.co\/track\/[\w -]+\/\d+$'

# Temporary user states
state = {}

# Authorized users file
AUTHORIZED_USERS_FILE = 'authorized_users.json'

# Load/save authorized users
def load_authorized_users():
    try:
        with open(AUTHORIZED_USERS_FILE, 'r') as f:
            data = json.load(f)
            return {int(k): datetime.datetime.fromisoformat(v) for k, v in data.items()}
    except FileNotFoundError:
        return {}
    except Exception as e:
        print(f"Error loading authorized users: {e}")
        return {}

def save_authorized_users():
    try:
        with open(AUTHORIZED_USERS_FILE, 'w') as f:
            data = {str(k): v.isoformat() for k, v in authorized_users.items()}
            json.dump(data, f, indent=2)
    except Exception as e:
        print(f"Error saving authorized users: {e}")

# Initialize client and users
client = TelegramClient(session_name, api_id, api_hash)
authorized_users = load_authorized_users()

# Handlers
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
        # Check if user is authorized
        now = datetime.datetime.utcnow()
        expiry = authorized_users.get(event.sender_id)
        if not expiry or expiry < now:
            await event.reply("You're not authorized to use this bot or your access has expired.")
            return

        input_text = event.message.text.split(maxsplit=1)[1]
        is_beatport = re.match(rf'{beatport_pattern}', input_text)
        is_crates = re.match(rf'{crates_pattern}', input_text)

        if is_beatport or is_crates:
            if is_crates:
                input_text = input_text.replace('crates.co', 'www.beatport.com')

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
        format_choice = event.data.decode('utf-8')
        input_text = state.get(event.chat_id)
        if not input_text:
            await event.edit("No URL found. Please start the process again using /download.")
            return

        await event.edit(f"You selected {format_choice.upper()}. Downloading the file...")

        url = urlparse(input_text)
        components = url.path.split('/')
        os.system(f'python orpheus.py {input_text}')

        download_dir = f'downloads/{components[-1]}'
        filename = os.listdir(download_dir)[0]
        filepath = f'{download_dir}/{filename}'

        converted_filepath = f'{download_dir}/{filename}.{format_choice}'
        if format_choice == 'flac':
            subprocess.run(['ffmpeg', '-i', filepath, converted_filepath])
        elif format_choice == 'mp3':
            subprocess.run(['ffmpeg', '-i', filepath, '-b:a', '320k', converted_filepath])

        audio = File(converted_filepath, easy=True)
        artist = audio.get('artist', ['Unknown Artist'])[0]
        title = audio.get('title', ['Unknown Title'])[0]

        for field in ['artist', 'title', 'album', 'genre']:
            if field in audio:
                audio[field] = [value.replace(";", ", ") for value in audio[field]]
        audio.save()

        new_filename = f"{artist} - {title}.{format_choice}".replace(";", ", ")
        new_filepath = f'{download_dir}/{new_filename}'

        os.rename(converted_filepath, new_filepath)
        await client.send_file(event.chat_id, new_filepath)

        shutil.rmtree(download_dir)
        del state[event.chat_id]
    except Exception as e:
        await event.reply(f"An error occurred during conversion: {e}")

@client.on(events.NewMessage(pattern='/add'))
async def add_user(event):
    try:
        if event.sender_id != YOUR_USER_ID:
            await event.reply("You don't have permission to use this command.")
            return

        if len(event.message.text.split()) < 2:
            await event.reply("Usage: /add <user_id>")
            return

        user_id = int(event.message.text.split()[1])
        expiry_date = datetime.datetime.utcnow() + datetime.timedelta(days=30)
        authorized_users[user_id] = expiry_date
        save_authorized_users()
        await event.reply(f"User {user_id} added with access until {expiry_date.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    except Exception as e:
        await event.reply(f"Error adding user: {e}")

@client.on(events.NewMessage(pattern='/remove'))
async def remove_user(event):
    try:
        if event.sender_id != YOUR_USER_ID:
            await event.reply("You don't have permission to use this command.")
            return

        if len(event.message.text.split()) < 2:
            await event.reply("Usage: /remove <user_id>")
            return

        user_id = int(event.message.text.split()[1])
        if user_id in authorized_users:
            del authorized_users[user_id]
            save_authorized_users()
            await event.reply(f"User {user_id} has been removed.")
        else:
            await event.reply("User not found in the authorized list.")
    except Exception as e:
        await event.reply(f"Error removing user: {e}")

@client.on(events.NewMessage(pattern='/list_users'))
async def list_users(event):
    try:
        if event.sender_id != YOUR_USER_ID:
            await event.reply("You don't have permission to use this command.")
            return

        if not authorized_users:
            await event.reply("No users are currently authorized.")
            return

        lines = ["**Authorized Users:**"]
        now = datetime.datetime.utcnow()
        for uid, expiry in authorized_users.items():
            status = "Active" if expiry > now else "Expired"
            lines.append(f"User ID: `{uid}` - Expires: `{expiry.strftime('%Y-%m-%d %H:%M:%S')}` UTC - {status}")

        await event.reply('\n'.join(lines), parse_mode='markdown')
    except Exception as e:
        await event.reply(f"Error listing users: {e}")

async def main():
    global authorized_users
    authorized_users = load_authorized_users()
    async with client:
        print("Client is running...")
        await client.run_until_disconnected()

if __name__ == '__main__':
    client.loop.run_until_complete(main())
