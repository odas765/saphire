import os
import re
import shutil
import asyncio
import logging
import json
import subprocess
import uuid
import datetime
import mutagen
from io import BytesIO
from telethon import TelegramClient, events, Button
from telethon.tl.types import DocumentAttributeAudio
from datetime import datetime, timedelta, timezone


# --- CONFIG ---
API_ID = 8349121
API_HASH = "9709d9b8c6c1aa3dd50107f97bb9aba6"
BOT_TOKEN = "7658651511:AAHyiRQ5Bz_tkVDr7wh6PEdA4vMQpdMjx88"
ADMIN_ID = 616584208  # Piklu's Telegram ID

BEATPORTDL_DIR = "/home/ubuntu/hi"
DOWNLOADS_DIR = os.path.join(BEATPORTDL_DIR, "downloads")
BANNER_PATH = os.path.join(BEATPORTDL_DIR, "banner.jpg")
USERS_FILE = os.path.join(BEATPORTDL_DIR, "users.json")

# --- LOGGING ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- TELETHON CLIENT ---
bot = TelegramClient("beatsource_bot", API_ID, API_HASH).start(bot_token=BOT_TOKEN)

# --- REGEX PATTERN FOR BEATSOURCE ---
attern = r"^https:\/\/www\.beatsource\.com\/(track|release)\/[\w\-\+]+\/\d+$"
attern = r"^https:\/\/www\.(beatport|beatsource)\.com\/(track|release)\/[\w\-\+]+\/\d+$"
attern = r"^https:\/\/www\.(beatport|beatsource)\.com\/(track|release|playlists\/share)\/[\w\-\+]+\/?\d*$"
patern = r"^https:\/\/www\.(beatport|beatsource)\.com\/(track|release|playlists\/share|chart)\/[\w\-\+]+\/?\d*$"
pattern = r"^https:\/\/www\.(beatport|beatsource)\.com(\/[a-z]{2})?\/(track|release|playlists\/share|chart)\/[\w\-\+]+\/?\d*(\?.*)?$"


user_format_choice = {}  # Stores user format choices (mp3/flac)
pending_links = {}  # Temporary storage for user links to prevent data overflow
active_downloads = {}
queued_users = set()
# --- DOWNLOAD QUEUE (single-worker to avoid cross-user file races) ---
download_queue = asyncio.Queue()
queue_worker_task = None  # will hold the worker task

# --- LOAD USER DATABASE ---
def load_users():
    if os.path.exists(USERS_FILE):
        with open(USERS_FILE, "r") as f:
            return json.load(f)
    return {}

def save_users():
    with open(USERS_FILE, "w") as f:
        json.dump(users, f, indent=4)

users = load_users()

# --- RESET USAGE IF NEW DAY ---
def reset_if_new_day(user):
    today = str(datetime.today().date())
    if user.get("last_reset") != today:
        user["track_downloads"] = 0
        user["album_downloads"] = 0
        user["last_reset"] = today
        save_users()

# --- PREMIUM CHECKER ---
def is_premium(user):
    if not user.get("premium"):
        return False
    expiry = user.get("expiry")
    if expiry and datetime.today().date() > datetime.fromisoformat(expiry).date():
        user["premium"] = False
        user["expiry"] = None
        save_users()
        return False
    return True


# --- START / HELP ---
@bot.on(events.NewMessage(pattern=r'^/(start|help)$'))
async def start_handler(event):
    user_id = str(event.sender_id)
    username = event.sender.username or "N/A"

    # Save or update user info
    if user_id not in users:
        users[user_id] = {
            "premium": False,
            "expiry": None,
            "last_reset": str(datetime.today().date()),
            "track_downloads": 0,
            "album_downloads": 0,
            "username": username
        }
    else:
        users[user_id]["username"] = username
    save_users()

    caption = (
        "🎧 **Hey DJ! 🎶**\n\n"
        "Welcome to **Beatport & Beatsource Downloader Bot** – your assistant for downloading full tracks, albums, playlists & charts.\n\n"
        "❓ **What I Can Do:**\n\n"
        "**⚡ Beatport Features:**\n"
        "🎵 Download original-quality Beatport releases\n"
        "💽 Send you organized, tagged audio files\n\n"
        "**🎶 Beatsource Features:**\n"
        "🎧 DJ Edits (Clean / Dirty versions)\n"
        "🥁 Intro / Outro edits\n"
        "📡 Short / Extended mixes\n"
        "🎹 Instrumentals / Acapellas (if available)\n"
        "🧩 Curated DJ packs (genre or label-based bundles)\n\n"
        "📋 **Commands:**\n"
        "➤ /myaccount – Check your daily usage\n"
        "➤ /updates – See latest bot updates\n\n"
        "🚀 Paste a Beatport or Beatsource link now and let’s get those bangers!\n\n"
        "**Examples:**\n"
        "`https://www.beatsource.com/track/...`\n"
        "`https://www.beatport.com/track/...`"
    )

    buttons = [
        [
            Button.url("🔮 Support", "https://ko-fi.com/zackant"),
            Button.url("📨 Contact", "https://t.me/zackantdev"),
        ],
        [
            Button.url("📢 Join Channel", "https://t.me/+UsTE5Ufq1W4wOWE1"),
        ],
    ]

    # Send GIF if exists, else fallback to text
    banner_path = "/home/ubuntu/hi/banner.gif"
    if os.path.exists(banner_path):
        await bot.send_file(
            event.chat_id,
            file=banner_path,
            caption=caption,
            buttons=buttons,
            parse_mode="markdown"
        )
    else:
        await event.reply(caption, buttons=buttons, parse_mode="markdown")
@bot.on(events.NewMessage(pattern=r"^/download (https?://www\.(beatport|beatsource)\.com/.+)$"))
async def download_prefix_handler(event):
    # Extract the link
    link = event.pattern_match.group(1).strip()

    # Reuse the same format selection logic
    await format_selection_handler(event)  # Pass the event directly
    
# --- /MYACCOUNT COMMAND ---
@bot.on(events.NewMessage(pattern=r"^/myaccount$"))
async def myaccount_handler(event):
    user_id = str(event.sender_id)
    users.setdefault(user_id, {
        "premium": False,
        "expiry": None,
        "last_reset": str(datetime.today().date()),
        "track_downloads": 0,
        "album_downloads": 0
    })
    user = users[user_id]
    reset_if_new_day(user)

    if is_premium(user):
        expire_date = user.get("expiry", "N/A")
        message = (
            f"✨ **Premium User**\n\n"
            f"✅ Unlimited downloads — no daily limits!\n"
            f"📆 **Expires:** {expire_date}\n\n"
            f"💟 Thank you for supporting the project!"
        )
        buttons = None
    else:
        track_used = user.get("track_downloads", 0)
        album_used = user.get("album_downloads", 0)
        message = (
            f"🎧 **Free User**\n\n"
            f"🎵 **Tracks used today:** {track_used}/2\n"
            f"💿 **Albums used today:** {album_used}/2\n"
            f"📅 **Reset every 24 hours**\n\n"
            f"🔥 Upgrade to **Premium ($5)** for unlimited downloads and send payment proof to @zackantdev"
        )
        buttons = [
            [Button.url("💳 Pay $5 Here", "https://ko-fi.com/zackant")],
        ]

    await event.reply(message, buttons=buttons, parse_mode="markdown")

# --- /ADD PREMIUM COMMAND ---
@bot.on(events.NewMessage(pattern=r"^/add (\d+)(?: (\d+))?$"))
async def add_premium_handler(event):
    if event.sender_id != ADMIN_ID:
        await event.reply("⛔ You are not authorized to use this command.")
        return

    args = event.pattern_match.groups()
    user_id = args[0]
    days = int(args[1]) if args[1] else 30

    expiry_date = (datetime.today() + timedelta(days=days)).date().isoformat()

    users.setdefault(user_id, {})
    users[user_id]["premium"] = True
    users[user_id]["expiry"] = expiry_date
    users[user_id]["ever_premium"] = True  # Track users who once bought Premium
    save_users()

    await event.reply(
        f"✅ Premium activated for user `{user_id}` for **{days} days**.\n"
        f"📆 Expires on: {expiry_date}",
        parse_mode="markdown"
    )

    # Notify the user privately if possible
    try:
        await bot.send_message(
            int(user_id),
            f"🎉 You’ve been upgraded to **Premium** for {days} days!\n"
            f"Expires on: `{expiry_date}`"
        )
    except Exception:
        pass


# --- /REMOVE PREMIUM COMMAND ---
@bot.on(events.NewMessage(pattern=r"^/remove (\d+)$"))
async def remove_premium_handler(event):
    if event.sender_id != ADMIN_ID:
        await event.reply("⛔ You are not authorized to use this command.")
        return

    user_id = event.pattern_match.group(1)
    if user_id in users:
        users[user_id]["premium"] = False
        users[user_id]["expiry"] = None
        save_users()
        await event.reply(f"🧹 Premium removed from user `{user_id}`.", parse_mode="markdown")
    else:
        await event.reply("⚠️ User not found in database.")


# --- /REMINDER COMMAND ---
@bot.on(events.NewMessage(pattern=r"^/reminder$"))
async def reminder_handler(event):
    if event.sender_id != ADMIN_ID:
        await event.reply("⛔ You are not authorized to use this command.")
        return

    today = datetime.today().date()
    expired_users = []

    for user_id, data in users.items():
        ever_premium = data.get("ever_premium", False)
        premium_status = data.get("premium", False)
        expiry_str = data.get("expiry")

        # ✅ Skip users who never purchased premium
        if not ever_premium:
            continue

        # ✅ Include users with expired or missing expiry date who are not premium now
        expired = False
        if not premium_status:
            if expiry_str:
                try:
                    expiry_date = datetime.strptime(expiry_str, "%Y-%m-%d").date()
                    if expiry_date < today:
                        expired = True
                except ValueError:
                    expired = True  # invalid date, treat as expired
            else:
                expired = True  # no expiry date, treat as expired

        if expired:
            expired_users.append(int(user_id))

    if not expired_users:
        await event.reply("✅ No expired Premium users found who need reminders.")
        return

    await event.reply(f"📬 Sending renewal reminders to {len(expired_users)} expired users...")

    reminder_text = (
        "😢 **Hey there, music lover!**\n\n"
        "Your **Premium access has expired** 💔\n"
        "We miss having you in our VIP zone!\n\n"
        "Renew Premium and enjoy **unlimited track & album downloads** again 🎶🔥\n\n"
      
    )

    sent_count = 0
    for uid in expired_users:
        try:
            await bot.send_message(
                uid,
                reminder_text,
                parse_mode="markdown",
                buttons=[Button.url("💳 Renew Premium", "https://ko-fi.com/zackant")]
            )
            sent_count += 1
            await asyncio.sleep(1)
        except Exception as e:
            print(f"⚠️ Could not send reminder to {uid}: {e}")
            continue

    await event.reply(f"✅ Reminder sent to **{sent_count}** expired Premium users successfully.")



# --- METADATA CARD WITH COVER, GENRE & CATALOG ---
def format_metadata_card_with_cover(file_path, is_album=False, is_playlist=False, is_chart=False):
    try:
        from mutagen.id3 import ID3, TXXX
        from mutagen import File as MutagenFile
        from io import BytesIO
        import os
        from datetime import datetime

        # --- Load audio ---
        audio_easy = MutagenFile(file_path, easy=True)
        audio_id3 = None
        try:
            audio_id3 = ID3(file_path)
        except Exception:
            pass

        if not audio_easy:
            return None, None

        folder_name = os.path.basename(os.path.dirname(file_path))
        album = audio_easy.get("album", ["Unknown Album"])[0]
        key = audio_easy.get("initialkey", ["N/A"])[0] if "initialkey" in audio_easy else audio_easy.get("key", ["N/A"])[0]

        # --- Function to get catalog from ID3 TXXX ---
        def get_catalog_number(path):
            try:
                tags = ID3(path)
                for frame in tags.getall("TXXX"):
                    if frame.desc.upper() == "CATALOGNUMBER":
                        return frame.text[0]
            except Exception:
                pass
            return "N/A"

        catalog = get_catalog_number(file_path)

        # --- Collect metadata from all tracks in folder (for albums/playlists/charts) ---
        all_bpms = []
        all_dates = []
        artist_list = set()
        label_list = set()
        genre_list = set()
        catalog_list = set()

        parent_folder = os.path.dirname(file_path)
        for root, _, files in os.walk(parent_folder):
            for f in files:
                if f.lower().endswith(('.flac', '.mp3', '.wav')):
                    try:
                        track_path = os.path.join(root, f)
                        track_audio_easy = MutagenFile(track_path, easy=True)
                        track_audio_id3 = None
                        try:
                            track_audio_id3 = ID3(track_path)
                        except Exception:
                            pass

                        if not track_audio_easy:
                            continue

                        # BPM
                        if "bpm" in track_audio_easy:
                            try:
                                all_bpms.append(float(track_audio_easy["bpm"][0]))
                            except:
                                pass

                        # Date
                        if "date" in track_audio_easy:
                            all_dates.append(track_audio_easy["date"][0])

                        # Artist
                        if "artist" in track_audio_easy:
                            artist_list.update(track_audio_easy["artist"])

                        # Label
                        if "label" in track_audio_easy:
                            label_list.update(track_audio_easy["label"])

                        # Genre
                        if "genre" in track_audio_easy:
                            genre_list.update(track_audio_easy["genre"])

                        # Catalog
                        if track_audio_id3:
                            for frame in track_audio_id3.getall("TXXX"):
                                if frame.desc.upper() == "CATALOGNUMBER":
                                    catalog_list.add(frame.text[0])
                    except Exception:
                        continue

        # --- BPM Range ---
        bpm_range = "N/A"
        if all_bpms:
            bpm_min = int(min(all_bpms))
            bpm_max = int(max(all_bpms))
            bpm_range = f"{bpm_min}" if bpm_min == bpm_max else f"{bpm_min}–{bpm_max}"

        # --- Release Date Range ---
        release_range = "Unknown"
        valid_dates = []
        for d in all_dates:
            try:
                valid_dates.append(datetime.strptime(d[:10], "%Y-%m-%d").date())
            except:
                continue
        if valid_dates:
            date_min = min(valid_dates)
            date_max = max(valid_dates)
            release_range = f"{date_min}" if date_min == date_max else f"{date_min} – {date_max}"

        # --- Artists ---
        sorted_artists = sorted(list(artist_list))
        if len(sorted_artists) > 10:
            display_artists = ", ".join(sorted_artists[:10]) + ", etc..."
        else:
            display_artists = ", ".join(sorted_artists) if sorted_artists else "Unknown Artist"

        # --- Labels ---
        sorted_labels = sorted(list(label_list))
        if len(sorted_labels) > 6:
            display_labels = ", ".join(sorted_labels[:6]) + ", etc..."
        else:
            display_labels = ", ".join(sorted_labels) if sorted_labels else (
                audio_easy.get("label") or audio_easy.get("organization") or audio_easy.get("publisher") or ["Unknown Label"]
            )[0]

        # --- Genres ---
        sorted_genres = sorted(list(genre_list))
        if is_album or is_playlist or is_chart:
            if len(sorted_genres) > 6:
                display_genres = ", ".join(sorted_genres[:6]) + ", etc..."
            else:
                display_genres = ", ".join(sorted_genres) if sorted_genres else "Unknown Genre"
        else:
            display_genres = audio_easy.get("genre", ["Unknown Genre"])[0]

        # --- Catalog display ---
        if is_album:
            catalog_display = ", ".join(sorted(catalog_list)) if catalog_list else catalog
        elif not is_album and not is_playlist and not is_chart:
            catalog_display = catalog
        else:
            catalog_display = None

        # --- Caption formatting ---
        if is_chart:
            chart_name = folder_name.replace("_", " -")
            caption = (
                f"🎶 Chart: {chart_name}\n"
                f"👤 Artists: {display_artists}\n"
                f"🏷️ Labels: {display_labels}\n"
                f"🧩 BPM: {bpm_range}\n"
                f"🎹 Key: {key}\n"
                f"📅 Release Dates: {release_range}\n"
                f"📻 Genres: {display_genres}"
            )
        elif is_playlist:
            caption = (
                f"🎶 Playlist: {folder_name}\n"
                f"👤 Artists: {display_artists}\n"
                f"🏷️ Labels: {display_labels}\n"
                f"🧩 BPM: {bpm_range}\n"
                f"🎹 Key: {key}\n"
                f"📅 Release Dates: {release_range}\n"
                f"📻 Genres: {display_genres}"
            )
        elif is_album:
            caption = (
                f"💽 Album: {album}\n"
                f"👤 Artists: {display_artists}\n"
                f"🏷️ Labels: {display_labels}\n"
                f"🧩 BPM: {bpm_range}\n"
                f"🎹 Key: {key}\n"
                f"📅 Release Dates: {release_range}\n"
                f"📻 Genres: {display_genres}\n"
            
            )
        else:
            title = audio_easy.get("title", [os.path.basename(file_path)])[0]
            artist = ", ".join(audio_easy.get("artist", ["Unknown Artist"]))
            label = audio_easy.get("label") or audio_easy.get("organization") or audio_easy.get("publisher") or ["Unknown Label"]
            date = audio_easy.get("date", ["Unknown"])[0]
            caption = (
                f"🎶 Title: {title}\n"
                f"👤 Artist: {artist}\n"
                f"💽 Album: {album}\n"
                f"🏷️ Label: {label[0]}\n"
                f"🧩 BPM: {bpm_range}\n"
                f"🎹 Key: {key}\n"
                f"📅 Release Date: {date}\n"
                f"📻 Genre: {display_genres}\n"
            
            )

        # --- Extract cover art ---
        cover_data = None
        try:
            if audio_id3:
                for tag in audio_id3.values():
                    if hasattr(tag, "mime") and hasattr(tag, "data") and tag.mime.startswith("image/"):
                        cover_data = BytesIO(tag.data)
                        break
            if cover_data is None and hasattr(audio_easy, "pictures"):
                for pic in audio_easy.pictures:
                    if pic.mime.startswith("image/"):
                        cover_data = BytesIO(pic.data)
                        break
            if is_chart:
                chart_cover = os.path.join(parent_folder, "cover.jpg")
                if os.path.exists(chart_cover):
                    with open(chart_cover, "rb") as f:
                        cover_data = BytesIO(f.read())
        except Exception:
            cover_data = None

        return caption, cover_data

    except Exception as e:
        print(f"⚠️ Error formatting metadata: {e}")
        return None, None
# --- PAYMENT CARD ---
async def send_payment_prompt(event):
    buttons = [
        [Button.url("💳 Pay $5 Here", "https://ko-fi.com/zackant")],
    ]
    await event.reply(
        "🚫 **Daily limit reached!**\n\n"
        "Upgrade to **Premium ($5)** for **unlimited downloads** and send the payment proof to @zackantdev",
        buttons=buttons,
        parse_mode="markdown"
    )



# --- FORMAT SELECTION (ONE JOB PER USER + QUEUE SUPPORT) ---
@bot.on(events.NewMessage(pattern=pattern))
async def format_selection_handler(event):
    # Extract actual link
    text = event.raw_text.strip()
    if text.startswith("/download "):
        link = text[len("/download "):].strip()
    else:
        link = text

    user_id = event.sender_id

    # Prevent same user from adding multiple downloads
    if user_id in active_downloads or user_id in queued_users:
        await event.reply("⚠️ You already have a download in progress or queued. Please wait for it to finish.")
        return

    # Generate a short unique ID for this request
    unique_id = str(uuid.uuid4())
    pending_links[unique_id] = link

    # Format selection buttons
    buttons = [
        [
            Button.inline("🎵 MP3", data=f"format_mp3:{user_id}:{unique_id}"),
            Button.inline("🎧 FLAC", data=f"format_flac:{user_id}:{unique_id}"),
            Button.inline("🎼 WAV", data=f"format_wav:{user_id}:{unique_id}")
        ]
    ]

    await event.reply(
        "🎚️ **Choose your audio format:**\n\nSelect whether you want your download in MP3, FLAC, or WAV.",
        buttons=buttons,
        parse_mode="markdown"
    )


# --- FORMAT BUTTON HANDLER (QUEUE-BASED + ONE JOB PER USER) ---
@bot.on(events.CallbackQuery(pattern=r"format_(mp3|flac|wav):(\d+):(.+)"))
async def format_button_callback(event):
    match = re.match(r"format_(mp3|flac|wav):(\d+):(.+)", event.data.decode())
    if not match:
        await event.answer("Invalid request.")
        return

    format_choice, user_id, unique_id = match.groups()
    user_id = int(user_id)

    # Only the original user can click their buttons
    if user_id != event.sender_id:
        await event.answer("This option isn’t for you.", alert=True)
        return

    # Retrieve stored link
    link = pending_links.pop(unique_id, None)
    if not link:
        await event.edit("⚠️ Session expired. Please resend your link.")
        return

    # Prevent multiple jobs from the same user
    if event.sender_id in active_downloads or event.sender_id in queued_users:
        await event.answer("⚠️ You already have a download in progress or queued. Please wait.", alert=True)
        return

    # Mark user as queued
    queued_users.add(event.sender_id)

    # Remember format choice
    user_format_choice[event.sender_id] = format_choice

    # Add job to queue
    queued_pos = download_queue.qsize() + 1
    await event.edit(
        f"✅ Selected format: **{format_choice.upper()}**\n"
        f"📥 Added to queue — position #{queued_pos}. Please wait..."
    )

    # Add job tuple (event, link, format_choice)
    await download_queue.put((event, link, format_choice))
# --- ASYNC MP3 CONVERSION FUNCTION (session-safe) ---
async def convert_flac_to_mp3(flac_path, output_dir):
    mp3_path = os.path.join(output_dir, os.path.splitext(os.path.basename(flac_path))[0] + ".mp3")
    process = await asyncio.create_subprocess_exec(
        "ffmpeg", "-y", "-i", flac_path,
        "-codec:a", "libmp3lame", "-b:a", "320k",
        mp3_path,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL
    )
    await process.communicate()
    return mp3_path

# --- ASYNC WAV CONVERSION FUNCTION (session-safe) ---
async def convert_flac_to_wav(flac_path, output_dir):
    wav_path = os.path.join(output_dir, os.path.splitext(os.path.basename(flac_path))[0] + ".wav")
    process = await asyncio.create_subprocess_exec(
        "ffmpeg", "-y", "-i", flac_path,
        "-codec:a", "pcm_s16le",
        wav_path,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL
    )
    await process.communicate()
    return wav_path


# --- MAIN DOWNLOAD PROCESS (Concurrency-safe version, fixed) ---
async def process_download(event, input_text, format_choice):
    import time

    user_id = event.sender_id

    # --- Queue management: remove user from queued list when starting ---
    if user_id in queued_users:
        queued_users.remove(user_id)

    # --- Safety: prevent double active state ---
    if user_id in active_downloads:
        await event.reply("❌ You already have an ongoing download. Wait for it to finish.")
        return

    # --- Mark as active ---
    active_downloads[user_id] = True

    users.setdefault(str(user_id), {
        "premium": False,
        "last_reset": str(datetime.today().date()),
        "track_downloads": 0,
        "album_downloads": 0
    })
    user = users[str(user_id)]
    reset_if_new_day(user)

    is_album = "/release/" in input_text
    is_playlist = "/playlist" in input_text
    is_chart = "/chart/" in input_text

    # --- Premium restriction ---
    if (is_playlist or is_chart) and not is_premium(user):
        await event.reply(
            "🚫 Playlists and Charts are only for Premium users.\n💎 Upgrade to Premium ($5).",
            buttons=[[Button.url("💳 Upgrade Now", "https://ko-fi.com/zackant")]],
            parse_mode="markdown"
        )
        active_downloads.pop(user_id, None)
        return

    # --- Free user daily limit ---
    if not is_premium(user):
        if is_album and user["album_downloads"] >= 2:
            await send_payment_prompt(event)
            active_downloads.pop(user_id, None)
            return
        if not is_album and not is_playlist and not is_chart and user["track_downloads"] >= 2:
            await send_payment_prompt(event)
            active_downloads.pop(user_id, None)
            return

    msg = await event.respond("🎧 Downloading your music... please wait ⏳", parse_mode="markdown")

    # --- Create session folder ---
    unique_id = str(uuid.uuid4())
    session_dir = os.path.join(BEATPORTDL_DIR, "sessions", f"{user_id}_{unique_id}")
    user_download_dir = os.path.join(session_dir, "downloads")
    os.makedirs(user_download_dir, exist_ok=True)

    try:
        # --- Cleanup old downloads (>1 hour) ---
        if os.path.exists(DOWNLOADS_DIR):
            cutoff = time.time() - 3600
            for d in os.listdir(DOWNLOADS_DIR):
                path = os.path.join(DOWNLOADS_DIR, d)
                if os.path.isdir(path) and os.path.getmtime(path) < cutoff:
                    shutil.rmtree(path, ignore_errors=True)

        # --- Snapshot before download ---
        before_folders = set(os.listdir(DOWNLOADS_DIR)) if os.path.exists(DOWNLOADS_DIR) else set()

        # --- Run Orpheus downloader ---
        process = await asyncio.create_subprocess_exec(
            "go", "run", "./cmd/beatportdl", input_text,
            cwd=BEATPORTDL_DIR,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            stdin=asyncio.subprocess.DEVNULL
        )
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=600)
        logger.info(stdout.decode())
        logger.error(stderr.decode())

        # --- Identify downloaded folder ---
        after_folders = set(os.listdir(DOWNLOADS_DIR)) if os.path.exists(DOWNLOADS_DIR) else set()
        new_folders = [f for f in after_folders - before_folders if os.path.isdir(os.path.join(DOWNLOADS_DIR, f))]
        if new_folders:
            latest_release = os.path.join(DOWNLOADS_DIR, new_folders[0])
        else:
            subdirs = [os.path.join(DOWNLOADS_DIR, d) for d in os.listdir(DOWNLOADS_DIR)
                       if os.path.isdir(os.path.join(DOWNLOADS_DIR, d))]
            if not subdirs:
                await msg.edit("⚠️ *No files found.*", parse_mode="markdown")
                shutil.rmtree(session_dir, ignore_errors=True)
                return
            latest_release = max(subdirs, key=os.path.getmtime)

        # --- Move to user session ---
        user_release_path = os.path.join(user_download_dir, os.path.basename(latest_release))
        shutil.move(latest_release, user_release_path)
        release_path = user_release_path

        # --- Playlist/chart track limit ---
        if is_playlist or is_chart:
            unique_tracks = set()
            for _, _, files in os.walk(release_path):
                for f in files:
                    if f.lower().endswith(('.flac', '.mp3', '.wav')):
                        unique_tracks.add(os.path.splitext(f)[0].lower().strip())
            if len(unique_tracks) > 50:
                await msg.edit(
                    f"🚫 This {'chart' if is_chart else 'playlist'} contains **{len(unique_tracks)} tracks**.\n"
                    "⚠️ Cannot download more than 50 tracks at once.",
                    parse_mode="markdown"
                )
                shutil.rmtree(session_dir, ignore_errors=True)
                return

        sent_files = 0
        metadata_card_sent = False

        for root, dirs, files in os.walk(release_path):
            audio_files = [f for f in files if f.endswith(('.flac', '.mp3', '.wav'))]
            if not audio_files:
                continue

            is_album_local = len(audio_files) > 1

            # --- Send metadata card once ---
            if not metadata_card_sent:
                caption, cover_data = format_metadata_card_with_cover(
                    os.path.join(root, audio_files[0]),
                    is_album=is_album_local,
                    is_playlist=is_playlist,
                    is_chart=is_chart
                )
                if caption:
                    if cover_data:
                        await bot.send_file(event.chat_id, file=cover_data, caption=caption, parse_mode="markdown")
                    else:
                        await event.reply(caption, parse_mode="markdown")
                metadata_card_sent = True

            # --- Convert FLAC to chosen format inside session ---
            if format_choice in ("mp3", "wav"):
                flac_files = [os.path.join(root, f) for f in files if f.endswith(".flac")]
                if flac_files:
                    if format_choice == "mp3":
                        converted_files = await asyncio.gather(*[convert_flac_to_mp3(fp, root) for fp in flac_files])
                        audio_files = [os.path.basename(f) for f in converted_files]
                    elif format_choice == "wav":
                        converted_files = await asyncio.gather(*[convert_flac_to_wav(fp, root) for fp in flac_files])
                        audio_files = [os.path.basename(f) for f in converted_files]

            # --- Send each audio file ---
            for f in audio_files:
                file_path = os.path.join(root, f)
                try:
                    audio = mutagen.File(file_path)
                    duration = int(getattr(audio.info, "length", 0)) if audio and hasattr(audio, "info") else 0
                    title_tag = os.path.splitext(f)[0]
                    artist_tag = "Unknown Artist"
                    if audio and hasattr(audio, "tags") and audio.tags:
                        if "TIT2" in audio.tags:
                            title_tag = str(audio.tags["TIT2"])
                        elif "title" in audio.tags:
                            title_tag = str(audio.tags["title"][0])
                        if "TPE1" in audio.tags:
                            artist_tag = str(audio.tags["TPE1"])
                        elif "artist" in audio.tags:
                            artist_tag = str(audio.tags["artist"][0])
                    await bot.send_file(
                        event.chat_id,
                        file=file_path,
                        attributes=[DocumentAttributeAudio(duration=duration, title=title_tag, performer=artist_tag)]
                    )
                    sent_files += 1
                except Exception:
                    await event.reply("⚠️ Couldn't send some files, please try again.")

        # --- Cleanup ---
        shutil.rmtree(session_dir, ignore_errors=True)

        if sent_files > 0:
            await msg.edit(f"✅ Sent successfully in **{format_choice.upper()}** format.", parse_mode="markdown")
            if not is_premium(user):
                if is_album_local:
                    user["album_downloads"] += 1
                else:
                    user["track_downloads"] += 1
                save_users()
        else:
            await msg.edit("⚠️ *Error — no files sent.*", parse_mode="markdown")

    except asyncio.TimeoutError:
        await msg.edit("⏱️ *Download took too long and was stopped.*", parse_mode="markdown")
    except Exception as e:
        await msg.edit(f"⚠️ *Error:* {e}", parse_mode="markdown")
    finally:
        # --- Always cleanup user state ---
        active_downloads.pop(user_id, None)
        queued_users.discard(user_id)
        shutil.rmtree(session_dir, ignore_errors=True)

@bot.on(events.NewMessage(pattern=r"^/updates$"))
async def updates_handler(event):
    message = (
        "📢 **Stay tuned for the latest bot updates, fixes, and new features!**\n\n"
        "👉 **Join our official channel for updates:**\n"
        "https://t.me/+UsTE5Ufq1W4wOWE1"
    )
    await event.reply(message, parse_mode="markdown")

# --- /TOTALUSERS COMMAND ---
@bot.on(events.NewMessage(pattern=r"^/totalusers$"))
async def total_users_handler(event):
    if event.sender_id != ADMIN_ID:
        await event.reply("⛔ You are not authorized to use this command.")
        return

    total = len(users)
    await event.reply(f"👥 **Total registered users:** {total}", parse_mode="markdown")


# --- /PREMIUM COMMAND ---
from datetime import datetime, timezone

@bot.on(events.NewMessage(pattern=r"^/premium$"))
async def premium_users_handler(event):
    if event.sender_id != ADMIN_ID:
        await event.reply("⛔ You are not authorized to use this command.")
        return

    today_utc = datetime.now(timezone.utc).date()
    premium_users = []

    for uid, data in users.items():
        expiry_str = data.get("expiry")
        if not expiry_str:
            continue  # skip if no expiry
        try:
            expiry_date = datetime.fromisoformat(expiry_str).date()
            if expiry_date >= today_utc:
                premium_users.append((uid, data))
        except Exception:
            continue  # skip invalid date formats

    total_premium = len(premium_users)

    if not premium_users:
        await event.reply("💎 No active premium users found.")
        return

    text = f"💎 **Total Premium Users:** {total_premium}\n\n"
    for uid, data in premium_users:
        expiry = data.get("expiry", "N/A")
        username = data.get("username", "N/A")
        text += f"👤 `{uid}` — @{username if username != 'N/A' else 'unknown'}\n📆 Expires: {expiry}\n\n"

    await event.reply(text.strip(), parse_mode="markdown")
# --- /ALERT COMMAND ---
@bot.on(events.NewMessage(pattern=r"^/alert$"))
async def alert_handler(event):
    if event.sender_id != ADMIN_ID:
        await event.reply("⛔ You are not authorized to use this command.")
        return

    today = datetime.today().date()
    alerts_sent = 0

    for user_id, data in users.items():
        if not data.get("premium"):
            continue

        expiry_str = data.get("expiry")
        if not expiry_str:
            continue

        try:
            expiry_date = datetime.strptime(expiry_str, "%Y-%m-%d").date()
        except ValueError:
            continue

        days_left = (expiry_date - today).days
        if days_left not in [3, 2, 1]:
            continue

        # Choose message based on days left
        if days_left == 3:
            message = (
                f"⚡ Hey DJ! Your Premium plan expires in 3 days — on {expiry_date}.\n\n"
                "Renew now to continue enjoying unlimited Beatsource downloads without interruption! 🎶🔥\n\n"
                "If you’ve already paid, please contact @zackantdev."
            )
        elif days_left == 2:
            message = (
                f"⏰ Reminder: Only 2 days left before your Premium expires — on {expiry_date}.\n\n"
                "Don’t lose your VIP access — renew today and keep downloading your favorite music non-stop! 🎧💿\n\n"
                "If you’ve already paid, please contact @zackantdev."
            )
        elif days_left == 1:
            message = (
                f"🚨 Final call! Your Premium plan expires tomorrow — on {expiry_date}.\n\n"
                "Renew now to avoid losing your unlimited track and album downloads! 💽✨\n\n"
                "If you’ve already paid, please contact @zackantdev."
            )

        # Send message
        try:
            await bot.send_message(
                int(user_id),
                message,
                parse_mode="markdown",
                buttons=[Button.url("💳 Renew Premium", "https://ko-fi.com/zackant")]
            )
            alerts_sent += 1
            await asyncio.sleep(1)
        except Exception:
            continue

    if alerts_sent == 0:
        await event.reply("✅ No users found whose Premium expires in 3, 2, or 1 day.")
    else:
        await event.reply(f"📬 Alert sent to {alerts_sent} Premium users successfully.")


# --- /BROADCAST (Free Users) ---
@bot.on(events.NewMessage(pattern=r"^/broadcast (.+)$"))
async def broadcast_free_users(event):
    if event.sender_id != ADMIN_ID:
        await event.reply("⛔ You are not authorized to use this command.")
        return

    message = event.pattern_match.group(1)
    sent = 0
    failed = 0

    await event.reply("📢 Starting broadcast to **free users**...")

    for user_id, data in users.items():
        if data.get("premium"):
            continue  # Skip premium users
        try:
            await bot.send_message(int(user_id), message)
            sent += 1
            await asyncio.sleep(0.5)  # small delay to avoid flood limits
        except Exception as e:
            failed += 1
            logger.warning(f"Could not send to {user_id}: {e}")

    await event.reply(f"✅ Broadcast completed!\n\n📨 Sent: {sent}\n⚠️ Failed: {failed}")

# --- /BROADCASTP (Premium Users) ---
@bot.on(events.NewMessage(pattern=r"^/broadcastp (.+)$"))
async def broadcast_premium_users(event):
    if event.sender_id != ADMIN_ID:
        await event.reply("⛔ You are not authorized to use this command.")
        return

    message = event.pattern_match.group(1)
    sent = 0
    failed = 0

    await event.reply("💎 Starting broadcast to **premium users**...")

    for user_id, data in users.items():
        if not data.get("premium"):
            continue  # Skip free users
        try:
            await bot.send_message(int(user_id), message)
            sent += 1
            await asyncio.sleep(0.5)
        except Exception as e:
            failed += 1
            logger.warning(f"Could not send to {user_id}: {e}")

    await event.reply(f"✅ Broadcast completed!\n\n💎 Sent: {sent}\n⚠️ Failed: {failed}")

# --- ADMIN COMMAND: Check Queue Status ---
@bot.on(events.NewMessage(pattern=r"^/queue$"))
async def queue_status_handler(event):
    # Only admin can use this command
    if event.sender_id != ADMIN_ID:
        await event.reply("🚫 You are not authorized to use this command.")
        return

    active_count = len(active_downloads)
    queued_count = download_queue.qsize()

    # Optional: show queued users (if you maintain `queued_users`)
    if queued_users:
        queued_list = "\n".join([f"• `{uid}`" for uid in queued_users])
    else:
        queued_list = "— none —"

    text = (
        f"📊 **Download Queue Status**\n\n"
        f"🟢 Active downloads: **{active_count}**\n"
        f"🕓 Queued tasks: **{queued_count}**\n\n"
        f"👥 **Queued Users:**\n{queued_list}"
    )

    await event.reply(text, parse_mode="markdown")
# --- QUEUE WORKER ---
async def queue_worker():
    while True:
        event, link, format_choice = await download_queue.get()
        try:
            # Notify user when their download begins
            await event.respond("🎧 Now downloading your link...")
            await process_download(event, link, format_choice)
        except Exception as e:
            await event.respond(f"⚠️ Error during download: {e}")
        finally:
            download_queue.task_done()

async def on_startup():
    global queue_worker_task
    if not queue_worker_task:
        queue_worker_task = asyncio.create_task(queue_worker())

def main():
    print("🤖 Bot is online... waiting for links.")
    # Start the queue worker before the bot begins listening
    bot.loop.create_task(on_startup())
    bot.run_until_disconnected()

if __name__ == "__main__":
    main()
