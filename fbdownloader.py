import os
import asyncio
import logging
import tempfile
from functools import wraps
from pathlib import Path
from dotenv import load_dotenv
from yt_dlp import YoutubeDL
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)
from telegram.error import RetryAfter, TelegramError

# ---- Load config ----
load_dotenv()
BOT_TOKEN = os.getenv('BOT_TOKEN')
BACKUP_CHANNEL = os.getenv('BACKUP_CHANNEL')  # channel username like @mybackupchannel or numeric id as string
MAX_DOWNLOAD_MB = int(os.getenv('MAX_DOWNLOAD_MB', '200'))  # safety cap
WORKERS = int(os.getenv('WORKERS', '3'))  # concurrent download workers
DOWNLOAD_TIMEOUT = int(os.getenv('DOWNLOAD_TIMEOUT', '600'))  # seconds

if not BOT_TOKEN or not BACKUP_CHANNEL:
    raise SystemExit('Please set BOT_TOKEN and BACKUP_CHANNEL in .env')

# ---- Logging ----
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---- Concurrency controls ----
global_semaphore = asyncio.Semaphore(WORKERS)
user_locks = {}  # user_id -> asyncio.Lock()

# ---- Utils ----
async def safe_send(bot_send_func, *args, **kwargs):
    """Wrap send functions to catch Flood waits and retry appropriately."""
    while True:
        try:
            return await bot_send_func(*args, **kwargs)
        except RetryAfter as e:
            wait = int(e.retry_after) + 1
            logger.warning('FloodWait encountered. Sleeping %s seconds', wait)
            await asyncio.sleep(wait)
        except TelegramError as e:
            # Other Telegram errors
            logger.exception('Telegram error: %s', e)
            raise

async def ensure_user_lock(user_id: int):
    if user_id not in user_locks:
        user_locks[user_id] = asyncio.Lock()
    return user_locks[user_id]

# Decorator to require channel membership check before processing
def require_backup_join(func):
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *a, **kw):
        user = update.effective_user
        chat_id = update.effective_chat.id if update.effective_chat else None
        # Only checks for private chats / DMs
        try:
            # If user already clicked "I have join" the callback will re-check membership.
            member = await context.bot.get_chat_member(BACKUP_CHANNEL, user.id)
            if member.status in ('left', 'kicked'):
                # Ask to join
                keyboard = [
                    [InlineKeyboardButton('Join BackUp Channel ✅', url=f'https://t.me/{BACKUP_CHANNEL.lstrip("@")}')],
                    [InlineKeyboardButton('I have join 👀', callback_data='check_join')],
                ]
                text = 'You Need To Join BackUp Channel First To Use Me.'
                await safe_send(context.bot.send_message, chat_id=user.id, text=text, reply_markup=InlineKeyboardMarkup(keyboard))
                return
        except Exception as e:
            # If get_chat_member fails (private channel or bot not admin), still prompt user to join via link
            logger.info('Could not verify membership: %s', e)
            keyboard = [
                [InlineKeyboardButton('Join BackUp Channel ✅', url=f'https://t.me/{BACKUP_CHANNEL.lstrip("@")}')],
                [InlineKeyboardButton('I have join 👀', callback_data='check_join')],
            ]
            await safe_send(context.bot.send_message, chat_id=user.id, text='You Need To Join BackUp Channel First To Use Me.', reply_markup=InlineKeyboardMarkup(keyboard))
            return
        return await func(update, context, *a, **kw)
    return wrapper

# ---- Handlers ----
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        'Hello! Send me a Facebook video/reel/story/image URL and I will download it for you.\n\n'
        'But first you must join our backup channel.'
    )
    keyboard = [
        [InlineKeyboardButton('Join BackUp Channel ✅', url=f'https://t.me/{BACKUP_CHANNEL.lstrip("@")}')],
        [InlineKeyboardButton('I have join 👀', callback_data='check_join')],
    ]
    await safe_send(context.bot.send_message, chat_id=update.effective_chat.id, text=text, reply_markup=InlineKeyboardMarkup(keyboard))


async def check_join_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = query.from_user
    try:
        member = await context.bot.get_chat_member(BACKUP_CHANNEL, user.id)
        if member.status in ('member', 'administrator', 'creator'):
            await safe_send(context.bot.send_message, chat_id=user.id, text='Thanks — you are verified! Now send the Facebook link.')
        else:
            await safe_send(context.bot.send_message, chat_id=user.id, text='I still can\'t see you in the channel. Please join the backup channel first.')
    except Exception as e:
        logger.info('Membership check error: %s', e)
        await safe_send(context.bot.send_message, chat_id=user.id, text='Could not verify membership. Make sure you joined the backup channel and try again.')


@require_backup_join
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    text = (update.message.text or '').strip()
    if not text:
        await safe_send(context.bot.send_message, chat_id=user.id, text='Please send a Facebook URL (video/reel/story/image).')
        return

    # Basic validation: check if likely Facebook link
    if 'facebook.com' not in text and 'fb.watch' not in text and 'fbcdn' not in text:
        await safe_send(context.bot.send_message, chat_id=user.id, text='That does not look like a Facebook link. Please send a valid Facebook video/reel/story/image URL.')
        return

    # Acquire per-user lock to prevent duplicate parallel downloads from same user
    lock = await ensure_user_lock(user.id)
    if lock.locked():
        await safe_send(context.bot.send_message, chat_id=user.id, text='You already have an ongoing request. Please wait for it to finish.')
        return

    await safe_send(context.bot.send_message, chat_id=user.id, text='Queued your request. Download will start shortly.')

    async with lock:
        # Use global semaphore for scaling
        async with global_semaphore:
            try:
                await download_and_send(user.id, text, context)
            except Exception as e:
                logger.exception('Error processing download: %s', e)
                await safe_send(context.bot.send_message, chat_id=user.id, text=f'Failed to process the link: {e}')


async def download_and_send(chat_id: int, url: str, context: ContextTypes.DEFAULT_TYPE):
    """Downloads content via yt-dlp and sends it to the user."""
    ydl_opts = {
        'outtmpl': os.path.join(tempfile.gettempdir(), 'tg_dl_%(id)s.%(ext)s'),
        'format': 'best',
        'noplaylist': True,
        'quiet': True,
        'no_warnings': True,
        'merge_output_format': 'mp4',
        'socket_timeout': 30,
        # prevent hanging
        'retries': 2,
    }

    loop = asyncio.get_event_loop()

    # Run yt-dlp in a thread to avoid blocking event loop
    def ytdl_extract():
        with YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)
            return info

    sent = False
    info = None
    try:
        info = await loop.run_in_executor(None, ytdl_extract)
    except Exception as e:
        logger.exception('yt-dlp failed: %s', e)
        await safe_send(context.bot.send_message, chat_id=chat_id, text=f'Could not download media: {e}')
        return

    # Determine file path
    if not info:
        await safe_send(context.bot.send_message, chat_id=chat_id, text='No downloadable media found.')
        return

    # If info is playlist or entries
    if 'entries' in info and info['entries']:
        info = info['entries'][0]

    filename = None
    if 'requested_formats' in info:
        # pick best format file
        # yt-dlp stores final filename in _filename if download happened
        filename = info.get('_filename')
    else:
        filename = info.get('_filename') or info.get('filepath')

    if not filename:
        # fallback: search temp dir for created file by id
        candidate = list(Path(tempfile.gettempdir()).glob(f"tg_dl_*{info.get('id','')}*"))
        filename = str(candidate[0]) if candidate else None

    if not filename or not Path(filename).exists():
        await safe_send(context.bot.send_message, chat_id=chat_id, text='Downloaded file not found on disk.')
        return

    # size limit check
    size_mb = Path(filename).stat().st_size / (1024 * 1024)
    if size_mb > MAX_DOWNLOAD_MB:
        await safe_send(context.bot.send_message, chat_id=chat_id, text=f'File too large ({size_mb:.1f} MB). Limit is {MAX_DOWNLOAD_MB} MB.')
        try:
            Path(filename).unlink()
        except Exception:
            pass
        return

    # Send file (video or photo depending on mime)
    await safe_send(context.bot.send_message, chat_id=chat_id, text=f'Uploading {Path(filename).name} ({size_mb:.1f} MB) ...')
    try:
        # Basic heuristic: send_video if the file is mp4 or has video ext
        ext = Path(filename).suffix.lower()
        if ext in ('.mp4', '.mkv', '.mov', '.webm'):
            with open(filename, 'rb') as f:
                await safe_send(context.bot.send_video, chat_id=chat_id, video=f, timeout=DOWNLOAD_TIMEOUT)
                sent = True
        elif ext in ('.jpg', '.jpeg', '.png', '.gif', '.webp'):
            with open(filename, 'rb') as f:
                await safe_send(context.bot.send_photo, chat_id=chat_id, photo=f)
                sent = True
        else:
            # send as document fallback
            with open(filename, 'rb') as f:
                await safe_send(context.bot.send_document, chat_id=chat_id, document=f)
                sent = True
    except RetryAfter as e:
        logger.warning('RetryAfter while sending file: %s', e)
        await asyncio.sleep(int(e.retry_after) + 1)
    except Exception as e:
        logger.exception('Failed to send file: %s', e)
        await safe_send(context.bot.send_message, chat_id=chat_id, text=f'Failed to upload file: {e}')
    finally:
        # cleanup
        try:
            Path(filename).unlink()
        except Exception:
            pass

    if sent:
        await safe_send(context.bot.send_message, chat_id=chat_id, text='Done!')


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await safe_send(context.bot.send_message, chat_id=update.effective_chat.id, text='Send a Facebook URL (video/reel/story/image) and I will try to download it for you.')


# ---- Main ----
async def main():
    application = ApplicationBuilder().token(BOT_TOKEN).build()

    application.add_handler(CommandHandler('start', start))
    application.add_handler(CommandHandler('help', help_command))
    application.add_handler(CallbackQueryHandler(check_join_callback, pattern='^check_join$'))

    # Messages in private chats only
    application.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & ~filters.COMMAND, handle_message))

    # Start the bot
    logger.info('Bot starting...')
    await application.initialize()
    await application.start()
    await application.updater.start_polling()
    await application.idle()


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info('Bot stopped')
      
