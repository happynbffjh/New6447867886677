import asyncio
import json
import logging
import os
import random
import re
import signal
import sys
from functools import wraps
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timezone

from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
    Update,
)
from telegram.ext import (
    Application,
    CallbackContext,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# ---------------------------
# Configuration - customize
# ---------------------------
# Replace with your bot token (or keep as env var)
REQUIRED_CHANNEL = -1002710971355

BOT_TOKEN = os.getenv("GIVEAWAY_BOT_TOKEN", "7875476980:AAGLYnxaDGgjQLELbDRsdgR6aC1wCIDwOCk")

# Admin IDs - update as needed
ADMIN_IDS: List[int] = [
    6284479489,  # primary
    8428346442
]

DATA_FILE = "giveaway_data.json"
LOG_LEVEL = logging.INFO

# Regex for code validation: PREFIX-XXXX-XXXX-XXXX (prefix letters/digits allowed)
CODE_REGEX = re.compile(r"^[A-Z0-9]+-[A-Z0-9]{4}-[A-Z0-9]{4}-[A-Z0-9]{4}$")

# ---------------------------
# Logging
# ---------------------------
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=LOG_LEVEL,
)
logger = logging.getLogger(__name__)

# ---------------------------
# Data helpers
# ---------------------------


def default_data() -> Dict:
    return {
        "codes": {},  # code -> {"redeemed_by": None/int, "redeemed_by_username": None/str, "redeemed_at": None/iso, "prize": None, "created_at": iso}
        "past_winners": [],
        "users": [],
        "leaderboard": {},  # user_id_str -> {"username": str, "score": int}
        "banned_users": [],
        "awaiting_screenshot": [],  # user ids expecting to upload screenshot
        "last_generated_codes": [],  # codes created by last /gencode
    }


def load_data() -> Dict:
    if not os.path.exists(DATA_FILE):
        data = default_data()
        save_data(data)
        return data
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        logger.error("Failed to load data file: %s - reinitializing", e)
        data = default_data()
        save_data(data)
        return data


def save_data(data: Dict) -> None:
    tmp = DATA_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
    os.replace(tmp, DATA_FILE)


# ---------------------------
# Utilities
# ---------------------------


def validate_code_format(code: str) -> bool:
    """Return True if code matches PREFIX-XXXX-XXXX-XXXX uppercase."""
    return bool(CODE_REGEX.match(code))


def initialize_code_details() -> Dict:
    return {
        "redeemed_by": None,
        "redeemed_by_username": None,
        "redeemed_at": None,
        "prize": None,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


def user_handle(user) -> str:
    if getattr(user, "username", None):
        return f"@{user.username}"
    return f"UserID:{user.id}"


# ---------------------------
# Decorators
# ---------------------------


def admin_only(func):
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        user = update.effective_user
        if not user:
            return
        if user.id not in ADMIN_IDS:
            if update.message:
                await update.message.reply_text("❌ Sorry, this is an admin-only command.")
            return
        return await func(update, context, *args, **kwargs)

    return wrapper


def check_banned(func):
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        user = update.effective_user
        if not user:
            return
        data = load_data()
        if user.id in data.get("banned_users", []):
            # reply using message if available else silent
            if update.message:
                await update.message.reply_text("🚫 You are banned from using this bot.")
            return
        return await func(update, context, *args, **kwargs)

    return wrapper

# ---------------------------
# Channel join decorator fix
# ---------------------------

CHANNEL_INVITE_URL = "https://t.me/+tttfU52Nm3xkNDA1"  # your channel invite link

def channel_required(func):
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        user_id = update.effective_user.id
        if not await is_member(user_id, context):
            keyboard = InlineKeyboardMarkup([[
                InlineKeyboardButton("🎵 Join Channel", url=CHANNEL_INVITE_URL)
            ]])
            await update.message.reply_text(
                "❌ You must join our channel first!\nAfter joining, press /start again.",
                reply_markup=keyboard
            )
            return
        return await func(update, context, *args, **kwargs)  # forward args/kwargs
    return wrapper


def check_banned(func):
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        user = update.effective_user
        if not user:
            return
        data = load_data()
        if user.id in data.get("banned_users", []):
            if update.message:
                await update.message.reply_text("🚫 You are banned from using this bot.")
            return
        return await func(update, context, *args, **kwargs)  # forward args/kwargs
    return wrapper
   
# ---------------------------
# Core Handlers
# ---------------------------


async def is_member(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> bool:
    try:
        member = await context.bot.get_chat_member(chat_id=REQUIRED_CHANNEL, user_id=user_id)
        return member.status not in ["left", "kicked"]
    except Exception:
        return False

@check_banned
@channel_required
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user:
        return

    data = load_data()
    if user.id not in data["users"]:
        data["users"].append(user.id)
        save_data(data)

    welcome_message = (
        "☁️ *WELCOME TO HEXBREAK VAULT GIVEAWAY BOT* ☁️\n\n"
        "☠️ *Claim Your Rewards Now!* ☠️\n\n"
        "How to redeem:\n"
        "• Send `/redeem <CODE>` (example: PREFIX-ABCD-1234-XYZ9)\n"
        "• Or send the code directly in the chat\n\n"
        "Commands: /help  \n\n"
        "Join our channel for updates."
    )

    keyboard = [
        [InlineKeyboardButton("✉️ Contact Owner", url="https://t.me/XD_HR")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_markdown(welcome_message, reply_markup=reply_markup)


@check_banned
@channel_required
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id

    # ---------------- USER HELP ----------------
    user_help = (
        "*Available Commands*\n\n"
        "*User Commands*\n"
        "/start - Show welcome menu\n"
        "/help - Show this message\n"
        "/redeem <CODE> - Redeem a giveaway code\n"
        "/leaderboard - Show top winners\n"
    )

    # ---------------- ADMIN HELP ----------------
    admin_help = (
        "\n*Admin Commands* (admins only)\n"
        "/stats - Show basic stats\n"
        "/listcodes - List all codes and status\n"
        "/addcode <CODE1> [CODE2]... - Add codes\n"
        "/addprize <CODE> <prize text> - Assign prize to a code\n"
        "/delcode <CODE1> [CODE2]... - Delete codes\n"
        "/gencode <amount> <prefix> - Generate codes\n"
        "/resetgiveaway - Reset past winners\n"
        "/broadcast <message> - Broadcast to all users\n"
        "/ban <user_id> - Ban a user\n"
        "/unban <user_id> - Unban a user\n"
        "/stopbot - Stop the bot\n\n"
        "Admins can upload a .txt file with prizes to assign to the last generated codes, "
        "or send prize lines directly in chat."
    )

    # If admin → show both menus
    if user_id in ADMIN_IDS:
        await update.message.reply_markdown(user_help + admin_help)

    # If normal user → only user commands
    else:
        await update.message.reply_markdown(user_help)


@check_banned
@channel_required
async def redeem(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text("❌ Please provide a code: /redeem <CODE>")
        return

    code = context.args[0].strip().upper()
    await process_redemption(update, context, code)


@check_banned
@channel_required
async def leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    data = load_data()
    lb = data.get("leaderboard", {})
    if not lb:
        await update.message.reply_text("🏆 Leaderboard is empty.")
        return
    sorted_lb = sorted(lb.items(), key=lambda kv: kv[1]["score"], reverse=True)
    text_lines = ["🏆 *Giveaway Leaderboard* 🏆\n"]
    for i, (uid, info) in enumerate(sorted_lb[:20], start=1):
        medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else ""
        text_lines.append(f"{medal} {info.get('username','User')} — {info.get('score',0)}")
    await update.message.reply_markdown("\n".join(text_lines))


@check_banned
@channel_required
async def process_redemption(update: Update, context: ContextTypes.DEFAULT_TYPE, code: Optional[str] = None):
    if not code:
        if not update.message or not update.message.text:
            return
        code = update.message.text.strip().upper()

    if not validate_code_format(code):
        await update.message.reply_text("❌ Invalid code format. Expected: NETFLIX-XXXX-XXXX-XXXX")
        return

    user = update.effective_user
    if not user:
        return

    data = load_data()

    # --- NEW: Limit one code per user ---
    if user.id in data.get("past_winners", []):
        await update.message.reply_text("⚠️ You have already redeemed a code. Wait for the next giveaway or admin reset.")
        return

    if code not in data["codes"]:
        await update.message.reply_text("🤔 That code does not exist.")
        return

    details = data["codes"][code]

    if details.get("redeemed_by"):
        await update.message.reply_text("⚠️ This code has already been redeemed.")
        return

    # Redeem code
    user_name = user_handle(user)
    now_iso = datetime.now(timezone.utc).isoformat()
    details["redeemed_by"] = user.id
    details["redeemed_by_username"] = user_name
    details["redeemed_at"] = now_iso

    prize_text = details.get("prize") or "Prize details not set. Please contact the admin."

    # Update past winners & leaderboard
    data["past_winners"].append(user.id)
    uid_str = str(user.id)
    if uid_str not in data["leaderboard"]:
        data["leaderboard"][uid_str] = {"username": user_name, "score": 0}
    data["leaderboard"][uid_str]["score"] += 1

    if user.id not in data.get("awaiting_screenshot", []):
        data["awaiting_screenshot"].append(user.id)

    save_data(data)

    success_message = (
        "🎉 Congratulations! 🎉\n\n"
        f"You redeemed: `{code}`\n"
        f"Prize: {prize_text}\n\n"
        "Please login where required (if applicable) and send a screenshot of your claim in this chat."
    )
    await update.message.reply_text(success_message)

    notification = (
        f"🔥 Prize Redeemed! 🔥\n\n"
        f"User: {user_name}\n"
        f"Code: {code}\n"
        f"Prize: {prize_text}\n"
        f"Time(UTC): {now_iso}\n"
    )
    for admin_id in ADMIN_IDS:
        try:
            await context.bot.send_message(chat_id=admin_id, text=notification)
        except Exception as e:
            logger.warning("Failed to notify admin %s: %s", admin_id, e)


# ---------------------------
# Admin commands
# ---------------------------


@admin_only
async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    data = load_data()
    total_codes = len(data["codes"])
    redeemed = sum(1 for c in data["codes"].values() if c.get("redeemed_by"))
    available = total_codes - redeemed
    users = len(data.get("users", []))
    banned = len(data.get("banned_users", []))
    awaiting = len(data.get("awaiting_screenshot", []))
    msg = (
        f"📊 Stats\n\n"
        f"Codes: {total_codes} total\n"
        f"Redeemed: {redeemed}\n"
        f"Available: {available}\n\n"
        f"Users: {users}\n"
        f"Banned users: {banned}\n"
        f"Awaiting screenshots: {awaiting}\n"
    )
    await update.message.reply_text(msg)


@admin_only
async def list_codes(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    data = load_data()
    # Only include unredeemed codes
    available_codes = {code: details for code, details in data["codes"].items() if not details.get("redeemed_by")}

    if not available_codes:
        await update.message.reply_text("No available codes found.")
        return

    lines = ["📋 Available Codes\n"]
    for code, details in available_codes.items():
        prize = details.get("prize") or "Not set"
        lines.append(f"• {code} — Prize: {prize}")

    text = "\n".join(lines)
    # If too long, send as file
    if len(text) > 4000:
        fname = "available_codes.txt"
        with open(fname, "w", encoding="utf-8") as f:
            f.write(text)
        await update.message.reply_document(document=open(fname, "rb"))
        os.remove(fname)
    else:
        await update.message.reply_text(text)


@admin_only
async def add_code(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text("Usage: /addcode CODE1 [CODE2] ...")
        return
    data = load_data()
    added = []
    skipped_invalid = []
    for raw in context.args:
        code = raw.strip().upper()
        if not validate_code_format(code):
            skipped_invalid.append(code)
            continue
        if code in data["codes"]:
            continue
        data["codes"][code] = initialize_code_details()
        added.append(code)
    save_data(data)
    resp = []
    if added:
        resp.append(f"✅ Added {len(added)} code(s).")
    if skipped_invalid:
        resp.append(f"⚠️ Skipped invalid format: {', '.join(skipped_invalid)}")
    if not resp:
        resp = ["No new codes added."]
    await update.message.reply_text("\n".join(resp))


@admin_only
async def add_prize(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if len(context.args) < 2:
        await update.message.reply_text("Usage: /addprize <CODE> <prize text...>")
        return
    code = context.args[0].strip().upper()
    prize = " ".join(context.args[1:])
    if not validate_code_format(code):
        await update.message.reply_text("❌ Invalid code format.")
        return
    data = load_data()
    if code not in data["codes"]:
        await update.message.reply_text("❌ Code not found.")
        return
    data["codes"][code]["prize"] = prize
    save_data(data)
    await update.message.reply_text(f"✅ Prize set for {code}.")


@admin_only
async def del_code(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text("Usage: /delcode CODE1 [CODE2] ...")
        return
    data = load_data()
    deleted = []
    for raw in context.args:
        code = raw.strip().upper()
        if code in data["codes"]:
            del data["codes"][code]
            deleted.append(code)
    save_data(data)
    if deleted:
        await update.message.reply_text(f"🗑️ Deleted {len(deleted)} code(s).")
    else:
        await update.message.reply_text("No matching codes found to delete.")


@admin_only
async def reset_giveaway(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    data = load_data()
    data["past_winners"] = []
    save_data(data)
    await update.message.reply_text("🧹 Giveaway reset: past winners list cleared.")


@admin_only
async def gencode(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if len(context.args) != 2:
        await update.message.reply_text("Usage: /gencode [amount] [prefix]")
        return
    try:
        amount, prefix = int(context.args[0]), context.args[1].upper()
    except ValueError:
        await update.message.reply_text("Invalid amount.")
        return

    data = load_data()
    generated = []

    def gen_segment():
        chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
        return "".join(random.choices(chars, k=4))

    for _ in range(amount):
        new_code = f"{prefix}-{gen_segment()}-{gen_segment()}-{gen_segment()}"
        data["codes"][new_code] = initialize_code_details()
        generated.append(new_code)

    data["last_generated_codes"] = generated
    save_data(data)

    # Build HTML formatted message
    codes_text = "\n".join(f"<code>{c}</code>" for c in generated)
    await update.message.reply_html(f"<b>✅ Generated {len(generated)} codes:</b>\n\n{codes_text}")

    await update.message.reply_text(
        "You can now assign prizes via .txt file or message."
    )


@admin_only
async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text("Usage: /broadcast <message>")
        return
    data = load_data()
    message = " ".join(context.args)
    user_ids = list(data.get("users", []))
    await update.message.reply_text(f"📢 Starting broadcast to {len(user_ids)} users...")
    success = 0
    fail = 0
    for uid in user_ids:
        try:
            await context.bot.send_message(chat_id=uid, text=message)
            success += 1
            # be polite
            await asyncio.sleep(0.05)
        except Exception:
            fail += 1
    await update.message.reply_text(f"Broadcast finished!\n✅ Success: {success}\n❌ Failed: {fail}")


@admin_only
async def ban_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text("Usage: /ban <user_id>")
        return
    try:
        uid = int(context.args[0])
    except ValueError:
        await update.message.reply_text("Invalid user id.")
        return
    data = load_data()
    if uid not in data["banned_users"]:
        data["banned_users"].append(uid)
        save_data(data)
        await update.message.reply_text(f"🚫 User {uid} banned.")
    else:
        await update.message.reply_text("User already banned.")


@admin_only
async def unban_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text("Usage: /unban <user_id>")
        return
    try:
        uid = int(context.args[0])
    except ValueError:
        await update.message.reply_text("Invalid user id.")
        return
    data = load_data()
    if uid in data["banned_users"]:
        data["banned_users"].remove(uid)
        save_data(data)
        await update.message.reply_text(f"✅ User {uid} unbanned.")
    else:
        await update.message.reply_text("User not in ban list.")


@admin_only
async def stop_bot(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("Bot is shutting down...")
    # Graceful shutdown
    os.kill(os.getpid(), signal.SIGINT)


# ---------------------------
# Admin file & prize handlers
# ---------------------------


async def process_prize_data(data: Dict, prizes: List[str], codes: Optional[List[str]] = None) -> Tuple[int, Optional[str]]:
    """Assign prizes list to codes list. Returns (assigned_count, error_msg)."""
    if codes is None:
        codes = data.get("last_generated_codes", [])
    if not codes:
        return 0, "No generated codes available (use /gencode first)."
    assigned = 0
    for i, code in enumerate(codes):
        if i < len(prizes):
            # only assign if code exists
            if code in data["codes"]:
                data["codes"][code]["prize"] = prizes[i]
                assigned += 1
        else:
            break
    save_data(data)
    return assigned, None


@admin_only
async def handle_admin_file(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin uploads a .txt file: each line becomes a prize assigned to last_generated_codes."""
    if not update.message or not update.message.document:
        await update.message.reply_text("Please upload a .txt file.")
        return
    doc = update.message.document
    if not doc.file_name.lower().endswith(".txt"):
        await update.message.reply_text("Please upload a .txt file.")
        return
    data = load_data()
    f = await doc.get_file()
    tmp_path = f"tmp_prizes_{doc.file_unique_id}.txt"
    await f.download_to_drive(tmp_path)
    try:
        with open(tmp_path, "r", encoding="utf-8") as fh:
            prizes = [line.strip() for line in fh if line.strip()]
        if not prizes:
            await update.message.reply_text("No prizes found in the file.")
            return
        assigned, err = await process_prize_data(data, prizes)
        if err:
            await update.message.reply_text(f"⚠️ {err}")
            return
        await update.message.reply_text(f"✅ Assigned {assigned} prizes from the uploaded file.")
    finally:
        try:
            os.remove(tmp_path)
        except Exception:
            pass


@admin_only
async def handle_admin_prizes(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin sends plain text message (one prize per line) to assign to last_generated_codes."""
    if not update.message or not update.message.text:
        await update.message.reply_text("Please send prize lines as text (one per line).")
        return
    data = load_data()
    prizes = [line.strip() for line in update.message.text.splitlines() if line.strip()]
    if not prizes:
        await update.message.reply_text("No prize lines found in the message.")
        return
    assigned, err = await process_prize_data(data, prizes)
    if err:
        await update.message.reply_text(f"⚠️ {err}")
        return
    await update.message.reply_text(f"✅ Assigned {assigned} prizes from message.")


# ---------------------------
# Screenshot & forward handling
# ---------------------------

@channel_required
@check_banned
async def handle_screenshot(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    If user is in awaiting_screenshot, forward the photo to admins and notify them.
    Admins are defined in ADMIN_IDS.
    """
    user = update.effective_user
    if not user or not update.message:
        return
    data = load_data()
    if user.id not in data.get("awaiting_screenshot", []):
        # Not expecting screenshot; ignore or optionally forward to owner
        await update.message.reply_text("I'm not currently expecting a screenshot from you, but thanks!")
        return

    # forward the sent photo(s) to admins
    for admin_id in ADMIN_IDS:
        try:
            # forward original message
            await context.bot.forward_message(chat_id=admin_id, from_chat_id=user.id, message_id=update.message.message_id)
            await context.bot.send_message(chat_id=admin_id, text=f"📸 Screenshot forwarded from {user_handle(user)}")
        except Exception as e:
            logger.error("Failed forward screenshot to admin %s: %s", admin_id, e)
    # remove user from awaiting list
    try:
        data["awaiting_screenshot"].remove(user.id)
    except ValueError:
        pass
    save_data(data)
    await update.message.reply_text("✅ Thanks for the screenshot! Admins have been notified.")


# ---------------------------
# Direct code handler (plain text)
# ---------------------------

@channel_required
@check_banned
async def handle_direct_code(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.message.text:
        return
    text = update.message.text.strip()
    candidate = text.upper().split()[0]
    if validate_code_format(candidate):
        await process_redemption(update, context, code=candidate)


# ---------------------------
# Forward user messages to owner (fallback)
# ---------------------------


async def forward_to_owner(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Fallback that forwards any non-admin non-command message to admins (owner)."""
    if not update.message:
        return
    user = update.effective_user
    if not user:
        return
    # don't forward admin messages (they have their own handlers)
    if user.id in ADMIN_IDS:
        return
    info = f"👆 Message from {user_handle(user)}\nType: {update.message.content_type}"
    for admin_id in ADMIN_IDS:
        try:
            await context.bot.forward_message(chat_id=admin_id, from_chat_id=update.message.chat_id, message_id=update.message.message_id)
            await context.bot.send_message(chat_id=admin_id, text=info)
        except Exception as e:
            logger.error("Failed forwarding to admin %s: %s", admin_id, e)
    # optionally notify user
    await update.message.reply_text("Message forwarded to the owner. Thank you.")


# ---------------------------
# Bot setup and main
# ---------------------------


def build_application() -> Application:
    app = Application.builder().token(BOT_TOKEN).build()

    # --- User commands ---
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("redeem", redeem))
    app.add_handler(CommandHandler("leaderboard", leaderboard))

    # --- Admin commands ---
    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(CommandHandler("listcodes", list_codes))
    app.add_handler(CommandHandler("addcode", add_code))
    app.add_handler(CommandHandler("addprize", add_prize))
    app.add_handler(CommandHandler("delcode", del_code))
    app.add_handler(CommandHandler("resetgiveaway", reset_giveaway))
    app.add_handler(CommandHandler("gencode", gencode))
    app.add_handler(CommandHandler("broadcast", broadcast))
    app.add_handler(CommandHandler("ban", ban_user))
    app.add_handler(CommandHandler("unban", unban_user))
    app.add_handler(CommandHandler("stopbot", stop_bot))

    # --- Admin file & text prize handlers ---
    app.add_handler(
        MessageHandler(filters.Document.ALL & filters.User(user_id=ADMIN_IDS), handle_admin_file)
    )
    app.add_handler(
        MessageHandler(filters.TEXT & filters.User(user_id=ADMIN_IDS) & (~filters.COMMAND), handle_admin_prizes)
    )

    # --- Screenshot handler (non-admins only) ---
    app.add_handler(
        MessageHandler(filters.PHOTO & (~filters.User(user_id=ADMIN_IDS)), handle_screenshot)
    )

    # --- Direct code handler (non-admin) ---
    app.add_handler(
    MessageHandler(filters.TEXT & (~filters.COMMAND) & (~filters.User(user_id=ADMIN_IDS)), handle_direct_code)
)

    # --- Forward fallback (non-admin) ---
    app.add_handler(
        MessageHandler(filters.ALL & (~filters.COMMAND) & (~filters.User(user_id=ADMIN_IDS)), forward_to_owner)
    )

    return app
 

import asyncio

def main():
    app = build_application()
    logger.info("Starting Giveaway Bot...")

    # Manual bot initialization for Pydroid3
    loop = asyncio.get_event_loop()
    loop.run_until_complete(app.bot.initialize())

    # Start polling
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()