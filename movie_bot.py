# movie_bot_complete.py
"""
MovieShop - complete Telegram bot + Payhero callback handling + admin panel.
Save as movie_bot_complete.py and set env variables in .env.

.env expected keys (examples):
TELEGRAM_TOKEN=...
ADMIN_TELEGRAM_ID=...
PAYHERO_USERNAME=...
PAYHERO_PASSWORD=...
PAYHERO_CHANNEL_ID=...
YOUR_PUBLIC_CALLBACK_URL=https://abc.ngrok-free.app
PAYHERO_API=https://backend.payhero.co.ke/api/v2/payments
PORT=5000
RUN_MODE_POLLING=1
DEBUG_TOKEN=my-debug-token-123
DB_FILE=bot.db
LOG_LEVEL=INFO
"""
import os
import time
import base64
import json
import logging
import sqlite3
import threading
from typing import Optional

import requests
from flask import Flask, request, jsonify
import telebot
from dotenv import load_dotenv

load_dotenv()

# ---------------- Config from .env ----------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN") or os.getenv("BOT_TOKEN")
ADMIN_TELEGRAM_ID = int(os.getenv("ADMIN_TELEGRAM_ID") or "0")
PAYHERO_USERNAME = os.getenv("PAYHERO_USERNAME") or os.getenv("PAYHERO_USER")
PAYHERO_PASSWORD = os.getenv("PAYHERO_PASSWORD") or os.getenv("PAYHERO_PASS")
PAYHERO_CHANNEL_ID = os.getenv("PAYHERO_CHANNEL_ID") or os.getenv("PAYHERO_CHANNEL")
YOUR_PUBLIC_CALLBACK_URL = os.getenv("YOUR_PUBLIC_CALLBACK_URL")  # e.g. https://abc.ngrok-free.app
PAYHERO_API = os.getenv("PAYHERO_API", "https://backend.payhero.co.ke/api/v2/payments")
PORT = int(os.getenv("PORT", "5000"))
RUN_MODE_POLLING = os.getenv("RUN_MODE_POLLING", "1") == "1"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
DB_FILE = os.getenv("DB_FILE", "bot.db")
DEBUG_TOKEN = os.getenv("DEBUG_TOKEN", "debug-secret-token")

missing = []
if not TELEGRAM_TOKEN:
    missing.append("TELEGRAM_TOKEN or BOT_TOKEN")
if not PAYHERO_USERNAME:
    missing.append("PAYHERO_USERNAME or PAYHERO_USER")
if not PAYHERO_PASSWORD:
    missing.append("PAYHERO_PASSWORD or PAYHERO_PASS")
if not PAYHERO_CHANNEL_ID:
    missing.append("PAYHERO_CHANNEL_ID or PAYHERO_CHANNEL")
if missing:
    raise SystemExit("Missing environment variables: " + ", ".join(missing))

try:
    PAYHERO_CHANNEL_ID = int(PAYHERO_CHANNEL_ID)
except Exception:
    raise SystemExit("PAYHERO_CHANNEL_ID must be an integer")

# ---------------- Logging ----------------
logging.basicConfig(level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
                    format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ---------------- Bot + Flask ----------------
bot = telebot.TeleBot(TELEGRAM_TOKEN)
app = Flask(__name__)

# ---------------- Database helpers ----------------
def get_db_conn():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        chat_id INTEGER PRIMARY KEY,
        balance REAL DEFAULT 0
    )""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS movies (
        id TEXT PRIMARY KEY,
        title TEXT,
        price REAL,
        file_id TEXT,
        thumb_url TEXT,
        description TEXT
    )""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS purchases (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_id INTEGER,
        movie_id TEXT,
        amount REAL,
        method TEXT,
        external_ref TEXT,
        ts INTEGER
    )""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS pending_payments (
        external_ref TEXT PRIMARY KEY,
        chat_id INTEGER,
        movie_id TEXT,
        amount REAL,
        type TEXT,
        phone TEXT,
        status TEXT,
        checkout_id TEXT,
        created_at INTEGER
    )""")
    conn.commit()
    conn.close()

init_db()

# ---------------- Core DB functions ----------------
def ensure_user(chat_id: int):
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("INSERT OR IGNORE INTO users(chat_id, balance) VALUES (?, ?)", (chat_id, 0.0))
    conn.commit()
    conn.close()

def get_balance(chat_id: int) -> float:
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("SELECT balance FROM users WHERE chat_id = ?", (chat_id,))
    row = cur.fetchone()
    conn.close()
    return 0.0 if not row else float(row["balance"])

def add_balance(chat_id: int, amount: float):
    conn = get_db_conn()
    cur = conn.cursor()
    ensure_user(chat_id)
    cur.execute("UPDATE users SET balance = balance + ? WHERE chat_id = ?", (amount, chat_id))
    conn.commit()
    conn.close()

def set_balance(chat_id: int, amount: float):
    conn = get_db_conn()
    cur = conn.cursor()
    ensure_user(chat_id)
    cur.execute("UPDATE users SET balance = ? WHERE chat_id = ?", (amount, chat_id))
    conn.commit()
    conn.close()

def charge_balance(chat_id: int, amount: float) -> bool:
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("SELECT balance FROM users WHERE chat_id = ?", (chat_id,))
    row = cur.fetchone()
    if not row or row["balance"] < amount - 1e-9:
        conn.close()
        return False
    cur.execute("UPDATE users SET balance = balance - ? WHERE chat_id = ?", (amount, chat_id))
    conn.commit()
    conn.close()
    return True

def add_movie(movie_id, title, price, file_id, thumb_url="", description=""):
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("""
      INSERT OR REPLACE INTO movies(id,title,price,file_id,thumb_url,description)
      VALUES (?, ?, ?, ?, ?, ?)""", (movie_id, title, price, file_id, thumb_url, description))
    conn.commit()
    conn.close()

def get_movies():
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM movies")
    rows = cur.fetchall()
    conn.close()
    return [dict(r) for r in rows]

def get_movie(mid):
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM movies WHERE id = ?", (mid,))
    r = cur.fetchone()
    conn.close()
    return dict(r) if r else None

def record_purchase(chat_id, movie_id, amount, method, external_ref=None):
    ts = int(time.time())
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("INSERT INTO purchases(chat_id,movie_id,amount,method,external_ref,ts) VALUES (?,?,?,?,?,?)",
                (chat_id, movie_id, amount, method, external_ref, ts))
    conn.commit()
    conn.close()

def create_pending_payment(external_ref, chat_id, movie_id, amount, ptype, phone, checkout_id=None):
    now = int(time.time())
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("""
      INSERT OR REPLACE INTO pending_payments(external_ref,chat_id,movie_id,amount,type,phone,status,checkout_id,created_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (external_ref, chat_id, movie_id, amount, ptype, phone, "QUEUED", checkout_id, now))
    conn.commit()
    conn.close()

def get_pending(external_ref):
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM pending_payments WHERE external_ref = ?", (external_ref,))
    r = cur.fetchone()
    conn.close()
    return dict(r) if r else None

def set_pending_status(external_ref, status):
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("UPDATE pending_payments SET status = ? WHERE external_ref = ?", (status, external_ref))
    conn.commit()
    conn.close()

def list_purchases(chat_id):
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM purchases WHERE chat_id = ? ORDER BY ts DESC LIMIT 50", (chat_id,))
    rows = cur.fetchall()
    conn.close()
    return [dict(r) for r in rows]

# ---------------- Utilities ----------------
def basic_auth_header(username, password):
    token = base64.b64encode(f"{username}:{password}".encode()).decode()
    return {"Authorization": f"Basic {token}", "Content-Type": "application/json"}

def make_external_reference(chat_id, what="TOPUP", movie_id=None):
    ts = int(time.time())
    if what == "TOPUP":
        return f"TOPUP-{chat_id}-{ts}"
    else:
        return f"PUR-{chat_id}-{movie_id}-{ts}"

def normalize_phone(text: str) -> Optional[str]:
    if not text:
        return None

    txt = text.strip().replace(" ", "").replace("-", "")

    # If number starts with +2547xxx or +2541xxx
    if txt.startswith("+254") and len(txt) >= 12:
        return txt

    # If number starts with 07xxx or 01xxx
    if txt.startswith("07") or txt.startswith("01"):
        return "+254" + txt[1:]

    # If number starts with 7xxx or 1xxx
    if txt.startswith("7") or txt.startswith("1"):
        return "+254" + txt

    # If number already starts with 2547 or 2541 (no +)
    if txt.startswith("254") and len(txt) >= 12:
        return "+" + txt

    # Otherwise invalid
    return None

# ---------------- Seed demo movies (defined after add_movie so it's safe) ----------------
# Replace FILE_ID_* placeholders with real file_ids you get via /get_file_id
# Also replace THUMB_FILE_ID_* placeholders with real Telegram photo file_ids (or a direct image URL)
add_movie(
  "xv 1",
  "GOOD FUCK",
  5.0,
  "BAACAgQAAxkBAAIFXmkDkWxp18UK6JUv0WqAlucWzvckAAKeIQACVFoZUPDWXcct9XkRNgQ",  # video file_id
  "AgACAgQAAxkBAAIFgWkDk9Vj-F2UR6z-nSTdlen1at90AAJQDWsbVFoZUIA6Ynm4Y1y5AQADAgADeAADNgQ",  # poster file_id
  "."
)
add_movie(
  "xv 2",
  "GOOD FUCK",
  5.0,
  "BAACAgQAAxkBAAIFhGkDll58d2mvqSyavPaWmyhox2OSAAK-IQACVFoZUGwizDcKNCnrNgQ",  # video file_id
  "AgACAgQAAxkBAAIFg2kDll5ntGN7fgPSYWb8Q48l5UbbAAI0DWsbVFoZUKeGvlsfxONQAQADAgADeAADNgQ",  # poster file_id
  "."
)
add_movie(
  "xv 3",
  "GOOD FUCK",
  5.0,
  "BAACAgQAAxkBAAIFh2kDlnS-kr_8PjbWKMgOiqMlo6RfAALKIQACVFoZUHlsKqNdFXn2NgQ",  # video file_id
  "AgACAgQAAxkBAAIFiGkDlnR5yE0iUBaLn318sFoFdYqOAAI_DWsbVFoZUCXW3zohI__VAQADAgADeAADNgQ",  # poster file_id
  "."
)
add_movie(
  "xv 4",
  "GOOD FUCK",
  5.0,
  "BAACAgQAAxkBAAIFi2kDl1JhJX5llrO7DO4sFUJyBpgWAALSIQACVFoZUItKi1_x2RnuNgQ",  # video file_id
  "AgACAgQAAxkBAAIFjGkDl1JntrDnO9WL_hV8tdFXlQjuAAJCDWsbVFoZUFSumo7dOaioAQADAgADeAADNgQ",  # poster file_id
  "."
)
add_movie(
  "xv 5",
  "GOOD FUCK",
  5.0,
  "BAACAgQAAxkBAAIFj2kDl2OYeQzA9N0GfdDhpM82qZQGAALUIQACVFoZUHPZ1QdxhQluNgQ",  # video file_id
  "AgACAgQAAxkBAAIFkGkDl2NqB5okNRebzhXwN9zHZpwAA0MNaxtUWhlQARw4gs9zEkgBAAMCAAN4AAM2BA",  # poster file_id
  "."
)
add_movie(
  "xv 6",
  "GOOD FUCK",
  5.0,
  " BAACAgQAAxkBAAIGAmkDoQ2RR2tkILv-n38WIlrJxMhRAALtIQACVFoZUMGOGu_Cm50aNgQ",  # video file_id
  "AgACAgQAAxkBAAIGA2kDoQ22argBmq72LtUS1pT-YpwYAAJ6DWsbVFoZUD-CtmOzvoIqAQADAgADeAADNgQ",  # poster file_id
  "."
)
add_movie(
  "xv 7",
  "GOOD FUCK",
  5.0,
  "BAACAgQAAxkBAAIGBmkDoSF9FAdFwe32llJfGySbT8TfAALuIQACVFoZUGyUzMnr4yLqNgQ",  # video file_id
  "AgACAgQAAxkBAAIGB2kDoSFDFUle3E_volqXgLjat2ARAAICC2sbVFohUGtnC1PbERuGAQADAgADeAADNgQ",  # poster file_id
  "."
)
add_movie(
  "xv 8",
  "GOOD FUCK",
  5.0,
  "BAACAgQAAxkBAAIGC2kDoTC4NASTy7l2Zh2uDrcM8uQOAAJ8GAACVFohUFmMZHnvbFutNgQ",  # video file_id
  "AgACAgQAAxkBAAIGCmkDoTBYEeIn0FazFO__IPCKr-4wAAIDC2sbVFohUJcLxl-9_2b9AQADAgADeAADNgQ",  # poster file_id
  "."
)
add_movie(
  "xv 9",
  "GOOD FUCK",
  5.0,
  "BAACAgQAAxkBAAIGDmkDoT4czPOAOophvtFHth7fC8cCAAKAGAACVFohUKYk6KuzDoyANgQ",  # video file_id
  "AgACAgQAAxkBAAIGD2kDoT7JSQsimSH7tDKBWOhK55kXAAIGC2sbVFohUAOaPGjnjFiBAQADAgADeAADNgQ",  # poster file_id
  "."
)
add_movie(
  "xv 10",
  "GOOD FUCK",
  5.0,
  "BAACAgQAAxkBAAIGM2kDsnzcAAGet88zIX3igrNSJfOhswAC8xgAAlRaIVB-lgJVLnZjBzYE",  # video file_id
  "AgACAgQAAxkBAAIGMmkDsnwF0p4N3OkPAAEh7Agy2gzRMQACXQtrG1RaIVBmegVK91HmmwEAAwIAA3gAAzYE",  # poster file_id
  "."
)
add_movie(
  "xv 11",
  "GOOD FUCK",
  10.0,
  "BAACAgQAAxkBAAIGN2kDso-ew3D_x0pDL8jvY7P3IJOlAAL2GAACVFohUDutVPu564KsNgQ",  # video file_id
  "AgACAgQAAxkBAAIGNmkDso_QV1QzzK_LbXzAAAH8EH7P9gACXgtrG1RaIVD1C_QSEyuldwEAAwIAA3gAAzYE",  # poster file_id
  "."
)
add_movie(
  "xv 12",
  "GOOD FUCK",
  5.0,
  "BAACAgQAAxkBAAIGO2kDsqNLj5B2LMDcEVCu8ufva7KdAAL3GAACVFohUBZcgfPT_xhMNgQ",  # video file_id
  "AgACAgQAAxkBAAIGOmkDsqMzLgndAlx4SP93dx6A2DR6AAJfC2sbVFohUPr9B0hc7B11AQADAgADeAADNgQ",  # poster file_id
  "."
)
add_movie(
  "xv 13",
  "GOOD FUCK",
  10.0,
  "BAACAgQAAxkBAAIGP2kDsrFVPXethopRjdYziIu9Mop9AAL8GAACVFohUCfhH09mUwABCTYE",  # video file_id
  "AgACAgQAAxkBAAIGPmkDsrH15EDUDZJnGHteX8OQB8NQAAJgC2sbVFohUMLzKM2nezjoAQADAgADeAADNgQ",  # poster file_id
  "."
)
add_movie(
  "xv 14",
  "GOOD FUCK",
  10.0,
  "BAACAgQAAxkBAAIGQ2kDsui7CasEBSVTMygW4k12VFpOAAL9GAACVFohUEqynJgGWvkfNgQ",  # video file_id
  "AgACAgQAAxkBAAIGQmkDsujetqxeW36xtPDFRfs404geAAJhC2sbVFohUFn7JqKdl3BvAQADAgADeAADNgQ",  # poster file_id
  "."
)
add_movie(
  "xv 15",
  "GOOD FUCK",
  10.0,
  "BAACAgQAAxkBAAIGR2kDtN9A5U0zxGoEA44LsbAnI-nOAAMZAAJUWiFQqQRIpVVD15A2BA",  # video file_id
  "AgACAgQAAxkBAAIGRmkDtN-cHqyRogaWTrG0iqx2T3IDAAJlC2sbVFohUF1h_mnwsTW2AQADAgADeAADNgQ",  # poster file_id
  "."
)
add_movie(
  "xv 16",
  "GOOD FUCK",
  5.0,
  "BAACAgQAAxkBAAIGS2kDtOzynKqy8eW1VdMYIWUN9yNEAAIBGQACVFohUAq6EJmhtAPLNgQ",  # video file_id
  "AgACAgQAAxkBAAIGSmkDtOwFHOi-547BCJeUEFQH4V01AAJmC2sbVFohUBuyO9LeIogJAQADAgADeAADNgQ",  # poster file_id
  "."
)
add_movie(
  "xv 17",
  "GOOD FUCK",
  5.0,
  "BAACAgQAAxkBAAIGT2kDtRvLqq_WdY9wSiw68E6hhSv5AAICGQACVFohUB9Z4j-UwOOlNgQ",  # video file_id
  "AgACAgQAAxkBAAIGTmkDtRvkUze86exHbF4CWWbiyqnlAAJnC2sbVFohUIxMI4Vh2YdPAQADAgADeAADNgQ",  # poster file_id
  "."
)
add_movie(
  "xv 18",
  "GOOD FUCK",
  5.0,
  "BAACAgQAAxkBAAIGV2kDtXWGcXBCky7LruV2CzTX5qaNAAIEGQACVFohUPHLy-8CAvD3NgQ",  # video file_id
  "AgACAgQAAxkBAAIGUmkDtSxAwRXbwhGoPTTNn_I_4F9pAAJoC2sbVFohUNzR5DhVxJY_AQADAgADeAADNgQ",  # poster file_id
  "."
)
add_movie(
  "xv 19",
  "GOOD FUCK",
  5.0,
  "BAACAgQAAxkBAAIGU2kDtSzzc1A5y0lQkC1RHj4GPyC_AAIDGQACVFohUJZdCT7FsHXENgQ",  # video file_id
  "AgACAgQAAxkBAAIGVmkDtXVv7PsyclQ9vh21hWAFMJkaAAJpC2sbVFohUI6GpCPKcS24AQADAgADeAADNgQ",  # poster file_id
  "."
)
add_movie(
  "xv 20",
  "GOOD FUCK",
  5.0,
  "BAACAgQAAxkBAAIGW2kDtZFXRbV1z9cMk9Uuc2tVjhwiAAIFGQACVFohUOPiMoQCSMO-NgQ",  # video file_id
  "AgACAgQAAxkBAAIGWmkDtZExeINrUnwk1S9VP7nbeJDEAAJqC2sbVFohUF04lo8qcnWvAQADAgADeAADNgQ",  # poster file_id
  "."
)
add_movie(
  "xv 21",
  "GOOD FUCK",
  5.0,
  " BAACAgQAAxkBAAIGX2kDtZ73LgFtfhmKME8rC5ApCHrlAAIGGQACVFohULYDEQdY4YFwNgQ",  # video file_id
  "AgACAgQAAxkBAAIGXmkDtZ52zRDwW_qNMT3nVOkNXXrUAAJrC2sbVFohUJc2GVi95rRlAQADAgADeAADNgQ",  # poster file_id
  "."
)
add_movie(
  "xv 22",
  "GOOD FUCK",
  5.0,
  "BAACAgQAAxkBAAIGY2kDtcaQjdW-9_fgxgOPYFk_Bo2cAAIHGQACVFohUFrH-lhC6GbPNgQ",  # video file_id
  "AgACAgQAAxkBAAIGYmkDtcbtmAg1eHNQK9x0akYW0O3MAAJsC2sbVFohUIPQyWa2Hc9oAQADAgADeAADNgQ",  # poster file_id
  "."
)
add_movie(
  "xv 23",
  "GOOD FUCK",
  5.0,
  "BAACAgQAAxkBAAIGZ2kDv8h_lrrLmcTJAAG2-qY_hRtEDAACCRkAAlRaIVBpTsDVS5iJsjYE",  # video file_id
  "AgACAgQAAxkBAAIGZmkDv8hLWttQO1HFRoMzG13IlVuLAAJtC2sbVFohUNEIXspW1EpvAQADAgADeAADNgQ",  # poster file_id
  "."
)
add_movie(
  "xv 24",
  "GOOD FUCK",
  5.0,
  "BAACAgQAAxkBAAIGa2kDv9N763py9l4rBsp5YVdFGdAXAAIKGQACVFohUJ-P_oMd7P9gNgQ",  # video file_id
  "AgACAgQAAxkBAAIGamkDv9N7czdr2UfT2lQiE94gfZ0FAAJuC2sbVFohUNfeJS2uptMTAQADAgADeAADNgQ",  # poster file_id
  "."
)
add_movie(
  "xv 25",
  "GOOD FUCK",
  5.0,
  "BAACAgQAAxkBAAIGb2kDv-FbfAE4oyRI9Wo7f--OEJYPAAIRGQACVFohUEPOlByFxVunNgQ",  # video file_id
  "AgACAgQAAxkBAAIGbmkDv-F5OBctrhqRxGd64wz2qsZAAAJ3C2sbVFohUL-POketsB3ZAQADAgADeAADNgQ",  # poster file_id
  "."
)
add_movie(
  "xv 26",
  "GOOD FUCK",
  5.0,
  "BAACAgQAAxkBAAIGc2kDwA-E0nZsSAsZDCswO_yvs60DAAIUGQACVFohUD5bAXHKUMlwNgQ",  # video file_id
  "AgACAgQAAxkBAAIGcmkDwA-hQPobBqYhIK2OQzJ64CmnAAJ4C2sbVFohUKcnH-ZfhDwYAQADAgADeAADNgQ",  # poster file_id
  "."
)
add_movie(
  "xv 27",
  "GOOD FUCK",
  5.0,
  "BAACAgQAAxkBAAIGd2kDwB4ieslpcrFeT3IDYVWlPgH-AAIVGQACVFohUKyhrX-8JjNoNgQ",  # video file_id
  "AgACAgQAAxkBAAIGdmkDwB4CW_a2WDN7Gj_O8KBn-C8dAAJ5C2sbVFohUJognrK2Q6bnAQADAgADeAADNgQ",  # poster file_id
  "."
)
add_movie(
  "xv 28",
  "GOOD FUCK",
  5.0,
  "BAACAgQAAxkBAAIGe2kDwCxWcUlhjVkQAAH0uKM3tW7-ZAACFhkAAlRaIVBIeCFBpIvq1jYE",  # video file_id
  "AgACAgQAAxkBAAIGemkDwCyW3_onq_lu7mCjURo2su3dAAJ6C2sbVFohUBcDJtZ9o72iAQADAgADeAADNgQ",  # poster file_id
  "."
)
add_movie(
  "xv 29",
  "GOOD FUCK",
  5.0,
  "BAACAgQAAxkBAAIGf2kDwDqEhdAFjLk1p1yuorGedbEXAAIXGQACVFohUKJPCTrSN3CnNgQ",  # video file_id
  "AgACAgQAAxkBAAIGfmkDwDr9jwSeDIF74ANCcha7MZaUAAJ8C2sbVFohUPbx1i02U58rAQADAgADeAADNgQ",  # poster file_id
  "."
)
add_movie(
  "xv 30",
  "GOOD FUCK",
  5.0,
  "BAACAgQAAxkBAAIGg2kDwFAd7Ra_4QyTHZPxNWdfdzmeAAIYGQACVFohUHE-UCeT__5qNgQ",  # video file_id
  "AgACAgQAAxkBAAIGgmkDwFCtOushy_pZfj-42a3BNil_AAJ7C2sbVFohUHTEuRbukFNkAQADAgADeAADNgQ",  # poster file_id
  "."
)

# ---------------- Flask health & debug endpoints ----------------
@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "time": int(time.time())}), 200

@app.route("/debug/pending", methods=["GET"])
def debug_pending():
    token = request.args.get("token", "")
    if token != DEBUG_TOKEN:
        return jsonify({"status": "forbidden"}), 403
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM pending_payments ORDER BY created_at DESC LIMIT 200")
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return jsonify(rows), 200

@app.route("/debug/echo", methods=["POST"])
def debug_echo():
    p = request.get_json(force=True, silent=True)
    logger.info("DEBUG ECHO payload: %s", json.dumps(p)[:2000])
    return jsonify({"received": True, "keys": list(p.keys())}), 200

# ---------------- Telegram handlers ----------------
def main_keyboard(chat_id):
    kb = telebot.types.ReplyKeyboardMarkup(resize_keyboard=True)
    row = ["🎞️ Movies", "💰 Balance"]
    if chat_id == ADMIN_TELEGRAM_ID:
        row.append("🛠️ Admin")
    kb.row(*row)
    kb.row("🛍️ Purchases", "❓ Help")
    return kb

@bot.message_handler(commands=["start"])
def cmd_start(msg):
    chat_id = msg.chat.id
    ensure_user(chat_id)
    text = "🎬 Welcome to XVIDEOS MovieShop!We are updating new videos everyday 20 videos! Use the keyboard to browse and buy movies For help chat with admin https://t.me/Chatme533bot."
    bot.send_message(chat_id, text, reply_markup=main_keyboard(chat_id))

@bot.message_handler(commands=["help", "howtopay"])
def cmd_help(msg):
    chat_id = msg.chat.id
    help_text = (
        "How to use:\n"
        "• Press 🎞️ Movies to view available movies.\n"
        "• On each movie card press Buy (Balance) or Buy (STK).\n"
        "• Buy (Balance) sends the movie immediately if you have enough balance.\n"
        "• Buy (STK) sends an M-Pesa prompt to your phone. After you complete payment, the bot delivers the movie.\n"
        "• Top-up: send /topup or press Buy (STK) and follow prompts.\n"
        "• CONTACT Admins: press https://t.me/Chatme533bot"
    )
    bot.send_message(chat_id, help_text)

@bot.message_handler(commands=["contact"])
def cmd_contact(msg):
    chat_id = msg.chat.id
    if ADMIN_TELEGRAM_ID:
        bot.send_message(chat_id, f"Admin contact: Telegram ID `{ADMIN_TELEGRAM_ID}`", parse_mode="Markdown")
    else:
        bot.send_message(chat_id, "Admin not set.")

@bot.message_handler(commands=["movies"])
def cmd_movies(msg):
    chat_id = msg.chat.id
    movies = get_movies()
    if not movies:
        bot.send_message(chat_id, "No movies available.")
        return
    for m in movies:
        caption = f"*{m['title']}*\nKES {m['price']}\n\n{m.get('description','')}"
        markup = telebot.types.InlineKeyboardMarkup()
        # two buttons: Buy (Balance) and Buy (STK)
        markup.add(
            telebot.types.InlineKeyboardButton(f"🧾 Buy (Balance) — KES {m['price']}", callback_data=f"pay_bal:{m['id']}"),
            telebot.types.InlineKeyboardButton(f"💳 Buy (STK) — KES {m['price']}", callback_data=f"pay_stk:{m['id']}")
        )
        markup.add(telebot.types.InlineKeyboardButton("ℹ️ Details", callback_data=f"details:{m['id']}"))
        try:
            if m.get("thumb_url"):
                bot.send_photo(chat_id, m["thumb_url"], caption=caption, parse_mode="Markdown", reply_markup=markup)
            else:
                bot.send_message(chat_id, caption, parse_mode="Markdown", reply_markup=markup)
        except Exception as e:
            logger.exception("Failed to send movie card: %s", e)
            bot.send_message(chat_id, caption, parse_mode="Markdown", reply_markup=markup)

@bot.callback_query_handler(func=lambda call: True)
def cb_handler(call):
    data = call.data or ""
    chat_id = call.message.chat.id
    ensure_user(chat_id)

    # Buy (STK)
    if data.startswith("pay_stk:"):
        movie_id = data.split(":",1)[1]
        movie = get_movie(movie_id)
        if not movie:
            bot.answer_callback_query(call.id, "Movie not found.")
            return
        bot.answer_callback_query(call.id)
        bot.send_message(chat_id, f"To pay KES {movie['price']} for *{movie['title']}* via STK, send your phone number (07XXXXXXXX or 01XXXXXXX or +2541XXXXXXXX).", parse_mode="Markdown")
        awaiting_phone[chat_id] = {"type":"purchase", "movie_id":movie_id, "amount": movie['price']}
        return

    # Buy (Balance)
    if data.startswith("pay_bal:"):
        movie_id = data.split(":",1)[1]
        movie = get_movie(movie_id)
        if not movie:
            bot.answer_callback_query(call.id, "Movie not found.")
            return
        bot.answer_callback_query(call.id)
        balance = get_balance(chat_id)
        if balance + 5e-9 >= float(movie['price']):
            ok = charge_balance(chat_id, float(movie['price']))
            if ok:
                record_purchase(chat_id, movie_id, movie['price'], "balance", external_ref=None)
                bot.send_message(chat_id, f"✅ Paid KES {movie['price']:.2f} from balance. Sending *{movie['title']}* now...", parse_mode="Markdown")
                send_movie_by_fileid(chat_id, movie['file_id'], movie['title'])
            else:
                bot.send_message(chat_id, "⚠️ Error charging your balance. Try again later.")
        else:
            bot.send_message(chat_id, f"❌ Insufficient balance (you have KES {balance:.2f}, movie costs KES {movie['price']:.2f}). Use /topup or Buy (STK).")
        return

    # Details
    if data.startswith("details:"):
        movie_id = data.split(":",1)[1]
        movie = get_movie(movie_id)
        bot.answer_callback_query(call.id)
        if not movie:
            bot.send_message(chat_id, "Movie not found.")
            return
        txt = f"*{movie['title']}*\nPrice: KES {movie['price']}\n\n{movie.get('description','')}"
        bot.send_message(chat_id, txt, parse_mode="Markdown")
        return

# in-memory awaiting phone map (chat_id -> dict)
awaiting_phone = {}

@bot.message_handler(commands=["topup"])
def cmd_topup(msg):
    chat_id = msg.chat.id
    ensure_user(chat_id)
    bot.send_message(chat_id, "To top up, send the amount you want to add (e.g. 50).")
    awaiting_phone[chat_id] = {"type":"topup_wait_amount"}

@bot.message_handler(commands=["balance"])
def cmd_balance(msg):
    chat_id = msg.chat.id
    bal = get_balance(chat_id)
    bot.send_message(chat_id, f"Your balance: KES {bal:.2f}")

@bot.message_handler(commands=["purchases"])
def cmd_purchases(msg):
    chat_id = msg.chat.id
    rows = list_purchases(chat_id)
    if not rows:
        bot.send_message(chat_id, "No purchases yet.")
        return
    txt = "Your purchases:\n\n"
    for r in rows[:20]:
        t = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(r["ts"]))
        txt += f"- {r['movie_id']} — KES {r['amount']} — {r['method']} — {t}\n"
    bot.send_message(chat_id, txt)

@bot.message_handler(commands=["get_file_id"])
def cmd_get_file_id(msg):
    bot.send_message(msg.chat.id, "Send a file (video/document/photo) and I'll reply with its file_id.")

@bot.message_handler(commands=["admin"])
def cmd_admin(msg):
    if msg.from_user.id != ADMIN_TELEGRAM_ID:
        bot.reply_to(msg, "Unauthorized.")
        return
    # show admin inline panel
    markup = telebot.types.InlineKeyboardMarkup()
    markup.add(telebot.types.InlineKeyboardButton("Admin Panel", callback_data="admin:panel"))
    bot.send_message(msg.chat.id, "Admin actions:", reply_markup=markup)

@bot.message_handler(commands=["admin_add", "admin_remove"])
def cmd_admin_modify(msg):
    if msg.from_user.id != ADMIN_TELEGRAM_ID:
        bot.reply_to(msg, "Unauthorized.")
        return
    parts = msg.text.split()
    if len(parts) < 3:
        bot.reply_to(msg, "Usage: /admin_add <chat_id> <amount>  OR  /admin_remove <chat_id> <amount>")
        return
    cmd = parts[0]
    try:
        target = int(parts[1])
        amount = float(parts[2])
    except:
        bot.reply_to(msg, "Invalid chat_id or amount.")
        return
    if cmd == "/admin_add":
        ensure_user(target)
        add_balance(target, amount)
        bot.reply_to(msg, f"Added KES {amount:.2f} to {target}.")
        try:
            bot.send_message(target, f"📥 Admin added KES {amount:.2f} to your balance.")
        except Exception:
            pass
    else:
        ensure_user(target)
        bal = get_balance(target)
        newbal = max(0.0, bal - amount)
        set_balance(target, newbal)
        bot.reply_to(msg, f"Removed KES {amount:.2f} from {target}. New balance {newbal:.2f}.")
        try:
            bot.send_message(target, f"⚠️ Admin removed KES {amount:.2f} from your balance. New balance KES {newbal:.2f}.")
        except Exception:
            pass

@bot.message_handler(content_types=['document','video','audio','photo'])
def handle_file_for_file_id(m):
    fid = None
    if m.document:
        fid = m.document.file_id
    elif m.video:
        fid = m.video.file_id
    elif m.audio:
        fid = m.audio.file_id
    elif m.photo:
        fid = m.photo[-1].file_id
    if fid:
        bot.reply_to(m, f"file_id: `{fid}`", parse_mode="Markdown")

@bot.message_handler(func=lambda m: True, content_types=['text'])
def handle_text(m):
    chat_id = m.chat.id
    txt = (m.text or "").strip()
    ensure_user(chat_id)

    state = awaiting_phone.get(chat_id)
    if state:
        # purchase via stk
        if state.get("type") == "purchase":
            phone = normalize_phone(txt)
            movie_id = state.get("movie_id")
            movie = get_movie(movie_id)
            awaiting_phone.pop(chat_id, None)
            if not movie:
                bot.send_message(chat_id, "Movie not found.")
                return
            if not phone:
                bot.send_message(chat_id, "Phone not recognised. Use 07XXXXXXXX or +2541XXXXXXXX.")
                return
            external = make_external_reference(chat_id, what="PUR", movie_id=movie_id)
            body = {
                "amount": movie["price"],
                "phone_number": phone,
                "channel_id": PAYHERO_CHANNEL_ID,
                "provider": "m-pesa",
                "external_reference": external,
                "customer_name": m.from_user.first_name or "",
                "callback_url": YOUR_PUBLIC_CALLBACK_URL
            }
            headers = basic_auth_header(PAYHERO_USERNAME, PAYHERO_PASSWORD)
            try:
                resp = requests.post(PAYHERO_API, headers=headers, json=body, timeout=30)
            except Exception as e:
                logger.exception("Payhero request failed: %s", e)
                bot.send_message(chat_id, "Sorry — there was a problem with your request. Try again later.")
                return
            if resp.status_code in (200,201):
                try:
                    data = resp.json()
                except Exception:
                    data = {}
                create_pending_payment(external, chat_id, movie_id, movie["price"], "purchase", phone, checkout_id=data.get("CheckoutRequestID"))
                bot.send_message(chat_id, f"✅ Payment request sent. You should receive a prompt on {phone}. Reference: `{external}`", parse_mode="Markdown")
            else:
                logger.warning("Payhero returned error: %s %s", resp.status_code, resp.text)
                bot.send_message(chat_id, "Sorry — there was a problem with your request. Payhero returned an error. Try again.")
            return

        # topup amount entry
        if state.get("type") == "topup_wait_amount":
            try:
                amount = float(txt)
                if amount <= 0:
                    raise ValueError()
            except:
                bot.send_message(chat_id, "Enter a valid numeric amount, e.g. 50")
                return
            awaiting_phone[chat_id] = {"type":"topup", "amount": amount}
            bot.send_message(chat_id, f"Top-up KES {amount:.2f} — now send your phone number (07XXXXXXXX or +2541XXXXXXXX).")
            return

        # topup phone entry
        if state.get("type") == "topup":
            phone = normalize_phone(txt)
            amount = state.get("amount")
            awaiting_phone.pop(chat_id, None)
            if not phone:
                bot.send_message(chat_id, "Phone not recognised. Use 07XXXXXXXX or +2541XXXXXXXX.")
                return
            external = make_external_reference(chat_id, what="TOPUP")
            body = {
                "amount": amount,
                "phone_number": phone,
                "channel_id": PAYHERO_CHANNEL_ID,
                "provider": "m-pesa",
                "external_reference": external,
                "customer_name": m.from_user.first_name or "",
                "callback_url": YOUR_PUBLIC_CALLBACK_URL
            }
            headers = basic_auth_header(PAYHERO_USERNAME, PAYHERO_PASSWORD)
            try:
                resp = requests.post(PAYHERO_API, headers=headers, json=body, timeout=30)
            except Exception as e:
                logger.exception("Topup request failed: %s", e)
                bot.send_message(chat_id, "Sorry — there was a problem with your request. Try again later.")
                return
            if resp.status_code in (200,201):
                try:
                    data = resp.json()
                except Exception:
                    data = {}
                create_pending_payment(external, chat_id, None, amount, "topup", phone, checkout_id=data.get("CheckoutRequestID"))
                bot.send_message(chat_id, f"✅ Top-up request sent. You should receive a prompt on {phone}. Reference: `{external}`", parse_mode="Markdown")
            else:
                logger.warning("Payhero topup returned error: %s %s", resp.status_code, resp.text)
                bot.send_message(chat_id, "Sorry — there was a problem with your request. Payhero returned an error. Try again.")
            return

    # keyboard quick handlers
    if txt == "🎞️ Movies" or txt.lower() == "/movies":
        cmd_movies(m)
        return
    if txt == "💰 Balance" or txt.lower() == "/balance":
        bot.send_message(chat_id, f"Your balance: KES {get_balance(chat_id):.2f}")
        return
    if txt == "🛍️ Purchases" or txt.lower() == "/purchases":
        cmd_purchases(m)
        return
    if txt == "❓ Help" or txt.lower() == "/help":
        cmd_help(m)
        return
    if txt == "🛠️ Admin" and chat_id == ADMIN_TELEGRAM_ID:
        cmd_admin(m)
        return

    bot.send_message(chat_id, "I didn't understand. Use /movies or the keyboard.")

# ---------------- Sending movies ----------------
def send_movie_by_fileid(chat_id, file_id, title="Your movie"):
    try:
        bot.send_chat_action(chat_id, "upload_document")
        bot.send_document(chat_id, file_id, caption=title)
    except Exception as e:
        logger.exception("Failed to send movie: %s", e)
        bot.send_message(chat_id, "Failed to send the movie. Contact admin.")

# ---------------- Callback processing (centralized) ----------------
def process_callback_payload(payload):
    """
    Robust processing of Payhero callback payload dict.
    Returns (response_dict, status_code)
    """
    logger.info("Processing callback payload: %s", json.dumps(payload)[:2000])

    def deep_find(dct, keys):
        if not isinstance(dct, dict):
            return None
        for k in keys:
            if k in dct:
                return dct[k]
        for v in dct.values():
            if isinstance(v, dict):
                res = deep_find(v, keys)
                if res is not None:
                    return res
            if isinstance(v, list):
                for item in v:
                    if isinstance(item, dict):
                        res = deep_find(item, keys)
                        if res is not None:
                            return res
        return None

    # Try many possible names for external ref
    external = deep_find(payload, ["ExternalReference", "external_reference", "externalReference", "reference", "reference_no"])
    # some providers put it under response.external_reference etc.
    if not external:
        # also check top-level keys that might hold a nested object with external reference
        if "response" in payload and isinstance(payload["response"], dict):
            external = deep_find(payload["response"], ["ExternalReference", "external_reference", "externalReference", "reference"])
    result_code = deep_find(payload, ["ResultCode", "result_code", "resultcode"])
    status_field = deep_find(payload, ["Status", "status", "status_text", "statusText"])

    if not external:
        logger.warning("Callback missing external ref. payload preview: %s", json.dumps(payload)[:1000])
        try:
            if ADMIN_TELEGRAM_ID:
                bot.send_message(ADMIN_TELEGRAM_ID, f"⚠️ Callback missing external reference. Payload:\n`{json.dumps(payload)[:1500]}`", parse_mode="Markdown")
        except Exception:
            logger.exception("Failed to notify admin about missing external ref")
        return {"status": False, "message": "missing external reference"}, 200

    pending = get_pending(external)
    if not pending:
        logger.warning("Unknown external reference: %s", external)
        try:
            if ADMIN_TELEGRAM_ID:
                bot.send_message(ADMIN_TELEGRAM_ID, f"⚠️ Unknown external ref: `{external}`\nPayload:\n`{json.dumps(payload)[:1500]}`", parse_mode="Markdown")
        except Exception:
            logger.exception("Failed to notify admin about unknown external ref")
        return {"status": False, "message": "unknown external reference"}, 200

    # determine success
    success = False
    try:
        if result_code is not None and str(result_code).isdigit() and int(result_code) == 0:
            success = True
        elif isinstance(status_field, str) and status_field.lower() in ("success", "completed", "ok"):
            success = True
    except Exception:
        success = False

    chat_id = pending["chat_id"]
    ptype = pending["type"]
    amount = float(pending["amount"])
    movie_id = pending["movie_id"]

    if success:
        set_pending_status(external, "success")
        if ptype == "topup":
            prev = get_balance(chat_id)
            add_balance(chat_id, amount)
            new_bal = get_balance(chat_id)
            record_purchase(chat_id, None, amount, "stk_topup", external_ref=external)
            try:
                bot.send_message(chat_id, f"✅ Top-up successful — KES {amount:.2f} added.\nPrevious balance: KES {prev:.2f}\nNew balance: KES {new_bal:.2f}")
            except Exception:
                logger.exception("Failed to notify user about topup success")
            return {"status": True}, 200
        elif ptype == "purchase":
            movie = get_movie(movie_id)
            if movie and movie.get("file_id"):
                try:
                    bot.send_message(chat_id, f"✅ Payment confirmed! Sending *{movie['title']}* now...", parse_mode="Markdown")
                    send_movie_by_fileid(chat_id, movie["file_id"], movie["title"])
                    record_purchase(chat_id, movie_id, amount, "stk", external_ref=external)
                    return {"status": True}, 200
                except Exception as e:
                    logger.exception("Delivery failed: %s", e)
                    bot.send_message(chat_id, "Payment confirmed but failed to send movie. Contact admin.")
                    return {"status": False, "message": "delivery failed"}, 200
            else:
                bot.send_message(chat_id, "Payment confirmed but movie missing. Contact admin.")
                return {"status": False, "message": "movie missing"}, 200
    else:
        set_pending_status(external, "failed")
        try:
            if ptype == "topup":
                bot.send_message(chat_id, f"⚠️ Top-up attempt (KES {amount:.2f}) failed or was cancelled. Please try again.")
            else:
                bot.send_message(chat_id, f"⚠️ Payment for your purchase (KES {amount:.2f}) failed or was cancelled. No movie was delivered.")
        except Exception:
            logger.exception("Failed to notify user on payment failure.")
        return {"status": True, "message": "not success"}, 200

# ---------------- Routes accepting GET/OPTIONS/POST ----------------
@app.route("/", methods=["GET","POST","OPTIONS"])
def root_handler():
    if request.method == "GET":
        return (f"MovieBot is running.\nPolling: {RUN_MODE_POLLING}\nPayhero callback (POST): /payhero_callback\n"), 200
    if request.method == "OPTIONS":
        return jsonify({"status": True, "message": "OK"}), 200
    # POST: attempt to parse JSON or form-encoded body
    payload = None
    try:
        payload = request.get_json(force=True, silent=True)
    except Exception:
        payload = None
    if not payload:
        # try parsing form or raw body
        raw = request.get_data(as_text=True) or ""
        # If it's form-encoded like payload={"...json..."} or data=..., try to extract JSON substring
        try:
            # try to parse raw as JSON directly
            payload = json.loads(raw)
        except Exception:
            # try to find a JSON substring
            import re
            m = re.search(r'(\{.*\})', raw, re.S)
            if m:
                try:
                    payload = json.loads(m.group(1))
                except Exception:
                    payload = None
            else:
                payload = None
    if not payload:
        logger.warning("POST to / with no usable JSON body; returning 400. Raw body preview: %s", (request.get_data(as_text=True) or "")[:1000])
        return jsonify({"status": False, "message": "empty or invalid POST body"}), 400
    resp, code = process_callback_payload(payload)
    return jsonify(resp), code

@app.route("/payhero_callback", methods=["GET","POST","OPTIONS"])
def payhero_callback():
    if request.method == "GET":
        return jsonify({"status": True, "message": "Payhero callback endpoint (POST only)"}), 200
    if request.method == "OPTIONS":
        return jsonify({"status": True, "message": "OK"}), 200

    payload = None
    try:
        payload = request.get_json(force=True, silent=True)
    except Exception:
        payload = None
    if not payload:
        raw = request.get_data(as_text=True) or ""
        try:
            payload = json.loads(raw)
        except Exception:
            import re
            m = re.search(r'(\{.*\})', raw, re.S)
            if m:
                try:
                    payload = json.loads(m.group(1))
                except Exception:
                    payload = None
            else:
                payload = None
    if not payload:
        logger.warning("POST to /payhero_callback with empty body; raw preview: %s", (request.get_data(as_text=True) or "")[:1000])
        return jsonify({"status": False, "message": "bad request"}), 400

    resp, code = process_callback_payload(payload)
    return jsonify(resp), code

# ---------------- Start Flask thread + polling ----------------
def start_flask():
    logger.info("Starting Flask callback server on port %s", PORT)
    app.run(host="0.0.0.0", port=PORT, threaded=True)

if __name__ == "__main__":
    flask_thread = threading.Thread(target=start_flask, daemon=True)
    flask_thread.start()

    # try to remove webhook safely
    try:
        bot.remove_webhook()
    except Exception as e:
        logger.warning("Could not remove webhook: %s", e)

    # start polling
    try:
        logger.info("Starting bot polling")
        bot.polling(none_stop=True)
    except Exception as e:
        logger.exception("Bot polling stopped with exception: %s", e)


