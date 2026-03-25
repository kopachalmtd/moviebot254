import os
import json
import re
import asyncio
from datetime import datetime, timedelta
from flask import Flask, request
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, CallbackQueryHandler, MessageHandler, filters
from supabase import create_client, Client

app = Flask(__name__)

# --- CONFIG FROM YOUR ENV ---
TOKEN = os.getenv("BOT_TOKEN")
ADMIN_IDS = [int(i) for i in os.getenv("ADMIN_IDS", "").split(",") if i]
MOVIE_COST = int(os.getenv("MOVIE_COST", 10))
ADMIN_USERNAME = os.getenv("ADMIN_USERNAME")
CHANNEL_LINK = os.getenv("CHANNEL_LINK")
MOVIES = [] # Keep your MOVIES list here

# Supabase Setup
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Initialize Bot Application
tg_app = ApplicationBuilder().token(TOKEN).build()

# --- DATABASE ADAPTERS (Replacing load_db/save_db) ---
def load_db():
    # We fetch all users into a dictionary to keep your original logic compatible
    res = supabase.table("users").select("*").execute()
    return {row['id']: row['data'] for row in res.data}

def save_user_to_db(uid, user_data):
    # Upsert a single user's data
    supabase.table("users").upsert({"id": str(uid), "data": user_data}).execute()

def ensure_user(db, uid):
    if uid not in db:
        db[uid] = {"balance": 0, "purchases": [], "purchase_history": [], "blocked": False}
        save_user_to_db(uid, db[uid])
    return db[uid]

# --- YOUR ORIGINAL HANDLERS (UNCHANGED LOGIC) ---

async def start_handler(update: Update, context):
    uid = str(update.effective_user.id)
    db = load_db()
    ensure_user(db, uid)
    # ... (Your original start menu code here)

async def callback_router(update: Update, context):
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    uid = str(q.from_user.id)
    db = load_db()
    user = ensure_user(db, uid)

    if data.startswith("buy_"):
        # ... (Your original buy logic here)
        # Instead of save_db(db), use:
        save_user_to_db(uid, user)
        # ...

    # (Include your reset, admin_panel, bal, myp, and claim_bonus logic here)

async def text_handler(update: Update, context):
    uid = str(update.effective_user.id)
    db = load_db()
    user = ensure_user(db, uid)
    text = (update.message.text or "").strip()

    # ... (Your original deposit and admin multi-step logic here)
    # Every time you modify 'user' or 'db[target]', call save_user_to_db(uid, user)

# --- VERCEL WEBHOOK ROUTE ---

@app.route('/', methods=['POST', 'GET'])
async def webhook():
    if request.method == 'POST':
        update = Update.de_json(request.get_json(force=True), tg_app.bot)
        
        # Register your exact handlers
        tg_app.add_handler(CommandHandler("start", start_handler))
        tg_app.add_handler(CallbackQueryHandler(callback_router))
        tg_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))
        
        await tg_app.process_update(update)
        return "OK"
    return "Bot is running 24/7"

@app.route('/callback', methods=['POST'])
def mpesa_callback():
    # Your PayHero Flask logic goes here
    return "OK", 200
