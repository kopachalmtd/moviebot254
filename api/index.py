import os
import re
import json
import asyncio
import requests
from flask import Flask, request
from supabase import create_client, Client
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ApplicationBuilder

app = Flask(__name__)

# ---------------- CONFIG ----------------
TOKEN = os.getenv("BOT_TOKEN")
ADMIN_IDS = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]
PAYHERO_API_URL = 'https://backend.payhero.co.ke/api/v2/payments'

# Supabase Setup
supabase: Client = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_ROLE_KEY"))

# Initialize Application (Without starting the polling loop)
application = ApplicationBuilder().token(TOKEN).build()

# ---------------- DATABASE HELPERS ----------------
def get_user(uid):
    res = supabase.table("users").select("data").eq("id", str(uid)).execute()
    if not res.data:
        default_data = {"balance": 0, "purchases": [], "bonus_claimed": False, "state": None}
        supabase.table("users").insert({"id": str(uid), "data": default_data}).execute()
        return default_data
    return res.data[0]['data']

def save_user(uid, data):
    supabase.table("users").upsert({"id": str(uid), "data": data}).execute()

def main_menu_keyboard(uid):
    user = get_user(uid)
    bal_btn = f"💰 KSH {user.get('balance', 0)}"
    kb = [
        [InlineKeyboardButton("🎥 Browse Movies", callback_data="browse_0")],
        [InlineKeyboardButton("💳 Deposit", callback_data="deposit"), InlineKeyboardButton(bal_btn, callback_data="bal")],
        [InlineKeyboardButton("💼 My Purchases", callback_data="myp")],
        [InlineKeyboardButton("🎁 Claim Bonus", callback_data="claim_bonus"), InlineKeyboardButton("🔄 Reset Account", callback_data="req_reset")],
        [InlineKeyboardButton("📢 Join Channel", url="https://t.me/YourChannel"), InlineKeyboardButton("👨‍💻 Admin", url="https://t.me/YourAdmin")],
    ]
    if int(uid) in ADMIN_IDS:
        kb.append([InlineKeyboardButton("🛠 Admin Panel", callback_data="admin_panel")])
    return InlineKeyboardMarkup(kb)

# ---------------- WEBHOOK ENTRY POINT ----------------
@app.route("/", methods=["POST"])
def telegram_webhook():
    # This is the secret to Vercel deployment:
    # We create a manual loop for every single request.
    update_data = request.get_json(force=True)
    
    async def handle_update():
        update = Update.de_json(update_data, application.bot)
        async with application:
            if update.message and update.message.text:
                uid = str(update.effective_user.id)
                text = update.message.text
                if text == "/start":
                    await update.message.reply_text("🎬 **MovieBot254**", reply_markup=main_menu_keyboard(uid))
                # Add your other text_handler logic here...
            
            elif update.callback_query:
                # Add your callback_router logic here...
                await update.callback_query.answer()
                await update.callback_query.message.reply_text("Action Received")

    # Force the async function to run and finish
    asyncio.run(handle_update())
    return "OK", 200

@app.route("/payhero-callback", methods=["POST"])
def payhero_callback():
    # Payment logic stays synchronous (no asyncio needed here)
    return "OK", 200

@app.route("/")
def index():
    return "Bot is running", 200
