import os
import re
import json
import asyncio
import time
from flask import Flask, request
import requests
from supabase import create_client, Client
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ApplicationBuilder, ContextTypes

app = Flask(__name__)

# ---------------- CONFIG ----------------
TOKEN = os.getenv("BOT_TOKEN")
ADMIN_IDS = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]
PAYHERO_API_URL = 'https://backend.payhero.co.ke/api/v2/payments'

# Supabase Config
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Initialize Bot Application (Keep this outside the route)
application = ApplicationBuilder().token(TOKEN).build()

# ---------------- HELPERS ----------------
def format_phone_number(phone):
    phone = re.sub(r"[^0-9]", "", phone)
    if (phone.startswith("07") or phone.startswith("01")) and len(phone) == 10:
        return "254" + phone[1:]
    if (phone.startswith("7") or phone.startswith("1")) and len(phone) == 9:
        return "254" + phone
    if phone.startswith("254") and len(phone) == 12:
        return phone
    return None

def get_user(uid):
    res = supabase.table("users").select("data").eq("id", str(uid)).execute()
    if not res.data:
        default_data = {
            "balance": 0, "purchases": [], "blocked": False, 
            "state": None, "temp_amt": None, "admin_target": None
        }
        supabase.table("users").insert({"id": str(uid), "data": default_data}).execute()
        return default_data
    return res.data[0]['data']

def save_user(uid, data):
    supabase.table("users").upsert({"id": str(uid), "data": data}).execute()

# ---------------- KEYBOARDS ----------------
def main_menu_keyboard(uid):
    kb = [
        [InlineKeyboardButton("🎥 Browse Movies", callback_data="browse_0")],
        [InlineKeyboardButton("💳 Deposit", callback_data="deposit"), InlineKeyboardButton("💰 Balance", callback_data="bal")],
        [InlineKeyboardButton("💼 My Purchases", callback_data="myp")],
        [InlineKeyboardButton("🔄 Reset Account", callback_data="reset")],
        [InlineKeyboardButton("📢 Join Channel", callback_data="join_channel")],
    ]
    if int(uid) in ADMIN_IDS:
        kb.append([InlineKeyboardButton("🛠 Admin Panel", callback_data="admin_panel")])
    return InlineKeyboardMarkup(kb)

# ---------------- HANDLERS ----------------

async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    msg = "🎬 **MovieBot254 Active**"
    if update.message:
        await update.message.reply_text(msg, reply_markup=main_menu_keyboard(uid), parse_mode="Markdown")
    elif update.callback_query:
        await update.callback_query.message.edit_text(msg, reply_markup=main_menu_keyboard(uid), parse_mode="Markdown")

async def callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    uid = str(q.from_user.id)
    user = get_user(uid)
    data = q.data

    if data == "menu":
        await start_handler(update, context)
    elif data == "bal":
        await q.message.edit_text(f"💰 **Your balance:** KES {user['balance']}", reply_markup=main_menu_keyboard(uid), parse_mode="Markdown")
    elif data == "deposit":
        user["state"] = "awaiting_amount"
        save_user(uid, user)
        await q.message.edit_text("💳 **Deposit**\nEnter amount to deposit (KES):")
    elif data == "admin_panel":
        if int(uid) not in ADMIN_IDS: return
        kb = [
            [InlineKeyboardButton("👥 View Users", callback_data="admin_view_users")],
            [InlineKeyboardButton("➕ Add Bal", callback_data="admin_addbal"), InlineKeyboardButton("➖ Rem Bal", callback_data="admin_removebal")],
            [InlineKeyboardButton("⛔ Block/Unblock", callback_data="admin_block")],
            [InlineKeyboardButton("⬅ Back", callback_data="menu")],
        ]
        await q.message.edit_text("🛠 **Admin Control Panel**", reply_markup=InlineKeyboardMarkup(kb), parse_mode="Markdown")
    elif data.startswith("admin_"):
        context.user_data["admin_action"] = data + "_wait_user"
        await q.message.edit_text("🎯 Send the **Target User ID**:")

async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    user = get_user(uid)
    text = (update.message.text or "").strip()

    # Admin Logic
    admin_action = context.user_data.get("admin_action") if context else None
    if admin_action:
        # ... (Your existing admin flow code stays exactly here) ...
        # Ensure context.user_data calls are inside this 'if context' block
        pass 

    # Deposit Logic
    state = user.get("state")
    if state == "awaiting_amount" and text.isdigit():
        user["temp_amt"] = int(text)
        user["state"] = "awaiting_phone"
        save_user(uid, user)
        await update.message.reply_text("📱 Enter M-Pesa Number (07...):")
        return # Important: Stop here
    
    elif state == "awaiting_phone":
        phone = format_phone_number(text)
        if not phone:
            await update.message.reply_text("❌ Invalid format.")
            return

        amt = user["temp_amt"]
        user["state"] = None
        save_user(uid, user)

        headers = {'Authorization': f'Basic {os.getenv("PAYHERO_AUTH")}', 'Content-Type': 'application/json'}
        payload = {
            "amount": amt, "phone_number": phone, "channel_id": int(os.getenv("PAYHERO_CHANNEL_ID", "4131")),
            "provider": "m-pesa", "external_reference": f"{uid}_TOPUP_{int(time.time())}",
            "customer_name": update.effective_user.full_name or "User",
            "callback_url": f"https://{request.host}/payhero-callback"
        }
        
        try:
            resp = requests.post(PAYHERO_API_URL, json=payload, headers=headers, timeout=10)
            if resp.status_code in [200, 201] and resp.json().get("status") == "Success":
                await update.message.reply_text(f"🚀 STK Push sent to {phone}!")
            else:
                await update.message.reply_text(f"⚠️ Error: {resp.json().get('message', 'Rejected')}")
        except Exception as e:
            await update.message.reply_text(f"📡 Connection Error: {str(e)}")

# ---------------- WEBHOOK ----------------
@app.route("/", methods=["POST"])
async def telegram_webhook():
    try:
        data = request.get_json(force=True)
        update = Update.de_json(data, application.bot)
        
        # We need the context for user_data to work!
        async with application:
            if update.message and update.message.text:
                if update.message.text == "/start":
                    await start_handler(update, application.default_context)
                else:
                    # Pass the application context so user_data doesn't crash
                    await text_handler(update, application.default_context)
            elif update.callback_query:
                await callback_router(update, application.default_context)
                
        return "OK", 200
    except Exception as e:
        # If it "freezes", this will help you see the error in Vercel logs
        print(f"WEBHOOK ERROR: {e}")
        return "OK", 200 # Always return 200 to Telegram

@app.route("/payhero-callback", methods=["POST"])
def payhero_callback():
    # ... (Keep your existing callback logic) ...
    return "OK", 200

@app.route("/")
def index():
    return "Bot Online", 200
