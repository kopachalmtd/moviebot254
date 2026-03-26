import os
import re
import json
import asyncio
import base64
import time
from flask import Flask, request
import requests
from supabase import create_client, Client
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ApplicationBuilder, CommandHandler, CallbackQueryHandler, MessageHandler, filters

app = Flask(__name__)

# ---------------- CONFIG ----------------
TOKEN = os.getenv("BOT_TOKEN")
ADMIN_IDS = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]
PAYHERO_API_URL = os.getenv("PAYHERO_API_URL", "https://backend.payhero.co.ke/api/v2/payments")

# Supabase Config
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Initialize Bot Application
application = ApplicationBuilder().token(TOKEN).build()

# ---------------- HELPERS ----------------
def format_phone_number(phone):
    """Sanitizes 07..., 01..., or 254... to 254... format"""
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
async def callback_router(update: Update, context):
    q = update.callback_query
    await q.answer()
    uid = str(q.from_user.id)
    user = get_user(uid)
    data = q.data

    if data == "menu":
        await q.message.edit_text("🎬 **Main Menu**", reply_markup=main_menu_keyboard(uid), parse_mode="Markdown")

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

    elif data == "admin_view_users":
        res = supabase.table("users").select("id, data").limit(15).execute()
        rows = [f"`{r['id']}` | KES {r['data'].get('balance',0)}" for r in res.data]
        text = "👥 **Recent Users:**\n" + "\n".join(rows)
        await q.message.edit_text(text, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅ Back", callback_data="admin_panel")]]), parse_mode="Markdown")

    elif data in ("admin_addbal", "admin_removebal", "admin_block"):
        user["state"] = f"{data}_wait_user"
        save_user(uid, user)
        await q.message.edit_text("🎯 Send the **Target User ID** now:")

async def text_handler(update: Update, context):
    uid = str(update.effective_user.id)
    user = get_user(uid)
    text = update.message.text.strip()
    state = user.get("state")

    # --- ADMIN LOGIC ---
    if state and state.startswith("admin_"):
        if state.endswith("_wait_user"):
            user["admin_target"] = text
            if "bal" in state:
                user["state"] = state.replace("_wait_user", "_wait_amount")
                save_user(uid, user)
                await update.message.reply_text(f"💰 Target: `{text}`. Enter Amount:")
            else:
                target_data = get_user(text)
                target_data["blocked"] = not target_data.get("blocked", False)
                save_user(text, target_data)
                user["state"] = None
                save_user(uid, user)
                await update.message.reply_text(f"✅ User `{text}` blocked status updated.")

        elif state.endswith("_wait_amount") and text.isdigit():
            target_uid = user.get("admin_target")
            amt = int(text)
            target_data = get_user(target_uid)
            target_data["balance"] = (target_data["balance"] + amt) if "addbal" in state else max(0, target_data["balance"] - amt)
            save_user(target_uid, target_data)
            user["state"] = None
            save_user(uid, user)
            await update.message.reply_text(f"✅ Success! `{target_uid}` balance updated.")

    # --- DEPOSIT LOGIC ---
    elif state == "awaiting_amount" and text.isdigit():
        user["temp_amt"] = int(text)
        user["state"] = "awaiting_phone"
        save_user(uid, user)
        await update.message.reply_text("📱 Enter M-Pesa Number (07... or 01... or 254...):")

    elif state == "awaiting_phone":
        formatted_phone = format_phone_number(text)
        if not formatted_phone:
            await update.message.reply_text("❌ Invalid format. Use 07XXXXXXXX.")
            return

        amt = user["temp_amt"]
        user["state"] = None
        save_user(uid, user)

        auth = base64.b64encode(f"{os.getenv('PAYHERO_USERNAME')}:{os.getenv('PAYHERO_PASSWORD')}".encode()).decode()
        payload = {
            "amount": amt,
            "phone_number": formatted_phone,
            "channel_id": os.getenv("PAYHERO_CHANNEL_ID"),
            "external_reference": f"{uid}_TOPUP_{int(time.time())}", # Unique Ref
            "callback_url": f"https://{request.host}/payhero-callback"
        }
        resp = requests.post(PAYHERO_API_URL, json=payload, headers={"Authorization": f"Basic {auth}", "Content-Type": "application/json"})
        if resp.status_code in [200, 201]:
            await update.message.reply_text(f"🚀 STK Push sent to {formatted_phone}. Check your phone!")
        else:
            await update.message.reply_text("⚠️ PayHero rejected the request. Check your credentials.")

# ---------------- VERCEL ENTRY ----------------
@app.route("/", methods=["POST"])
async def telegram_webhook():
    data = request.get_json(force=True)
    update = Update.de_json(data, application.bot)
    async with application:
        if update.message and update.message.text:
            if update.message.text == "/start":
                await update.message.reply_text("🎬 **MovieBot254 Active**", reply_markup=main_menu_keyboard(str(update.effective_user.id)), parse_mode="Markdown")
            else:
                await text_handler(update, None)
        elif update.callback_query:
            await callback_router(update, None)
    return "OK", 200

@app.route("/payhero-callback", methods=["POST"])
def payhero_callback():
    payload = request.get_json(force=True)
    data = payload.get("response", payload.get("data", {}))
    
    status = str(data.get("Status", "")).strip().capitalize()
    ref = data.get("ExternalReference", "")
    
    if "_TOPUP_" in ref:
        uid = ref.split("_TOPUP_")[0]
        amount = data.get("Amount", 0)
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"

        if status == "Success":
            user = get_user(uid)
            user["balance"] += int(float(amount))
            save_user(uid, user)
            requests.post(url, json={"chat_id": uid, "text": f"✅ **Payment Received!**\nKES {amount} added to your balance.", "parse_mode": "Markdown"})
        
        elif status in ["Cancelled", "Failed"]:
            desc = data.get("Description", "Transaction failed or timed out.")
            requests.post(url, json={"chat_id": uid, "text": f"❌ **Transaction {status}**\n{desc}", "parse_mode": "Markdown"})
            
    return "OK", 200

@app.route("/")
def index():
    return "Bot Online", 200
