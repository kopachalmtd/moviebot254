import os
import re
import json
import asyncio
import base64
from datetime import datetime
from flask import Flask, request, jsonify
import requests
from supabase import create_client, Client
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ApplicationBuilder, CommandHandler, CallbackQueryHandler, MessageHandler, filters

app = Flask(__name__)

# ---------------- CONFIG ----------------
TOKEN = os.getenv("BOT_TOKEN")
ADMIN_IDS = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]
MOVIE_COST = int(os.getenv("MOVIE_COST", "10"))
PAYHERO_API_URL = os.getenv("PAYHERO_API_URL")

# Supabase Config
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Initialize Bot Application for Webhook mode
# Note: We don't use run_polling() on Vercel
application = ApplicationBuilder().token(TOKEN).build()

# ---------------- SUPABASE HELPERS ----------------
def get_user(uid):
    res = supabase.table("users").select("data").eq("id", str(uid)).execute()
    if not res.data:
        default_data = {
            "balance": 0, "purchases": [], "purchase_history": [],
            "pending_payments": [], "blocked": False, "state": None, "temp_amt": None
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
        [InlineKeyboardButton("💳 Deposit", callback_data="deposit")],
        [InlineKeyboardButton("💼 My Purchases", callback_data="myp")],
        [InlineKeyboardButton("💰 Balance", callback_data="bal")],
        [InlineKeyboardButton("🔄 Reset Account", callback_data="reset")],
        [InlineKeyboardButton("📢 Join Channel", callback_data="join_channel")],
        [InlineKeyboardButton("☎ Contact Admin", callback_data="contact_admin")],
        [InlineKeyboardButton("🎁 Claim Bonus", callback_data="claim_bonus")],
    ]
    if int(uid) in ADMIN_IDS:
        kb.append([InlineKeyboardButton("🛠 Admin Panel", callback_data="admin_panel")])
    return InlineKeyboardMarkup(kb)
    if data == "admin_panel":
        if int(uid) not in ADMIN_IDS:
            await q.message.edit_text("Unauthorized.")
            return
        kb = [
            [InlineKeyboardButton("👥 View Users", callback_data="admin_view_users")],
            [InlineKeyboardButton("➕ Add Balance", callback_data="admin_addbal"), InlineKeyboardButton("➖ Remove Balance", callback_data="admin_removebal")],
            [InlineKeyboardButton("⛔ Block/Unblock User", callback_data="admin_block")],
            [InlineKeyboardButton("🗑️ Delete User", callback_data="admin_delete")],
            [InlineKeyboardButton("⬅ Back", callback_data="menu")],
        ]
        await q.message.edit_text("Admin Panel", reply_markup=InlineKeyboardMarkup(kb))
        return

    # Admin actions (view, add, remove, block, delete)
    if data == "admin_view_users":
        if int(uid) not in ADMIN_IDS:
            await q.message.edit_text("Unauthorized.")
            return
        db = load_db()
        rows = []
        for i, (u_id, uinfo) in enumerate(db.items()):
            if i >= 50:
                break
            rows.append(f"{u_id} — KES {uinfo.get('balance',0)} — purchases {len(uinfo.get('purchases',[]))} — blocked:{uinfo.get('blocked',False)}")
        text = "Users:\n" + ("\n".join(rows) if rows else "No users")
        await q.message.edit_text(text, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅ Back", callback_data="admin_panel")]]))
        return

    if data in ("admin_addbal", "admin_removebal", "admin_block", "admin_delete"):
        if int(uid) not in ADMIN_IDS:
            await q.message.edit_text("Unauthorized.")
            return
        # set admin multi-step state
        context.user_data["admin_action"] = data + "_wait_user"
        await q.message.edit_text("Send the target user's numeric ID now (e.g. 6725602268).")
        return

    # admin approve/reset callbacks
    if data.startswith("admin_approve_reset_") or data.startswith("admin_reject_reset_"):
        if int(uid) not in ADMIN_IDS:
            await q.message.edit_text("Unauthorized.")
            return
        parts = data.split("_")
        target = parts[-1]
        db = load_db()
        target_user = db.get(target)
        if not target_user:
            await q.message.edit_text("User not found.")
            return
        if data.startswith("admin_approve_reset_"):
            del db[target]
            save_db(db)
            try:
                await context.bot.send_message(chat_id=int(target), text="✅ Your account reset was approved by admin and your data has been deleted.")
            except Exception:
                pass
            await q.message.edit_text(f"User {target} reset approved and data deleted.")
        else:
            target_user["reset_request"] = None
            save_db(db)
            try:
                await context.bot.send_message(chat_id=int(target), text="❌ Your account reset request was rejected by admin.")
            except Exception:
                pass
            await q.message.edit_text(f"User {target} reset rejected.")
        return

# ---------------- HANDLERS ----------------
async def start_handler(update: Update, context):
    uid = str(update.effective_user.id)
    get_user(uid) # Ensure user exists
    await update.message.reply_text("🎬 **Welcome to MovieBot254!**", 
                                  reply_markup=main_menu_keyboard(uid), parse_mode="Markdown")

async def callback_router(update: Update, context):
    q = update.callback_query
    await q.answer()
    uid = str(q.from_user.id)
    user = get_user(uid)
    data = q.data

    if data == "bal":
        await q.message.edit_text(f"💰 Your balance: KES {user['balance']}", 
                                 reply_markup=main_menu_keyboard(uid))
    
    elif data == "deposit":
        user["state"] = "awaiting_amount"
        save_user(uid, user)
        await q.message.edit_text("💳 **Deposit**\nEnter amount to deposit (KES):")

async def text_handler(update: Update, context):
    uid = str(update.effective_user.id)
    user = get_user(uid)
    text = update.message.text
    
    if user.get("state") == "awaiting_amount" and text.isdigit():
        user["temp_amt"] = int(text)
        user["state"] = "awaiting_phone"
        save_user(uid, user)
        await update.message.reply_text("📱 Enter Safaricom Number (07...):")
        
    elif user.get("state") == "awaiting_phone":
        phone = re.sub(r"[^0-9]", "", text)
        amt = user["temp_amt"]
        user["state"] = None
        save_user(uid, user)
        
        # PayHero STK Push Logic
        auth = base64.b64encode(f"{os.getenv('PAYHERO_USERNAME')}:{os.getenv('PAYHERO_PASSWORD')}".encode()).decode()
        payload = {
            "amount": amt,
            "phone_number": phone,
            "channel_id": os.getenv("PAYHERO_CHANNEL_ID"),
            "external_reference": f"{uid}_TOPUP_{amt}",
            "callback_url": f"https://{request.host}/payhero-callback"
        }
        requests.post(PAYHERO_API_URL, json=payload, headers={"Authorization": f"Basic {auth}"})
        await update.message.reply_text("✅ STK Push sent! Complete on your phone.")

# ---------------- VERCEL ROUTES ----------------

@app.route("/", methods=["POST"])
async def telegram_webhook():
    """Process updates from Telegram"""
    update = Update.de_json(request.get_json(force=True), application.bot)
    
    # Manually route since we are in serverless mode
    if update.message and update.message.text:
        if update.message.text == "/start":
            await start_handler(update, None)
        else:
            await text_handler(update, None)
    elif update.callback_query:
        await callback_router(update, None)
        
    return "OK", 200

@app.route("/payhero-callback", methods=["POST"])
def payhero_callback():
    """Update Supabase balance when payment is successful"""
    data = request.get_json(force=True).get("response", {})
    if data.get("Status") == "Success":
        ref = data.get("ExternalReference", "")
        uid = ref.split("_TOPUP_")[0]
        amount = int(float(data.get("Amount", 0)))
        
        user = get_user(uid)
        user["balance"] += amount
        save_user(uid, user)
        
        # Notify user (Optional: Requires a separate bot instance call)
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": uid, "text": f"✅ Received KES {amount}!"})
        
    return "OK", 200

@app.route("/", methods=["GET"])
def index():
    return "Bot is Active", 200
