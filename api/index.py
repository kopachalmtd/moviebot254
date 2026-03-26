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
from telegram.ext import ApplicationBuilder, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes

app = Flask(__name__)

# ---------------- CONFIG ----------------
TOKEN = os.getenv("BOT_TOKEN")
ADMIN_IDS = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]
PAYHERO_API_URL = 'https://backend.payhero.co.ke/api/v2/payments'

# Supabase Config
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Initialize Bot Application
application = ApplicationBuilder().token(TOKEN).build()

# ---------------- HELPERS (Supabase Version of load/save) ----------------
def get_user(uid):
    res = supabase.table("users").select("data").eq("id", str(uid)).execute()
    if not res.data:
        default_data = {"balance": 0, "purchases": [], "blocked": False}
        supabase.table("users").insert({"id": str(uid), "data": default_data}).execute()
        return default_data
    return res.data[0]['data']

def save_user(uid, data):
    supabase.table("users").upsert({"id": str(uid), "data": data}).execute()

def format_phone_number(phone):
    phone = re.sub(r"[^0-9]", "", phone)
    if (phone.startswith("07") or phone.startswith("01")) and len(phone) == 10:
        return "254" + phone[1:]
    return phone if len(phone) == 12 else None

# ---------------- HANDLERS ----------------
async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    kb = [
        [InlineKeyboardButton("🎥 Browse Movies", callback_data="browse_0")],
        [InlineKeyboardButton("💳 Deposit", callback_data="deposit"), InlineKeyboardButton("💰 Balance", callback_data="bal")],
    ]
    if int(uid) in ADMIN_IDS:
        kb.append([InlineKeyboardButton("🛠 Admin Panel", callback_data="admin_panel")])
    
    await update.message.reply_text("🎬 **Main Menu**", reply_markup=InlineKeyboardMarkup(kb), parse_mode="Markdown")

async def callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    uid = str(q.from_user.id)
    data = q.data

    # --- YOUR NEW BACK TO MENU LOGIC ---
    if data == "menu":
        await start_handler(update, context)
        return

    if data == "bal":
        user = get_user(uid)
        await q.message.edit_text(f"💰 **Balance:** KES {user['balance']}", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅ Back", callback_data="menu")]]))
    
    elif data == "deposit":
        context.user_data["state"] = "awaiting_deposit_amount"
        await q.message.edit_text("💳 Enter amount to deposit (KES):")

    elif data == "admin_panel":
        if int(uid) not in ADMIN_IDS: return
        kb = [
            [InlineKeyboardButton("➕ Add Bal", callback_data="admin_addbal"), InlineKeyboardButton("➖ Rem Bal", callback_data="admin_removebal")],
            [InlineKeyboardButton("⛔ Block User", callback_data="admin_block"), InlineKeyboardButton("🗑 Delete User", callback_data="admin_delete")],
            [InlineKeyboardButton("⬅ Back", callback_data="menu")]
        ]
        await q.message.edit_text("🛠 **Admin Panel**", reply_markup=InlineKeyboardMarkup(kb))

    elif data.startswith("admin_"):
        context.user_data["admin_action"] = data + "_wait_user"
        await q.message.edit_text("🎯 Send the **Target User ID**: ")

async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    text = (update.message.text or "").strip()

    # --- ADMIN MULTI-STEP LOGIC ---
    admin_action = context.user_data.get("admin_action")
    if admin_action:
        if admin_action.endswith("_wait_user"):
            context.user_data["admin_target_user"] = text
            if admin_action.startswith("admin_addbal") or admin_action.startswith("admin_removebal"):
                context.user_data["admin_action"] = admin_action.replace("_wait_user", "_wait_amount")
                await update.message.reply_text("Enter amount (KES):")
                return
            
            target = context.user_data.get("admin_target_user")
            target_data = get_user(target)
            
            if admin_action.startswith("admin_block"):
                target_data["blocked"] = not target_data.get("blocked", False)
                save_user(target, target_data)
                await update.message.reply_text(f"User {target} blocked: {target_data['blocked']}")
            
            elif admin_action.startswith("admin_delete"):
                supabase.table("users").delete().eq("id", target).execute()
                await update.message.reply_text(f"User {target} deleted from database.")

            context.user_data.pop("admin_action", None)
            return

        if admin_action.endswith("_wait_amount"):
            target = context.user_data.get("admin_target_user")
            if not text.isdigit():
                await update.message.reply_text("Enter numeric amount only.")
                return
            
            amt = int(text)
            target_data = get_user(target)
            
            if admin_action.startswith("admin_addbal"):
                target_data["balance"] += amt
                save_user(target, target_data)
                await update.message.reply_text(f"✅ Added KES {amt} to {target}.")
                try: await context.bot.send_message(chat_id=int(target), text=f"✅ Admin added KES {amt}. New balance: KES {target_data['balance']}")
                except: pass
            
            elif admin_action.startswith("admin_removebal"):
                target_data["balance"] = max(0, target_data["balance"] - amt)
                save_user(target, target_data)
                await update.message.reply_text(f"❌ Removed KES {amt} from {target}.")

            context.user_data.pop("admin_action", None)
            return

    # --- USER DEPOSIT FLOW ---
    state = context.user_data.get("state")
    if state == "awaiting_deposit_amount" and text.isdigit():
        context.user_data["temp_amt"] = int(text)
        context.user_data["state"] = "awaiting_phone"
        await update.message.reply_text("📱 Enter M-Pesa Number (07...):")
    
    elif state == "awaiting_phone":
        phone = format_phone_number(text)
        if not phone:
            await update.message.reply_text("❌ Invalid number.")
            return
        
        # PayHero Logic Here (using the headers from your previous snippet)
        # ... (PayHero requests.post logic) ...
        await update.message.reply_text("🚀 STK Push Sent!")
        context.user_data.clear()

# ---------------- WEBHOOK ----------------
@app.route("/", methods=["POST"])
async def telegram_webhook():
    data = request.get_json(force=True)
    update = Update.de_json(data, application.bot)
    async with application:
        if update.message and update.message.text:
            if update.message.text == "/start":
                await start_handler(update, None)
            else:
                await text_handler(update, None)
        elif update.callback_query:
            await callback_router(update, None)
    return "OK", 200
