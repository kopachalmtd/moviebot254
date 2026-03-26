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

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

application = ApplicationBuilder().token(TOKEN).build()

# ---------------- SUPABASE HELPERS ----------------
def get_user(uid):
    res = supabase.table("users").select("data").eq("id", str(uid)).execute()
    if not res.data:
        default_data = {
            "balance": 0, "purchases": [], "purchase_history": [],
            "pending_payments": [], "blocked": False, "state": None, "temp_amt": None, "admin_target": None
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
        [InlineKeyboardButton("🎁 Claim Bonus", callback_data="claim_bonus")],
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

    # --- ADMIN PANEL MAIN ---
    if data == "admin_panel":
        if int(uid) not in ADMIN_IDS: return
        kb = [
            [InlineKeyboardButton("👥 View Users", callback_data="admin_view_users")],
            [InlineKeyboardButton("➕ Add Bal", callback_data="admin_addbal"), InlineKeyboardButton("➖ Rem Bal", callback_data="admin_removebal")],
            [InlineKeyboardButton("⛔ Block/Unblock", callback_data="admin_block")],
            [InlineKeyboardButton("🗑️ Delete User", callback_data="admin_delete")],
            [InlineKeyboardButton("⬅ Back", callback_data="menu")],
        ]
        await q.message.edit_text("🛠 **Admin Panel**", reply_markup=InlineKeyboardMarkup(kb), parse_mode="Markdown")

    elif data == "admin_view_users":
        if int(uid) not in ADMIN_IDS: return
        res = supabase.table("users").select("id, data").limit(20).execute()
        rows = [f"ID: `{r['id']}` | Bal: {r['data'].get('balance',0)}" for r in res.data]
        text = "👥 **Recent Users:**\n" + "\n".join(rows)
        await q.message.edit_text(text, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅ Back", callback_data="admin_panel")]]), parse_mode="Markdown")

    elif data in ("admin_addbal", "admin_removebal", "admin_block", "admin_delete"):
        if int(uid) not in ADMIN_IDS: return
        user["state"] = f"{data}_wait_user"
        save_user(uid, user)
        await q.message.edit_text("🎯 Send the **Target User ID**:")

    # --- STANDARD BUTTONS ---
    elif data == "bal":
        await q.message.edit_text(f"💰 Your balance: KES {user['balance']}", reply_markup=main_menu_keyboard(uid))
    
    elif data == "deposit":
        user["state"] = "awaiting_amount"
        save_user(uid, user)
        await q.message.edit_text("💳 **Deposit**\nEnter amount to deposit (KES):")

    elif data == "menu":
        await q.message.edit_text("🎬 **Main Menu**", reply_markup=main_menu_keyboard(uid))

async def text_handler(update: Update, context):
    uid = str(update.effective_user.id)
    user = get_user(uid)
    text = update.message.text.strip()
    state = user.get("state")

    # --- ADMIN TEXT LOGIC ---
    if state and state.startswith("admin_"):
        if state.endswith("_wait_user"):
            user["admin_target"] = text
            if "bal" in state:
                user["state"] = state.replace("_wait_user", "_wait_amount")
                save_user(uid, user)
                await update.message.reply_text(f"💰 User `{text}` selected. Enter Amount:")
            else:
                target_uid = text
                target_data = get_user(target_uid)
                if "block" in state:
                    target_data["blocked"] = not target_data.get("blocked", False)
                    save_user(target_uid, target_data)
                    await update.message.reply_text(f"✅ User `{target_uid}` Blocked status: {target_data['blocked']}")
                elif "delete" in state:
                    supabase.table("users").delete().eq("id", target_uid).execute()
                    await update.message.reply_text(f"🗑 User `{target_uid}` deleted from database.")
                user["state"] = None
                save_user(uid, user)

        elif state.endswith("_wait_amount") and text.isdigit():
            target_uid = user.get("admin_target")
            amt = int(text)
            target_data = get_user(target_uid)
            if "addbal" in state:
                target_data["balance"] += amt
            else:
                target_data["balance"] = max(0, target_data["balance"] - amt)
            save_user(target_uid, target_data)
            user["state"] = None
            save_user(uid, user)
            await update.message.reply_text(f"✅ Updated! User `{target_uid}` new balance: {target_data['balance']}")

    # --- DEPOSIT TEXT LOGIC ---
    elif state == "awaiting_amount" and text.isdigit():
        user["temp_amt"] = int(text)
        user["state"] = "awaiting_phone"
        save_user(uid, user)
        await update.message.reply_text("📱 Enter Safaricom Number (07...):")

    elif state == "awaiting_phone":
        # ... (Your existing PayHero Logic here) ...
        user["state"] = None
        save_user(uid, user)
        await update.message.reply_text("✅ STK Push Request Sent!")

# ---------------- VERCEL WEBHOOK ----------------
@app.route("/", methods=["POST"])
async def telegram_webhook():
    data = request.get_json(force=True)
    update = Update.de_json(data, application.bot)
    async with application:
        if update.message and update.message.text:
            if update.message.text == "/start":
                await update.message.reply_text("🎬 **Welcome!**", reply_markup=main_menu_keyboard(str(update.effective_user.id)), parse_mode="Markdown")
            else:
                await text_handler(update, None)
        elif update.callback_query:
            await callback_router(update, None)
    return "OK", 200
