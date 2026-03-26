import os
import re
import json
import asyncio
import time
from datetime import datetime, timedelta
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

application = ApplicationBuilder().token(TOKEN).build()

# ---------------- MOVIE DATA ----------------
MOVIES = [
    {"id": "movie1", "title": "Alice Boderland 1", "file_id": "BAACAgQAAxkBAAIB3mfhaf5rWiqi9HzvzqdtydVtoesdAAJ8DgACZmnoUEwzbnnZQMf3NgQ"},
    {"id": "movie2", "title": "Alice Boderland 2", "file_id": "BAACAgQAAxkBAAIBrWfhXaAb46Mg0dGuXEKQ27Lqm445AAKADgACZmnoUKH1QfB4s-TWNgQ"},
    {"id": "movie3", "title": "Alice Boderland 3", "file_id": "BAACAgQAAxkBAAIBrmfhXaDkr1R6c_c5QJPVpBPVXDE2AAKMDgACZmnoUMbTCkbPcfRPNgQ"},
    {"id": "movie4", "title": "Alice Boderland 4", "file_id": "BAACAgQAAxkBAAIBr2fhXaDHJJjSTKdDlp-jhrf1r1bXAAItEgAChHZBUX74xHg7vtMaNgQ"},
    {"id": "movie5", "title": "Alice Boderland 5", "file_id": "BAACAgQAAxkBAAIBsGfhXaBjoKL_Zc34ftXCKAABk2GHMQACHA8AAoR2SVHpn31gCC1H-DYE"},
    {"id": "movie6", "title": "Alice Boderland 6", "file_id": "BAACAgQAAxkBAAIBsWfhXaCx7GIjUxndXbIC7pHLcQSWAAIzDwAChHZJUSmLgjWwJ80WNgQ"},
    {"id": "movie7", "title": "Alice Boderland 7", "file_id": "BAACAgQAAxkBAAIBsmfhXaCw-VLbSdQXoff8pn__gW_0AAJBDwAChHZJUbIzfUEIfjzpNgQ"},
    {"id": "movie8", "title": "Alice Boderland 8", "file_id": "BAACAgQAAxkBAAIBs2fhXaDYlEm4EBpICh8TdYkdWjXfAAJTDwAChHZJUQ6UNzpAV03ENgQ"},
]

# ---------------- HELPERS ----------------
def get_user(uid):
    res = supabase.table("users").select("data").eq("id", str(uid)).execute()
    now_ts = int(time.time())
    if not res.data:
        default_data = {
            "balance": 0, "purchases": [], "movie_count": 0, "bonus_claimed": False,
            "state": None, "last_seen": now_ts, "admin_action": None
        }
        supabase.table("users").insert({"id": str(uid), "data": default_data}).execute()
        return default_data
    data = res.data[0]['data']
    data["last_seen"] = now_ts # Update activity
    return data

def save_user(uid, data):
    supabase.table("users").upsert({"id": str(uid), "data": data}).execute()

# ---------------- KEYBOARDS ----------------
def main_menu_keyboard(uid):
    kb = [
        [InlineKeyboardButton("🎥 Browse Movies", callback_data="browse_movies")],
        [InlineKeyboardButton("💳 Deposit", callback_data="deposit"), InlineKeyboardButton("💰 Balance", callback_data="bal")],
        [InlineKeyboardButton("💼 My Purchases", callback_data="myp")],
        [InlineKeyboardButton("🎁 Claim Bonus (5+ Movies)", callback_data="claim_bonus")],
        [InlineKeyboardButton("🔄 Request Reset", callback_data="req_reset")],
        [InlineKeyboardButton("📢 Join Channel", url="https://t.me/YourChannelLink")],
        [InlineKeyboardButton("📞 Contact Admin", url="https://t.me/YourAdminUsername")],
    ]
    if int(uid) in ADMIN_IDS:
        kb.append([InlineKeyboardButton("🛠 Admin Panel", callback_data="admin_panel")])
    return InlineKeyboardMarkup(kb)

# ---------------- HANDLERS ----------------

async def start_handler(update: Update):
    uid = str(update.effective_user.id)
    await update.effective_message.reply_text("🎬 **MovieBot254 Active**", reply_markup=main_menu_keyboard(uid), parse_mode="Markdown")

async def callback_router(update: Update):
    q = update.callback_query
    await q.answer()
    uid = str(q.from_user.id)
    user = get_user(uid)
    data = q.data

    if data == "menu":
        await q.message.edit_text("🎬 **Main Menu**", reply_markup=main_menu_keyboard(uid), parse_mode="Markdown")
    
    elif data == "bal":
        await q.message.edit_text(f"Dear Customer, your balance is **KSH {user['balance']}**.", reply_markup=main_menu_keyboard(uid), parse_mode="Markdown")

    elif data == "browse_movies":
        kb = [[InlineKeyboardButton(m["title"], callback_data=f"view_{m['id']}")] for m in MOVIES]
        kb.append([InlineKeyboardButton("⬅ Back", callback_data="menu")])
        await q.message.edit_text("🎥 **Select a Movie (KSH 10 each):**", reply_markup=InlineKeyboardMarkup(kb))

    elif data.startswith("view_"):
        m_id = data.replace("view_", "")
        movie = next((m for m in MOVIES if m["id"] == m_id), None)
        kb = [[InlineKeyboardButton(f"💳 Buy {movie['title']} (KSH 10)", callback_data=f"buy_{m_id}")], [InlineKeyboardButton("⬅ Back", callback_data="browse_movies")]]
        await q.message.edit_text(f"🎬 **{movie['title']}**\nCost: KSH 10", reply_markup=InlineKeyboardMarkup(kb))

    elif data.startswith("buy_"):
        m_id = data.replace("buy_", "")
        movie = next((m for m in MOVIES if m["id"] == m_id), None)
        if user["balance"] < 10:
            await q.message.edit_text(f"Dear Customer, you have insufficient balance. Your bal is **KSH {user['balance']}**. Please top up.", reply_markup=main_menu_keyboard(uid), parse_mode="Markdown")
            return
        
        user["balance"] -= 10
        user["movie_count"] = user.get("movie_count", 0) + 1
        user["purchases"].append(movie["title"])
        save_user(uid, user)
        await application.bot.send_video(chat_id=uid, video=movie["file_id"], caption=f"✅ Purchase Successful: {movie['title']}")

    elif data == "claim_bonus":
        if user.get("movie_count", 0) >= 5 and not user.get("bonus_claimed", False):
            user["balance"] += 20
            user["bonus_claimed"] = True
            save_user(uid, user)
            await q.message.edit_text("🎁 **Bonus Claimed!** KSH 20 added to your account.", reply_markup=main_menu_keyboard(uid))
        else:
            await q.message.edit_text("❌ You need to buy at least 5 movies to claim the KSH 20 bonus.", reply_markup=main_menu_keyboard(uid))

    elif data == "req_reset":
        for admin in ADMIN_IDS:
            kb = [[InlineKeyboardButton("✅ Approve", callback_data=f"admin_apprreset_{uid}"), InlineKeyboardButton("❌ Reject", callback_data=f"admin_rejreset_{uid}")]]
            await application.bot.send_message(chat_id=admin, text=f"⚠️ **Reset Request** from User `{uid}`.", reply_markup=InlineKeyboardMarkup(kb))
        await q.message.edit_text("📩 Reset request sent to Admin for approval.", reply_markup=main_menu_keyboard(uid))

    elif data == "deposit":
        user["state"] = "awaiting_amount"
        save_user(uid, user)
        await q.message.edit_text("💳 **Deposit**\nEnter amount to deposit (KES):")

    elif data == "admin_panel":
        if int(uid) not in ADMIN_IDS: return
        kb = [
            [InlineKeyboardButton("📢 Broadcast to All", callback_data="admin_bc")],
            [InlineKeyboardButton("➕ Add Bal", callback_data="admin_addbal")],
            [InlineKeyboardButton("⬅ Back", callback_data="menu")]
        ]
        # Inactivity Check
        six_months_ago = int(time.time()) - (180 * 24 * 60 * 60)
        inactives = supabase.table("users").select("id").lt("data->last_seen", six_months_ago).execute()
        if inactives.data:
            kb.insert(0, [InlineKeyboardButton(f"⚠️ Remove {len(inactives.data)} Inactive Users", callback_data="admin_clear_inactive")])
        await q.message.edit_text("🛠 **Admin Control Panel**", reply_markup=InlineKeyboardMarkup(kb))

    elif data == "admin_bc":
        user["admin_action"] = "bc_wait_msg"
        save_user(uid, user)
        await q.message.edit_text("📝 Send the message you want to broadcast to **ALL users**:")

    elif data.startswith("admin_apprreset_"):
        target_uid = data.split("_")[-1]
        t_data = get_user(target_uid)
        t_data["balance"] = 0
        save_user(target_uid, t_data)
        await application.bot.send_message(chat_id=target_uid, text="🔄 Your account has been reset by Admin.")
        await q.message.edit_text(f"✅ User {target_uid} reset approved.")

async def text_handler(update: Update):
    uid = str(update.effective_user.id)
    user = get_user(uid)
    text = (update.message.text or "").strip()

    # --- ADMIN BROADCAST ---
    if user.get("admin_action") == "bc_wait_msg":
        user["admin_action"] = None
        save_user(uid, user)
        all_users = supabase.table("users").select("id").execute()
        count = 0
        for u in all_users.data:
            try:
                await application.bot.send_message(chat_id=u["id"], text=f"🔔 **Broadcast:**\n\n{text}", parse_mode="Markdown")
                count += 1
            except: pass
        await update.message.reply_text(f"✅ Broadcast sent to {count} users.")
        return

    # --- DEPOSIT FLOW ---
    if user.get("state") == "awaiting_amount" and text.isdigit():
        user["temp_amt"] = int(text)
        user["state"] = "awaiting_phone"
        save_user(uid, user)
        await update.message.reply_text("📱 Enter M-Pesa Number (07...):")
        return
    
    elif user.get("state") == "awaiting_phone":
        phone = format_phone_number(text)
        if not phone:
            await update.message.reply_text("❌ Invalid phone.")
            return

        amt = user["temp_amt"]
        user["state"] = None
        save_user(uid, user)

        headers = {'Authorization': f'Basic {os.getenv("PAYHERO_AUTH")}', 'Content-Type': 'application/json'}
        payload = {
            "amount": amt, "phone_number": phone, "channel_id": int(os.getenv("PAYHERO_CHANNEL_ID", "4131")),
            "provider": "m-pesa", "external_reference": f"{uid}_TOPUP_{int(time.time())}",
            "callback_url": f"https://{request.host}/payhero-callback"
        }
        
        resp = requests.post(PAYHERO_API_URL, json=payload, headers=headers)
        # Fix: Check for 'Success' OR 'success' and assume push sent if 200 OK
        if resp.status_code in [200, 201]:
            await update.message.reply_text(f"🚀 STK Push sent to {phone}. Confirm on your phone.")
        else:
            await update.message.reply_text(f"⚠️ PayHero Error: {resp.json().get('message', 'Rejected')}")

# ---------------- WEBHOOK & CALLBACK ----------------

@app.route("/", methods=["POST"])
async def telegram_webhook():
    data = request.get_json(force=True)
    update = Update.de_json(data, application.bot)
    async with application:
        if update.message and update.message.text:
            if update.message.text == "/start": await start_handler(update)
            else: await text_handler(update)
        elif update.callback_query: await callback_router(update)
    return "OK", 200

@app.route("/payhero-callback", methods=["POST"])
def payhero_callback():
    payload = request.get_json(force=True)
    inner_data = payload.get("response", payload.get("data", {}))
    status = str(inner_data.get("Status", "")).strip().capitalize()
    ref = inner_data.get("ExternalReference", "")
    
    if "_TOPUP_" in ref:
        uid = ref.split("_TOPUP_")[0]
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
        
        if status == "Success":
            user = get_user(uid)
            user["balance"] += int(float(inner_data.get("Amount", 0)))
            save_user(uid, user)
            requests.post(url, json={"chat_id": uid, "text": "✅ **Payment Successful!** Balance updated.", "reply_markup": main_menu_keyboard(uid).to_dict()})
        else:
            # Handle Cancelled/Failed by showing the menu buttons again
            requests.post(url, json={"chat_id": uid, "text": f"❌ **Payment {status}**. Please try again.", "reply_markup": main_menu_keyboard(uid).to_dict()})
            
    return "OK bot", 200
