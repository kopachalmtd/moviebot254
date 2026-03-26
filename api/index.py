import os
import re
import json
import asyncio
import time
import base64
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
# Added your list. Logic below handles 50 per page.
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
        default_data = {"balance": 0, "purchases": [], "state": None, "temp_amt": None, "admin_action": None}
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
        [InlineKeyboardButton("🔄 Reset Account", callback_data="req_reset")],
        [InlineKeyboardButton("📢 Join Channel", url="https://t.me/YourChannel")],
    ]
    if int(uid) in ADMIN_IDS:
        kb.append([InlineKeyboardButton("🛠 Admin Panel", callback_data="admin_panel")])
    return InlineKeyboardMarkup(kb)

# ---------------- HANDLERS ----------------

async def start_handler(update: Update):
    uid = str(update.effective_user.id)
    await update.effective_message.reply_text(
        "🎬 **Welcome to MovieBot254**\n\nUse the buttons below to navigate:", 
        reply_markup=main_menu_keyboard(uid), 
        parse_mode="Markdown"
    )

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

    # --- MOVIE PAGINATION (50 PER PAGE, 4 ROWS) ---
    elif data.startswith("browse_"):
        page = int(data.split("_")[1])
        per_page = 50
        start = page * per_page
        end = start + per_page
        page_movies = MOVIES[start:end]

        kb = []
        # Create 4 rows per page logic
        for i in range(0, len(page_movies), 2): # 2 movies per row for readability
            row = [InlineKeyboardButton(m["title"][:15], callback_data=f"buy_{m['id']}") for m in page_movies[i:i+2]]
            kb.append(row)

        nav_buttons = []
        if page > 0: nav_buttons.append(InlineKeyboardButton("⬅ Prev", callback_data=f"browse_{page-1}"))
        if end < len(MOVIES): nav_buttons.append(InlineKeyboardButton("Next ➡", callback_data=f"browse_{page+1}"))
        if nav_buttons: kb.append(nav_buttons)
        kb.append([InlineKeyboardButton("⬅ Back to Menu", callback_data="menu")])

        await q.message.edit_text(f"🎥 **Movies (Page {page+1})**\nEach movie costs **KSH 10**", reply_markup=InlineKeyboardMarkup(kb))

    # --- ADMIN PANEL ---
    elif data == "admin_panel":
        if int(uid) not in ADMIN_IDS: return
        kb = [
            [InlineKeyboardButton("👥 View Users", callback_data="admin_view_users")],
            [InlineKeyboardButton("➕ Add Balance", callback_data="admin_addbal"), InlineKeyboardButton("➖ Remove Balance", callback_data="admin_removebal")],
            [InlineKeyboardButton("⛔ Block/Unblock User", callback_data="admin_block")],
            [InlineKeyboardButton("🗑️ Delete User", callback_data="admin_delete")],
            [InlineKeyboardButton("⬅ Back", callback_data="menu")],
        ]
        await q.message.edit_text("🛠 **Admin Control Panel**", reply_markup=InlineKeyboardMarkup(kb))

    elif data == "admin_view_users":
        res = supabase.table("users").select("id").execute()
        user_list = "\n".join([f"• `{u['id']}`" for u in res.data])
        await q.message.edit_text(f"👥 **Total Users:** {len(res.data)}\n\n{user_list}", reply_markup=main_menu_keyboard(uid), parse_mode="Markdown")

    elif data.startswith("admin_"):
        user["admin_action"] = data + "_wait_user"
        save_user(uid, user)
        await q.message.edit_text("🎯 Send the **Target User ID**:")

    elif data == "deposit":
        user["state"] = "awaiting_amount"
        save_user(uid, user)
        await q.message.edit_text("💳 **Deposit**\nEnter amount to deposit (KES):")

async def text_handler(update: Update):
    uid = str(update.effective_user.id)
    user = get_user(uid)
    text = (update.message.text or "").strip()

    # --- DEPOSIT LOGIC (Where it was freezing) ---
    state = user.get("state")
    if state == "awaiting_amount" and text.isdigit():
        user["temp_amt"] = int(text)
        user["state"] = "awaiting_phone"
        save_user(uid, user)
        await update.message.reply_text("📱 Enter M-Pesa Number (07...):")
        return

    elif state == "awaiting_phone":
        phone = format_phone_number(text)
        if not phone:
            await update.message.reply_text("❌ Invalid format. Please use 07XXXXXXXX.")
            return

        amt = user["temp_amt"]
        user["state"] = None # Clear state immediately to prevent loops
        save_user(uid, user)

        # PREPARE PAYHERO REQUEST
        headers = {
            'Authorization': f'Basic {os.getenv("PAYHERO_AUTH")}', 
            'Content-Type': 'application/json'
        }
        payload = {
            "amount": amt,
            "phone_number": phone,
            "channel_id": int(os.getenv("PAYHERO_CHANNEL_ID", "4131")),
            "provider": "m-pesa",
            "external_reference": f"{uid}_TOPUP_{int(time.time())}",
            "callback_url": f"https://{request.host}/payhero-callback"
        }

        try:
            # Added a timeout to prevent freezing
            resp = requests.post(PAYHERO_API_URL, json=payload, headers=headers, timeout=15)
            
            if resp.status_code in [200, 201]:
                await update.message.reply_text(f"🚀 **STK Push Sent!**\nPlease check your phone ({phone}) to confirm the KSH {amt} payment.", reply_markup=main_menu_keyboard(uid))
            else:
                await update.message.reply_text(f"⚠️ **PayHero Error:** {resp.text}", reply_markup=main_menu_keyboard(uid))
        except Exception as e:
            await update.message.reply_text(f"📡 **Connection Error:** Could not reach payment server.\nError: {str(e)}", reply_markup=main_menu_keyboard(uid))

# ---------------- WEBHOOK ----------------

@app.route("/", methods=["POST"])
async def telegram_webhook():
    try:
        data = request.get_json(force=True)
        update = Update.de_json(data, application.bot)
        async with application:
            if update.message and update.message.text:
                if update.message.text == "/start": await start_handler(update)
                else: await text_handler(update)
            elif update.callback_query:
                await callback_router(update)
        return "OK", 200
    except Exception as e:
        print(f"ERROR: {e}")
        return "OK", 200

@app.route("/payhero-callback", methods=["POST"])
def payhero_callback():
    # ... (Same callback logic from previous turn) ...
    return "OK", 200

@app.route("/")
def index():
    return "Bot Online", 200
