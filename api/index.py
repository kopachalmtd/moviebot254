import os
import re
import json
import time
import requests
from flask import Flask, request
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
        default_data = {"balance": 0, "purchases": [], "state": None, "temp_amt": None, "admin_action": None, "blocked": False}
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
    # Using reply_text so it appears at the bottom
    await update.effective_message.reply_text(
        "🎬 **MovieBot254 Main Menu**\nSelect an option below:", 
        reply_markup=main_menu_keyboard(uid), 
        parse_mode="Markdown"
    )

async def callback_router(update: Update):
    q = update.callback_query
    await q.answer()
    uid = str(q.from_user.id)
    user = get_user(uid)
    data = q.data

    # Use q.message.reply_text to send NEW messages at the bottom
    if data == "menu":
        await start_handler(update)
    
    elif data == "bal":
        await q.message.reply_text(f"Dear customer, your balance is **KSH {user['balance']}**.", reply_markup=main_menu_keyboard(uid), parse_mode="Markdown")

    elif data.startswith("browse_"):
        page = int(data.split("_")[1])
        per_page = 50
        start, end = page * per_page, (page + 1) * per_page
        page_movies = MOVIES[start:end]

        kb = []
        # Create a grid: 2 movies per row (approx 4-5 rows visible)
        for i in range(0, len(page_movies), 2):
            row = [InlineKeyboardButton(m["title"][:15], callback_data=f"buy_{m['id']}") for m in page_movies[i:i+2]]
            kb.append(row)

        nav = []
        if page > 0: nav.append(InlineKeyboardButton("⬅ Prev", callback_data=f"browse_{page-1}"))
        if end < len(MOVIES): nav.append(InlineKeyboardButton("Next ➡", callback_data=f"browse_{page+1}"))
        if nav: kb.append(nav)
        kb.append([InlineKeyboardButton("⬅ Back to Menu", callback_data="menu")])

        await q.message.reply_text(f"🎥 **Movie Catalog (Page {page+1})**\nEach movie is KSH 10.", reply_markup=InlineKeyboardMarkup(kb))

    elif data.startswith("buy_"):
        m_id = data.split("_")[1]
        movie = next((m for m in MOVIES if m["id"] == m_id), None)
        if user["balance"] < 10:
            await q.message.reply_text(f"Dear customer, you have insufficient balance. Your balance is **KSH {user['balance']}**. Please top up.", reply_markup=main_menu_keyboard(uid))
        else:
            user["balance"] -= 10
            user["purchases"].append(movie["title"])
            save_user(uid, user)
            await application.bot.send_video(chat_id=uid, video=movie["file_id"], caption=f"✅ Enjoy your movie: {movie['title']}")

    elif data == "admin_panel":
        if int(uid) not in ADMIN_IDS: return
        kb = [
            [InlineKeyboardButton("👥 View Users", callback_data="admin_view_users")],
            [InlineKeyboardButton("➕ Add Balance", callback_data="admin_addbal"), InlineKeyboardButton("➖ Remove Balance", callback_data="admin_removebal")],
            [InlineKeyboardButton("⛔ Block/Unblock User", callback_data="admin_block")],
            [InlineKeyboardButton("🗑️ Delete User", callback_data="admin_delete")],
            [InlineKeyboardButton("⬅ Back", callback_data="menu")],
        ]
        await q.message.reply_text("🛠 **Admin Control Panel**", reply_markup=InlineKeyboardMarkup(kb))

    elif data == "deposit":
        user["state"] = "awaiting_amount"
        save_user(uid, user)
        await q.message.reply_text("💳 **Deposit**\nEnter amount to deposit (KES):")

async def text_handler(update: Update):
    uid = str(update.effective_user.id)
    user = get_user(uid)
    text = (update.message.text or "").strip()

    # Admin Logic (Add/Remove Balance etc)
    if user.get("admin_action") and user["admin_action"].endswith("_wait_user"):
        user["admin_target"] = text
        action = user["admin_action"]
        if "addbal" in action or "removebal" in action:
            user["admin_action"] = action.replace("_wait_user", "_wait_amount")
            save_user(uid, user)
            await update.message.reply_text(f"Targeting User `{text}`. Enter amount:")
            return
        elif "delete" in action:
            supabase.table("users").delete().eq("id", text).execute()
            await update.message.reply_text(f"✅ User `{text}` deleted from database.")
            user["admin_action"] = None
            save_user(uid, user)
            return

    # Deposit Logic
    if user.get("state") == "awaiting_amount" and text.isdigit():
        user["temp_amt"] = int(text)
        user["state"] = "awaiting_phone"
        save_user(uid, user)
        await update.message.reply_text("📱 Enter M-Pesa Number (07...):")
    
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
        
        try:
            resp = requests.post(PAYHERO_API_URL, json=payload, headers=headers, timeout=15)
            if resp.status_code in [200, 201]:
                await update.message.reply_text(f"🚀 STK Push sent to {phone}. Confirm on your phone!")
            else:
                await update.message.reply_text(f"⚠️ PayHero error: {resp.text}", reply_markup=main_menu_keyboard(uid))
        except Exception as e:
            await update.message.reply_text(f"📡 Connection error. Please try again.", reply_markup=main_menu_keyboard(uid))

# ---------------- WEBHOOK & CALLBACK ----------------

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
    except: return "OK", 200

@app.route("/payhero-callback", methods=["POST"])
def payhero_callback():
    payload = request.get_json(force=True)
    inner_data = payload.get("response", payload.get("data", {}))
    status = str(inner_data.get("Status", inner_data.get("status", ""))).strip().lower()
    ref = inner_data.get("ExternalReference", inner_data.get("external_reference", ""))
    
    if "_TOPUP_" in ref:
        uid = ref.split("_TOPUP_")[0]
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
        
        if status == "success":
            user = get_user(uid)
            user["balance"] += int(float(inner_data.get("Amount", 0)))
            save_user(uid, user)
            requests.post(url, json={"chat_id": uid, "text": "✅ **Payment Confirmed!** Balance updated.", "reply_markup": main_menu_keyboard(uid).to_dict()})
        else:
            requests.post(url, json={"chat_id": uid, "text": f"❌ **Payment {status.capitalize()}**. Please try again.", "reply_markup": main_menu_keyboard(uid).to_dict()})
    return "OK", 200

@app.route("/")
def index():
    return "Bot kamaa Online", 200
