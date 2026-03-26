import os
import re
import json
import time
import requests
import asyncio
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
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Initialize Application
application = ApplicationBuilder().token(TOKEN).build()

# ---------------- MOVIE DATABASE ----------------
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

# ---------------- DATABASE HELPERS ----------------
def get_user(uid):
    res = supabase.table("users").select("data").eq("id", str(uid)).execute()
    if not res.data:
        default_data = {
            "balance": 0, "purchases": [], "bonus_claimed": False, 
            "state": None, "temp_amt": None, "admin_action": None, "admin_target": None
        }
        supabase.table("users").insert({"id": str(uid), "data": default_data}).execute()
        return default_data
    return res.data[0]['data']

def save_user(uid, data):
    supabase.table("users").upsert({"id": str(uid), "data": data}).execute()

def format_phone(phone):
    phone = re.sub(r"[^0-9]", "", phone)
    if (phone.startswith("07") or phone.startswith("01")) and len(phone) == 10:
        return "254" + phone[1:]
    return phone if len(phone) == 12 and phone.startswith("254") else None

# ---------------- KEYBOARDS ----------------
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

# ---------------- HANDLERS ----------------
async def start_handler(update: Update):
    uid = str(update.effective_user.id)
    await update.effective_message.reply_text("🎬 **MovieBot254 Active**\nSelect an option:", reply_markup=main_menu_keyboard(uid), parse_mode="Markdown")

async def callback_router(update: Update):
    q = update.callback_query
    await q.answer()
    uid = str(q.from_user.id)
    user = get_user(uid)
    data = q.data

    if data == "menu":
        await start_handler(update)
    elif data == "bal":
        await q.message.reply_text(f"Dear customer, your balance is **KSH {user['balance']}**.", reply_markup=main_menu_keyboard(uid))
    elif data == "myp":
        p = user.get("purchases", [])
        msg = "💼 **Purchases:**\n" + ("\n".join(p) if p else "None yet.")
        await q.message.reply_text(msg, reply_markup=main_menu_keyboard(uid))
    elif data == "claim_bonus":
        if len(user.get("purchases", [])) >= 5 and not user.get("bonus_claimed"):
            user["balance"] += 20
            user["bonus_claimed"] = True
            save_user(uid, user)
            await q.message.reply_text("🎁 **Bonus Added!** KSH 20 credited.", reply_markup=main_menu_keyboard(uid))
        else:
            await q.message.reply_text("❌ Buy 5 movies first or bonus already claimed.", reply_markup=main_menu_keyboard(uid))
    elif data == "req_reset":
        for admin in ADMIN_IDS:
            kb = [[InlineKeyboardButton("✅ Approve", callback_data=f"admin_approvereset_{uid}")]]
            await application.bot.send_message(chat_id=admin, text=f"⚠️ Reset Request: `{uid}`", reply_markup=InlineKeyboardMarkup(kb))
        await q.message.reply_text("📩 Reset request sent to admin.")
    elif data.startswith("browse_"):
        page = int(data.split("_")[1])
        per_page = 50
        movies = MOVIES[page*per_page:(page+1)*per_page]
        kb = []
        for i in range(0, len(movies), 2):
            row = [InlineKeyboardButton(m["title"][:15], callback_data=f"buy_{m['id']}") for m in movies[i:i+2]]
            kb.append(row)
        nav = []
        if page > 0: nav.append(InlineKeyboardButton("⬅", callback_data=f"browse_{page-1}"))
        if len(MOVIES) > (page+1)*per_page: nav.append(InlineKeyboardButton("➡", callback_data=f"browse_{page+1}"))
        if nav: kb.append(nav)
        kb.append([InlineKeyboardButton("⬅ Menu", callback_data="menu")])
        await q.message.reply_text(f"🎥 **Movies (Pg {page+1})**", reply_markup=InlineKeyboardMarkup(kb))
    elif data.startswith("buy_"):
        m_id = data.split("_")[1]
        movie = next((m for m in MOVIES if m["id"] == m_id), None)
        if user["balance"] < 10:
            await q.message.reply_text(f"Insufficient Balance. Your bal: KSH {user['balance']}. Please top up.", reply_markup=main_menu_keyboard(uid))
        else:
            user["balance"] -= 10
            user["purchases"].append(movie["title"])
            save_user(uid, user)
            await application.bot.send_video(chat_id=uid, video=movie["file_id"], caption=f"✅ {movie['title']}")
    elif data == "deposit":
        user["state"] = "wait_amt"
        save_user(uid, user)
        await q.message.reply_text("💳 Enter Amount (KSH):")
    elif data == "admin_panel":
        if int(uid) not in ADMIN_IDS: return
        kb = [
            [InlineKeyboardButton("👥 Users", callback_data="admin_view"), InlineKeyboardButton("📢 Broadcast", callback_data="admin_bc")],
            [InlineKeyboardButton("➕ Add Bal", callback_data="admin_addbal"), InlineKeyboardButton("➖ Rem Bal", callback_data="admin_removebal")],
            [InlineKeyboardButton("⛔ Block", callback_data="admin_block"), InlineKeyboardButton("🗑 Delete", callback_data="admin_delete")],
            [InlineKeyboardButton("⬅ Menu", callback_data="menu")]
        ]
        await q.message.reply_text("🛠 **Admin Panel**", reply_markup=InlineKeyboardMarkup(kb))
    elif data.startswith("admin_"):
        if "view" in data:
            res = supabase.table("users").select("id").execute()
            await q.message.reply_text(f"Total Users: {len(res.data)}")
        elif "approvereset_" in data:
            t_uid = data.split("_")[-1]
            t_data = get_user(t_uid)
            t_data["balance"] = 0
            save_user(t_uid, t_data)
            await application.bot.send_message(chat_id=t_uid, text="🔄 Account reset successful.")
            await q.message.reply_text("✅ Reset approved.")
        else:
            user["admin_action"] = data + "_wait_user"
            save_user(uid, user)
            await q.message.reply_text("🎯 Send Target User ID:")

async def text_handler(update: Update):
    uid = str(update.effective_user.id)
    user = get_user(uid)
    text = (update.message.text or "").strip()

    if user.get("admin_action"):
        action = user["admin_action"]
        if action.endswith("_wait_user"):
            user["admin_target"] = text
            if "bal" in action:
                user["admin_action"] = action.replace("_wait_user", "_wait_amt")
                save_user(uid, user)
                await update.message.reply_text("Enter amount:")
            else:
                user["admin_action"] = None
                save_user(uid, user)
                await update.message.reply_text(f"Processing {action} on {text}...")
            return
        if action.endswith("_wait_amt") and text.isdigit():
            target_id = user["admin_target"]
            target_user = get_user(target_id)
            amt = int(text)
            if "addbal" in action: target_user["balance"] += amt
            else: target_user["balance"] = max(0, target_user["balance"] - amt)
            save_user(target_id, target_user)
            user["admin_action"] = None
            save_user(uid, user)
            await update.message.reply_text(f"✅ Balance updated for `{target_id}`.")
            return

    if user.get("state") == "wait_amt" and text.isdigit():
        user["temp_amt"] = int(text)
        user["state"] = "wait_phone"
        save_user(uid, user)
        await update.message.reply_text("📱 Enter M-Pesa Number:")
    elif user.get("state") == "wait_phone":
        phone = format_phone(text)
        if not phone:
            await update.message.reply_text("Invalid phone.")
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
            r = requests.post(PAYHERO_API_URL, json=payload, headers=headers, timeout=15)
            if r.status_code < 300:
                await update.message.reply_text(f"🚀 Push sent to {phone}!")
            else:
                await update.message.reply_text(f"Rejected by PayHero: {r.text}")
        except:
            await update.message.reply_text("Connection error.")

# ---------------- WEBHOOKS ----------------
@app.route("/", methods=["POST"])
def telegram_webhook():
    data = request.get_json(force=True)
    update = Update.de_json(data, application.bot)
    
    # Run the async handlers within the current loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    async def process():
        async with application:
            if update.message:
                if update.message.text == "/start": await start_handler(update)
                else: await text_handler(update)
            elif update.callback_query: await callback_router(update)
            
    loop.run_until_complete(process())
    return "OK", 200

@app.route("/payhero-callback", methods=["POST"])
def payhero_callback():
    payload = request.get_json(force=True)
    d = payload.get("response", payload.get("data", {}))
    status = str(d.get("Status", d.get("status", ""))).lower()
    ref = d.get("ExternalReference", d.get("external_reference", ""))
    
    if "_TOPUP_" in ref:
        uid = ref.split("_TOPUP_")[0]
        if "success" in status:
            u = get_user(uid)
            u["balance"] += int(float(d.get("Amount", 0)))
            save_user(uid, u)
            requests.post(f"https://api.telegram.org/bot{TOKEN}/sendMessage", json={"chat_id": uid, "text": "✅ Payment Received!", "reply_markup": main_menu_keyboard(uid).to_dict()})
    return "OK", 200

@app.route("/")
def index(): return "Bot Online", 200
