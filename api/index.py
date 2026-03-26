import os
import re
import json
import asyncio
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
    # Handle both message and callback query updates
    if update.message:
        await update.message.reply_text("🎬 **MovieBot254 Active**", reply_markup=main_menu_keyboard(uid), parse_mode="Markdown")
    elif update.callback_query:
        await update.callback_query.message.edit_text("🎬 **Main Menu**", reply_markup=main_menu_keyboard(uid), parse_mode="Markdown")

async def callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    uid = str(q.from_user.id)
    user = get_user(uid)
    data = q.data

    # BACK TO MENU (Added as requested)
    if data == "menu":
        await start_handler(update, context)
        return

    if data == "bal":
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
    
    # Trigger Admin state from buttons
    elif data.startswith("admin_") and data != "admin_panel":
        context.user_data["admin_action"] = data + "_wait_user"
        await q.message.edit_text("🎯 Send the **Target User ID**:")
        return

    # fallback (Added as requested)
    else:
        try:
            await q.message.edit_text("Unrecognized action. Use /start to open menu.")
        except:
            pass

async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    # get_user acts as ensure_user/load_db for Supabase
    user = get_user(uid)
    text = (update.message.text or "").strip()

    # --- ADMIN MULTI-STEP LOGIC (Added as requested) ---
    admin_action = context.user_data.get("admin_action")
    if admin_action:
        if admin_action.endswith("_wait_user"):
            context.user_data["admin_target_user"] = text
            if admin_action.startswith("admin_addbal") or admin_action.startswith("admin_removebal"):
                context.user_data["admin_action"] = admin_action.replace("_wait_user", "_wait_amount")
                await update.message.reply_text("Enter amount (KES):")
                return
            
            # block/delete immediate
            target = context.user_data.get("admin_target_user")
            target_data = get_user(target) # Fetches from Supabase
            
            if admin_action.startswith("admin_block"):
                target_data["blocked"] = not target_data.get("blocked", False)
                save_user(target, target_data) # Saves to Supabase
                await update.message.reply_text(f"User {target} blocked toggled to {target_data['blocked']}.")
            
            elif admin_action.startswith("admin_delete"):
                supabase.table("users").delete().eq("id", target).execute()
                await update.message.reply_text(f"User {target} deleted.")

            context.user_data.pop("admin_action", None)
            context.user_data.pop("admin_target_user", None)
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
                await update.message.reply_text(f"Added KES {amt} to {target}. New balance: KES {target_data['balance']}")
                try: await context.bot.send_message(chat_id=int(target), text=f"✅ Admin added KES {amt} to your account.")
                except: pass
            
            elif admin_action.startswith("admin_removebal"):
                target_data["balance"] = max(0, target_data["balance"] - amt)
                save_user(target, target_data)
                await update.message.reply_text(f"Removed KES {amt} from {target}. New balance: KES {target_data['balance']}")
                try: await context.bot.send_message(chat_id=int(target), text=f"❌ Admin removed KES {amt} from your account.")
                except: pass

            context.user_data.pop("admin_action", None)
            context.user_data.pop("admin_target_user", None)
            return

    # --- DEPOSIT FLOW (Kept existing) ---
    state = user.get("state")
    if state == "awaiting_amount" and text.isdigit():
        user["temp_amt"] = int(text)
        user["state"] = "awaiting_phone"
        save_user(uid, user)
        await update.message.reply_text("📱 Enter M-Pesa Number (07...):")
    
    elif state == "awaiting_phone":
        formatted_phone = format_phone_number(text)
        if not formatted_phone:
            await update.message.reply_text("❌ Invalid format. Use 07XXXXXXXX.")
            return

        amt = user["temp_amt"]
        user["state"] = None
        save_user(uid, user)

        headers = {'Authorization': f'Basic {os.getenv("PAYHERO_AUTH")}', 'Content-Type': 'application/json'}
        payload = {
            "amount": amt,
            "phone_number": formatted_phone,
            "channel_id": int(os.getenv("PAYHERO_CHANNEL_ID", "4131")),
            "provider": "m-pesa",
            "external_reference": f"{uid}_TOPUP_{int(time.time())}",
            "customer_name": update.effective_user.full_name or "MovieBot User",
            "callback_url": f"https://{request.host}/payhero-callback"
        }
        
        resp = requests.post(PAYHERO_API_URL, json=payload, headers=headers)
        if resp.status_code in [200, 201] and resp.json().get("status") == "Success":
            await update.message.reply_text(f"🚀 STK Push sent to {formatted_phone}!")
        else:
            await update.message.reply_text(f"⚠️ Error: {resp.json().get('message', 'Rejected')}")

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

@app.route("/payhero-callback", methods=["POST"])
def payhero_callback():
    payload = request.get_json(force=True)
    data = payload.get("response", payload.get("data", {}))
    status = str(data.get("Status", "")).strip().capitalize()
    ref = data.get("ExternalReference", "")
    
    if "_TOPUP_" in ref:
        uid = ref.split("_TOPUP_")[0]
        amount = data.get("Amount", 0)
        if status == "Success":
            user = get_user(uid)
            user["balance"] += int(float(amount))
            save_user(uid, user)
            requests.post(f"https://api.telegram.org/bot{TOKEN}/sendMessage", json={"chat_id": uid, "text": f"✅ Payment Received: KES {amount}"})
            
    return "OK", 200

@app.route("/")
def index():
    return "Bot 2 Online", 200
