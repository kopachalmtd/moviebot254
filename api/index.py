import os
import re
import requests
import telebot
from flask import Flask, request, jsonify
from supabase import create_client, Client

app = Flask(__name__)

# --- CONFIG ---
TOKEN = os.environ.get("BOT_TOKEN")
ADMIN_IDS = [int(i) for i in os.environ.get("ADMIN_IDS", "6725602268").split(",")]
PAYHERO_USER = os.environ.get("PAYHERO_USERNAME")
PAYHERO_PASS = os.environ.get("PAYHERO_PASSWORD")
PAYHERO_API_URL = "https://backend.payhero.co.ke/api/v2/payments"

bot = telebot.TeleBot(TOKEN, threaded=False)
supabase: Client = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_SERVICE_ROLE_KEY"))

# --- DB HELPERS ---
def get_user(uid):
    res = supabase.table("users").select("data").eq("id", str(uid)).execute()
    if not res.data:
        default = {"balance": 0, "purchases": [], "blocked": False}
        supabase.table("users").insert({"id": str(uid), "data": default}).execute()
        return default
    return res.data[0]['data']

def save_user(uid, data):
    supabase.table("users").upsert({"id": str(uid), "data": data}).execute()

# --- KEYBOARDS ---
def main_menu(uid):
    markup = telebot.types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        telebot.types.InlineKeyboardButton("🎥 Browse Movies", callback_data="browse_0"),
        telebot.types.InlineKeyboardButton("💳 Deposit", callback_data="deposit"),
        telebot.types.InlineKeyboardButton("💼 My Purchases", callback_data="myp"),
        telebot.types.InlineKeyboardButton("💰 Balance", callback_data="bal")
    )
    if int(uid) in ADMIN_IDS:
        markup.add(telebot.types.InlineKeyboardButton("🛠 Admin Panel", callback_data="admin_panel"))
    return markup

# --- CALLBACK ROUTER (Makes buttons work) ---
@bot.callback_query_handler(func=lambda call: True)
def callback_listener(call):
    uid = str(call.from_user.id)
    user = get_user(uid)
    data = call.data

    if data == "menu":
        bot.edit_message_text("🎬 **Main Menu**", call.message.chat.id, call.message.message_id, 
                             reply_markup=main_menu(uid), parse_mode="Markdown")

    elif data.startswith("browse_"):
        page = int(data.split("_")[1])
        # Your pagination logic here (3 columns, 15 movies per page)
        bot.edit_message_text("🎥 **Select a Movie:**", call.message.chat.id, call.message.message_id, 
                             reply_markup=main_menu(uid)) # Replace with your grid function

    elif data == "bal":
        bot.answer_callback_query(call.id, f"Current Balance: KES {user['balance']}", show_alert=True)

    elif data == "deposit":
        msg = bot.send_message(call.message.chat.id, "Enter amount to deposit (KES):")
        bot.register_next_step_handler(msg, process_deposit_amount)

    elif data == "admin_panel":
        if int(uid) in ADMIN_IDS:
            markup = telebot.types.InlineKeyboardMarkup()
            markup.add(telebot.types.InlineKeyboardButton("👥 View Users", callback_data="admin_view"))
            markup.add(telebot.types.InlineKeyboardButton("⬅ Back", callback_data="menu"))
            bot.edit_message_text("🛠 **Admin Panel**", call.message.chat.id, call.message.message_id, reply_markup=markup)

# --- DEPOSIT LOGIC (STK PUSH) ---
def process_deposit_amount(message):
    amount = message.text
    if not amount.isdigit():
        return bot.reply_to(message, "❌ Please enter a valid number.")
    
    msg = bot.send_message(message.chat.id, "Enter Safaricom Number (07... or 01...):")
    bot.register_next_step_handler(msg, lambda m: trigger_stk(m, amount))

def trigger_stk(message, amount):
    phone = message.text
    uid = str(message.from_user.id)
    
    # Payload for PayHero
    payload = {
        "amount": amount,
        "phone_number": phone,
        "channel_id": 1, # M-Pesa
        "external_reference": f"{uid}_TOPUP_{amount}",
        "callback_url": f"https://{request.host}/payhero-callback"
    }
    
    # Call PayHero API
    import base64
    auth = base64.b64encode(f"{PAYHERO_USER}:{PAYHERO_PASS}".encode()).decode()
    headers = {"Authorization": f"Basic {auth}", "Content-Type": "application/json"}
    
    try:
        r = requests.post(PAYHERO_API_URL, json=payload, headers=headers)
        if r.status_code == 200:
            bot.send_message(message.chat.id, "✅ STK Push sent! Enter PIN on your phone.")
        else:
            bot.send_message(message.chat.id, f"❌ Failed: {r.text}")
    except Exception as e:
        bot.send_message(message.chat.id, f"❌ Error: {str(e)}")

# --- VERCEL ENTRY POINT ---
@app.route("/", methods=["POST"])
def webhook():
    if request.headers.get('content-type') == 'application/json':
        json_string = request.get_data().decode('utf-8')
        update = telebot.types.Update.de_json(json_string)
        bot.process_new_updates([update])
        return "OK", 200
    return "Forbidden", 403

@app.route("/payhero-callback", methods=["POST"])
def callback():
    data = request.json.get("response", request.json)
    if data.get("Status") == "Success":
        ref = data.get("ExternalReference", "")
        uid = ref.split("_TOPUP_")[0]
        amount = int(float(data.get("Amount", 0)))
        
        user = get_user(uid)
        user["balance"] += amount
        save_user(uid, user)
        bot.send_message(uid, f"✅ **Deposit Confirmed!**\nAdded: KES {amount}\nNew Balance: KES {user['balance']}")
    return "OK", 200

@app.route("/")
def index():
    return "MovieBot is Active", 200
