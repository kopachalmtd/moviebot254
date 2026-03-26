import os
import json
import telebot
from flask import Flask, request, jsonify
from supabase import create_client, Client
from datetime import datetime

app = Flask(__name__)

# --- CONFIGURATION ---
TOKEN = os.environ.get("BOT_TOKEN")
ADMIN_IDS = [int(i) for i in os.environ.get("ADMIN_IDS", "6725602268").split(",")]
MOVIE_COST = int(os.environ.get("MOVIE_COST", 10))
PAYHERO_USER = os.environ.get("PAYHERO_USERNAME")
PAYHERO_PASS = os.environ.get("PAYHERO_PASSWORD")

# Supabase Setup
supabase: Client = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_SERVICE_ROLE_KEY"))
bot = telebot.TeleBot(TOKEN, threaded=False)

# --- DATABASE HANDLERS ---
def get_user_data(uid):
    res = supabase.table("users").select("data").eq("id", str(uid)).execute()
    if not res.data:
        default_data = {"balance": 0, "purchases": [], "blocked": False}
        supabase.table("users").insert({"id": str(uid), "data": default_data}).execute()
        return default_data
    return res.data[0]['data']

def save_user_data(uid, data):
    supabase.table("users").upsert({"id": str(uid), "data": data}).execute()

# --- MOVIE DATA (Your List) ---
MOVIES = [
    {"id": "m1", "title": "Inception", "file_id": "FILE_ID_HERE"},
    {"id": "m2", "title": "The Matrix", "file_id": "FILE_ID_HERE"}
]

# --- BOT LOGIC ---
@bot.message_handler(commands=['start'])
def start(message):
    uid = message.from_user.id
    get_user_data(uid) # Initialize user
    markup = telebot.types.InlineKeyboardMarkup()
    markup.add(telebot.types.InlineKeyboardButton("🎥 Browse Movies", callback_data="browse_0"))
    markup.add(telebot.types.InlineKeyboardButton("💰 Balance", callback_data="bal"),
               telebot.types.InlineKeyboardButton("💳 Deposit", callback_data="deposit"))
    bot.send_message(message.chat.id, "🎬 **Welcome to D-Movies!**\nSelect an option:", reply_markup=markup, parse_mode="Markdown")

@bot.callback_query_handler(func=lambda call: True)
def callback_query(call):
    uid = str(call.from_user.id)
    user = get_user_data(uid)
    data = call.data

    if data == "bal":
        bot.edit_message_text(f"💰 Your balance: KES {user['balance']}", call.message.chat.id, call.message.message_id,
                             reply_markup=telebot.types.InlineKeyboardMarkup().add(telebot.types.InlineKeyboardButton("⬅ Back", callback_data="menu")))

    elif data.startswith("buy_"):
        mid = data.split("_")[1]
        if user['balance'] >= MOVIE_COST:
            user['balance'] -= MOVIE_COST
            user['purchases'].append(mid)
            save_user_data(uid, user)
            bot.answer_callback_query(call.id, "✅ Purchase Successful!")
            bot.send_message(call.message.chat.id, f"🍿 Enjoy your movie! (Sending file...)")
            # Logic to send file_id from MOVIES list goes here
        else:
            bot.answer_callback_query(call.id, "❌ Insufficient Balance", show_alert=True)

# --- VERCEL ROUTES ---
@app.route('/', methods=['POST'])
def webhook():
    if request.headers.get('content-type') == 'application/json':
        json_string = request.get_data().decode('utf-8')
        update = telebot.types.Update.de_json(json_string)
        bot.process_new_updates([update])
        return "OK", 200
    return "Forbidden", 403

@app.route('/callback', methods=['POST'])
def mpesa_callback():
    """ Handles PayHero M-Pesa Callbacks """
    payload = request.json
    if payload.get("status") == "Success":
        uid = payload.get("external_reference")
        amount = int(float(payload.get("amount", 0)))
        user = get_user_data(uid)
        user['balance'] += amount
        save_user_data(uid, user)
        bot.send_message(uid, f"✅ Deposit of KES {amount} confirmed! New balance: KES {user['balance']}")
    return "OK", 200

@app.route('/')
def index():
    return "Movie Bot is Live 24/7", 200
