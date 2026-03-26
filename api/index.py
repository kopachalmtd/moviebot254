import os
import requests
import telebot
from flask import Flask, request, jsonify
from supabase import create_client, Client

app = Flask(__name__)

# --- CONFIG ---
TOKEN = "7292856168:AAFYNTxrWzyxpO1RMEC_BL5rw7E5-xfcmSM"
# Use your actual Supabase credentials from Vercel Env Variables
supabase = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_SERVICE_ROLE_KEY"))
bot = telebot.TeleBot(TOKEN, threaded=False)

MOVIES_PER_PAGE = 12
# Add your full list of 145 movies here
MOVIES = [
    {"id": "m1", "title": "Alice Boderland", "file_id": "FILE_ID_1"},
    {"id": "m2", "title": "See", "file_id": "FILE_ID_2"},
    # ... include the rest of your list
]

# --- DB HELPERS ---
def get_user_data(uid):
    res = supabase.table("users").select("data").eq("id", str(uid)).execute()
    if not res.data:
        default = {"balance": 0, "purchases": [], "state": None}
        supabase.table("users").insert({"id": str(uid), "data": default}).execute()
        return default
    return res.data[0]['data']

def save_user_data(uid, data):
    supabase.table("users").upsert({"id": str(uid), "data": data}).execute()

# --- KEYBOARDS ---
def main_menu():
    markup = telebot.types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        telebot.types.InlineKeyboardButton("🎥 Browse Movies", callback_data="browse_0"),
        telebot.types.InlineKeyboardButton("💳 Deposit", callback_data="deposit"),
        telebot.types.InlineKeyboardButton("💰 Balance", callback_data="bal"),
        telebot.types.InlineKeyboardButton("💼 My Purchases", callback_data="myp")
    )
    return markup

# --- BOT HANDLERS ---
@bot.message_handler(commands=['start'])
def start(message):
    bot.send_message(message.chat.id, "🎬 **Welcome to MovieBot254!**", 
                     reply_markup=main_menu(), parse_mode="Markdown")

@bot.callback_query_handler(func=lambda call: True)
def handle_query(call):
    uid = str(call.from_user.id)
    user = get_user_data(uid)
    
    if call.data.startswith("browse_"):
        page = int(call.data.split("_")[1])
        start_idx = page * MOVIES_PER_PAGE
        end_idx = start_idx + MOVIES_PER_PAGE
        page_movies = MOVIES[start_idx:end_idx]
        
        markup = telebot.types.InlineKeyboardMarkup(row_width=2)
        for m in page_movies:
            markup.add(telebot.types.InlineKeyboardButton(m["title"], callback_data=f"buy_{m['id']}"))
        
        nav = []
        if page > 0: nav.append(telebot.types.InlineKeyboardButton("⬅ Prev", callback_data=f"browse_{page-1}"))
        if end_idx < len(MOVIES): nav.append(telebot.types.InlineKeyboardButton("Next ➡", callback_data=f"browse_{page+1}"))
        if nav: markup.row(*nav)
        markup.add(telebot.types.InlineKeyboardButton("🏠 Menu", callback_data="menu"))
        
        bot.edit_message_text("🎥 **Choose a movie (Ksh 10):**", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")

    elif call.data == "bal":
        bot.answer_callback_query(call.id, f"Balance: Ksh {user['balance']}", show_alert=True)

# --- VERCEL WEBHOOK ROUTE ---
@app.route('/', methods=['POST'])
def telegram_webhook():
    if request.headers.get('content-type') == 'application/json':
        json_string = request.get_data().decode('utf-8')
        update = telebot.types.Update.de_json(json_string)
        bot.process_new_updates([update])
        return "OK", 200
    return "Forbidden", 403

# --- PAYHERO CALLBACK ROUTE ---
@app.route('/payhero-callback', methods=['POST'])
def payhero_callback():
    payload = request.get_json()
    # Check for success and extract 'ExternalReference' (formatted as USERID_TOPUP_AMT)
    # Update balance in Supabase here...
    return "OK", 200

@app.route('/')
def index():
    return "MovieBot is Live on Vercel", 200
