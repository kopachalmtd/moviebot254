import os
import re
import requests
import json
import telebot
from flask import Flask, request, jsonify
from supabase import create_client, Client
from datetime import datetime, timedelta

app = Flask(__name__)

# --- CONFIG ---
TOKEN = os.environ.get("BOT_TOKEN")
ADMIN_IDS = [int(i) for i in os.environ.get("ADMIN_IDS", "6725602268").split(",")]
MOVIE_COST = 10
MOVIES_PER_PAGE = 15 # 3 cols x 5 rows

# PayHero Config
PAYHERO_API_URL = "https://backend.payhero.co.ke/api/v2/payments"
PAYHERO_USER = os.environ.get("PAYHERO_USERNAME")
PAYHERO_PASS = os.environ.get("PAYHERO_PASSWORD")

bot = telebot.TeleBot(TOKEN, threaded=False)
supabase: Client = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_SERVICE_ROLE_KEY"))

# --- DB HELPERS ---
def get_user(uid):
    res = supabase.table("users").select("data").eq("id", str(uid)).execute()
    if not res.data:
        default = {"balance": 0, "purchases": [], "blocked": False, "history": []}
        supabase.table("users").insert({"id": str(uid), "data": default}).execute()
        return default
    return res.data[0]['data']

def save_user(uid, data):
    supabase.table("users").upsert({"id": str(uid), "data": data}).execute()

# --- KEYBOARDS ---
def main_menu_keyboard(uid):
    markup = telebot.types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        telebot.types.InlineKeyboardButton("🎥 Browse Movies", callback_data="browse_0"),
        telebot.types.InlineKeyboardButton("💳 Deposit", callback_data="deposit"),
        telebot.types.InlineKeyboardButton("💼 My Purchases", callback_data="myp"),
        telebot.types.InlineKeyboardButton("💰 Balance", callback_data="bal"),
        telebot.types.InlineKeyboardButton("🎁 Claim Bonus", callback_data="claim_bonus"),
        telebot.types.InlineKeyboardButton("☎ Contact Admin", callback_data="contact_admin")
    )
    if int(uid) in ADMIN_IDS:
        markup.add(telebot.types.InlineKeyboardButton("🛠 Admin Panel", callback_data="admin_panel"))
    return markup

# --- MOVIE BROWSER LOGIC ---
MOVIES = [{"id": str(i), "title": f"Movie {i}", "file_id": "..."} for i in range(1, 51)] # Example list

def get_movie_grid(page):
    start = page * MOVIES_PER_PAGE
    end = start + MOVIES_PER_PAGE
    page_movies = MOVIES[start:end]
    
    markup = telebot.types.InlineKeyboardMarkup(row_width=3)
    btns = [telebot.types.InlineKeyboardButton(m["title"], callback_data=f"movie_{m['id']}") for m in page_movies]
    markup.add(*btns)
    
    nav = []
    if page > 0: nav.append(telebot.types.InlineKeyboardButton("⬅ Prev", callback_data=f"browse_{page-1}"))
    if end < len(MOVIES): nav.append(telebot.types.InlineKeyboardButton("Next ➡", callback_data=f"browse_{page+1}"))
    if nav: markup.add(*nav)
    markup.add(telebot.types.InlineKeyboardButton("⬅ Back to Menu", callback_data="menu"))
    return markup

# --- BOT HANDLERS ---
@bot.message_handler(commands=['start'])
def start(message):
    uid = str(message.from_user.id)
    get_user(uid)
    bot.send_message(message.chat.id, "🎬 **Welcome to MovieBot!**", 
                     reply_markup=main_menu_keyboard(uid), parse_mode="Markdown")

@bot.callback_query_handler(func=lambda call: True)
def handle_callbacks(call):
    uid = str(call.from_user.id)
    user = get_user(uid)
    data = call.data

    if data == "menu":
        bot.edit_message_text("🎬 **Main Menu**", call.message.chat.id, call.message.message_id, 
                             reply_markup=main_menu_keyboard(uid), parse_mode="Markdown")
    
    elif data.startswith("browse_"):
        page = int(data.split("_")[1])
        bot.edit_message_text("🎥 **Choose a movie:**", call.message.chat.id, call.message.message_id, 
                             reply_markup=get_movie_grid(page), parse_mode="Markdown")

    elif data == "bal":
        bot.answer_callback_query(call.id, f"Current Balance: KES {user['balance']}", show_alert=True)

    elif data == "admin_panel":
        if int(uid) in ADMIN_IDS:
            markup = telebot.types.InlineKeyboardMarkup()
            markup.add(telebot.types.InlineKeyboardButton("👥 View Users", callback_data="admin_view"))
            markup.add(telebot.types.InlineKeyboardButton("⬅ Back", callback_data="menu"))
            bot.edit_message_text("🛠 **Admin Panel**", call.message.chat.id, call.message.message_id, reply_markup=markup)

# --- FLASK & CALLBACKS ---
@app.route("/", methods=["POST"])
def webhook():
    if request.headers.get('content-type') == 'application/json':
        json_string = request.get_data().decode('utf-8')
        update = telebot.types.Update.de_json(json_string)
        bot.process_new_updates([update])
        return "OK", 200
    return "Forbidden", 403

@app.route("/payhero-callback", methods=["POST"])
def payhero_callback():
    payload = request.get_json(force=True)
    resp = payload.get("response", payload)
    
    ext = resp.get("ExternalReference") or resp.get("external_reference")
    if ext and "_TOPUP_" in ext:
        uid = ext.split("_TOPUP_")[0]
        amount = int(float(resp.get("Amount", 0)))
        
        if str(resp.get("Status")).lower() in ("success", "completed"):
            user = get_user(uid)
            user["balance"] += amount
            save_user(uid, user)
            bot.send_message(uid, f"✅ **Deposit Confirmed!**\nAdded: KES {amount}\nBalance: KES {user['balance']}", parse_mode="Markdown")
            
    return jsonify({"status": "ok"}), 200

@app.route("/")
def index():
    return "MovieBot Live", 200
