from flask import Flask, request, jsonify
import os
import telebot
from supabase import create_client, Client

app = Flask(__name__)

# Load Env Vars
TOKEN = os.environ.get("BOT_TOKEN")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

# Initialize Clients
bot = telebot.TeleBot(TOKEN, threaded=False)
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

@app.route('/', methods=['GET'])
def home():
    return "Bot is alive and reaching Vercel!", 200

@app.route('/', methods=['POST'])
def webhook():
    if request.headers.get('content-type') == 'application/json':
        json_string = request.get_data().decode('utf-8')
        update = telebot.types.Update.de_json(json_string)
        bot.process_new_updates([update])
        return "OK", 200
    return "Forbidden", 403

@bot.message_handler(commands=['start'])
def start(message):
    bot.reply_to(message, "✅ Vercel Connection Successful! Your bot is online.")

# PayHero Callback Placeholder
@app.route('/callback', methods=['POST'])
def callback():
    print("Callback received:", request.json)
    return "OK", 200
