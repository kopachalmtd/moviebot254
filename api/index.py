import os
import telebot
from flask import Flask, request

app = Flask(__name__)

# Get the token from Vercel settings
TOKEN = os.environ.get("BOT_TOKEN")

# This check prevents the 'NoneType' crash you saw in the logs
if not TOKEN:
    raise ValueError("BOT_TOKEN is not set in Vercel Environment Variables!")

bot = telebot.TeleBot(TOKEN, threaded=False)

@app.route('/')
def index():
    return "✅ Bot Server is running 24/7!", 200

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
    bot.reply_to(message, "🚀 Bot is now running 24/7 on Vercel!")
