import os
import telebot
from flask import Flask, request

app = Flask(__name__)

# Get variables and provide a dummy string if they are missing to prevent 'NoneType' errors
TOKEN = os.environ.get("BOT_TOKEN", "MISSING_TOKEN")
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")

# This line was crashing because TOKEN was None
bot = telebot.TeleBot(TOKEN, threaded=False)

@app.route('/')
def home():
    if TOKEN == "MISSING_TOKEN":
        return "❌ Error: BOT_TOKEN is missing in Vercel Environment Variables.", 500
    return "✅ Bot is alive and TOKEN is loaded!", 200
