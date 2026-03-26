import os
import requests
import json
import telebot
from flask import Flask, request, jsonify
from supabase import create_client, Client

app = Flask(__name__)

# --- CONFIG ---
TOKEN = os.environ.get("BOT_TOKEN")
PAYHERO_API_URL = "https://backend.payhero.co.ke/api/v2/payments"
PAYHERO_USER = os.environ.get("PAYHERO_USERNAME")
PAYHERO_PASS = os.environ.get("PAYHERO_PASSWORD")

bot = telebot.TeleBot(TOKEN, threaded=False)
supabase: Client = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_SERVICE_ROLE_KEY"))

# --- DB HELPERS ---
def get_user_data(uid):
    res = supabase.table("users").select("data").eq("id", str(uid)).execute()
    if not res.data:
        default = {"balance": 0, "purchases": [], "blocked": False}
        supabase.table("users").insert({"id": str(uid), "data": default}).execute()
        return default
    return res.data[0]['data']

def save_user_data(uid, data):
    supabase.table("users").upsert({"id": str(uid), "data": data}).execute()

# --- PAYHERO HELPERS ---
def payhero_headers():
    import base64
    auth = base64.b64encode(f"{PAYHERO_USER}:{PAYHERO_PASS}".encode()).decode()
    return {"Authorization": f"Basic {auth}", "Content-Type": "application/json"}

def call_payhero(body):
    try:
        r = requests.post(PAYHERO_API_URL, json=body, headers=payhero_headers(), timeout=20)
        return r.status_code, r.json() if r.status_code == 200 else {"raw": r.text}
    except Exception as e:
        return 0, {"error": str(e)}

# --- WEBHOOK ROUTES ---

@app.route("/", methods=["GET"])
def root():
    return "MovieBot Flask server running", 200

@app.route("/", methods=["POST"])
def telegram_webhook():
    if request.headers.get('content-type') == 'application/json':
        json_string = request.get_data().decode('utf-8')
        update = telebot.types.Update.de_json(json_string)
        bot.process_new_updates([update])
        return "OK", 200
    return "Forbidden", 403

@app.route("/payhero-callback", methods=["POST"])
def payhero_callback():
    try:
        payload = request.get_json(force=True)
        resp = payload.get("response") if "response" in payload else payload
        
        # Extract Reference & UserID
        external = resp.get("ExternalReference") or resp.get("external_reference")
        if not external: return jsonify({"status": "no ref"}), 200
        
        user_id = str(external).split("_TOPUP_")[0]
        amount = int(float(resp.get("Amount", 0)))
        status_text = str(resp.get("Status", "")).lower()
        result_code = int(resp.get("ResultCode", 1))

        if result_code == 0 or status_text in ("success", "completed"):
            user = get_user_data(user_id)
            user["balance"] += amount
            save_user_data(user_id, user)
            
            # Send notification directly to user via Bot
            bot.send_message(user_id, f"✅ *Payment Successful!*\nAdded: KES {amount}\nNew Balance: KES {user['balance']}", parse_mode="Markdown")
        
        return jsonify({"message": "ok"}), 200
    except Exception as e:
        print(f"Callback Error: {e}")
        return jsonify({"error": str(e)}), 200

# --- BOT HANDLERS ---
@bot.message_handler(commands=['start'])
def start(message):
    uid = str(message.from_user.id)
    get_user_data(uid)
    markup = telebot.types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        telebot.types.InlineKeyboardButton("🎥 Browse Movies", callback_data="browse_0"),
        telebot.types.InlineKeyboardButton("💳 Deposit", callback_data="deposit"),
        telebot.types.InlineKeyboardButton("💼 My Purchases", callback_data="myp"),
        telebot.types.InlineKeyboardButton("💰 Balance", callback_data="bal"),
        telebot.types.InlineKeyboardButton("🔄 Reset Account", callback_data="reset")
    )
    bot.send_message(message.chat.id, "🎬 *Main Menu*", reply_markup=markup, parse_mode="Markdown")

# Example for STK Push initiation
@bot.message_handler(func=lambda m: m.text and m.text.isdigit() and len(m.text) == 12)
def handle_deposit(message):
    # Logic to trigger call_payhero goes here...
    pass
