import os
import re
import json
import asyncio
import base64
from datetime import datetime
from flask import Flask, request, jsonify
import requests
from supabase import create_client, Client
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ApplicationBuilder, CommandHandler, CallbackQueryHandler, MessageHandler, filters

app = Flask(__name__)

# ---------------- CONFIG ----------------
TOKEN = os.getenv("BOT_TOKEN")
ADMIN_IDS = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]
MOVIE_COST = int(os.getenv("MOVIE_COST", "10"))
PAYHERO_API_URL = os.getenv("PAYHERO_API_URL")

# Supabase Config
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Initialize Bot Application for Webhook mode
# Note: We don't use run_polling() on Vercel
application = ApplicationBuilder().token(TOKEN).build()

# ---------------- SUPABASE HELPERS ----------------
def get_user(uid):
    res = supabase.table("users").select("data").eq("id", str(uid)).execute()
    if not res.data:
        default_data = {
            "balance": 0, "purchases": [], "purchase_history": [],
            "pending_payments": [], "blocked": False, "state": None, "temp_amt": None
        }
        supabase.table("users").insert({"id": str(uid), "data": default_data}).execute()
        return default_data
    return res.data[0]['data']

def save_user(uid, data):
    supabase.table("users").upsert({"id": str(uid), "data": data}).execute()

# ---------------- KEYBOARDS ----------------
def main_menu_keyboard(user_id: str = None):
    kb = [
        [InlineKeyboardButton("🎥 Browse Movies", callback_data="browse_0")],
        [InlineKeyboardButton("💳 Deposit", callback_data="deposit")],
        [InlineKeyboardButton("💼 My Purchases", callback_data="myp")],
        [InlineKeyboardButton("💰 Balance", callback_data="bal")],
        [InlineKeyboardButton("🔄 Reset Account", callback_data="reset")],
        [InlineKeyboardButton("📢 Join Channel", callback_data="join_channel")],
        [InlineKeyboardButton("☎ Contact Admin", callback_data="contact_admin")],
        [InlineKeyboardButton("🎁 Claim Bonus", callback_data="claim_bonus")],
    ]
    if user_id and int(user_id) in ADMIN_IDS:
        kb.append([InlineKeyboardButton("🛠 Admin Panel", callback_data="admin_panel")])
    return InlineKeyboardMarkup(kb)

async def send_main_menu(chat_id: int):
    db = load_db()
    user = db.get(str(chat_id), {})
    kb = main_menu_keyboard(str(chat_id))
    try:
        await telegram_app.bot.send_message(chat_id=int(chat_id), text="🎬 Welcome to MovieBot! Use the menu below:", reply_markup=kb)
    except Exception as e:
        print("send_main_menu error:", e)

# consumer that reads notify_queue and informs users (runs in bot event loop)
async def notify_consumer():
    while True:
        item = await asyncio.to_thread(notify_queue.get)  # blocking pop
        uid = item["user_id"]
        try:
            if item.get("success"):
                await telegram_app.bot.send_message(
                    chat_id=int(uid),
                    text=(f"✅ Payment successful!\n"
                          f"Previous balance: KES {item.get('old_balance')}\n"
                          f"Current balance: KES {item.get('new_balance')}\n\n"
                          "Thank you — enjoy your movies!")
                )
            elif item.get("cancelled"):
                await telegram_app.bot.send_message(chat_id=int(uid), text="⚠️ Payment cancelled by user. No changes made.")
            elif item.get("failed"):
                await telegram_app.bot.send_message(chat_id=int(uid), text="❌ Payment failed. No changes made.")
            # send menu after notification
            await send_main_menu(int(uid))
        except Exception as e:
            print("notify_consumer send error:", e)
        await asyncio.sleep(0.2)

# periodic reminder job
async def reminder_job():
    while True:
        db = load_db()
        now = datetime.utcnow()
        for uid, u in list(db.items()):
            try:
                last = u.get("last_reminder")
                send_msg = False
                if not last:
                    send_msg = True
                else:
                    try:
                        last_dt = datetime.fromisoformat(last)
                        if now - last_dt >= timedelta(hours=10):
                            send_msg = True
                    except Exception:
                        send_msg = True
                if send_msg:
                    try:
                        await telegram_app.bot.send_message(chat_id=int(uid), text="📽️ Dear customer, don't forget to watch your favorite movies! Welcome back 🙂")
                        u["last_reminder"] = now.isoformat()
                    except Exception:
                        pass
            except Exception:
                pass
        save_db(db)
        await asyncio.sleep(10 * 3600)

async def startup_tasks(app):
    # this function will be passed to ApplicationBuilder.post_init(...)
    print("BOT STARTUP TASKS: scheduling notify_consumer and reminder_job")
    # schedule background worker coroutines on the bot's loop
    app.create_task(notify_consumer())
    app.create_task(reminder_job())

# ---------------- Handlers ----------------
async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    db = load_db()
    ensure_user(db, uid)
    save_db(db)
    # send menu that shows admin button when applicable
    kb = main_menu_keyboard(uid)
    try:
        if update.callback_query:
            await update.callback_query.message.edit_text("🎬 Welcome to MovieBot!", reply_markup=kb)
        else:
            await update.message.reply_text("🎬 Welcome to MovieBot!", reply_markup=kb)
    except Exception:
        # fallback
        await update.message.reply_text("🎬 Welcome to MovieBot!", reply_markup=kb)

async def show_movies_page(q, page: int):
    start = page * MOVIES_PER_PAGE
    end = start + MOVIES_PER_PAGE
    page_movies = MOVIES[start:end]

    kb = []
    row = []

    max_cols = 3   # 4 buttons per row
    max_rows = 20   # 4 rows = 50 movies per grid

    count = 0

    for m in page_movies:
        row.append(InlineKeyboardButton(m["title"], callback_data=f"movie_{m['id']}"))
        count += 1

        # If row full → push row
        if len(row) == max_cols:
            kb.append(row)
            row = []

        # Stop adding when 16 movies displayed
        if len(kb) == max_rows:
            break

    # Add leftover row
    if row and len(kb) < max_rows:
        kb.append(row)

    # Navigation buttons
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅ Prev", callback_data=f"browse_{page-1}"))
    if end < len(MOVIES):
        nav.append(InlineKeyboardButton("Next ➡", callback_data=f"browse_{page+1}"))

    if nav:
        kb.append(nav)

    # Back button
    kb.append([InlineKeyboardButton("⬅ Back", callback_data="menu")])

    try:
        await q.message.edit_text(
            "🎥 Choose a movie:",
            reply_markup=InlineKeyboardMarkup(kb)
        )
    except Exception:
        await q.message.reply_text(
            "🎥 Choose a movie:",
            reply_markup=InlineKeyboardMarkup(kb)
        )

async def callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    uid = str(q.from_user.id)
    db = load_db()
    user = ensure_user(db, uid)

    # BROWSE
    if data.startswith("browse_"):
        page = int(data.split("_",1)[1])
        await show_movies_page(q, page)
        return

    # MOVIE PREVIEW
    if data.startswith("movie_"):
        movie_id = data.split("_",1)[1]
        mv = next((m for m in MOVIES if m["id"] == movie_id), None)
        if not mv:
            await q.message.edit_text("Movie not found.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅ Back", callback_data="browse_0")]]))
            return
        kb = InlineKeyboardMarkup([[InlineKeyboardButton(f"Buy (KES {MOVIE_COST})", callback_data=f"buy_{movie_id}")],
                                   [InlineKeyboardButton("⬅ Back", callback_data="browse_0")]])
        if mv.get("photo_id"):
            try:
                await q.message.reply_photo(photo=mv["photo_id"], caption=f"{mv['title']}\nPrice: KES {MOVIE_COST}", reply_markup=kb)
            except Exception:
                await q.message.edit_text(f"{mv['title']}\nPrice: KES {MOVIE_COST}", reply_markup=kb)
        else:
            await q.message.edit_text(f"{mv['title']}\nPrice: KES {MOVIE_COST}", reply_markup=kb)
        return

    # BUY
    if data.startswith("buy_"):
        movie_id = data.split("_",1)[1]
        mv = next((m for m in MOVIES if m["id"] == movie_id), None)
        if not mv:
            await q.message.edit_text("Movie not found.")
            return
        if user.get("blocked"):
            await q.message.edit_text("❌ Your account is blocked. Contact admin.")
            return
        if user.get("balance", 0) < MOVIE_COST:
            await q.message.edit_text("❌ Insufficient balance. Please deposit.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("💳 Deposit", callback_data="deposit")]]))
            return
        # deduct, record purchase, send
        user["balance"] -= MOVIE_COST
        user.setdefault("purchases", []).append(movie_id)
        user.setdefault("purchase_history", []).append({"movie": movie_id, "ts": datetime.utcnow().isoformat()})
        save_db(db)
        try:
            await context.bot.send_video(chat_id=int(uid), video=mv["file_id"], caption=mv["title"])
            await q.message.edit_text(f"✅ Movie delivered. Remaining balance: KES {user['balance']}", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅ Back", callback_data="browse_0")]]))
        except Exception:
            await q.message.edit_text("✅ Purchase recorded but failed to send movie file. Contact admin.")
        return

    # DEPOSIT - start
    if data == "deposit":
        context.user_data["await_amount"] = True
        await q.message.edit_text("Enter amount to deposit (KES):")
        return

    # MY PURCHASES
    if data == "myp":
        if not user.get("purchases"):
            await q.message.edit_text("❌ You have no purchases yet.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅ Back", callback_data="menu")]]))
            return
        for pid in user.get("purchases", []):
            mv = next((m for m in MOVIES if m["id"] == pid), None)
            if mv:
                try:
                    await context.bot.send_video(chat_id=int(uid), video=mv["file_id"], caption=mv["title"])
                except Exception:
                    pass
        await q.message.edit_text("🎁 Sent your purchased movies.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅ Back", callback_data="menu")]]))
        return

    # BALANCE
    if data == "bal":
        await q.message.edit_text(f"💰 Your balance: KES {user.get('balance',0)}", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅ Back", callback_data="menu")]]))
        return

    # RESET request
    if data == "reset":
        user["reset_request"] = datetime.utcnow().isoformat()
        save_db(db)
        await q.message.edit_text("⚠️ Reset requested. Admin will review. You can cancel this request before approval.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel Reset", callback_data="cancel_reset")]]))
        # notify admins
        for admin in ADMIN_IDS:
            try:
                kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ Approve Reset", callback_data=f"admin_approve_reset_{uid}"),
                     InlineKeyboardButton("❌ Reject Reset", callback_data=f"admin_reject_reset_{uid}")]
                ])
                await context.bot.send_message(chat_id=int(admin), text=f"User {uid} requested account reset. Approve?", reply_markup=kb)
            except Exception:
                pass
        return

    if data == "cancel_reset":
        user["reset_request"] = None
        save_db(db)
        await q.message.edit_text("✅ Reset request cancelled.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅ Back", callback_data="menu")]]))
        # notify admins
        for admin in ADMIN_IDS:
            try:
                kb = InlineKeyboardMarkup([
                    [InlineKeyboardButton("❌ Reject Reset", callback_data=f"admin_reject_reset_{uid}")]
                ])
                await context.bot.send_message(chat_id=int(admin), text=f"User {uid} requested account reset. Approve?", reply_markup=kb)
            except Exception:
                pass
        return

    # Join channel
    if data == "join_channel":
        if CHANNEL_LINK:
            await q.message.edit_text(f"📢 Please join our channel: {CHANNEL_LINK}", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅ Back", callback_data="menu")]]))
        else:
            await q.message.edit_text("📢 Channel link not configured.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅ Back", callback_data="menu")]]))
        return

    # Contact admin
    if data == "contact_admin":
        contact_text = "Contact admin:\n"
        if ADMIN_USERNAME:
            contact_text += f"@{ADMIN_USERNAME}\n"
        contact_text += "Or use the admin panel if you have issues."
        await q.message.edit_text(contact_text, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅ Back", callback_data="menu")]]))
        return

    # Claim bonus
    if data == "claim_bonus":
        now = datetime.utcnow()
        recent = 0
        for rec in user.get("purchase_history", []):
            try:
                ts = datetime.fromisoformat(rec["ts"])
                if now - ts <= timedelta(hours=24):
                    recent += 1
            except Exception:
                pass
        if recent >= BONUS_REQUIRED and not user.get("bonus_claimed", False):
            user["balance"] = user.get("balance",0) + BONUS_AMOUNT
            user["bonus_claimed"] = True
            save_db(db)
            await q.message.edit_text(f"🎉 Bonus approved! KES {BONUS_AMOUNT} added to your balance. Current balance: KES {user['balance']}", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅ Back", callback_data="menu")]]))
        else:
            await q.message.edit_text("❌ You are not eligible for the bonus or already claimed it.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅ Back", callback_data="menu")]]))
        return



    # ADMIN PANEL (top-level)
    if data == "admin_panel":
        if int(uid) not in ADMIN_IDS:
            await q.message.edit_text("Unauthorized.")
            return
        kb = [
            [InlineKeyboardButton("👥 View Users", callback_data="admin_view_users")],
            [InlineKeyboardButton("➕ Add Balance", callback_data="admin_addbal"), InlineKeyboardButton("➖ Remove Balance", callback_data="admin_removebal")],
            [InlineKeyboardButton("⛔ Block/Unblock User", callback_data="admin_block")],
            [InlineKeyboardButton("🗑️ Delete User", callback_data="admin_delete")],
            [InlineKeyboardButton("⬅ Back", callback_data="menu")],
        ]
        await q.message.edit_text("Admin Panel", reply_markup=InlineKeyboardMarkup(kb))
        return

    # Admin actions (view, add, remove, block, delete)
    if data == "admin_view_users":
        if int(uid) not in ADMIN_IDS:
            await q.message.edit_text("Unauthorized.")
            return
        db = load_db()
        rows = []
        for i, (u_id, uinfo) in enumerate(db.items()):
            if i >= 50:
                break
            rows.append(f"{u_id} — KES {uinfo.get('balance',0)} — purchases {len(uinfo.get('purchases',[]))} — blocked:{uinfo.get('blocked',False)}")
        text = "Users:\n" + ("\n".join(rows) if rows else "No users")
        await q.message.edit_text(text, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅ Back", callback_data="admin_panel")]]))
        return

    if data in ("admin_addbal", "admin_removebal", "admin_block", "admin_delete"):
        if int(uid) not in ADMIN_IDS:
            await q.message.edit_text("Unauthorized.")
            return
        # set admin multi-step state
        context.user_data["admin_action"] = data + "_wait_user"
        await q.message.edit_text("Send the target user's numeric ID now (e.g. 6725602268).")
        return

    # admin approve/reset callbacks
    if data.startswith("admin_approve_reset_") or data.startswith("admin_reject_reset_"):
        if int(uid) not in ADMIN_IDS:
            await q.message.edit_text("Unauthorized.")
            return
        parts = data.split("_")
        target = parts[-1]
        db = load_db()
        target_user = db.get(target)
        if not target_user:
            await q.message.edit_text("User not found.")
            return
        if data.startswith("admin_approve_reset_"):
            del db[target]
            save_db(db)
            try:
                await context.bot.send_message(chat_id=int(target), text="✅ Your account reset was approved by admin and your data has been deleted.")
            except Exception:
                pass
            await q.message.edit_text(f"User {target} reset approved and data deleted.")
        else:
            target_user["reset_request"] = None
            save_db(db)
            try:
                await context.bot.send_message(chat_id=int(target), text="❌ Your account reset request was rejected by admin.")
            except Exception:
                pass
            await q.message.edit_text(f"User {target} reset rejected.")
        return

    # BACK TO MENU
    if data == "menu":
        await start_handler(update, context)
        return

    # fallback
    await q.message.edit_text("Unrecognized action. Use /start to open menu.")

# text handler (deposit + admin multi-step)
async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    db = load_db()
    ensure_user(db, uid)

    text = (update.message.text or "").strip()

    admin_action = context.user_data.get("admin_action")
    if admin_action:
        # admin flows: state "admin_addbal_wait_user", etc
        if admin_action.endswith("_wait_user"):
            context.user_data["admin_target_user"] = text.strip()
            if admin_action.startswith("admin_addbal") or admin_action.startswith("admin_removebal"):
                context.user_data["admin_action"] = admin_action.replace("_wait_user", "_wait_amount")
                await update.message.reply_text("Enter amount (KES):")
                return
            # block/delete immediate
            target = context.user_data.get("admin_target_user")
            db = load_db()
            if admin_action.startswith("admin_block"):
                if target in db:
                    db[target]["blocked"] = not db[target].get("blocked", False)
                    save_db(db)
                    await update.message.reply_text(f"User {target} blocked toggled to {db[target]['blocked']}.")
                else:
                    await update.message.reply_text("User not found.")
            elif admin_action.startswith("admin_delete"):
                if target in db:
                    del db[target]
                    save_db(db)
                    await update.message.reply_text(f"User {target} deleted.")
                else:
                    await update.message.reply_text("User not found.")
            context.user_data.pop("admin_action", None)
            context.user_data.pop("admin_target_user", None)
            return

        if admin_action.endswith("_wait_amount"):
            target = context.user_data.get("admin_target_user")
            if not text.isdigit():
                await update.message.reply_text("Enter numeric amount only.")
                return
            amt = int(text)
            db = load_db()
            if target not in db:
                await update.message.reply_text("User not found.")
                context.user_data.pop("admin_action", None)
                context.user_data.pop("admin_target_user", None)
                return
            if admin_action.startswith("admin_addbal"):
                db[target]["balance"] = db[target].get("balance", 0) + amt
                save_db(db)
                await update.message.reply_text(f"Added KES {amt} to {target}. New balance: KES {db[target]['balance']}")
                try:
                    await context.bot.send_message(chat_id=int(target), text=f"✅ Admin added KES {amt} to your account. New balance: KES {db[target]['balance']}")
                except Exception:
                    pass
            elif admin_action.startswith("admin_removebal"):
                db[target]["balance"] = max(0, db[target].get("balance", 0) - amt)
                save_db(db)
                await update.message.reply_text(f"Removed KES {amt} from {target}. New balance: KES {db[target]['balance']}")
                try:
                    await context.bot.send_message(chat_id=int(target), text=f"❌ Admin removed KES {amt} from your account. New balance: KES {db[target]['balance']}")
                except Exception:
                    pass
            context.user_data.pop("admin_action", None)
            context.user_data.pop("admin_target_user", None)
            return

    # Deposit flow: amount -> phone -> call PayHero
    if context.user_data.get("await_amount"):
        if not text.isdigit():
            await update.message.reply_text("Enter numbers only (e.g. 50)")
            return
        context.user_data["amount"] = int(text)
        context.user_data["await_amount"] = False
        context.user_data["await_phone"] = True
        await update.message.reply_text("Enter Safaricom phone number (07XXXXXXXX or 01XXXXXXXX):")
        return

    if context.user_data.get("await_phone"):
        phone = re.sub(r"[^0-9]", "", text)
        amt = context.user_data.get("amount")
        context.user_data["await_phone"] = False
        status, resp = await send_stk_push(uid, phone, amt)
        try:
            print("PAYHERO SEND:", status, resp)
        except Exception:
            pass
        if isinstance(resp, dict) and (resp.get("success") is True or str(resp.get("status","")).lower() in ("queued","pending","ok")):
            db = load_db()
            user = ensure_user(db, uid)
            user.setdefault("pending_payments", []).append(amt)
            save_db(db)
            await update.message.reply_text("✅ STK Push sent. Complete it on your phone. We'll auto-confirm when callback arrives.")
        else:
            await update.message.reply_text(f"❌ Failed to send STK push. Response: {resp}")
        return

      # fallback
    await update.message.reply_text("Use the menu buttons. /start to open menu.")

# ---------------- VERCEL ROUTES ----------------

@app.route("/", methods=["POST"])
async def telegram_webhook():
    """Process updates from Telegram"""
    update = Update.de_json(request.get_json(force=True), application.bot)
    
    # Manually route since we are in serverless mode
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
    """Update Supabase balance when payment is successful"""
    data = request.get_json(force=True).get("response", {})
    if data.get("Status") == "Success":
        ref = data.get("ExternalReference", "")
        uid = ref.split("_TOPUP_")[0]
        amount = int(float(data.get("Amount", 0)))
        
        user = get_user(uid)
        user["balance"] += amount
        save_user(uid, user)
        
        # Notify user (Optional: Requires a separate bot instance call)
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": uid, "text": f"✅ Received KES {amount}!"})
        
    return "OK", 200

@app.route("/", methods=["GET"])
def index():
    return "Bot is Active", 200
