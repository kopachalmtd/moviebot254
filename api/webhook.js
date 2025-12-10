import fetch from "node-fetch";

export default async function handler(req, res) {
  if (req.method !== "POST") {
    return res.status(200).send("Bot is running ✅");
  }

  const body = req.body;

  if (body.message) {
    const chatId = body.message.chat.id;
    const text = body.message.text;

    let reply = "Hello! Send /start";

    if (text === "/start") {
      reply = "✅ Bot is now working on Vercel!";
    }

    await fetch(`https://api.telegram.org/bot${process.env.BOT_TOKEN}/sendMessage`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        chat_id: chatId,
        text: reply
      })
    });
  }

  return res.status(200).json({ ok: true });
}
