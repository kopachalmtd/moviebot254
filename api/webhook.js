export default async function handler(req, res) {
  if (req.method === "POST") {
    const message = req.body.message;
    if (message?.text) {
      const chatId = message.chat.id;
      const reply = `You said: ${message.text}`;
      await fetch(`https://api.telegram.org/bot${process.env.BOT_TOKEN}/sendMessage`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ chat_id: chatId, text: reply })
      });
    }
    res.status(200).send("OK");
  } else {
    res.status(200).send("Bot is running");
  }
}
