import os
import requests
from dotenv import load_dotenv

load_dotenv(override=True)
token = os.environ["TELEGRAM_BOT_TOKEN"]

r = requests.get(f"https://api.telegram.org/bot{token}/getUpdates", timeout=30)
updates = r.json().get("result", [])
print(f"{len(updates)} updates found\n")

chats = set()
topics = {}

for u in updates:
    msg = u.get("message") or u.get("channel_post") or {}
    chat = msg.get("chat", {})
    if chat.get("id"):
        chats.add((chat["id"], chat.get("title", "?")))

    tid = msg.get("message_thread_id")
    if tid:
        reply = msg.get("reply_to_message", {})
        created = reply.get("forum_topic_created", {})
        name = created.get("name")
        if name:
            topics[tid] = name
        elif tid not in topics:
            topics[tid] = "(unknown - post directly in topic)"

for cid, title in chats:
    print(f"CHAT_ID: {cid}   ({title})")

print()
for tid, name in sorted(topics.items()):
    print(f"topic_id: {tid:<6} {name}")