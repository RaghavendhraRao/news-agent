import os
import requests
from dotenv import load_dotenv, dotenv_values

print("keys in .env:", [repr(k) for k in dotenv_values(".env").keys()])

load_dotenv(override=True)
token = os.environ["TELEGRAM_BOT_TOKEN"]
print("token id:", token.split(":")[0])

r = requests.get(f"https://api.telegram.org/bot{token}/getMe", timeout=30)
print("status:", r.status_code)
print(r.text[:200])