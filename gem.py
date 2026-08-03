import os
import requests
from dotenv import load_dotenv

load_dotenv(override=True)
key = os.environ["GEMINI_API_KEY"]
URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-lite:generateContent"
BODY = {"contents": [{"parts": [{"text": "say ok"}]}]}

attempts = {
    "url param  ": dict(params={"key": key}),
    "goog header": dict(headers={"x-goog-api-key": key}),
    "bearer     ": dict(headers={"Authorization": f"Bearer {key}"}),
}

"""
for name, kw in attempts.items():
    try:
        r = requests.post(URL, json=BODY, timeout=60, **kw)
        print(name, "->", r.status_code, r.text[:90].replace("\n", " "))
    except Exception as e:
        print(name, "-> EXC", e)
"""

r = requests.post(
    "https://generativelanguage.googleapis.com/v1beta/models/gemini-flash-lite-latest:generateContent",
    headers={"x-goog-api-key": os.environ["GEMINI_API_KEY"]},
    json={"contents": [{"parts": [{"text": "say ok"}]}]},
    timeout=60)
print(r.status_code, r.text[:200])