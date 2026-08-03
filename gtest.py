import requests

r = requests.get(
    "https://api.gdeltproject.org/api/v2/doc/doc",
    params={"query": "gold", "mode": "artlist", "format": "json",
            "maxrecords": 5, "timespan": "1d"},
    headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
    timeout=45)

print("status:", r.status_code)
print("headers:", dict(r.headers))
print("body:", r.text[:500])