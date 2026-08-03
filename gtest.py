import requests

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

r = requests.get("https://api.gdeltproject.org/api/v2/doc/doc",
                 params={"query": "gold", "mode": "artlist", "format": "json",
                         "maxrecords": 5, "timespan": "1d"},
                 headers={"User-Agent": UA}, timeout=45)
print("status:", r.status_code)
print("body:", r.text[:300])