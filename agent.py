"""
Personal Daily News Collector
=============================
Pipeline:  GDELT + RSS  ->  dedupe  ->  keyword prefilter  ->  Gemini batch  ->  Telegram topics

Run:  python agent.py
Env:  TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, GEMINI_API_KEY
"""

import os
import re
import json
import time
import html
import hashlib
import sqlite3
import logging
from datetime import datetime, timedelta, timezone

import requests
from dotenv import load_dotenv

import gdelt_files

load_dotenv(override=True)
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("agent")


class RateLimited(Exception):
    """GDELT blocked this IP. Abort the run rather than deepening the block."""

# --------------------------------------------------------------------------
# CONFIG
# --------------------------------------------------------------------------

BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
GEMINI_KEY = os.environ["GEMINI_API_KEY"]

GEMINI_MODEL = "gemini-flash-lite-latest"  # alias: always current Flash-Lite. "gemini-flash-latest" = better quality, lower quota
DB_PATH = "seen.db"
LOOKBACK_MINUTES = 1440                   # FIRST RUN ONLY. Set back to 45 once it works.
MAX_PER_CATEGORY = 20                     # GDELT records per category. Raise to 60 once stable.
MAX_SENT_PER_CATEGORY = 6                 # hard cap on Telegram messages per run per category
GDELT_SLEEP = 6                           # GDELT allows ~1 req / 5s per IP. Do not lower this.

# Each category = one Telegram forum topic.
# topic_id: create the topic in your supergroup, send a message in it, then read
#           message_thread_id from https://api.telegram.org/bot<TOKEN>/getUpdates
CATEGORIES = {
    "Gold & Metals": {
        "topic_id": None,
        "gdelt": '(gold OR bullion OR "gold price" OR "precious metals") '
                 '(rally OR crash OR record OR reserve OR import OR duty OR smuggl)',
        "must_match": ["gold", "bullion", "silver", "metal", "sona"],
    },
    "Fuel & Energy": {
        "topic_id": None,
        "gdelt": '("crude oil" OR petrol OR diesel OR "fuel price" OR OPEC OR "natural gas") '
                 '(price OR hike OR cut OR shortage OR pipeline OR refinery)',
        "must_match": ["oil", "petrol", "diesel", "fuel", "opec", "gas", "crude"],
    },
    "Wars & Conflict": {
        "topic_id": None,
        "gdelt": '(airstrike OR ceasefire OR offensive OR "border clash" OR insurgency OR shelling)',
        "must_match": ["strike", "war", "clash", "ceasefire", "troops", "attack", "militar"],
    },
    "AI & Tech": {
        "topic_id": None,
        "gdelt": '("artificial intelligence" OR "language model" OR "AI chip" OR "AI regulation") '
                 '(launch OR ban OR funding OR breakthrough OR lawsuit)',
        "must_match": ["ai ", "artificial intelligence", "model", "chip", "openai", "gemini"],
    },
}

# Localisation matrix: Country -> State -> District/village terms.
# GDELT DOC search is ENGLISH-ONLY (native-language search was phased out),
# so list English spellings + common transliterations. Gemini translates the OUTPUT.
LOCALITY = {
    "enabled": False,               # True = restrict to the places below
    "country_code": "IN",           # GDELT country code: IN, US, UK. None = worldwide.
    "state": "Kerala",
    "districts": ["Thrissur", "Palakkad", "Chalakudy"],
}

SCAN_TOP = 200      # how many of the most-mentioned URLs to resolve titles for

# Fast wire feeds — these beat GDELT's 15-min cycle for breaking news.
RSS_FEEDS = [
    ("Wars & Conflict", "https://feeds.bbci.co.uk/news/world/rss.xml"),
    ("AI & Tech", "https://feeds.arstechnica.com/arstechnica/technology-lab"),
]

# --------------------------------------------------------------------------
# STATE  (dedupe memory)
# --------------------------------------------------------------------------

def db_init():
    con = sqlite3.connect(DB_PATH)
    con.execute("""CREATE TABLE IF NOT EXISTS seen (
                       key TEXT PRIMARY KEY,
                       ts  TEXT NOT NULL)""")
    # keep the file small enough to commit back to git
    con.execute("DELETE FROM seen WHERE ts < ?",
                ((datetime.now(timezone.utc) - timedelta(days=7)).isoformat(),))
    con.commit()
    return con


def norm_title(t: str) -> str:
    """Syndicated copies share a headline but not a URL. Normalise to catch both."""
    t = re.sub(r"[^a-z0-9 ]", "", t.lower())
    return re.sub(r"\s+", " ", t).strip()


def is_new(con, url: str, title: str) -> bool:
    keys = [hashlib.sha1(url.encode()).hexdigest(),
            hashlib.sha1(norm_title(title).encode()).hexdigest()]
    cur = con.execute("SELECT 1 FROM seen WHERE key IN (?,?)", keys)
    if cur.fetchone():
        return False
    now = datetime.now(timezone.utc).isoformat()
    con.executemany("INSERT OR IGNORE INTO seen VALUES (?,?)", [(k, now) for k in keys])
    con.commit()
    return True

# --------------------------------------------------------------------------
# INGEST
# --------------------------------------------------------------------------

def gdelt_query(query: str) -> list:
    """One GDELT DOC 2.0 call. Free, no API key. Respect the rate limit."""
    params = {
        "query": query,
        "mode": "artlist",
        "format": "json",
        "maxrecords": MAX_PER_CATEGORY,
        "timespan": f"{LOOKBACK_MINUTES}min",
        "sort": "datedesc",
    }
    for attempt in range(4):
        try:
            r = requests.get("https://api.gdeltproject.org/api/v2/doc/doc",
                             params=params, timeout=45,
                             headers={"User-Agent": "personal-news-collector/1.0"})
            if r.status_code == 429:
                raise RateLimited("GDELT returned 429")
            r.raise_for_status()
            # GDELT sometimes returns HTML error pages with a 200
            if not r.text.strip().startswith("{"):
                log.warning("GDELT returned non-JSON, skipping")
                return []
            return r.json().get("articles", [])
        except RateLimited:
            raise
        except Exception as e:
            log.warning("GDELT attempt %d failed: %s", attempt + 1, e)
            time.sleep(10 * (attempt + 1))
    return []


def build_query(base: str) -> str:
    """Layer the locality matrix on top of a category query."""
    q = base
    if LOCALITY["enabled"]:
        if LOCALITY.get("sourcecountry"):
            q += f' sourcecountry:{LOCALITY["sourcecountry"]}'
        places = [LOCALITY["state"], *LOCALITY["districts"]]
        places = [p for p in places if p]
        if places:
            q += " (" + " OR ".join(f'"{p}"' for p in places) + ")"
    return q


def fetch_rss(url: str) -> list:
    """Minimal RSS parse — no feedparser dependency needed for standard feeds."""
    try:
        r = requests.get(url, timeout=30, headers={"User-Agent": "personal-news-collector/1.0"})
        r.raise_for_status()
    except Exception as e:
        log.warning("RSS %s failed: %s", url, e)
        return []
    items = re.findall(r"<item>(.*?)</item>", r.text, re.S | re.I)
    out = []
    for it in items[:25]:
        t = re.search(r"<title>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</title>", it, re.S)
        l = re.search(r"<link>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</link>", it, re.S)
        if t and l:
            out.append({"title": html.unescape(t.group(1).strip()),
                        "url": l.group(1).strip(),
                        "domain": re.sub(r"^https?://([^/]+).*", r"\1", l.group(1)),
                        "language": "English",
                        "sourcecountry": ""})
    return out


def collect(con) -> dict:
    """Returns {category: [article, ...]} of unseen, prefiltered candidates."""
    buckets = {c: [] for c in CATEGORIES}

    # GDELT via static data files -- one download serves all categories,
    # no per-query requests, no throttling.
    gd = gdelt_files.collect_by_title(CATEGORIES, LOCALITY,
                                      scan_top=SCAN_TOP,
                                      max_per_cat=MAX_PER_CATEGORY)
    for name, arts in gd.items():
        buckets[name].extend(arts)

    for name, url in RSS_FEEDS:
        if name in buckets:
            buckets[name].extend(fetch_rss(url))

    # dedupe + cheap keyword prefilter (this is what protects your Gemini quota)
    out = {}
    for name, arts in buckets.items():
        must = CATEGORIES[name]["must_match"]
        kept = []
        for a in arts:
            title, url = a.get("title", "").strip(), a.get("url", "").strip()
            if not title or not url:
                continue
            # GDELT rows were already keyword-matched on the resolved title
            if "place" not in a and must and not gdelt_files.kw_hit(title, must):
                continue
            if not is_new(con, url, title):
                continue
            kept.append({"title": title, "url": url,
                         "domain": a.get("domain", ""),
                         "lang": a.get("language", ""),
                         "country": a.get("sourcecountry", "")})
        out[name] = kept[:25]          # cap what reaches the LLM
        log.info("%-16s -> %d after dedupe+filter", name, len(out[name]))
    return out

# --------------------------------------------------------------------------
# GEMINI  (batched — one call judges up to 25 articles)
# --------------------------------------------------------------------------

PROMPT = """You are a news triage analyst for a personal intelligence feed.

Category: {category}
Locality of interest: {locality}

Below are {n} candidate headlines. For EACH, decide if it is genuinely newsworthy
and genuinely about the category. Reject: opinion columns, listicles, stock tips,
horoscopes, sponsored content, and stale rehashes.

For every article you KEEP, return:
  "i"       : the article's index number
  "title_en": the headline in clear English (translate if needed)
  "summary" : ONE sentence, max 25 words, stating the concrete fact — a number,
              a name, a place, or a decision. Never "discusses" or "highlights".
  "impact"  : one of "high", "medium", "low"

Return ONLY a JSON array. No markdown, no prose, no code fences.
If nothing qualifies, return [].

ARTICLES:
{articles}"""


def gemini_batch(category: str, articles: list) -> list:
    if not articles:
        return []
    listing = "\n".join(
        f'{i}. [{a["country"] or "?"}/{a["lang"] or "?"}] {a["title"]}'
        for i, a in enumerate(articles))
    prompt = PROMPT.format(category=category, n=len(articles), articles=listing,
                           locality=f'{LOCALITY["state"]}, {", ".join(LOCALITY["districts"])}'
                                    if LOCALITY["enabled"] else "worldwide")

    url = (f"https://generativelanguage.googleapis.com/v1beta/models/"
           f"{GEMINI_MODEL}:generateContent")
    body = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.2, "responseMimeType": "application/json"},
    }
    for attempt in range(3):
        try:
            r = requests.post(url, headers={"x-goog-api-key": GEMINI_KEY},
                              json=body, timeout=90)
            if r.status_code == 429:
                wait = 30 * (attempt + 1)
                log.warning("Gemini 429, backing off %ss", wait)
                time.sleep(wait)
                continue
            if not r.ok:
                log.error("Gemini HTTP %s: %s", r.status_code, r.text[:200])
                r.raise_for_status()
            txt = r.json()["candidates"][0]["content"]["parts"][0]["text"]
            txt = re.sub(r"^```(?:json)?|```$", "", txt.strip(), flags=re.M).strip()
            picked = json.loads(txt)
            results = []
            for p in picked:
                idx = int(p.get("i", -1))
                if 0 <= idx < len(articles):
                    a = dict(articles[idx])
                    a.update(title_en=p.get("title_en", a["title"]),
                             summary=p.get("summary", ""),
                             impact=p.get("impact", "low"))
                    results.append(a)
            return results
        except Exception as e:
            log.warning("Gemini attempt %d failed: %s", attempt + 1, e)
            time.sleep(15)
    return []

# --------------------------------------------------------------------------
# TELEGRAM
# --------------------------------------------------------------------------

DOT = {"high": "\U0001F534", "medium": "\U0001F7E1", "low": "\U0001F7E2"}
DOT_DEFAULT = "\u26AA"


def esc(s: str) -> str:
    return html.escape(s, quote=False)


def send(text: str, topic_id):
    payload = {"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML",
               "disable_web_page_preview": True}
    if topic_id:
        payload["message_thread_id"] = topic_id
    try:
        r = requests.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                          json=payload, timeout=30)
        if r.status_code == 429:
            time.sleep(int(r.json().get("parameters", {}).get("retry_after", 5)) + 1)
            requests.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                          json=payload, timeout=30)
        elif not r.ok:
            log.error("Telegram error: %s", r.text[:300])
    except Exception as e:
        log.error("Telegram send failed: %s", e)
    time.sleep(1.2)   # stay under Telegram's ~20 msg/min per-chat limit


def deliver(category: str, items: list, topic_id):
    if not items:
        return
    order = {"high": 0, "medium": 1, "low": 2}
    items.sort(key=lambda x: order.get(x["impact"], 3))
    stamp = datetime.now(timezone.utc).strftime("%d %b %H:%M UTC")
    send(f"<b>{esc(category)}</b>  \u00b7  {stamp}", topic_id)
    for a in items[:MAX_SENT_PER_CATEGORY]:
        src = esc(a["domain"] or "source")
        body = (f'{DOT.get(a["impact"], DOT_DEFAULT)} <b>{esc(a["title_en"])}</b>\n'
                f'{esc(a["summary"])}\n'
                f'<a href="{esc(a["url"])}">{src}</a>')
        send(body, topic_id)

# --------------------------------------------------------------------------

def main():
    con = db_init()
    candidates = collect(con)
    total = 0
    for category, arts in candidates.items():
        picked = gemini_batch(category, arts)
        log.info("%-16s -> %d after Gemini", category, len(picked))
        deliver(category, picked, CATEGORIES[category]["topic_id"])
        total += min(len(picked), MAX_SENT_PER_CATEGORY)
        time.sleep(5)   # spread Gemini calls to stay under RPM
    log.info("Run complete. %d items delivered.", total)
    con.close()


if __name__ == "__main__":
    main()