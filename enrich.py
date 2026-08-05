"""
Hourly enrichment: turn headlines into real 120-150 word summaries.

  py enrich.py            enrich up to MAX_ITEMS unenriched stories
  py enrich.py 20         enrich at most 20

Runs on a slower cycle than ingest on purpose. Ingest and publishing are
free and can run every 15 minutes; this step costs LLM calls, so it runs
hourly over whatever has accumulated.

Roughly half of news pages block scrapers or render via JavaScript, so text
extraction fails for those. Those stories keep their short headline-derived
summary rather than getting an invented long one -- a missing summary is
better than a confident fabrication.
"""

import re
import sys
import json
import time
import logging
from concurrent.futures import ThreadPoolExecutor

import requests

import store
import agent
import gdelt_files

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("enrich")

MAX_ITEMS = 72          # per hourly run (~18 Gemini calls)
BATCH = 4               # articles per Gemini call (full text is token-heavy)
MIN_CHARS = 500         # below this, extraction failed -- skip rather than invent

PROMPT = """You are a news editor writing digest entries.

Below are {n} news articles, each with its headline and extracted page text.
The text may contain navigation menus, cookie notices, bylines or other
website furniture -- ignore all of it.

For EACH article write a summary of 120-150 words that:
- opens with the single most important concrete fact (who, what, where, number)
- covers the key details a reader needs without opening the original
- stays strictly to what the text supports; never speculate or add background
  you were not given
- reads as plain reported prose, no bullet points, no "the article says"

If the text is not a news article, is mostly boilerplate, or is too thin to
summarise honestly, return null for that item's "lead" instead of guessing.

Return ONLY a JSON array, one object per article:
  {{"i": <index>, "lead": "<120-150 words, or null>"}}

ARTICLES:
{articles}"""


def pending(con, limit: int) -> list:
    cols = ["url", "title", "category"]
    return [dict(zip(cols, r)) for r in con.execute(
        """SELECT url, title, category FROM published
           WHERE lead IS NULL ORDER BY ts DESC LIMIT ?""", (limit,))]


def fetch_texts(items: list, workers: int = 6) -> list:
    with ThreadPoolExecutor(max_workers=workers) as ex:
        texts = list(ex.map(lambda a: gdelt_files.article_text(a["url"]), items))
    for a, t in zip(items, texts):
        a["text"] = t
    return [a for a in items if len(a.get("text", "")) >= MIN_CHARS]


def summarise(batch: list) -> dict:
    listing = "\n\n".join(
        f'{i}. HEADLINE: {a["title"]}\n   TEXT: {a["text"][:2200]}'
        for i, a in enumerate(batch))
    prompt = PROMPT.format(n=len(batch), articles=listing)

    url = ("https://generativelanguage.googleapis.com/v1beta/models/"
           f"{agent.GEMINI_MODEL}:generateContent")
    body = {"contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {"temperature": 0.3,
                                 "responseMimeType": "application/json"}}
    for attempt in range(3):
        try:
            r = requests.post(url, headers={"x-goog-api-key": agent.GEMINI_KEY},
                              json=body, timeout=120)
            if r.status_code == 429:
                wait = 40 * (attempt + 1)
                log.warning("quota hit, backing off %ss", wait)
                time.sleep(wait)
                continue
            if not r.ok:
                log.error("Gemini HTTP %s: %s", r.status_code, r.text[:180])
                return {}
            txt = r.json()["candidates"][0]["content"]["parts"][0]["text"]
            txt = re.sub(r"^```(?:json)?|```$", "", txt.strip(), flags=re.M).strip()
            out = {}
            for p in json.loads(txt):
                idx, lead = int(p.get("i", -1)), p.get("lead")
                if 0 <= idx < len(batch) and lead and len(lead.split()) >= 60:
                    out[batch[idx]["url"]] = lead.strip()
            return out
        except Exception as e:
            log.warning("attempt %d failed: %s", attempt + 1, e)
            time.sleep(12)
    return {}


def main(argv):
    limit = int(argv[0]) if argv and argv[0].isdigit() else MAX_ITEMS
    con = store.connect()

    left = store.budget_left(con)
    if left < 4:
        log.warning("daily Gemini budget exhausted (%d used) - skipping",
                    store.calls_today(con))
        return
    limit = min(limit, left * BATCH)

    todo = pending(con, limit)
    if not todo:
        log.info("nothing to enrich")
        return
    log.info("candidates: %d", len(todo))

    usable = fetch_texts(todo)
    log.info("with usable text: %d/%d", len(usable), len(todo))
    if not usable:
        return

    written = 0
    for i in range(0, len(usable), BATCH):
        if store.budget_left(con) < 2:
            log.warning("budget reached mid-run, stopping at %d", written)
            break
        store.record_call(con, "enrich")
        leads = summarise(usable[i:i + BATCH])
        if leads:
            con.executemany("UPDATE published SET lead=? WHERE url=?",
                            [(v, k) for k, v in leads.items()])
            con.commit()
            written += len(leads)
        time.sleep(5)      # stay under requests-per-minute

    log.info("wrote %d long summaries", written)
    con.close()


if __name__ == "__main__":
    main(sys.argv[1:])
