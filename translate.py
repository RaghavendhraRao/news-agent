"""
Translate enriched stories into additional languages.

  py translate.py            all languages in LANGUAGES
  py translate.py hi ml      only these

Cost model: a translation is written once and stored forever, so this pays
only for articles it has not seen before -- roughly 40 an hour in steady
state, not 40 every run. That is what keeps multi-language inside a free
tier; re-translating the whole feed each cycle would not fit.
"""

import re
import sys
import json
import time
import logging

import requests

import store

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("translate")

LANGUAGES = store.EXTRA_LANGUAGES

BATCH = 6
PER_RUN = 60            # per language per hourly run (~10 calls)

PROMPT = """Translate these {n} news items into {language}.

Rules:
- Translate the headline and the summary. Keep the meaning exact.
- Do NOT translate proper nouns that are normally left in English in
  {language} news writing (organisation names, tickers, product names).
- Keep numbers, dates and units exactly as given.
- Natural {language} as a newspaper would write it, not literal word-order.
- If an item cannot be translated faithfully, return null for it.

Return ONLY a JSON array:
  {{"i": <index>, "title": "<translated headline>", "lead": "<translated summary>"}}

ITEMS:
{items}"""


def translate_batch(batch: list, lang_code: str, lang_name: str) -> dict:
    listing = "\n\n".join(
        f'{i}. HEADLINE: {a["title"]}\n   SUMMARY: {a["lead"]}'
        for i, a in enumerate(batch))
    prompt = PROMPT.format(n=len(batch), language=lang_name, items=listing)

    url = store.GEMINI_URL
    body = {"contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {"temperature": 0.2,
                                 "responseMimeType": "application/json"}}
    for attempt in range(3):
        try:
            r = requests.post(url, headers={"x-goog-api-key": store.gemini_key()},
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
                i = int(p.get("i", -1))
                if 0 <= i < len(batch) and p.get("title"):
                    out[batch[i]["url"]] = {"title": p["title"],
                                            "lead": p.get("lead") or ""}
            return out
        except Exception as e:
            log.warning("attempt %d failed: %s", attempt + 1, e)
            time.sleep(12)
    return {}


def main(argv):
    langs = {k: v for k, v in LANGUAGES.items() if not argv or k in argv}
    if not langs:
        log.error("no matching languages; known: %s", ", ".join(LANGUAGES))
        return
    con = store.connect()

    for code, name in langs.items():
        left = store.budget_left(con)
        if left < 3:
            log.warning("daily Gemini budget exhausted - skipping %s", name)
            break
        todo = store.untranslated(con, code, min(PER_RUN, left * BATCH))
        if not todo:
            log.info("%s: nothing new", name)
            continue
        log.info("%s: %d items", name, len(todo))
        done = 0
        for i in range(0, len(todo), BATCH):
            if store.budget_left(con) < 2:
                log.warning("budget reached, stopping %s at %d", name, done)
                break
            store.record_call(con, f"translate:{code}")
            got = translate_batch(todo[i:i + BATCH], code, name)
            if got:
                done += store.save_translations(con, code, got)
            time.sleep(5)
        log.info("%s: translated %d", name, done)

    con.close()


if __name__ == "__main__":
    main([a for a in sys.argv[1:] if not a.startswith("-")])
