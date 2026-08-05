"""
Main run loop, backed by the rolling index.

  py run.py            normal run: ingest -> categorise -> summarise -> deliver
  py run.py --alerts   alerts only (cheap, no LLM unless something spiked)
  py run.py --stats    print index health and exit

Categories are matched on GDELT GKG themes rather than headline keywords.
Themes are a curated taxonomy applied by GDELT across 100+ languages, so
recall is far better than guessing at English words -- and it costs nothing,
because the themes are already in the index.
"""

import re
import sys
import time
import logging

import store
import gdelt_files
import agent          # reuses agent.py's Gemini + Telegram functions

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("run")

# ---------------------------------------------------------------- config

KEEP_RAW_HOURS = 12       # article rows; search reaches back this far
KEEP_AGG_HOURS = 72       # hourly counts; trending/alerts read these
FRESH_MINUTES = 90      # hourly cadence: widen the delivery window
CATCHUP_SLICES = 4      # 15-min slices to walk back each run        # how far back to look for deliverable articles
TITLES_PER_CATEGORY = 12  # no LLM cost here, only page fetches  # page fetches per category per run
ALERT_TOPIC_ID = None     # set to a Telegram topic id to enable alerts

# GKG theme fragments. A record matches if ANY fragment appears in its themes.
# Categories are matched on GKG themes where GDELT's taxonomy covers the
# topic well, and on title keywords where it does not. Verified against live
# data: SPORT/TRANSPORT/HEALTH/GOVERNMENT are richly populated, but SPACE,
# MOVIE and ENTERTAINMENT return nothing -- those need keywords.
CATEGORY_RULES = {
    "Top stories":        {"themes": [], "kw": [], "catch_all": True},
    "Politics":           {"themes": ["ELECTION", "GOVERNMENT_", "DEMOCRACY",
                                      "LEGISLATION", "POLITICAL_TURMOIL"]},
    "Wars & Conflict":    {"themes": ["ARMEDCONFLICT", "MILITARY", "SIEGE",
                                      "REBELLION", "INSURGENCY",
                                      "FRAGILITY_CONFLICT"]},
    "Business & Economy": {"themes": ["ECON_INFLATION", "ECON_TRADE",
                                      "ECON_STOCKMARKET", "ECON_INTEREST_RATE",
                                      "ECON_BANKRUPTCY"]},
    "Gold & Metals":      {"themes": ["MINING", "ECON_COMMODITY"],
                           "kw": ["gold", "silver", "bullion", "steel"]},
    "Fuel & Energy":      {"themes": ["ENV_OIL", "ENV_GAS", "ENV_COAL",
                                      "ENERGY", "POWER_OUTAGE"]},
    "Science":            {"themes": ["SCIENCE", "RESEARCH"]},
    "Space":              {"themes": ["SATELLITE"],
                           "kw": ["space", "nasa", "isro", "rocket", "orbit",
                                  "satellite", "spacex", "lunar", "mars"]},
    "AI & Technology":    {"themes": ["CYBER"],
                           "kw": ["ai", "artificial intelligence", "chatgpt",
                                  "openai", "chip", "semiconductor",
                                  "software", "app", "startup"]},
    "Health":             {"themes": ["HEALTH_", "MEDICAL", "DISEASE"]},
    "Nature & Climate":   {"themes": ["NATURAL_DISASTER", "CLIMATE",
                                      "ENV_FORESTRY", "ENV_BIOFUEL"],
                           "kw": ["flood", "cyclone", "earthquake", "wildfire",
                                  "drought", "monsoon"]},
    "Roads & Transport":  {"themes": ["TRANSPORT", "ROAD", "RAIL", "AVIATION"],
                           "kw": ["highway", "metro", "railway", "airport",
                                  "traffic"]},
    "Sports":             {"themes": ["SPORT"],
                           "kw": ["cricket", "football", "olympic", "match",
                                  "tournament", "league"]},
    "Movies & Entertainment": {"themes": ["MUSIC", "FILM"],
                           "kw": ["film", "movie", "actor", "actress", "album",
                                  "box office", "series", "trailer", "singer"]},
    "Crypto":             {"themes": [],
                           "kw": ["crypto", "bitcoin", "ethereum", "blockchain",
                                  "stablecoin", "token", "web3", "defi"]},
    "Stocks & Markets":   {"themes": ["ECON_STOCKMARKET"],
                           "kw": ["stocks", "shares", "nasdaq", "sensex",
                                  "nifty", "ipo", "index", "rally"]},
    "Chips & Semiconductors": {"themes": [],
                           "kw": ["semiconductor", "chipmaker", "foundry",
                                  "tsmc", "nvidia", "wafer", "fab", "gpu"]},
    "Startups & Funding": {"themes": [],
                           "kw": ["startup", "funding", "series a", "series b",
                                  "venture", "valuation", "acquisition"]},
    "Education":          {"themes": ["EDUCATION"],
                           "kw": ["school", "university", "exam", "student"]},
    "Crime & Justice":    {"themes": ["ARREST", "TRIAL", "CORRUPTION"],
                           "kw": ["arrested", "court", "verdict", "police"]},
    "Weather":            {"themes": ["NATURAL_DISASTER_"],
                           "kw": ["rain", "heatwave", "storm", "snow", "forecast"]},
}

# Telegram topics only exist for a few of these; the rest are web-only.
TELEGRAM_CATEGORIES = ["Gold & Metals", "Fuel & Energy",
                       "Wars & Conflict", "AI & Technology"]


def match_category(themes: str, title: str, rule: dict) -> int:
    """Score a record against one category. Themes are language-independent
    (GDELT applies them across 100+ languages); keywords are the fallback for
    topics its taxonomy does not cover."""
    th = themes or ""
    score = sum(2 for f in rule.get("themes", []) if f in th)
    if rule.get("kw"):
        t = (title or "").lower()
        score += sum(1 for k in rule["kw"]
                     if re.search(r"\b" + re.escape(k) + r"s?\b", t))
    return score


def select_candidates(con, locality=None, minutes=FRESH_MINUTES,
                      per_category=TITLES_PER_CATEGORY, rules=None) -> dict:
    """Pull fresh rows from the index and bucket them by category."""
    rules = rules or CATEGORY_RULES
    since = store._since(minutes / 60)
    sql = """SELECT url, source, country, adm1, place, themes, tone, image
             FROM articles WHERE seen_at >= ?"""
    args = [since]
    if locality and locality.get("enabled"):
        if locality.get("country_code"):
            sql += " AND country = ?"
            args.append(locality["country_code"])
        terms = [t for t in [locality.get("state")] +
                 list(locality.get("districts", [])) if t]
        if terms:
            sql += " AND (" + " OR ".join("place LIKE ?" for _ in terms) + ")"
            args += [f"%{t}%" for t in terms]

    rows = list(con.execute(sql, args))
    log.info("fresh rows in window: %d", len(rows))

    out = {}
    for cat, rule in rules.items():
        hits = []
        for url, src, country, adm1, place, themes, tone, image in rows:
            if rule.get("catch_all"):
                score = 1
            else:
                score = match_category(themes, url, rule)
            if not score:
                continue
            hits.append({"url": url, "domain": src or "", "lang": "",
                         "country": country or "", "adm1": adm1 or "",
                         "place": place or "", "image": image or "",
                         "tone": tone if tone is not None else 0.0,
                         "score": score})
        hits.sort(key=lambda x: (-x["score"], x["tone"]))
        out[cat] = hits[:per_category]
        log.info("%-22s -> %d matches", cat, len(out[cat]))
    return out


def send_alerts(con):
    """Volume spikes: places covered far more heavily than their own norm."""
    hits = store.agg_trending(con, "place", window_h=1, baseline_h=48,
                              limit=8, min_count=5)
    hits = [h for h in hits if not h["cold_start"] and (h["lift"] or 0) >= 3.0]
    if not hits:
        log.info("no spikes")
        return
    lines = ["<b>\u26A0 Unusual coverage</b>"]
    for h in hits[:5]:
        lines.append(f"\u2022 {agent.esc(h['key'])} "
                     f"\u2014 {h['count']} stories ({h['lift']}\u00d7 normal)")
    agent.send("\n".join(lines), ALERT_TOPIC_ID)
    log.info("alerted on %d spikes", len(hits))


def main(argv):
    con = store.connect()

    if "--stats" in argv:
        print("index:", store.stats(con))
        print("history:", store.history_hours(con), "hours")
        return

    # 1. ingest every slice published since the last run. GDELT publishes
    # every 15 minutes but the manifest names only the newest, so on an
    # hourly schedule three of every four slices would be skipped.
    # ingest_slice() is idempotent -- already-seen slices cost nothing.
    ingested = 0
    for url in store.latest_slices("gkg"):
        ingested += store.ingest_slice(con, url)
        back = url
        for _ in range(CATCHUP_SLICES):
            back = gdelt_files._prev_slice(back)
            ingested += store.ingest_slice(con, back)
    log.info("ingested %d new rows", ingested)

    if "--alerts" in argv:
        # compact first: it is what populates the hourly aggregates that
        # alerting reads. Alerting before it would score against stale counts.
        store.compact(con, KEEP_RAW_HOURS, KEEP_AGG_HOURS)
        send_alerts(con)
        return

    # 2. bucket by theme
    buckets = select_candidates(con, agent.LOCALITY)

    # 3. resolve titles, drop anything already sent
    seen = agent.db_init()
    total = 0

    # Resolve every title once, across all categories, reusing anything a
    # previous run already fetched. Categories overlap (Top stories duplicates
    # the rest), so per-category fetching repeats the same pages.
    every = {a["url"]: a for arts in buckets.values() for a in arts}
    known = store.cached_titles(con, list(every))
    missing = [{"url": u} for u in every if u not in known]
    log.info("titles: %d cached, %d to fetch", len(known), len(missing))
    if missing:
        gdelt_files.resolve_titles(missing, workers=16)
        fetched = {m["url"]: m["title"] for m in missing if m.get("title")}
        store.save_titles(con, fetched)
        known.update(fetched)
    for a in every.values():
        a["title"] = known.get(a["url"], "")

    for cat, arts in buckets.items():
        if not arts:
            continue
        fresh = [a for a in arts
                 if gdelt_files.looks_like_title(a.get("title", ""))
                 and agent.is_new(seen, a["url"], a["title"])]
        log.info("%-16s -> %d after dedupe", cat, len(fresh))
        if not fresh:
            continue

        # No LLM here. This loop runs every 15 minutes; summarising 21
        # categories per run would cost ~2,000 Gemini calls a day against a
        # free tier of ~1,000. Headlines publish immediately; enrich.py adds
        # summaries hourly and translate.py adds languages after that.
        for a in fresh:
            a["title_en"] = a["title"]
            a["summary"] = ""
            a["impact"] = "high" if a["score"] >= 4 else (
                "medium" if a["score"] >= 2 else "low")
        store.save_published(con, cat, fresh)
        total += len(fresh)
        log.info("%-22s -> %d published", cat, len(fresh))

    # 4. housekeeping, then alerts (compaction feeds the aggregates)
    store.prune_published(con, 72)
    r = store.compact(con, KEEP_RAW_HOURS, KEEP_AGG_HOURS)
    log.info("compacted: %s", r)
    send_alerts(con)
    log.info("Run complete. %d items delivered.", total)
    seen.close()
    con.close()


if __name__ == "__main__":
    main(sys.argv[1:])
