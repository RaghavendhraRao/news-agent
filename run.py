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
FRESH_MINUTES = 90        # how far back to look for deliverable articles
TITLES_PER_CATEGORY = 20  # page fetches per category per run
ALERT_TOPIC_ID = None     # set to a Telegram topic id to enable alerts

# GKG theme fragments. A record matches if ANY fragment appears in its themes.
# Narrow fragments beat broad ones. "ECON_PRICE" looks relevant to Gold but
# matches every utility-bill story; "TECH" matches half the corpus. Each entry
# below was chosen to be specific enough that a match is usually a real match.
CATEGORY_THEMES = {
    "Gold & Metals":   ["GOLD", "MINING", "PRECIOUS_METAL", "STEEL",
                        "ECON_COMMODITY"],
    "Fuel & Energy":   ["ENV_OIL", "ENV_GAS", "ENV_COAL", "PETROLEUM",
                        "ENERGY", "POWER_OUTAGE", "FUELPRICE"],
    "Wars & Conflict": ["ARMEDCONFLICT", "MILITARY", "FRAGILITY_CONFLICT",
                        "SIEGE", "REBELLION", "INSURGENCY", "AIR_STRIKE"],
    "AI & Tech":       ["CYBER", "SCIENCE_TECH", "ARTIFICIAL_INTELLIGENCE",
                        "INFO_COMM_TECH"],
}


def select_candidates(con, locality=None, minutes=FRESH_MINUTES,
                      per_category=TITLES_PER_CATEGORY) -> dict:
    """Pull fresh rows from the index and bucket them by theme."""
    since = store._since(minutes / 60)
    sql = """SELECT url, source, country, adm1, place, themes, tone
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
    for cat, frags in CATEGORY_THEMES.items():
        hits = []
        for url, src, country, adm1, place, themes, tone in rows:
            th = themes or ""
            score = sum(1 for f in frags if f in th)
            if score:
                hits.append({"url": url, "domain": src or "", "lang": "",
                             "country": country or "", "place": place or "",
                             "tone": tone if tone is not None else 0.0,
                             "score": score})
        # rank by how many category signals matched, then by severity of tone.
        # Sorting on tone alone surfaces the most dramatic story, not the most
        # relevant one.
        hits.sort(key=lambda x: (-x["score"], x["tone"]))
        out[cat] = hits[:per_category]
        log.info("%-16s -> %d theme matches", cat, len(out[cat]))
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

    # 1. ingest whatever is newest (manifest usually lists one slice)
    for url in store.latest_slices("gkg"):
        store.ingest_slice(con, url)

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
    for cat, arts in buckets.items():
        if not arts:
            continue
        gdelt_files.resolve_titles(arts)
        fresh = [a for a in arts
                 if gdelt_files.looks_like_title(a.get("title", ""))
                 and agent.is_new(seen, a["url"], a["title"])]
        log.info("%-16s -> %d after dedupe", cat, len(fresh))
        if not fresh:
            continue

        picked = agent.gemini_batch(cat, fresh)
        log.info("%-16s -> %d after Gemini", cat, len(picked))
        agent.deliver(cat, picked, agent.CATEGORIES[cat]["topic_id"])
        total += min(len(picked), agent.MAX_SENT_PER_CATEGORY)
        time.sleep(4)

    # 4. housekeeping, then alerts (compaction feeds the aggregates)
    r = store.compact(con, KEEP_RAW_HOURS, KEEP_AGG_HOURS)
    log.info("compacted: %s", r)
    send_alerts(con)
    log.info("Run complete. %d items delivered.", total)
    seen.close()
    con.close()


if __name__ == "__main__":
    main(sys.argv[1:])
