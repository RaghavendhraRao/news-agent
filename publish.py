"""
Write the static feed the web app reads.

  py publish.py

Produces docs/feed.json. GitHub Pages serves docs/ directly, so publishing
is just a file write plus a commit -- no server, no database, no per-reader
cost. Gemini has already run by this point, so serving the feed to ten
readers costs exactly what serving it to one costs: nothing.
"""

import os
import json
import logging
from datetime import datetime, timezone

import store

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
log = logging.getLogger("publish")

OUT_DIR = "docs"
FEED = os.path.join(OUT_DIR, "feed.json")

FEED_HOURS = 48
MAX_ITEMS = 300

# Languages the feed is published in. Each extra language costs another pass
# of LLM calls at publish time, so add them deliberately: at ~25 items per
# call and 48 runs a day, every language is roughly 100 extra calls daily
# against a free tier of ~1,000. English-only needs no translation at all.
LANGUAGES = ["en"] + list(store.EXTRA_LANGUAGES)


def split_place(place: str):
    """
    GDELT writes places as "City, State, Country" (3 parts), "State, Country"
    (2) or "Country" (1). Parsing this is what gives the app a real
    country -> state -> place tree without any external lookup table.
    """
    parts = [p.strip() for p in (place or "").split(",") if p.strip()]
    if len(parts) >= 3:
        return parts[-1], parts[-2], parts[0]
    if len(parts) == 2:
        return parts[-1], parts[0], None
    if len(parts) == 1:
        return parts[0], None, None
    return None, None, None


def geo_tree(items: list) -> dict:
    """country -> state -> [places], counted, so the UI can hide empty levels."""
    tree = {}
    for i in items:
        c, st, pl = split_place(i.get("place"))
        if not c:
            continue
        node = tree.setdefault(c, {"n": 0, "states": {}})
        node["n"] += 1
        if st:
            sn = node["states"].setdefault(st, {"n": 0, "places": {}})
            sn["n"] += 1
            if pl:
                sn["places"][pl] = sn["places"].get(pl, 0) + 1
    return {c: {"n": v["n"],
                "states": {s: {"n": sv["n"],
                               "places": dict(sorted(sv["places"].items(),
                                                     key=lambda x: -x[1])[:40])}
                           for s, sv in sorted(v["states"].items(),
                                               key=lambda x: -x[1]["n"])[:60]}}
            for c, v in sorted(tree.items(), key=lambda x: -x[1]["n"])}


def build(con) -> dict:
    items = store.get_published(con, hours=FEED_HOURS, limit=MAX_ITEMS)

    spikes_all = spikes = [s for s in store.agg_trending(con, "place", window_h=3,
                                            baseline_h=48, limit=12,
                                            min_count=4)
              if not s["cold_start"] and (s["lift"] or 0) >= 2.0]

    # annotate each item with its parsed geo so the client can filter locally
    for i in items:
        c, st, pl = split_place(i.get("place"))
        i["geo"] = {"country": c, "state": st, "place": pl}

    # #Trending: stories from places currently seeing abnormal coverage.
    # Derived, not a separate LLM pass -- it reuses the spike detection.
    hot = {s["key"] for s in spikes}
    for i in items:
        i["trending"] = bool(i.get("place") and i["place"] in hot)

    # attach stored translations; missing ones simply fall back to English
    tx = store.translations_for(con, [i["url"] for i in items])
    for i in items:
        if i["url"] in tx:
            i["t"] = tx[i["url"]]

    categories = sorted({i["category"] for i in items if i["category"]})
    tree = geo_tree(items)

    return {
        "generated": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "window_hours": FEED_HOURS,
        "counts": {"items": len(items), "spikes": len(spikes),
                   "trending": sum(1 for i in items if i.get("trending")),
                   "enriched": sum(1 for i in items if i.get("lead"))},
        "categories": categories,
        "geo": tree,
        "languages": LANGUAGES,
        "language_names": store.LANGUAGE_NAMES,
        "spikes": [{"place": s["key"], "n": s["count"], "lift": s["lift"]}
                   for s in spikes],
        "items": items,
    }


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    con = store.connect()
    feed = build(con)
    with open(FEED, "w", encoding="utf-8") as f:
        json.dump(feed, f, ensure_ascii=False, separators=(",", ":"))
    size = os.path.getsize(FEED) / 1024
    log.info("wrote %s: %d items, %d spikes, %.0f KB",
             FEED, feed["counts"]["items"], feed["counts"]["spikes"], size)
    con.close()


if __name__ == "__main__":
    main()
