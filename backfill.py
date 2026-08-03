"""
Warm the index with recent history.

Trending and alerting need a baseline. Starting from an empty index they
have nothing to compare against, so this walks back through GDELT's
15-minute slices and ingests them.

  py backfill.py 12      # last 12 hours (48 slices, ~4 min, ~230MB)

GDELT publishes a slice every 15 minutes at :00 :15 :30 :45 UTC.
Missing slices are normal and skipped quietly.
"""
import sys
import logging
from datetime import datetime, timedelta, timezone

import store

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")

BASE = "http://data.gdeltproject.org/gdeltv2/{}.gkg.csv.zip"


def slice_ids(hours: int):
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    now -= timedelta(minutes=now.minute % 15)
    for i in range(1, hours * 4 + 1):
        yield (now - timedelta(minutes=15 * i)).strftime("%Y%m%d%H%M%S")


if __name__ == "__main__":
    hours = int(sys.argv[1]) if len(sys.argv) > 1 else 6
    con = store.connect()
    total = 0
    for sid in slice_ids(hours):
        total += store.ingest_slice(con, BASE.format(sid))
    store.prune(con)
    print("\nbackfilled", total, "rows")
    print("stats:", store.stats(con))
    print("history:", store.history_hours(con), "hours")
