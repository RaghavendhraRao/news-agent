"""
Rolling index of GDELT GKG records.

Each run appends the newest slice(s) and prunes anything older than
RETENTION_HOURS. History is what makes search, trending and alerting
possible -- a stateless run can do none of them.

Titles are NOT fetched at ingest time. GKG carries ~1,200 records per
15-minute slice and fetching every headline would be absurd. Instead the
index stores URL, themes, geography and tone (all free), and titles are
resolved lazily only for records that actually get surfaced.
"""

import io
import re
import sys
import csv
import zipfile
import sqlite3
import logging
from datetime import datetime, timedelta, timezone

import requests

log = logging.getLogger("agent.store")

# GDELT theme and location fields regularly exceed Python's default 128KB
# CSV field cap. Without this, whole slices fail to parse and are lost
# silently -- the run reports success having ingested nothing.
csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

DB_PATH = "news.db"

# Languages the feed is published in. Adding one costs a translation pass for
# new articles only (~40/hour), not for the whole feed every rebuild.
EXTRA_LANGUAGES = {"hi": "Hindi", "ml": "Malayalam"}
LANGUAGE_NAMES = {"en": "English", **EXTRA_LANGUAGES}

# Hard ceiling on Gemini calls per UTC day, kept under the free tier with
# room for retries. Every LLM caller checks this before firing. Without it
# the quota failure is invisible: runs still "succeed", they just stop
# producing summaries, and nothing in the logs says why.
DAILY_CALL_BUDGET = 900
RETENTION_HOURS = 72

# GKG 2.1 column positions
G_DATE, G_SOURCE, G_URL = 1, 3, 4
G_THEMES, G_LOCATIONS, G_PERSONS, G_ORGS, G_TONE = 7, 9, 11, 13, 15
G_IMAGE = 18          # V2.1SHARINGIMAGE -- og:image, ~88% populated

SCHEMA = """
CREATE TABLE IF NOT EXISTS articles (
    url        TEXT PRIMARY KEY,
    source     TEXT,
    title      TEXT,
    themes     TEXT,
    persons    TEXT,
    orgs       TEXT,
    country    TEXT,
    adm1       TEXT,
    place      TEXT,
    lat        REAL,
    lon        REAL,
    tone       REAL,
    image      TEXT,
    seen_at    TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_seen    ON articles(seen_at);
CREATE INDEX IF NOT EXISTS ix_country ON articles(country, seen_at);
CREATE INDEX IF NOT EXISTS ix_adm1    ON articles(country, adm1, seen_at);

CREATE VIRTUAL TABLE IF NOT EXISTS search_idx
    USING fts5(url UNINDEXED, blob, tokenize='porter');

CREATE TABLE IF NOT EXISTS api_calls (
    day  TEXT NOT NULL,
    kind TEXT NOT NULL,
    n    INTEGER NOT NULL,
    PRIMARY KEY (day, kind)
);

CREATE TABLE IF NOT EXISTS slices (
    slice_id TEXT PRIMARY KEY,
    done_at  TEXT NOT NULL
);

-- hourly rollups. Created here rather than in compact() so that readers
-- (trending, alerts) never hit a missing table on a fresh database.
CREATE TABLE IF NOT EXISTS agg (
    hour TEXT NOT NULL,
    kind TEXT NOT NULL,          -- 'place' | 'theme' | 'country'
    key  TEXT NOT NULL,
    n    INTEGER NOT NULL,
    PRIMARY KEY (hour, kind, key)
);
CREATE INDEX IF NOT EXISTS ix_agg ON agg(kind, hour);

-- LLM output. Summaries cost quota to produce, so they are kept rather than
-- discarded after delivery: the web feed republishes them for free.
CREATE TABLE IF NOT EXISTS published (
    url      TEXT PRIMARY KEY,
    category TEXT NOT NULL,
    title    TEXT NOT NULL,
    summary  TEXT,
    impact   TEXT,
    image    TEXT,
    lead     TEXT,          -- 120-150 word summary, written from article text
    source   TEXT,
    country  TEXT,
    place    TEXT,
    ts       TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_pub ON published(ts);

-- Translations are permanent: an article is translated once, ever. That is
-- what makes multi-language affordable -- cost scales with new articles,
-- not with how often the site rebuilds.
CREATE TABLE IF NOT EXISTS translations (
    url   TEXT NOT NULL,
    lang  TEXT NOT NULL,
    title TEXT,
    lead  TEXT,
    ts    TEXT NOT NULL,
    PRIMARY KEY (url, lang)
);
"""


# Columns added after the first release. CREATE TABLE IF NOT EXISTS leaves an
# existing table untouched, so a database made by an older version keeps the
# old shape and inserts fail on column count. Adding them explicitly means
# upgrading never requires deleting the index and re-backfilling.
MIGRATIONS = {
    "articles":  [("image", "TEXT")],
    "published": [("image", "TEXT"), ("lead", "TEXT")],
}


def _migrate(con):
    for table, cols in MIGRATIONS.items():
        try:
            have = {r[1] for r in con.execute(f"PRAGMA table_info({table})")}
        except sqlite3.OperationalError:
            continue
        if not have:
            continue
        for name, decl in cols:
            if name not in have:
                con.execute(f"ALTER TABLE {table} ADD COLUMN {name} {decl}")
                log.info("migrated: %s.%s added", table, name)
    con.commit()


def connect(path: str = DB_PATH) -> sqlite3.Connection:
    con = sqlite3.connect(path)
    con.executescript(SCHEMA)
    _migrate(con)
    con.commit()
    return con


# ---------------------------------------------------------------- ingest

def latest_slices(kind: str = "gkg") -> list:
    """Newest available slice URLs from GDELT's manifest."""
    suffix = {"gkg": "gkg.csv.zip", "events": "export.CSV.zip"}[kind]
    try:
        r = requests.get("http://data.gdeltproject.org/gdeltv2/lastupdate.txt",
                         headers={"User-Agent": UA}, timeout=30)
        r.raise_for_status()
    except Exception as e:
        log.warning("manifest fetch failed: %s", e)
        return []
    return [ln.split()[-1] for ln in r.text.strip().splitlines()
            if ln.strip().endswith(suffix)]


def _parse_locations(field: str):
    """
    V1LOCATIONS: type#FullName#CountryCode#ADM1Code#Lat#Long#FeatureID
    Prefers the most specific entry (type 3/4 = city or landmark).
    """
    best = None
    for loc in field.split(";"):
        p = loc.split("#")
        if len(p) < 6:
            continue
        try:
            gtype = int(p[0])
        except ValueError:
            continue
        cand = {"type": gtype, "place": p[1], "country": p[2],
                "adm1": p[3], "lat": p[4], "lon": p[5]}
        if best is None or gtype > best["type"]:
            best = cand
    return best


def _to_float(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def slice_timestamp(slice_id: str) -> str:
    """
    GDELT names slices YYYYMMDDHHMMSS. Using this as the record time -- rather
    than wall-clock ingest time -- is what makes a backfill produce real
    history. Stamping backfilled rows with "now" collapses the time axis and
    silently breaks trending and alerting.
    """
    m = re.match(r"(\d{14})", slice_id)
    if m:
        return datetime.strptime(m.group(1), "%Y%m%d%H%M%S").replace(
            tzinfo=timezone.utc).isoformat()
    return datetime.now(timezone.utc).isoformat()


def ingest_slice(con, url: str) -> int:
    """Download one GKG slice into the index. Returns rows inserted."""
    slice_id = url.rsplit("/", 1)[-1]
    if con.execute("SELECT 1 FROM slices WHERE slice_id=?", (slice_id,)).fetchone():
        log.info("slice already ingested: %s", slice_id)
        return 0

    try:
        r = requests.get(url, headers={"User-Agent": UA}, timeout=120)
        r.raise_for_status()
        z = zipfile.ZipFile(io.BytesIO(r.content))
        stream = io.TextIOWrapper(z.open(z.namelist()[0]),
                                  encoding="utf-8", errors="replace")
        rows = [x for x in csv.reader(stream, delimiter="\t") if len(x) > G_TONE]
    except Exception as e:
        log.warning("slice fetch failed %s: %s", slice_id, e)
        return 0

    now = slice_timestamp(slice_id)
    recs, blobs = [], []
    for x in rows:
        u = x[G_URL].strip()
        if not u:
            continue
        loc = _parse_locations(x[G_LOCATIONS]) or {}
        themes = ";".join(sorted({t.split(",")[0] for t in x[G_THEMES].split(";") if t}))
        persons = x[G_PERSONS][:120]
        orgs = x[G_ORGS][:120]
        tone = _to_float((x[G_TONE].split(",") or [None])[0])

        img = x[G_IMAGE].strip() if len(x) > G_IMAGE else ""
        recs.append((u, x[G_SOURCE], None, themes, persons, orgs,
                     loc.get("country"), loc.get("adm1"), loc.get("place"),
                     _to_float(loc.get("lat")), _to_float(loc.get("lon")),
                     tone, img or None, now))
        # searchable text: url slug + place + people + orgs + themes
        slug = re.sub(r"[-_/]+", " ", re.sub(r"^https?://", "", u))
        blobs.append((u, " ".join([slug, loc.get("place", "") or ""])))

    cur = con.executemany(
        "INSERT OR IGNORE INTO articles VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", recs)
    con.executemany("INSERT INTO search_idx (url, blob) VALUES (?,?)", blobs)
    con.execute("INSERT OR REPLACE INTO slices VALUES (?,?)",
                (slice_id, datetime.now(timezone.utc).isoformat()))
    con.commit()
    log.info("ingested %s: %d rows", slice_id, cur.rowcount)
    return cur.rowcount


def prune(con, hours: int = RETENTION_HOURS) -> int:
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    urls = [r[0] for r in con.execute(
        "SELECT url FROM articles WHERE seen_at < ?", (cutoff,))]
    if urls:
        con.executemany("DELETE FROM articles WHERE url=?", [(u,) for u in urls])
        con.executemany("DELETE FROM search_idx WHERE url=?", [(u,) for u in urls])
    con.execute("DELETE FROM slices WHERE done_at < ?", (cutoff,))
    con.commit()
    return len(urls)


# ---------------------------------------------------------------- query

def _since(hours):
    return (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()


def search(con, query: str, hours: int = 72, limit: int = 20,
           country: str | None = None) -> list:
    """Full-text search across slug, place, people, orgs and themes."""
    sql = """SELECT a.url, a.source, a.title, a.place, a.country, a.tone, a.seen_at
             FROM search_idx s JOIN articles a ON a.url = s.url
             WHERE search_idx MATCH ? AND a.seen_at >= ?"""
    args = [query, _since(hours)]
    if country:
        sql += " AND a.country = ?"
        args.append(country)
    sql += " ORDER BY a.seen_at DESC LIMIT ?"
    args.append(limit)
    try:
        return [dict(zip(["url", "source", "title", "place",
                          "country", "tone", "seen_at"], r))
                for r in con.execute(sql, args)]
    except sqlite3.OperationalError as e:
        log.warning("bad search query %r: %s", query, e)
        return []


def history_hours(con) -> float:
    """How much history the index actually holds. Trending and alerting are
    meaningless until this exceeds the baseline window."""
    row = con.execute("SELECT MIN(seen_at), MAX(seen_at) FROM articles").fetchone()
    if not row or not row[0]:
        return 0.0
    lo = datetime.fromisoformat(row[0])
    hi = datetime.fromisoformat(row[1])
    return round((hi - lo).total_seconds() / 3600, 2)


def trending(con, country: str | None = None, window_h: int = 6,
             baseline_h: int = 48, limit: int = 10, by: str = "place") -> list:
    """
    Ranks by lift: how much more a place/theme appears in the recent window
    than its longer-run baseline. Raw counts just surface whatever is always
    busy; lift surfaces what changed.
    """
    col = {"place": "place", "theme": "themes", "country": "country"}[by]

    def counts(hours):
        sql = f"SELECT {col} FROM articles WHERE seen_at >= ? AND {col} IS NOT NULL"
        args = [_since(hours)]
        if country:
            sql += " AND country = ?"
            args.append(country)
        out = {}
        for (val,) in con.execute(sql, args):
            items = val.split(";") if col == "themes" else [val]
            for it in items:
                if it:
                    out[it] = out.get(it, 0) + 1
        return out

    have = history_hours(con)
    if have < window_h * 2:
        # Not enough history for a baseline: fall back to raw volume and
        # say so, rather than emitting identical meaningless lift values.
        recent = counts(window_h)
        out = [{"key": k, "count": n, "lift": None, "cold_start": True}
               for k, n in sorted(recent.items(), key=lambda x: -x[1]) if n >= 3]
        return out[:limit]

    recent, base = counts(window_h), counts(baseline_h)
    effective_base = min(baseline_h, have)
    scale = window_h / effective_base
    scored = []
    for k, n in recent.items():
        if n < 3:
            continue                      # ignore one-off noise
        expected = max(base.get(k, 0) * scale, 0.5)
        scored.append({"key": k, "count": n,
                       "lift": round(n / expected, 2), "cold_start": False})
    scored.sort(key=lambda x: (-x["lift"], -x["count"]))
    return scored[:limit]


def spikes(con, min_count: int = 5, min_lift: float = 3.0,
           window_h: int = 1, baseline_h: int = 48) -> list:
    """
    Alert candidates: places seeing abnormally heavy coverage right now.
    Returns nothing until there is real history -- firing alerts off a
    cold index would mean alerting on everything.
    """
    if history_hours(con) < baseline_h / 2:
        log.info("alerting disabled: only %.1fh of history", history_hours(con))
        return []
    return [t for t in trending(con, window_h=window_h, baseline_h=baseline_h,
                                limit=50, by="place")
            if t["count"] >= min_count and (t["lift"] or 0) >= min_lift]


def countries(con, hours: int = 24) -> list:
    return [{"country": c, "n": n} for c, n in con.execute(
        """SELECT country, COUNT(*) n FROM articles
           WHERE seen_at >= ? AND country IS NOT NULL AND country != ''
           GROUP BY country ORDER BY n DESC""", (_since(hours),))]


def states(con, country: str, hours: int = 24) -> list:
    return [{"adm1": a, "sample": p, "n": n} for a, p, n in con.execute(
        """SELECT adm1, MAX(place), COUNT(*) n FROM articles
           WHERE country = ? AND seen_at >= ? AND adm1 IS NOT NULL AND adm1 != ''
           GROUP BY adm1 ORDER BY n DESC""", (country, _since(hours)))]


def places(con, country: str, adm1: str, hours: int = 24) -> list:
    return [{"place": p, "n": n} for p, n in con.execute(
        """SELECT place, COUNT(*) n FROM articles
           WHERE country = ? AND adm1 = ? AND seen_at >= ? AND place IS NOT NULL
           GROUP BY place ORDER BY n DESC""", (country, adm1, _since(hours)))]


def stats(con) -> dict:
    row = con.execute(
        "SELECT COUNT(*), MIN(seen_at), MAX(seen_at) FROM articles").fetchone()
    return {"articles": row[0], "oldest": row[1], "newest": row[2],
            "slices": con.execute("SELECT COUNT(*) FROM slices").fetchone()[0]}


# ---------------------------------------------------------------- compaction

def compact(con, keep_raw_hours: int = 24, keep_agg_hours: int = 72,
            top_per_hour: int = 400) -> dict:
    """
    Baselines need days of history; article rows only need to live long enough
    to be delivered and deduped. So roll raw rows up into hourly counts before
    pruning them. The aggregate table is a fraction of the size and is what
    trending and alerting actually read.
    """
    for kind, col in (("place", "place"), ("country", "country")):
        con.execute(f"""
            INSERT OR REPLACE INTO agg (hour, kind, key, n)
            SELECT substr(seen_at,1,13), ?, {col}, COUNT(*)
            FROM articles WHERE {col} IS NOT NULL AND {col} != ''
            GROUP BY substr(seen_at,1,13), {col}
        """, (kind,))

    # themes are multi-valued per row, so expand them in Python
    rows = con.execute("""SELECT substr(seen_at,1,13), themes FROM articles
                          WHERE themes IS NOT NULL AND themes != ''""").fetchall()
    counts = {}
    for hour, themes in rows:
        for t in themes.split(";"):
            if t:
                counts[(hour, t)] = counts.get((hour, t), 0) + 1
    con.executemany("INSERT OR REPLACE INTO agg VALUES (?,'theme',?,?)",
                    [(h, k, n) for (h, k), n in counts.items() if n >= 2])

    # keep only the busiest keys per hour -- the long tail is noise
    con.execute("""DELETE FROM agg WHERE rowid NOT IN (
                       SELECT rowid FROM agg a WHERE n >= (
                           SELECT MIN(n) FROM (
                               SELECT n FROM agg b
                               WHERE b.hour=a.hour AND b.kind=a.kind
                               ORDER BY n DESC LIMIT ?)))""", (top_per_hour,))

    raw_cut = (datetime.now(timezone.utc) - timedelta(hours=keep_raw_hours)).isoformat()
    agg_cut = (datetime.now(timezone.utc) - timedelta(hours=keep_agg_hours)).strftime("%Y-%m-%dT%H")
    old = [r[0] for r in con.execute("SELECT url FROM articles WHERE seen_at < ?", (raw_cut,))]
    if old:
        con.executemany("DELETE FROM articles WHERE url=?", [(u,) for u in old])
        con.executemany("DELETE FROM search_idx WHERE url=?", [(u,) for u in old])
    con.execute("DELETE FROM agg WHERE hour < ?", (agg_cut,))
    con.execute("DELETE FROM slices WHERE done_at < ?", (raw_cut,))
    con.commit()
    if old:
        con.execute("VACUUM")
    return {"pruned_articles": len(old),
            "agg_rows": con.execute("SELECT COUNT(*) FROM agg").fetchone()[0]}


def agg_trending(con, kind: str = "place", window_h: int = 3,
                 baseline_h: int = 48, limit: int = 10,
                 min_count: int = 4) -> list:
    """Trending computed from the compacted table, so it still works after
    raw article rows have been pruned."""
    now = datetime.now(timezone.utc)
    w = (now - timedelta(hours=window_h)).strftime("%Y-%m-%dT%H")
    b = (now - timedelta(hours=baseline_h)).strftime("%Y-%m-%dT%H")

    recent = dict(con.execute(
        "SELECT key, SUM(n) FROM agg WHERE kind=? AND hour>=? GROUP BY key",
        (kind, w)))
    base = dict(con.execute(
        "SELECT key, SUM(n) FROM agg WHERE kind=? AND hour>=? AND hour<? GROUP BY key",
        (kind, b, w)))
    if not base:
        return [{"key": k, "count": n, "lift": None, "cold_start": True}
                for k, n in sorted(recent.items(), key=lambda x: -x[1])
                if n >= min_count][:limit]

    span = max(baseline_h - window_h, 1)
    out = []
    for k, n in recent.items():
        if n < min_count:
            continue
        expected = max(base.get(k, 0) * (window_h / span), 0.5)
        out.append({"key": k, "count": n, "lift": round(n / expected, 2),
                    "cold_start": False})
    out.sort(key=lambda x: (-x["lift"], -x["count"]))
    return out[:limit]


# ---------------------------------------------------------------- published

def save_published(con, category: str, items: list) -> int:
    now = datetime.now(timezone.utc).isoformat()
    rows = [(a["url"], category, a.get("title_en") or a.get("title", ""),
             a.get("summary", ""), a.get("impact", "low"),
             a.get("image") or None, a.get("lead") or None,
             a.get("domain", ""), a.get("country", ""), a.get("place", ""), now)
            for a in items if a.get("url")]
    con.executemany(
        "INSERT OR REPLACE INTO published VALUES (?,?,?,?,?,?,?,?,?,?,?)", rows)
    con.commit()
    return len(rows)


def get_published(con, hours: int = 48, limit: int = 400) -> list:
    cols = ["url", "category", "title", "summary", "impact", "image", "lead",
            "source", "country", "place", "ts"]
    return [dict(zip(cols, r)) for r in con.execute(
        f"SELECT {','.join(cols)} FROM published WHERE ts >= ? "
        "ORDER BY ts DESC LIMIT ?", (_since(hours), limit))]


def prune_published(con, hours: int = 72):
    con.execute("DELETE FROM published WHERE ts < ?", (_since(hours),))
    con.commit()


def untranslated(con, lang: str, limit: int = 40) -> list:
    """Enriched stories that have no translation in this language yet."""
    return [dict(zip(["url", "title", "lead"], r)) for r in con.execute(
        """SELECT p.url, p.title, p.lead FROM published p
           LEFT JOIN translations t ON t.url = p.url AND t.lang = ?
           WHERE t.url IS NULL AND p.lead IS NOT NULL
           ORDER BY p.ts DESC LIMIT ?""", (lang, limit))]


def save_translations(con, lang: str, rows: dict) -> int:
    now = datetime.now(timezone.utc).isoformat()
    con.executemany(
        "INSERT OR REPLACE INTO translations VALUES (?,?,?,?,?)",
        [(u, lang, v.get("title"), v.get("lead"), now) for u, v in rows.items()])
    con.commit()
    return len(rows)


def translations_for(con, urls: list) -> dict:
    if not urls:
        return {}
    out = {}
    q = ",".join("?" * len(urls))
    for u, lang, title, lead in con.execute(
            f"SELECT url, lang, title, lead FROM translations WHERE url IN ({q})",
            urls):
        out.setdefault(u, {})[lang] = {"title": title, "lead": lead}
    return out


# ---------------------------------------------------------------- quota

def _today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def record_call(con, kind: str, n: int = 1):
    con.execute("""INSERT INTO api_calls (day, kind, n) VALUES (?,?,?)
                   ON CONFLICT(day, kind) DO UPDATE SET n = n + ?""",
                (_today(), kind, n, n))
    con.commit()


def calls_today(con) -> int:
    r = con.execute("SELECT COALESCE(SUM(n),0) FROM api_calls WHERE day=?",
                    (_today(),)).fetchone()
    return r[0] if r else 0


def budget_left(con) -> int:
    return max(DAILY_CALL_BUDGET - calls_today(con), 0)


def call_breakdown(con) -> dict:
    return {k: n for k, n in con.execute(
        "SELECT kind, n FROM api_calls WHERE day=? ORDER BY n DESC", (_today(),))}


# ---------------------------------------------------------------- title cache

def cached_titles(con, urls: list) -> dict:
    """Titles already resolved in an earlier run. Articles live in the index
    for 12 hours and categories overlap heavily, so without this the same
    page is fetched again every hour for as long as it stays in the window."""
    if not urls:
        return {}
    out = {}
    CH = 500
    for i in range(0, len(urls), CH):
        chunk = urls[i:i + CH]
        q = ",".join("?" * len(chunk))
        for u, t in con.execute(
                f"SELECT url, title FROM articles WHERE url IN ({q}) "
                "AND title IS NOT NULL", chunk):
            out[u] = t
    return out


def save_titles(con, titles: dict):
    if titles:
        con.executemany("UPDATE articles SET title=? WHERE url=?",
                        [(t, u) for u, t in titles.items()])
        con.commit()
