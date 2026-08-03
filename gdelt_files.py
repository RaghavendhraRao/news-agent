"""
GDELT ingest via static data files instead of the DOC 2.0 API.

Why: the DOC API sits in front of GDELT's ElasticSearch clusters and is
aggressively throttled (persistent 429s regardless of IP or backoff).
The data files live on a different host, are published every 15 minutes,
and are not rate limited. They also carry ADM1/ADM2 geography, which the
DOC API does not expose -- so this is strictly more capable for
country -> state -> district filtering.

Trade-off: events files give SOURCEURL but no headline, so titles are
resolved by fetching each shortlisted page's <title> tag.
"""

import re
import io
import html as _html
import csv
import zipfile
import logging
from concurrent.futures import ThreadPoolExecutor

import requests

log = logging.getLogger("agent.gdelt")

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

LASTUPDATE = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"

# Events 2.0 column positions (0-indexed, 61 columns total)
C_EVENTCODE   = 26
C_NUMMENTIONS = 31
C_AVGTONE     = 34
C_GEO_FULL    = 52
C_GEO_CC      = 53
C_GEO_ADM1    = 54
C_GEO_ADM2    = 55
C_GEO_LAT     = 56
C_GEO_LON     = 57
C_URL         = 60


def latest_export_url() -> str | None:
    """The manifest lists the newest export / mentions / gkg files."""
    try:
        r = requests.get(LASTUPDATE, headers={"User-Agent": UA}, timeout=30)
        r.raise_for_status()
    except Exception as e:
        log.warning("lastupdate fetch failed: %s", e)
        return None
    for line in r.text.strip().splitlines():
        parts = line.split()
        if parts and parts[-1].endswith(".export.CSV.zip"):
            return parts[-1]
    return None


def _prev_slice(url: str) -> str:
    """Step back one 15-minute slice."""
    from datetime import datetime as _dt, timedelta as _td
    m = re.search(r"(\d{14})", url)
    if not m:
        return url
    t = _dt.strptime(m.group(1), "%Y%m%d%H%M%S") - _td(minutes=15)
    return url.replace(m.group(1), t.strftime("%Y%m%d%H%M%S"))


def fetch_events(url: str, _retries: int = 2) -> list:
    try:
        r = requests.get(url, headers={"User-Agent": UA}, timeout=90)
        if r.status_code == 404 and _retries > 0:
            # GDELT sometimes lists a slice before the file is published
            log.info("slice not published yet, stepping back 15 min")
            return fetch_events(_prev_slice(url), _retries - 1)
        r.raise_for_status()
        z = zipfile.ZipFile(io.BytesIO(r.content))
        stream = io.TextIOWrapper(z.open(z.namelist()[0]),
                                  encoding="utf-8", errors="replace")
        return [row for row in csv.reader(stream, delimiter="\t") if len(row) > C_URL]
    except Exception as e:
        log.warning("events fetch failed: %s", e)
        return []


def geo_match(row: list, locality: dict) -> bool:
    """Country -> state -> district, matched against the readable place name."""
    if not locality.get("enabled"):
        return True
    if locality.get("country_code") and row[C_GEO_CC] != locality["country_code"]:
        return False
    place = row[C_GEO_FULL].lower()
    terms = [t.lower() for t in
             ([locality.get("state")] + list(locality.get("districts", []))) if t]
    return any(t in place for t in terms) if terms else True


def kw_hit(text: str, keywords) -> bool:
    """Word-boundary match. Substring matching makes short keys like "ai"
    fire on unrelated words, which wastes LLM tokens downstream."""
    t = text.lower()
    for k in keywords:
        k = k.strip().lower()
        if not k:
            continue
        if re.search(r"\b" + re.escape(k) + r"(s|es|ed|ing)?\b", t):
            return True
    return False


def slug_title(url: str) -> str:
    """Fallback headline derived from the URL slug."""
    slug = re.sub(r"[?#].*$", "", url).rstrip("/").split("/")[-1]
    slug = re.sub(r"\.(html?|php|aspx?)$", "", slug)
    slug = re.sub(r"-?\d{6,}$", "", slug)
    return _html.unescape(re.sub(r"[-_]+", " ", slug)).strip().title()


def real_title(url: str) -> str:
    """Fetch the page's <title>. Falls back to the slug on any failure."""
    try:
        r = requests.get(url, headers={"User-Agent": UA}, timeout=12,
                         allow_redirects=True)
        m = re.search(r"<title[^>]*>(.*?)</title>", r.text[:200000],
                      re.S | re.I)
        if m:
            t = _html.unescape(re.sub(r"\s+", " ", m.group(1))).strip()
            t = re.split(r"\s+[|\-–—]\s+", t)[0]
            if len(t) > 15:
                return t[:200]
    except Exception:
        pass
    return slug_title(url)


def looks_like_title(t: str) -> bool:
    """Reject slug-fallback garbage: numeric ids, file extensions, no spaces.
    Sending these to the LLM wastes quota and produces nonsense summaries."""
    if not t or len(t) < 18:
        return False
    if t.count(" ") < 2:
        return False
    if re.match(r"^[\d\W]+$", t):
        return False
    if re.search(r"\.(cms|html?|php|aspx|ece)$", t, re.I):
        return False
    letters = sum(c.isalpha() for c in t)
    return letters / max(len(t), 1) > 0.6


def resolve_titles(articles: list, workers: int = 8) -> list:
    """Titles are fetched in parallel -- these are ordinary news sites, not GDELT."""
    with ThreadPoolExecutor(max_workers=workers) as ex:
        titles = list(ex.map(lambda a: real_title(a["url"]), articles))
    for a, t in zip(articles, titles):
        a["title"] = t
    return articles


def collect_events(categories: dict, locality: dict, max_per_cat: int = 25) -> dict:
    """
    One download serves every category -- no per-category requests, so no
    throttling surface at all. Returns {category: [article dicts]}.
    """
    url = latest_export_url()
    if not url:
        return {c: [] for c in categories}

    log.info("GDELT file: %s", url.rsplit("/", 1)[-1])
    rows = fetch_events(url)
    log.info("events in slice: %d", len(rows))

    rows = [r for r in rows if geo_match(r, locality)]
    log.info("after geo filter: %d", len(rows))

    out = {}
    for name, cfg in categories.items():
        hits, seen_urls = [], set()
        for r in rows:
            u = r[C_URL].strip()
            if not u or u in seen_urls:
                continue
            haystack = (u + " " + r[C_GEO_FULL]).lower()
            if not any(k.strip() in haystack for k in cfg["must_match"]):
                continue
            seen_urls.add(u)
            hits.append({
                "url": u,
                "domain": re.sub(r"^https?://(?:www\.)?([^/]+).*", r"\1", u),
                "lang": "",
                "country": r[C_GEO_CC],
                "place": r[C_GEO_FULL],
                "mentions": int(r[C_NUMMENTIONS] or 0),
            })
        hits.sort(key=lambda x: -x["mentions"])   # most-covered events first
        out[name] = hits[:max_per_cat]
        log.info("%-16s -> %d url matches", name, len(out[name]))
    return out


def collect_by_title(categories: dict, locality: dict,
                     scan_top: int = 200, max_per_cat: int = 25) -> dict:
    """
    Higher-recall variant: dedupe URLs, take the most-mentioned events,
    resolve real headlines, then categorise on the headline rather than
    the URL slug. Slower (one page fetch per candidate) but far more accurate.
    """
    url = latest_export_url()
    if not url:
        return {c: [] for c in categories}

    log.info("GDELT file: %s", url.rsplit("/", 1)[-1])
    rows = fetch_events(url)
    log.info("events in slice: %d", len(rows))

    rows = [r for r in rows if geo_match(r, locality)]
    log.info("after geo filter: %d", len(rows))

    best = {}
    for r in rows:
        u = r[C_URL].strip()
        if not u:
            continue
        m = int(r[C_NUMMENTIONS] or 0)
        if u not in best or m > best[u]["mentions"]:
            best[u] = {"url": u,
                       "domain": re.sub(r"^https?://(?:www\.)?([^/]+).*", r"\1", u),
                       "lang": "", "country": r[C_GEO_CC],
                       "place": r[C_GEO_FULL], "mentions": m}

    cands = sorted(best.values(), key=lambda x: -x["mentions"])[:scan_top]
    log.info("unique urls: %d, resolving %d titles", len(best), len(cands))
    resolve_titles(cands)

    out = {}
    for name, cfg in categories.items():
        hits = [a for a in cands if kw_hit(a["title"], cfg["must_match"])]
        out[name] = hits[:max_per_cat]
        log.info("%-16s -> %d title matches", name, len(out[name]))
    return out
