from __future__ import annotations

import hashlib
import html
import json
import logging
import math
import os
import re
import time
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple
from urllib.parse import quote, urlparse

import pandas as pd
import requests

logger = logging.getLogger("Auto_Trade_Logger")

ROOT = Path(__file__).resolve().parents[1]
STATE_DIR = ROOT / "intermediary_files" / "news_sentiment"
REPORTS_DIR = ROOT / "reports"
SUMMARY_PATH = STATE_DIR / "latest.json"
ARCHIVE_DIR = STATE_DIR / "archive"
TOPICS_DIR = STATE_DIR / "topics"
TOPICS_SUMMARY_PATH = STATE_DIR / "market_topics_latest.json"

DEFAULT_RSS_FEEDS = (
    "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms",
    "https://www.moneycontrol.com/rss/business.xml",
    "https://www.livemint.com/rss/markets",
    "https://feeds.feedburner.com/ndtvprofit-latest",
)

NEWSAPI_BASE = os.getenv("AT_NEWSAPI_BASE", "https://saurav.tech/NewsAPI")
NEWSAPI_CATEGORIES = {
    "business": "business",
    "technology": "technology",
    "science": "science",
}
NEWSAPI_COUNTRY = os.getenv("AT_NEWSAPI_COUNTRY", "in")
NEWSAPI_SOURCES = {"bbc-news": "bbc-news", "cnn": "cnn", "google-news": "google-news"}

SOURCE_WEIGHTS = {
    "reuters.com": 1.25,
    "economictimes.indiatimes.com": 1.15,
    "cnbctv18.com": 1.10,
    "ndtvprofit.com": 1.05,
    "moneycontrol.com": 1.05,
    "livemint.com": 1.05,
    "news.google.com": 0.95,
    "truthsocial.com": 1.10,
}

REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; AutoTraderNewsSentiment/1.0; +https://github.com/The-Great-One/Auto_Trader)",
    "Accept": "application/rss+xml, application/xml, text/xml;q=0.9, */*;q=0.8",
}

NEGATIVE_TYPES = {"bearish", "risk", "regulatory", "earnings_negative", "rumor"}
POSITIVE_TYPES = {"bullish", "earnings_positive", "news"}

TYPE_PATTERNS: Dict[str, Sequence[str]] = {
    "bullish": (
        "bullish",
        "buy",
        "accumulate",
        "breakout",
        "outperform",
        "upgrade",
        "all time high",
        "new high",
        "strong guidance",
        "beat estimates",
        "beats estimates",
        "order win",
        "order book",
        "upside",
        "long setup",
        "momentum intact",
        "rises",
        "gains",
        "jumps",
        "surges",
        "rebounds",
        "record high",
        "tops picks",
        "top picks",
    ),
    "bearish": (
        "bearish",
        "sell",
        "short",
        "breakdown",
        "downgrade",
        "miss estimates",
        "missed estimates",
        "weak guidance",
        "profit warning",
        "overvalued",
        "distribution",
        "lower circuit",
        "exit",
        "cut target",
        "margin pressure",
        "falls",
        "drops",
        "slumps",
        "plunges",
        "tumbles",
        "sinks",
        "spooked",
    ),
    "earnings_positive": (
        "results",
        "earnings beat",
        "beat on revenue",
        "beat on profit",
        "raised guidance",
        "record profit",
        "margin expansion",
        "strong quarter",
        "profit rises",
        "revenue rises",
        "q1",
        "q2",
        "q3",
        "q4",
    ),
    "earnings_negative": (
        "earnings miss",
        "revenue miss",
        "profit miss",
        "margin contraction",
        "weak quarter",
        "guidance cut",
        "below estimates",
        "results disappoint",
        "profit falls",
        "revenue falls",
    ),
    "news": (
        "announces",
        "launches",
        "partnership",
        "acquisition",
        "merger",
        "stake sale",
        "stake buy",
        "approval",
        "contract",
        "order",
        "wins",
        "capex",
        "buyback",
        "top picks",
    ),
    "risk": (
        "rates",
        "inflation",
        "cpi",
        "fed",
        "rbi",
        "yield",
        "crude",
        "oil",
        "war",
        "attack",
        "sanction",
        "tariff",
        "tariffs",
        "recession",
        "global risk",
        "trade war",
    ),
    "regulatory": (
        "sebi",
        "investigation",
        "probe",
        "fraud",
        "lawsuit",
        "penalty",
        "ban",
        "default",
        "pledge",
        "governance",
        "resigns",
    ),
    "rumor": (
        "rumor",
        "unconfirmed",
        "hearing",
        "sources say",
        "reportedly",
        "maybe",
        "looks like",
        "could be",
        "might be",
    ),
    "meme": (
        "to the moon",
        "moon",
        "rocket",
        "diamond hands",
        "yolo",
        "100x",
        "multibagger",
        "lambo",
        "apeing",
    ),
}

TYPE_WEIGHTS = {
    "bullish": 0.45,
    "bearish": -0.45,
    "earnings_positive": 0.35,
    "earnings_negative": -0.35,
    "news": 0.15,
    "risk": -0.20,
    "regulatory": -0.40,
    "rumor": -0.10,
    "meme": 0.0,
}

TOPIC_CONFIGS = {
    "trump_market": {
        "label": "Trump market impact",
        "queries": [
            '("Donald Trump" OR Trump OR "Truth Social") (tariff OR tariffs OR trade OR market OR stocks OR oil OR fed OR china) when:7d',
            '("Donald Trump" OR Trump) (markets OR stocks OR trade war OR tariffs) when:7d',
        ],
        "feeds_env": "AT_TRUMP_RSS_FEEDS",
    },
    "gift_nifty": {
        "label": "GIFT Nifty pre-market",
        "queries": [
            '"GIFT Nifty" (India OR NSE OR BSE OR SGX) when:1d',
            '"GIFT Nifty" (sgx OR pre-market OR opening OR outlook) when:1d',
        ],
        "feeds_env": "AT_GIFT_NIFTY_RSS_FEEDS",
    },
    "india_vix": {
        "label": "India VIX volatility",
        "queries": [
            '"India VIX" (fear OR volatility OR options OR spike OR surge OR low) when:3d',
            '("India VIX" OR "VIX India") (Nifty OR market OR panic OR calm) when:3d',
        ],
        "feeds_env": "AT_INDIA_VIX_RSS_FEEDS",
    },
    "sector_it": {
        "label": "IT sector focus",
        "queries": [
            '("IT sector" OR TCS OR Infosys OR Wipro OR HCLTech OR "tech stocks") India when:3d',
            '(NIFTY IT OR "information technology") (earnings OR outlook OR upgrade OR downgrade) when:3d',
        ],
        "feeds_env": "AT_SECTOR_IT_RSS",
    },
    "sector_banking": {
        "label": "Banking sector focus",
        "queries": [
            '("banking sector" OR HDFC Bank OR ICICI Bank OR SBI OR Kotak OR Axis) India when:3d',
            '(NIFTY Bank OR "bank stocks" OR NPAs OR credit growth) when:3d',
        ],
        "feeds_env": "AT_SECTOR_BANKING_RSS",
    },
    "sector_pharma": {
        "label": "Pharma sector focus",
        "queries": [
            '("pharma sector" OR Sun Pharma OR Dr Reddy OR Cipla OR Divis OR Lupin) India when:3d',
            '(NIFTY Pharma OR "pharmaceutical" OR "drug approval" OR FDA) when:3d',
        ],
        "feeds_env": "AT_SECTOR_PHARMA_RSS",
    },
    "sector_energy": {
        "label": "Energy & Oil sector focus",
        "queries": [
            '("oil and gas" OR Reliance OR ONGC OR BPCL OR HPCL OR IOC) India when:3d',
            '(crude oil OR "energy stocks" OR NIFTY Energy) India when:3d',
        ],
        "feeds_env": "AT_SECTOR_ENERGY_RSS",
    },
    "sector_auto": {
        "label": "Auto sector focus",
        "queries": [
            '("auto sector" OR Maruti OR M&M OR Tata Motors OR Bajaj Auto OR Hero Moto) India when:3d',
            '(NIFTY Auto OR "car sales" OR "EV" OR "vehicle sales") when:3d',
        ],
        "feeds_env": "AT_SECTOR_AUTO_RSS",
    },
    "sector_metals": {
        "label": "Metals sector focus",
        "queries": [
            '("metal stocks" OR Tata Steel OR JSW Steel OR Hindalco OR Vedanta) India when:3d',
            '(NIFTY Metal OR copper OR aluminium OR steel prices) when:3d',
        ],
        "feeds_env": "AT_SECTOR_METALS_RSS",
    },
    "sector_fmcg": {
        "label": "FMCG sector focus",
        "queries": [
            '("FMCG sector" OR ITC OR HUL OR Nestle OR Britannia OR Dabur) India when:3d',
            '(NIFTY FMCG OR "consumer goods" OR rural demand) when:3d',
        ],
        "feeds_env": "AT_SECTOR_FMCG_RSS",
    },
}


def _env_flag(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _safe_float(value, default=0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _safe_int(value, default=0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _normalize_symbol(symbol: str) -> str:
    return str(symbol or "").strip().upper()


def _load_symbol_aliases() -> Dict[str, List[str]]:
    raw = os.getenv("AT_NEWS_SYMBOL_ALIASES_JSON", "").strip()
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except Exception as exc:
        logger.warning("Failed to parse AT_NEWS_SYMBOL_ALIASES_JSON: %s", exc)
        return {}

    aliases: Dict[str, List[str]] = {}
    if isinstance(data, dict):
        for key, value in data.items():
            vals = value if isinstance(value, list) else [value]
            aliases[_normalize_symbol(key)] = [str(v).strip() for v in vals if str(v).strip()]
    return aliases


def _symbol_query_terms(symbol: str, asset_class: Optional[str] = None, etf_theme: str = "") -> List[str]:
    symbol = _normalize_symbol(symbol)
    aliases = _load_symbol_aliases().get(symbol, [])
    terms = [f"${symbol}", symbol]
    if asset_class and str(asset_class).upper() == "ETF":
        if etf_theme:
            terms.append(str(etf_theme).strip())
        if symbol.startswith("NIFTY"):
            terms.extend(["NIFTY", "Nifty 50"])
    for alias in aliases:
        if alias not in terms:
            terms.append(alias)
    return [t for t in terms if t]


def _split_env_list(name: str) -> List[str]:
    raw = os.getenv(name, "").strip()
    values: List[str] = []
    if not raw:
        return values
    for part in re.split(r"[\n,]+", raw):
        item = str(part or "").strip()
        if item and item not in values:
            values.append(item)
    return values


def discover_symbols(limit: int = 30) -> List[str]:
    symbols: List[str] = []
    seen = set()

    for raw in _split_env_list("AT_NEWS_EXTRA_SYMBOLS"):
        sym = _normalize_symbol(raw)
        if sym and sym not in seen:
            symbols.append(sym)
            seen.add(sym)

    holdings_path = ROOT / "intermediary_files" / "Holdings.feather"
    if holdings_path.exists():
        try:
            holdings = pd.read_feather(holdings_path)
            for sym in holdings.get("tradingsymbol", pd.Series(dtype=str)).astype(str):
                sym_n = _normalize_symbol(sym)
                if sym_n and sym_n not in seen:
                    symbols.append(sym_n)
                    seen.add(sym_n)
        except Exception as exc:
            logger.warning("Failed to inspect Holdings.feather for news symbols: %s", exc)

    instruments_path = ROOT / "intermediary_files" / "Instruments.feather"
    if instruments_path.exists() and len(symbols) < limit:
        try:
            instruments = pd.read_feather(instruments_path)
            col = instruments.get("tradingsymbol", pd.Series(dtype=str)).astype(str)
            held = set()
            if holdings_path.exists():
                try:
                    h = pd.read_feather(holdings_path)
                    held = set(h["tradingsymbol"].astype(str).str.upper().tolist())
                except Exception:
                    pass
            for sym in col.head(limit * 4):
                sym_n = _normalize_symbol(sym)
                if sym_n and sym_n not in seen:
                    if sym_n in held or len(symbols) < limit // 2:
                        symbols.append(sym_n)
                        seen.add(sym_n)
                if len(symbols) >= limit:
                    break
        except Exception as exc:
            logger.warning("Failed to inspect Instruments.feather for news symbols: %s", exc)

    return symbols[:limit]


def symbol_is_held(symbol: str, holdings: Optional[pd.DataFrame]) -> bool:
    if holdings is None or holdings.empty or "tradingsymbol" not in holdings.columns:
        return False
    tradingsymbols = holdings["tradingsymbol"].astype(str).str.upper()
    return _normalize_symbol(symbol) in set(tradingsymbols.tolist())


def _configured_feeds() -> List[str]:
    feeds = _split_env_list("AT_NEWS_RSS_FEEDS")
    if feeds:
        return feeds
    return list(DEFAULT_RSS_FEEDS)


def _google_news_search_feed(symbol: str, *, asset_class: Optional[str] = None, etf_theme: str = "") -> Optional[str]:
    if not _env_flag("AT_NEWS_GOOGLE_SEARCH_ENABLED", True):
        return None

    normalized = _normalize_symbol(symbol)
    terms = []
    seeded = [f"{normalized}.NS", f'"{normalized} NSE"', normalized]
    for term in seeded + _symbol_query_terms(symbol, asset_class=asset_class, etf_theme=etf_theme):
        cleaned = str(term or "").strip()
        if cleaned and cleaned not in terms:
            terms.append(cleaned)
        if len(terms) >= 4:
            break

    if not terms:
        return None

    query = " OR ".join(term if term.startswith('"') else (f'"{term}"' if " " in term else term) for term in terms)
    query = f"({query}) India stock market when:7d"
    return f"https://news.google.com/rss/search?q={quote(query)}&hl=en-IN&gl=IN&ceid=IN:en"


def _google_topic_feed(query: str) -> str:
    return f"https://news.google.com/rss/search?q={quote(query)}&hl=en-IN&gl=IN&ceid=IN:en"


def _strip_html(text: str) -> str:
    clean = re.sub(r"<[^>]+>", " ", str(text or ""))
    clean = html.unescape(clean)
    return re.sub(r"\s+", " ", clean).strip()


def _source_name(url: str) -> str:
    host = (urlparse(url).netloc or "").lower().replace("www.", "")
    return host


def _parse_published_at(value: str) -> Optional[int]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return int(parsedate_to_datetime(text).timestamp())
    except Exception:
        pass
    try:
        ts = pd.Timestamp(text)
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        return int(ts.timestamp())
    except Exception:
        return None


def _entry_children_text(elem, *names: str) -> str:
    for child in list(elem):
        tag = (child.tag or "").split("}")[-1].lower()
        if tag in names:
            text = "".join(child.itertext()) if len(list(child)) else (child.text or "")
            if text and text.strip():
                return text.strip()
    return ""


def _parse_feed_items(xml_text: str, feed_url: str) -> List[dict]:
    try:
        root = ET.fromstring(xml_text)
    except Exception as exc:
        raise ValueError(f"Invalid RSS/XML payload: {exc}") from exc

    source = _source_name(feed_url)
    items: List[dict] = []
    for elem in root.iter():
        tag = (elem.tag or "").split("}")[-1].lower()
        if tag not in {"item", "entry"}:
            continue

        title = _entry_children_text(elem, "title")
        summary = _entry_children_text(elem, "description", "summary", "content")
        link = _entry_children_text(elem, "link")
        if not link:
            for child in list(elem):
                child_tag = (child.tag or "").split("}")[-1].lower()
                if child_tag == "link":
                    href = child.attrib.get("href")
                    if href:
                        link = href
                        break
        published_raw = _entry_children_text(elem, "pubdate", "published", "updated")
        published_at = _parse_published_at(published_raw)

        text_blob = _strip_html(f"{title} {summary}")
        if not text_blob:
            continue

        items.append(
            {
                "title": _strip_html(title),
                "summary": _strip_html(summary),
                "text": text_blob,
                "link": link,
                "published_at": published_at,
                "published_raw": published_raw,
                "source": source,
                "feed_url": feed_url,
            }
        )
    return items


def fetch_rss_entries(feed_url: str, *, timeout: int = 20) -> dict:
    try:
        resp = requests.get(feed_url, headers=REQUEST_HEADERS, timeout=timeout)
        resp.raise_for_status()
        entries = _parse_feed_items(resp.text, feed_url)
        return {
            "feed_url": feed_url,
            "source": _source_name(feed_url),
            "status": "ok",
            "entries": entries,
        }
    except Exception as exc:
        logger.warning("RSS fetch failed for %s: %s", feed_url, exc)
        return {
            "feed_url": feed_url,
            "source": _source_name(feed_url),
            "status": "error",
            "error": str(exc),
            "entries": [],
        }


def fetch_newsapi_category(category: str, country: str = "", timeout: int = 15) -> dict:
    """Fetch headlines from SauravKanchan/NewsAPI (no API key required)."""
    country = country or NEWSAPI_COUNTRY
    url = f"{NEWSAPI_BASE}/top-headlines/category/{category}/{country}.json"
    try:
        resp = requests.get(url, headers=REQUEST_HEADERS, timeout=timeout)
        resp.raise_for_status()
        payload = resp.json()
    except Exception as exc:
        logger.warning("NewsAPI fetch failed for %s/%s: %s", category, country, exc)
        return {"url": url, "status": "error", "error": str(exc), "entries": []}

    entries = []
    for article in (payload.get("articles") or []):
        title = _strip_html(str(article.get("title") or ""))
        desc = _strip_html(str(article.get("description") or ""))
        text_blob = f"{title} {desc}".strip()
        if not text_blob:
            continue
        published_at = None
        pub_str = str(article.get("publishedAt") or "")
        if pub_str:
            try:
                published_at = int(pd.Timestamp(pub_str).timestamp())
            except Exception:
                pass
        source_name = (article.get("source") or {}).get("name", "newsapi")
        entries.append({
            "title": title,
            "summary": desc,
            "text": text_blob,
            "link": str(article.get("url") or ""),
            "published_at": published_at,
            "published_raw": pub_str,
            "source": source_name.lower().replace(" ", ""),
            "feed_url": url,
        })
    return {"url": url, "status": "ok", "category": category, "country": country, "entries": entries}


def fetch_newsapi_source(source_id: str, timeout: int = 15) -> dict:
    """Fetch everything from a specific news source via NewsAPI (no API key required)."""
    url = f"{NEWSAPI_BASE}/everything/{source_id}.json"
    try:
        resp = requests.get(url, headers=REQUEST_HEADERS, timeout=timeout)
        resp.raise_for_status()
        payload = resp.json()
    except Exception as exc:
        logger.warning("NewsAPI source fetch failed for %s: %s", source_id, exc)
        return {"url": url, "status": "error", "error": str(exc), "entries": []}

    entries = []
    for article in (payload.get("articles") or []):
        title = _strip_html(str(article.get("title") or ""))
        desc = _strip_html(str(article.get("description") or ""))
        text_blob = f"{title} {desc}".strip()
        if not text_blob:
            continue
        published_at = None
        pub_str = str(article.get("publishedAt") or "")
        if pub_str:
            try:
                published_at = int(pd.Timestamp(pub_str).timestamp())
            except Exception:
                pass
        source_name = (article.get("source") or {}).get("name", source_id)
        entries.append({
            "title": title,
            "summary": desc,
            "text": text_blob,
            "link": str(article.get("url") or ""),
            "published_at": published_at,
            "published_raw": pub_str,
            "source": source_name.lower().replace(" ", ""),
            "feed_url": url,
        })
    return {"url": url, "status": "ok", "source_id": source_id, "entries": entries}


def _regex_hit(pattern: str, text: str) -> bool:
    escaped = re.escape(pattern)
    if re.search(r"[A-Za-z0-9]", pattern):
        return re.search(rf"(?<![A-Za-z0-9]){escaped}(?![A-Za-z0-9])", text, flags=re.IGNORECASE) is not None
    return re.search(escaped, text, flags=re.IGNORECASE) is not None


def classify_text(text: str) -> dict:
    text = re.sub(r"\s+", " ", str(text or "").strip().lower())
    matched: Dict[str, List[str]] = {}
    score = 0.0

    for label, patterns in TYPE_PATTERNS.items():
        hits = [pat for pat in patterns if _regex_hit(pat.lower(), text)]
        if hits:
            matched[label] = hits
            score += TYPE_WEIGHTS.get(label, 0.0) * min(2, len(hits))

    types = list(matched.keys()) or ["uncategorized"]
    score = max(-1.0, min(1.0, score))
    confidence = min(1.0, 0.2 + 0.15 * sum(len(v) for v in matched.values()))
    if "meme" in matched and len(matched) == 1:
        confidence = min(confidence, 0.35)
    return {
        "types": types,
        "matches": matched,
        "sentiment": round(score, 4),
        "confidence": round(confidence, 4),
    }


def _source_weight(source: str) -> float:
    src = str(source or "").lower()
    for key, weight in SOURCE_WEIGHTS.items():
        if key in src:
            return weight
    return 1.0


def _recency_weight(published_at: Optional[int]) -> float:
    if not published_at:
        return 1.0
    age_hours = max(0.0, (time.time() - float(published_at)) / 3600.0)
    if age_hours <= 6:
        return 1.45
    if age_hours <= 24:
        return 1.25
    if age_hours <= 72:
        return 1.05
    return max(0.6, 1.0 / math.sqrt(1.0 + age_hours / 48.0))


def _symbol_match(text: str, symbol: str, *, asset_class: Optional[str] = None, etf_theme: str = "") -> bool:
    haystack = str(text or "")
    if not haystack.strip():
        return False

    for term in _symbol_query_terms(symbol, asset_class=asset_class, etf_theme=etf_theme):
        token = str(term or "").strip()
        if token and _regex_hit(token, haystack):
            return True
    return False


def _event_id(kind: str, key: str, entry: dict) -> str:
    base = "|".join(
        [
            kind,
            key,
            str(entry.get("link") or ""),
            str(entry.get("published_at") or entry.get("published_raw") or ""),
            str(entry.get("title") or ""),
        ]
    )
    return hashlib.sha1(base.encode("utf-8", errors="ignore")).hexdigest()


def _append_archive_rows(rows: Sequence[dict]) -> None:
    if not rows:
        return

    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    by_day: Dict[str, List[dict]] = {}
    for row in rows:
        fetched_day = str(pd.Timestamp.utcfromtimestamp(int(row.get("fetched_at", time.time()))).date())
        by_day.setdefault(fetched_day, []).append(row)

    for day, day_rows in by_day.items():
        path = ARCHIVE_DIR / f"{day}.jsonl"
        seen = set()
        if path.exists():
            try:
                for line in path.read_text().splitlines():
                    if not line.strip():
                        continue
                    payload = json.loads(line)
                    event_id = str(payload.get("event_id") or "")
                    if event_id:
                        seen.add(event_id)
            except Exception:
                seen = set()
        with path.open("a", encoding="utf-8") as fh:
            for row in day_rows:
                event_id = str(row.get("event_id") or "")
                if event_id in seen:
                    continue
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")
                seen.add(event_id)


def _dedupe_entries(kind: str, key: str, entries: Sequence[dict]) -> List[dict]:
    deduped: List[dict] = []
    seen = set()
    for entry in entries:
        event_id = _event_id(kind, key, entry)
        if event_id in seen:
            continue
        seen.add(event_id)
        payload = dict(entry)
        payload["event_id"] = event_id
        deduped.append(payload)
    return deduped


def archive_entries(kind: str, key: str, entries: Sequence[dict]) -> None:
    fetched_at = int(time.time())
    rows = []
    for entry in _dedupe_entries(kind, key, entries):
        cls = entry.get("classification") or classify_text(entry.get("text") or "")
        rows.append(
            {
                "event_id": entry.get("event_id") or _event_id(kind, key, entry),
                "kind": kind,
                "key": key,
                "fetched_at": fetched_at,
                "published_at": entry.get("published_at"),
                "published_raw": entry.get("published_raw"),
                "source": entry.get("source"),
                "feed_url": entry.get("feed_url"),
                "link": entry.get("link"),
                "title": entry.get("title"),
                "summary": entry.get("summary"),
                "text": entry.get("text"),
                "classification": cls,
            }
        )
    _append_archive_rows(rows)


def _analyze_entries(kind: str, key: str, entries: Sequence[dict], *, item_label: str = "item") -> dict:
    if not entries:
        return {
            kind: key,
            f"{item_label}_count": 0,
            f"bullish_{item_label}s": 0,
            f"bearish_{item_label}s": 0,
            "weighted_sentiment": 0.0,
            "type_counts": {},
            "dominant_types": [],
            "sample_headlines": [],
            "top_items": [],
            "generated_at": int(time.time()),
            "status": f"no_{item_label}s",
        }

    type_counts: Dict[str, int] = {}
    scored = []
    weighted_sum = 0.0
    total_weight = 0.0
    bullish = 0
    bearish = 0

    for entry in entries:
        text = str(entry.get("text") or "")
        cls = entry.get("classification") or classify_text(text)
        weight = max(0.4, cls.get("confidence", 0.0)) * _source_weight(entry.get("source")) * _recency_weight(entry.get("published_at"))
        signed = cls.get("sentiment", 0.0) * weight
        weighted_sum += signed
        total_weight += weight
        for label in cls.get("types") or []:
            type_counts[label] = type_counts.get(label, 0) + 1
        if any(t in NEGATIVE_TYPES for t in cls.get("types") or []):
            bearish += 1
        if any(t in POSITIVE_TYPES for t in cls.get("types") or []):
            bullish += 1
        scored.append(
            {
                "title": entry.get("title"),
                "summary": entry.get("summary"),
                "link": entry.get("link"),
                "source": entry.get("source"),
                "published_at": entry.get("published_at"),
                "classification": cls,
                "weight": round(weight, 4),
            }
        )

    scored.sort(key=lambda x: abs(x["classification"].get("sentiment", 0.0)) * x["weight"], reverse=True)
    dominant_types = [label for label, _ in sorted(type_counts.items(), key=lambda kv: (-kv[1], kv[0]))[:4]]
    weighted_sentiment = weighted_sum / total_weight if total_weight else 0.0
    return {
        kind: key,
        f"{item_label}_count": len(entries),
        f"bullish_{item_label}s": bullish,
        f"bearish_{item_label}s": bearish,
        "weighted_sentiment": round(max(-1.0, min(1.0, weighted_sentiment)), 4),
        "type_counts": type_counts,
        "dominant_types": dominant_types,
        "sample_headlines": [s.get("title") for s in scored[:5] if s.get("title")],
        "top_items": scored[:5],
        "generated_at": int(time.time()),
        "status": "ok",
    }


def infer_news_trade_bias(analysis: dict) -> dict:
    sentiment = _safe_float(analysis.get("weighted_sentiment"), 0.0)
    item_count = _safe_int(analysis.get("item_count"), 0)
    bearish = _safe_int(analysis.get("bearish_items"), 0)
    bullish = _safe_int(analysis.get("bullish_items"), 0)
    dominant = set(analysis.get("dominant_types") or [])

    min_items = max(2, _safe_int(os.getenv("AT_NEWS_MIN_ITEMS", "3"), 3))
    buy_block_threshold = _safe_float(os.getenv("AT_NEWS_BUY_BLOCK_THRESHOLD", "-0.18"), -0.18)
    sell_force_threshold = _safe_float(os.getenv("AT_NEWS_SELL_FORCE_THRESHOLD", "-0.35"), -0.35)
    positive_boost_threshold = _safe_float(os.getenv("AT_NEWS_POSITIVE_BOOST_THRESHOLD", "0.22"), 0.22)

    block_buy = (
        item_count >= min_items
        and sentiment <= buy_block_threshold
        and bearish >= max(2, bullish)
        and bool(dominant & NEGATIVE_TYPES)
    )
    force_sell = (
        item_count >= min_items
        and sentiment <= sell_force_threshold
        and bearish >= max(2, bullish + 1)
        and bool(dominant & (NEGATIVE_TYPES - {"rumor"}))
    )
    positive_boost = (
        item_count >= min_items
        and sentiment >= positive_boost_threshold
        and bullish >= bearish + 1
        and bool(dominant & POSITIVE_TYPES)
    )

    reasons = []
    if block_buy:
        reasons.append(f"negative rss/news flow {sentiment:.2f}")
    if force_sell:
        reasons.append(f"credible bearish rss/news flow {sentiment:.2f}")
    if positive_boost:
        reasons.append(f"supportive rss/news flow {sentiment:.2f}")

    return {
        "block_buy": block_buy,
        "force_sell": force_sell,
        "positive_boost": positive_boost,
        "reason": "; ".join(reasons) if reasons else "neutral",
    }


def analyze_news(symbol: str, entries: Sequence[dict]) -> dict:
    summary = _analyze_entries("symbol", _normalize_symbol(symbol), entries)
    summary["symbol"] = summary.pop("symbol")
    summary["item_count"] = summary.pop("item_count")
    summary["bullish_items"] = summary.pop("bullish_items")
    summary["bearish_items"] = summary.pop("bearish_items")
    summary["trade_bias"] = infer_news_trade_bias(summary)
    return summary


def save_analysis(analysis: dict) -> Path:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    path = STATE_DIR / f"{_normalize_symbol(analysis.get('symbol'))}.json"
    path.write_text(json.dumps(analysis, indent=2))
    return path


def load_analysis(symbol: str, max_age_minutes: Optional[int] = None) -> Optional[dict]:
    path = STATE_DIR / f"{_normalize_symbol(symbol)}.json"
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text())
    except Exception as exc:
        logger.warning("Failed to load news sentiment snapshot for %s: %s", symbol, exc)
        return None
    if max_age_minutes is not None:
        generated_at = _safe_int(data.get("generated_at"), 0)
        if generated_at <= 0 or time.time() - generated_at > max_age_minutes * 60:
            return None
    return data


def write_summary(analyses: Sequence[dict]) -> dict:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    active = [a for a in analyses if a.get("status") == "ok"]
    summary = {
        "generated_at": int(time.time()),
        "symbols": len(analyses),
        "active": active,
        "top_bullish": sorted(active, key=lambda x: x.get("weighted_sentiment", 0.0), reverse=True)[:5],
        "top_bearish": sorted(active, key=lambda x: x.get("weighted_sentiment", 0.0))[:5],
        "feeds": _configured_feeds(),
        "archive_dir": str(ARCHIVE_DIR),
    }
    SUMMARY_PATH.write_text(json.dumps(summary, indent=2))
    (REPORTS_DIR / "news_sentiment_latest.json").write_text(json.dumps(summary, indent=2))
    return summary


def fetch_and_analyze_symbol(symbol: str, *, asset_class: Optional[str] = None, etf_theme: str = "") -> dict:
    per_feed_limit = max(5, _safe_int(os.getenv("AT_NEWS_MAX_ITEMS_PER_FEED", "40"), 40))
    matches: List[dict] = []
    feed_status: List[dict] = []
    feed_urls = _configured_feeds()
    google_feed = _google_news_search_feed(symbol, asset_class=asset_class, etf_theme=etf_theme)
    if google_feed and google_feed not in feed_urls:
        feed_urls.append(google_feed)

    for feed_url in feed_urls:
        fetched = fetch_rss_entries(feed_url)
        feed_status.append(
            {
                "feed_url": feed_url,
                "source": fetched.get("source"),
                "status": fetched.get("status"),
                "error": fetched.get("error"),
                "entry_count": len(fetched.get("entries") or []),
            }
        )
        if fetched.get("status") != "ok":
            continue
        count = 0
        for entry in fetched.get("entries") or []:
            if _symbol_match(entry.get("text"), symbol, asset_class=asset_class, etf_theme=etf_theme):
                entry = dict(entry)
                entry["classification"] = classify_text(entry.get("text") or "")
                matches.append(entry)
                count += 1
                if count >= per_feed_limit:
                    break

    # Also fetch NewsAPI business headlines for India (no API key required)
    if _env_flag("AT_NEWS_NEWSAPI_ENABLED", True):
        for cat in NEWSAPI_CATEGORIES:
            newsapi_result = fetch_newsapi_category(cat)
            if newsapi_result.get("status") == "ok":
                feed_status.append({
                    "feed_url": newsapi_result.get("url", ""),
                    "source": f"newsapi_{cat}",
                    "status": "ok",
                    "error": None,
                    "entry_count": len(newsapi_result.get("entries") or []),
                })
                count = 0
                for entry in newsapi_result.get("entries") or []:
                    if _symbol_match(entry.get("text"), symbol, asset_class=asset_class, etf_theme=etf_theme):
                        entry = dict(entry)
                        entry["classification"] = classify_text(entry.get("text") or "")
                        matches.append(entry)
                        count += 1
                        if count >= per_feed_limit:
                            break
            else:
                feed_status.append({
                    "feed_url": newsapi_result.get("url", ""),
                    "source": f"newsapi_{cat}",
                    "status": "error",
                    "error": newsapi_result.get("error"),
                    "entry_count": 0,
                })

    matches = _dedupe_entries("symbol", _normalize_symbol(symbol), matches)
    archive_entries("symbol", _normalize_symbol(symbol), matches)
    analysis = analyze_news(symbol, matches)
    analysis["feed_status"] = feed_status
    analysis["archive_dir"] = str(ARCHIVE_DIR)
    save_analysis(analysis)
    return analysis


def _topic_feed_urls(topic: str) -> List[str]:
    cfg = TOPIC_CONFIGS.get(topic, {})
    feeds: List[str] = []
    feeds_env = str(cfg.get("feeds_env") or "").strip()
    if feeds_env:
        feeds.extend(_split_env_list(feeds_env))
    for query in cfg.get("queries") or []:
        url = _google_topic_feed(str(query))
        if url not in feeds:
            feeds.append(url)
    return feeds


def save_topic_analysis(topic: str, analysis: dict) -> Path:
    TOPICS_DIR.mkdir(parents=True, exist_ok=True)
    path = TOPICS_DIR / f"{topic}.json"
    path.write_text(json.dumps(analysis, indent=2))
    return path


def load_topic_analysis(topic: str, max_age_minutes: Optional[int] = None) -> Optional[dict]:
    path = TOPICS_DIR / f"{topic}.json"
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text())
    except Exception as exc:
        logger.warning("Failed to load topic analysis for %s: %s", topic, exc)
        return None
    if max_age_minutes is not None:
        generated_at = _safe_int(data.get("generated_at"), 0)
        if generated_at <= 0 or time.time() - generated_at > max_age_minutes * 60:
            return None
    return data


def fetch_and_analyze_topic(topic: str) -> dict:
    cfg = TOPIC_CONFIGS.get(topic, {})
    entries: List[dict] = []
    feed_status: List[dict] = []
    for feed_url in _topic_feed_urls(topic):
        fetched = fetch_rss_entries(feed_url)
        feed_status.append(
            {
                "feed_url": feed_url,
                "source": fetched.get("source"),
                "status": fetched.get("status"),
                "error": fetched.get("error"),
                "entry_count": len(fetched.get("entries") or []),
            }
        )
        if fetched.get("status") != "ok":
            continue
        for entry in fetched.get("entries") or []:
            entry = dict(entry)
            entry["classification"] = classify_text(entry.get("text") or "")
            entries.append(entry)

    # For sector topics, also pull NewsAPI business headlines for India
    if topic.startswith("sector_") and _env_flag("AT_NEWS_NEWSAPI_ENABLED", True):
        for cat in NEWSAPI_CATEGORIES:
            newsapi_result = fetch_newsapi_category(cat)
            if newsapi_result.get("status") == "ok":
                feed_status.append({
                    "feed_url": newsapi_result.get("url", ""),
                    "source": f"newsapi_{cat}",
                    "status": "ok",
                    "error": None,
                    "entry_count": len(newsapi_result.get("entries") or []),
                })
                for entry in newsapi_result.get("entries") or []:
                    entry = dict(entry)
                    entry["classification"] = classify_text(entry.get("text") or "")
                    entries.append(entry)

    entries = _dedupe_entries("topic", topic, entries)
    archive_entries("topic", topic, entries)
    summary = _analyze_entries("topic", topic, entries)
    summary["topic"] = summary.pop("topic")
    summary["item_count"] = summary.pop("item_count")
    summary["bullish_items"] = summary.pop("bullish_items")
    summary["bearish_items"] = summary.pop("bearish_items")
    summary["label"] = cfg.get("label", topic)
    summary["feed_status"] = feed_status
    summary["archive_dir"] = str(ARCHIVE_DIR)
    save_topic_analysis(topic, summary)
    return summary


def fetch_and_analyze_topics(topics: Optional[Sequence[str]] = None) -> dict:
    topic_list = [str(t).strip() for t in (topics or TOPIC_CONFIGS.keys()) if str(t).strip()]
    analyses = [fetch_and_analyze_topic(topic) for topic in topic_list]
    payload = {
        "generated_at": int(time.time()),
        "topics": analyses,
        "archive_dir": str(ARCHIVE_DIR),
    }
    TOPICS_SUMMARY_PATH.write_text(json.dumps(payload, indent=2))
    (REPORTS_DIR / "market_topics_latest.json").write_text(json.dumps(payload, indent=2))
    return payload


def latest_topic_snapshot(topics: Optional[Sequence[str]] = None, max_age_minutes: int = 240) -> dict:
    topic_list = [str(t).strip() for t in (topics or TOPIC_CONFIGS.keys()) if str(t).strip()]
    out = []
    for topic in topic_list:
        loaded = load_topic_analysis(topic, max_age_minutes=max_age_minutes)
        if loaded:
            out.append(loaded)
    return {"generated_at": int(time.time()), "topics": out}


def apply_news_overlay(base_decision: str, symbol: str, holdings: Optional[pd.DataFrame] = None) -> Tuple[str, Optional[dict]]:
    if not _env_flag("AT_NEWS_SENTIMENT_ENABLED", False):
        return str(base_decision).upper(), None

    ttl_minutes = max(1, _safe_int(os.getenv("AT_NEWS_SENTIMENT_TTL_MINUTES", "120"), 120))
    analysis = load_analysis(symbol, max_age_minutes=ttl_minutes)
    if not analysis or analysis.get("status") != "ok":
        return str(base_decision).upper(), None

    bias = analysis.get("trade_bias") or infer_news_trade_bias(analysis)
    decision = str(base_decision).upper()
    held = symbol_is_held(symbol, holdings)

    if decision == "BUY" and bias.get("block_buy"):
        return "HOLD", {
            "source": "RSS_NEWS_SENTIMENT",
            "action": "blocked_buy",
            "reason": bias.get("reason"),
            "weighted_sentiment": analysis.get("weighted_sentiment"),
            "dominant_types": analysis.get("dominant_types"),
            "item_count": analysis.get("item_count"),
        }

    if decision == "HOLD" and held and bias.get("force_sell"):
        return "SELL", {
            "source": "RSS_NEWS_SENTIMENT",
            "action": "forced_sell",
            "reason": bias.get("reason"),
            "weighted_sentiment": analysis.get("weighted_sentiment"),
            "dominant_types": analysis.get("dominant_types"),
            "item_count": analysis.get("item_count"),
        }

    if decision == "BUY" and bias.get("positive_boost"):
        return decision, {
            "source": "RSS_NEWS_SENTIMENT",
            "action": "confirmed_buy",
            "reason": bias.get("reason"),
            "weighted_sentiment": analysis.get("weighted_sentiment"),
            "dominant_types": analysis.get("dominant_types"),
            "item_count": analysis.get("item_count"),
        }

    return decision, None


def fetch_and_analyze_many(symbols: Sequence[str]) -> List[dict]:
    analyses = [fetch_and_analyze_symbol(symbol) for symbol in symbols]
    write_summary(analyses)
    return analyses


SECTOR_TOPIC_PREFIX = "sector_"

SECTOR_STOCK_MAP: Dict[str, List[str]] = {
    "sector_it": ["TCS", "INFY", "WIPRO", "HCLTECH", "TECHM"],
    "sector_banking": ["HDFCBANK", "ICICIBANK", "SBIN", "KOTAKBANK", "AXISBANK", "BANKBARODA"],
    "sector_pharma": ["SUNPHARMA", "DRREDDY", "CIPLA", "DIVISLAB", "LUPIN", "AUROPHARMA"],
    "sector_energy": ["RELIANCE", "ONGC", "BPCL", "HINDPETRO", "IOC", "GAIL"],
    "sector_auto": ["MARUTI", "M&M", "TATAMOTORS", "BAJAJ-AUTO", "HEROMOTOCO", "EICHERMOT"],
    "sector_metals": ["TATASTEEL", "JSWSTEEL", "HINDALCO", "VEDL", "COALINDIA", "NMDC"],
    "sector_fmcg": ["ITC", "HINDUNILVR", "NESTLEIND", "BRITANNIA", "DABUR", "MARICO"],
}


def compute_sector_rotation(max_age_minutes: int = 360) -> dict:
    """Rank sectors by news sentiment and pick the best stock from hot sectors."""
    sector_scores: List[dict] = []
    for topic_key, stock_list in SECTOR_STOCK_MAP.items():
        topic_data = load_topic_analysis(topic_key, max_age_minutes=max_age_minutes)
        if not topic_data or topic_data.get("status") != "ok":
            continue
        weighted = _safe_float(topic_data.get("weighted_sentiment"), 0.0)
        item_count = _safe_int(topic_data.get("item_count"), 0)
        dominant = topic_data.get("dominant_types") or []
        bullish = "bullish" in dominant or "earnings_positive" in dominant
        bearish = "bearish" in dominant or "earnings_negative" in dominant or "risk" in dominant
        sector_scores.append({
            "topic": topic_key,
            "label": (TOPIC_CONFIGS.get(topic_key) or {}).get("label", topic_key),
            "weighted_sentiment": weighted,
            "item_count": item_count,
            "dominant_types": dominant,
            "bullish": bullish,
            "bearish": bearish,
            "top_picks": stock_list[:3],
        })

    sector_scores.sort(key=lambda s: s["weighted_sentiment"], reverse=True)

    picks: List[dict] = []
    for sector in sector_scores:
        if sector["bullish"] and not sector["bearish"] and sector["item_count"] >= 2:
            for sym in sector["top_picks"]:
                sym_data = load_analysis(sym, max_age_minutes=max_age_minutes)
                if sym_data and sym_data.get("status") == "ok":
                    sym_sent = _safe_float(sym_data.get("weighted_sentiment"), 0.0)
                    if sym_sent > 0.0:
                        picks.append({
                            "symbol": sym,
                            "sector": sector["label"],
                            "sector_sentiment": sector["weighted_sentiment"],
                            "symbol_sentiment": sym_sent,
                            "reason": f"Sector {sector['label']} is bullish (sentiment {sector['weighted_sentiment']:.2f}), {sym} sentiment {sym_sent:.2f}",
                        })
                        break

    result = {
        "generated_at": int(time.time()),
        "sector_ranking": sector_scores,
        "hot_picks": picks[:5],
    }
    (REPORTS_DIR / "sector_rotation_latest.json").write_text(json.dumps(result, indent=2))
    return result
