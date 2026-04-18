from __future__ import annotations

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
from urllib.parse import urlparse

import pandas as pd
import requests

from .twitter_sentiment import (
    POSITIVE_TYPES,
    NEGATIVE_TYPES,
    _env_flag,
    _normalize_symbol,
    _safe_float,
    _safe_int,
    _symbol_query_terms,
    classify_tweet,
    discover_symbols,
    symbol_is_held,
)

logger = logging.getLogger("Auto_Trade_Logger")

ROOT = Path(__file__).resolve().parents[1]
STATE_DIR = ROOT / "intermediary_files" / "news_sentiment"
REPORTS_DIR = ROOT / "reports"
SUMMARY_PATH = STATE_DIR / "latest.json"

DEFAULT_RSS_FEEDS = (
    "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms",
    "https://www.moneycontrol.com/rss/business.xml",
    "https://www.livemint.com/rss/markets",
    "https://feeds.feedburner.com/ndtvprofit-latest",
)

SOURCE_WEIGHTS = {
    "reuters.com": 1.25,
    "economictimes.indiatimes.com": 1.15,
    "cnbctv18.com": 1.10,
    "ndtvprofit.com": 1.05,
    "moneycontrol.com": 1.05,
    "livemint.com": 1.05,
    "news.google.com": 0.95,
}

REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; AutoTraderNewsSentiment/1.0; +https://github.com/The-Great-One/Auto_Trader)",
    "Accept": "application/rss+xml, application/xml, text/xml;q=0.9, */*;q=0.8",
}


def _configured_feeds() -> List[str]:
    raw = os.getenv("AT_NEWS_RSS_FEEDS", "").strip()
    feeds = []
    if raw:
        for part in re.split(r"[\n,]+", raw):
            url = str(part or "").strip()
            if url and url not in feeds:
                feeds.append(url)
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
    return f"https://news.google.com/rss/search?q={requests.utils.quote(query)}&hl=en-IN&gl=IN&ceid=IN:en"


def _strip_html(text: str) -> str:
    clean = re.sub(r"<[^>]+>", " ", str(text or ""))
    clean = html.unescape(clean)
    return re.sub(r"\s+", " ", clean).strip()


def _source_name(url: str) -> str:
    host = (urlparse(url).netloc or "").lower()
    host = host.replace("www.", "")
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


def _symbol_match(text: str, symbol: str, *, asset_class: Optional[str] = None, etf_theme: str = "") -> bool:
    haystack = str(text or "")
    if not haystack.strip():
        return False

    for term in _symbol_query_terms(symbol, asset_class=asset_class, etf_theme=etf_theme):
        token = str(term or "").strip()
        if not token:
            continue
        if re.search(rf"(?<![A-Za-z0-9]){re.escape(token)}(?![A-Za-z0-9])", haystack, flags=re.IGNORECASE):
            return True
    return False


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


def analyze_news(symbol: str, entries: Sequence[dict]) -> dict:
    if not entries:
        return {
            "symbol": _normalize_symbol(symbol),
            "item_count": 0,
            "weighted_sentiment": 0.0,
            "type_counts": {},
            "dominant_types": [],
            "sample_headlines": [],
            "generated_at": int(time.time()),
            "status": "no_news",
        }

    type_counts: Dict[str, int] = {}
    scored = []
    weighted_sum = 0.0
    total_weight = 0.0
    bullish = 0
    bearish = 0

    for entry in entries:
        text = str(entry.get("text") or "")
        cls = classify_tweet(text)
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
    dominant_types = [
        label for label, _ in sorted(type_counts.items(), key=lambda kv: (-kv[1], kv[0]))[:4]
    ]
    weighted_sentiment = weighted_sum / total_weight if total_weight else 0.0
    summary = {
        "symbol": _normalize_symbol(symbol),
        "item_count": len(entries),
        "bullish_items": bullish,
        "bearish_items": bearish,
        "weighted_sentiment": round(max(-1.0, min(1.0, weighted_sentiment)), 4),
        "type_counts": type_counts,
        "dominant_types": dominant_types,
        "sample_headlines": [s.get("title") for s in scored[:5] if s.get("title")],
        "top_items": scored[:5],
        "generated_at": int(time.time()),
        "status": "ok",
    }
    summary["trade_bias"] = infer_news_trade_bias(summary)
    return summary


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
        if generated_at <= 0:
            return None
        if time.time() - generated_at > max_age_minutes * 60:
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
        feed_status.append({
            "feed_url": feed_url,
            "source": fetched.get("source"),
            "status": fetched.get("status"),
            "error": fetched.get("error"),
            "entry_count": len(fetched.get("entries") or []),
        })
        if fetched.get("status") != "ok":
            continue
        count = 0
        for entry in fetched.get("entries") or []:
            if _symbol_match(entry.get("text"), symbol, asset_class=asset_class, etf_theme=etf_theme):
                matches.append(entry)
                count += 1
                if count >= per_feed_limit:
                    break

    analysis = analyze_news(symbol, matches)
    analysis["feed_status"] = feed_status
    save_analysis(analysis)
    return analysis


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
