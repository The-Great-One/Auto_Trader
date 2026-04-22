#!/usr/bin/env python3
from __future__ import annotations

import json
import math
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import yfinance as yf

from Auto_Trader.news_sentiment import classify_text, fetch_rss_entries, _recency_weight, _source_weight

REPORTS_DIR = ROOT / "reports"
OUT_PATH = REPORTS_DIR / "global_macro_latest.json"
IST = timezone(timedelta(hours=5, minutes=30))
UTC = timezone.utc

MARKETS: list[dict[str, Any]] = [
    {"label": "S&P 500", "ticker": "^GSPC", "region": "North America", "country": "US", "city": "New York", "lat": 40.7128, "lon": -74.0060, "kind": "equity"},
    {"label": "Nasdaq", "ticker": "^IXIC", "region": "North America", "country": "US", "city": "New York", "lat": 40.7128, "lon": -74.0060, "kind": "equity"},
    {"label": "VIX", "ticker": "^VIX", "region": "North America", "country": "US", "city": "Chicago", "lat": 41.8781, "lon": -87.6298, "kind": "volatility"},
    {"label": "US 10Y", "ticker": "^TNX", "region": "North America", "country": "US", "city": "Washington", "lat": 38.9072, "lon": -77.0369, "kind": "rates"},
    {"label": "FTSE 100", "ticker": "^FTSE", "region": "Europe", "country": "UK", "city": "London", "lat": 51.5072, "lon": -0.1276, "kind": "equity"},
    {"label": "DAX", "ticker": "^GDAXI", "region": "Europe", "country": "Germany", "city": "Frankfurt", "lat": 50.1109, "lon": 8.6821, "kind": "equity"},
    {"label": "CAC 40", "ticker": "^FCHI", "region": "Europe", "country": "France", "city": "Paris", "lat": 48.8566, "lon": 2.3522, "kind": "equity"},
    {"label": "Nikkei 225", "ticker": "^N225", "region": "Asia", "country": "Japan", "city": "Tokyo", "lat": 35.6762, "lon": 139.6503, "kind": "equity"},
    {"label": "Hang Seng", "ticker": "^HSI", "region": "Asia", "country": "Hong Kong", "city": "Hong Kong", "lat": 22.3193, "lon": 114.1694, "kind": "equity"},
    {"label": "Shanghai Comp", "ticker": "000001.SS", "region": "Asia", "country": "China", "city": "Shanghai", "lat": 31.2304, "lon": 121.4737, "kind": "equity"},
    {"label": "Nifty 50", "ticker": "^NSEI", "region": "Asia", "country": "India", "city": "Mumbai", "lat": 19.0760, "lon": 72.8777, "kind": "equity"},
    {"label": "Sensex", "ticker": "^BSESN", "region": "Asia", "country": "India", "city": "Mumbai", "lat": 19.0760, "lon": 72.8777, "kind": "equity"},
    {"label": "USD/INR", "ticker": "INR=X", "region": "FX", "country": "India", "city": "Mumbai", "lat": 19.0760, "lon": 72.8777, "kind": "fx"},
    {"label": "Brent", "ticker": "BZ=F", "region": "Commodities", "country": "Global", "city": "London", "lat": 51.5072, "lon": -0.1276, "kind": "commodity"},
    {"label": "Gold", "ticker": "GC=F", "region": "Commodities", "country": "Global", "city": "Zurich", "lat": 47.3769, "lon": 8.5417, "kind": "commodity"},
    {"label": "Dollar Index", "ticker": "DX-Y.NYB", "region": "FX", "country": "US", "city": "New York", "lat": 40.7128, "lon": -74.0060, "kind": "fx"},
]

EVENTS: list[dict[str, Any]] = [
    {
        "key": "middle_east",
        "label": "Middle East / oil shock",
        "query": '(Israel OR Iran OR Gaza OR Red Sea OR Houthis OR Strait of Hormuz oil) when:3d',
        "region": "Middle East",
        "lat": 31.7683,
        "lon": 35.2137,
        "sectors": ["energy", "aviation", "metals"],
        "market_impacts": ["oil", "gold", "risk_off"],
        "india_impact": "Higher oil is usually negative for India, but can help ONGC / Oil names.",
    },
    {
        "key": "ukraine",
        "label": "Russia / Ukraine war",
        "query": '(Russia Ukraine war missile attack sanctions NATO) when:3d',
        "region": "Europe",
        "lat": 50.4501,
        "lon": 30.5234,
        "sectors": ["energy", "defence", "metals"],
        "market_impacts": ["gas", "oil", "risk_off"],
        "india_impact": "Risk-off spillover, commodity volatility, possible support for defence and metal exporters.",
    },
    {
        "key": "taiwan_china",
        "label": "China / Taiwan tension",
        "query": '(China Taiwan military drills chip exports semiconductor) when:3d',
        "region": "Asia",
        "lat": 25.0330,
        "lon": 121.5654,
        "sectors": ["semiconductors", "electronics", "it"],
        "market_impacts": ["asia_equities", "tech", "risk_off"],
        "india_impact": "Can hit Asian risk appetite, but may help India as an alternate manufacturing story.",
    },
    {
        "key": "fed",
        "label": "Federal Reserve / US rates",
        "query": '(Federal Reserve rates inflation Powell Treasury yields) when:3d',
        "region": "North America",
        "lat": 38.8977,
        "lon": -77.0365,
        "sectors": ["banks", "it", "rate_sensitive"],
        "market_impacts": ["usd", "yields", "equities"],
        "india_impact": "Hawkish Fed is usually negative for EM flows and can pressure Nifty and INR.",
    },
    {
        "key": "ecb",
        "label": "ECB / Europe growth",
        "query": '(ECB rates Europe recession inflation eurozone) when:3d',
        "region": "Europe",
        "lat": 50.1109,
        "lon": 8.6821,
        "sectors": ["banks", "industrials", "exporters"],
        "market_impacts": ["europe_equities", "eurusd", "risk"],
        "india_impact": "Mostly second-order, but weak Europe can hurt global risk appetite and exporters.",
    },
    {
        "key": "opec",
        "label": "OPEC / crude supply",
        "query": '(OPEC oil production cuts crude supply Brent) when:3d',
        "region": "Middle East",
        "lat": 24.4539,
        "lon": 54.3773,
        "sectors": ["energy", "aviation", "paint", "chemicals"],
        "market_impacts": ["oil", "inflation", "currencies"],
        "india_impact": "Higher crude is negative for India and oil-consuming sectors, positive for upstream producers.",
    },
    {
        "key": "tariffs_trade",
        "label": "Tariffs / trade shock",
        "query": '(tariffs trade war export restrictions sanctions supply chain) when:3d',
        "region": "Global",
        "lat": 1.3521,
        "lon": 103.8198,
        "sectors": ["metals", "industrials", "it"],
        "market_impacts": ["global_trade", "em_equities", "usd"],
        "india_impact": "Hurts cyclicals and export visibility, but can also redirect supply chains toward India.",
    },
]

SEVERITY_HINTS = {
    "war": 25,
    "missile": 22,
    "attack": 20,
    "strike": 16,
    "sanctions": 16,
    "emergency": 16,
    "crisis": 14,
    "ceasefire": -8,
    "pause": -6,
    "cuts": 8,
    "inflation": 8,
    "tariff": 12,
    "drill": 10,
    "default": 18,
    "surge": 8,
}


def google_topic_feed(query: str) -> str:
    return f"https://news.google.com/rss/search?q={quote(query)}&hl=en-IN&gl=IN&ceid=IN:en"


def safe_float(v: Any) -> float | None:
    try:
        out = float(v)
        if math.isnan(out) or math.isinf(out):
            return None
        return out
    except Exception:
        return None


def fetch_market_snapshot() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for spec in MARKETS:
        ticker = spec["ticker"]
        try:
            fi = yf.Ticker(ticker).fast_info
            last = safe_float(fi.get("lastPrice") or fi.get("regularMarketPrice"))
            prev = safe_float(fi.get("previousClose"))
            open_px = safe_float(fi.get("open"))
            day_high = safe_float(fi.get("dayHigh"))
            day_low = safe_float(fi.get("dayLow"))
            change_pct = ((last / prev) - 1.0) * 100.0 if last is not None and prev not in (None, 0) else None
            rows.append(
                {
                    **spec,
                    "last": round(last, 4) if last is not None else None,
                    "previous_close": round(prev, 4) if prev is not None else None,
                    "open": round(open_px, 4) if open_px is not None else None,
                    "day_high": round(day_high, 4) if day_high is not None else None,
                    "day_low": round(day_low, 4) if day_low is not None else None,
                    "change_pct": round(change_pct, 2) if change_pct is not None else None,
                    "fetched_at": datetime.now(UTC).isoformat(),
                    "status": "ok",
                }
            )
        except Exception as exc:
            rows.append({**spec, "status": "error", "error": str(exc), "fetched_at": datetime.now(UTC).isoformat()})
    return rows


def score_severity(text: str, published_at: int | None) -> int:
    hay = (text or "").lower()
    raw = 10
    for token, pts in SEVERITY_HINTS.items():
        if token in hay:
            raw += pts
    age_boost = 0
    if published_at:
        age_hours = max(0.0, (time.time() - published_at) / 3600.0)
        if age_hours <= 6:
            age_boost = 12
        elif age_hours <= 24:
            age_boost = 8
        elif age_hours <= 48:
            age_boost = 4
    return max(5, min(100, raw + age_boost))


def summarize_event(spec: dict[str, Any]) -> dict[str, Any]:
    feed = google_topic_feed(spec["query"])
    fetched = fetch_rss_entries(feed, timeout=20)
    entries = (fetched.get("entries") or [])[:6]
    if not entries:
        return {
            **spec,
            "feed_url": feed,
            "status": "quiet",
            "items": 0,
            "severity": 0,
            "weighted_sentiment": 0.0,
            "headline": None,
            "summary": "No major fresh headlines detected.",
            "top_items": [],
        }

    weighted_sum = 0.0
    total_weight = 0.0
    top_items = []
    max_severity = 0
    for entry in entries:
        text = f"{entry.get('title') or ''} {entry.get('summary') or ''}".strip()
        cls = classify_text(text)
        weight = max(0.35, cls.get("confidence", 0.0)) * _source_weight(entry.get("source")) * _recency_weight(entry.get("published_at"))
        weighted_sum += cls.get("sentiment", 0.0) * weight
        total_weight += weight
        sev = score_severity(text, entry.get("published_at"))
        max_severity = max(max_severity, sev)
        top_items.append(
            {
                "title": entry.get("title"),
                "source": entry.get("source"),
                "published_at": entry.get("published_at"),
                "published_raw": entry.get("published_raw"),
                "link": entry.get("link"),
                "sentiment": cls.get("sentiment"),
                "confidence": cls.get("confidence"),
                "types": cls.get("types"),
                "severity": sev,
            }
        )
    top_items.sort(key=lambda x: (x.get("severity") or 0, abs(x.get("sentiment") or 0.0)), reverse=True)
    weighted_sentiment = weighted_sum / total_weight if total_weight else 0.0
    headline = top_items[0].get("title") if top_items else None
    summary = headline or "Event cluster detected"
    status = "live" if max_severity >= 35 else "watch"
    return {
        **spec,
        "feed_url": feed,
        "status": status,
        "items": len(entries),
        "severity": max_severity,
        "weighted_sentiment": round(max(-1.0, min(1.0, weighted_sentiment)), 4),
        "headline": headline,
        "summary": summary,
        "top_items": top_items[:4],
        "updated_at": datetime.now(UTC).isoformat(),
    }


def derive_macro_drivers(markets: list[dict[str, Any]], events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    drivers: list[dict[str, Any]] = []
    by_label = {m["label"]: m for m in markets if m.get("status") == "ok"}
    for label in ["VIX", "Brent", "Gold", "Dollar Index", "USD/INR", "S&P 500", "Nasdaq", "Nifty 50"]:
        m = by_label.get(label)
        if not m or m.get("change_pct") is None:
            continue
        ch = float(m["change_pct"])
        if abs(ch) < 0.35 and label not in {"VIX", "USD/INR", "Brent"}:
            continue
        direction = "up" if ch > 0 else "down"
        impact = "risk_off" if label in {"VIX", "Gold", "Dollar Index", "USD/INR", "Brent"} and ch > 0 else "risk_on"
        if label in {"S&P 500", "Nasdaq", "Nifty 50"} and ch < 0:
            impact = "risk_off"
        drivers.append({
            "type": "market",
            "label": label,
            "headline": f"{label} {direction} {ch:+.2f}%",
            "strength": round(abs(ch) * (1.6 if label == "VIX" else 1.0), 2),
            "impact": impact,
        })
    for e in events:
        if e.get("severity", 0) < 25:
            continue
        drivers.append({
            "type": "event",
            "label": e["label"],
            "headline": e.get("headline") or e.get("summary"),
            "strength": round(float(e.get("severity", 0)) / 10.0, 2),
            "impact": "risk_off" if e.get("weighted_sentiment", 0.0) <= 0 else "mixed",
        })
    drivers.sort(key=lambda x: x.get("strength", 0), reverse=True)
    return drivers[:8]


def region_summary(markets: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    by_region: dict[str, list[float]] = {}
    for m in markets:
        if m.get("status") != "ok" or m.get("change_pct") is None:
            continue
        by_region.setdefault(m["region"], []).append(float(m["change_pct"]))
    for region, vals in by_region.items():
        rows.append({"region": region, "avg_change_pct": round(sum(vals) / len(vals), 2), "count": len(vals)})
    rows.sort(key=lambda x: x["avg_change_pct"])
    return rows


def main() -> int:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    markets = fetch_market_snapshot()
    events = [summarize_event(spec) for spec in EVENTS]
    events.sort(key=lambda x: x.get("severity", 0), reverse=True)
    payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "markets": markets,
        "events": events,
        "drivers": derive_macro_drivers(markets, events),
        "region_summary": region_summary(markets),
    }
    OUT_PATH.write_text(json.dumps(payload, indent=2, ensure_ascii=False))
    print(str(OUT_PATH))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
