#!/usr/bin/env python3
"""Fetch upcoming economic events, earnings, and sector-level index moves.

Data sources:
  - Economic calendar: investing.com RSS + Yahoo Finance earnings calendar
  - Sector heatmap: NSE sector indices via yfinance
  - Earnings: yfinance calendar for tracked universe symbols
"""
from __future__ import annotations

import json
import math
import re
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import yfinance as yf

from Auto_Trader.news_sentiment import fetch_rss_entries, classify_text

REPORTS_DIR = ROOT / "reports"
OUT_PATH = REPORTS_DIR / "economic_calendar_sector_latest.json"
IST = timezone(timedelta(hours=5, minutes=30))
UTC = timezone.utc

# ── Economic events ──────────────────────────────────────────────
ECO_FEEDS = [
    {
        "source": "investing_economic",
        "url": "https://www.investing.com/rss/news_301.rss",
        "label": "Global Economic Events",
        "region": "Global",
    },
    {
        "source": "investing_india",
        "url": "https://www.investing.com/rss/news_95.rss",
        "label": "India Economic Events",
        "region": "India",
    },
    {
        "source": "investing_us",
        "url": "https://www.investing.com/rss/news_11.rss",
        "label": "US Economic Events",
        "region": "US",
    },
]

ECO_KEYWORDS = {
    "high": ["fed ", "fomc", "rbi", "cpi", "inflation", "gdp", "nonfarm", "non-farm", "jobs report", "unemployment", "interest rate", "repo rate", "rate decision", "powell", "monetary policy", "budget", "opec"],
    "medium": ["pmi", "manufacturing", "services pmi", "industrial production", "retail sales", "trade balance", "current account", "fiscal deficit", "icaan", "wpi", "ipi"],
    "low": ["consumer confidence", "business confidence", "housing starts", "building permits"],
}

# ── NSE Sector indices for heatmap ──────────────────────────────
SECTOR_INDICES = [
    {"label": "Nifty IT", "ticker": "^CNXIT", "sector": "IT"},
    {"label": "Nifty Pharma", "ticker": "^CNXPHARMA", "sector": "Pharma"},
    {"label": "Nifty Auto", "ticker": "^CNXAUTO", "sector": "Auto"},
    {"label": "Nifty FMCG", "ticker": "^CNXFMCG", "sector": "FMCG"},
    {"label": "Nifty Metal", "ticker": "^CNXMETAL", "sector": "Metals"},
    {"label": "Nifty Energy", "ticker": "^CNXENERGY", "sector": "Energy"},
    {"label": "Nifty Realty", "ticker": "^CNXREALTY", "sector": "Realty"},
    {"label": "Nifty Infra", "ticker": "^CNXINFRA", "sector": "Infra"},
    {"label": "Nifty PSE", "ticker": "^CNXPSE", "sector": "PSE"},
    {"label": "Nifty MNC", "ticker": "^CNXMNC", "sector": "MNC"},
    {"label": "Nifty Media", "ticker": "^CNXMEDIA", "sector": "Media"},
    {"label": "Nifty Fin Service", "ticker": "^CNXFIN", "sector": "Financial Services"},
    {"label": "Nifty 50", "ticker": "^NSEI", "sector": "Broad Market"},
    {"label": "Nifty 100", "ticker": "^CNX100", "sector": "Broad Market"},
    {"label": "Nifty 200", "ticker": "^CNX200", "sector": "Broad Market"},
]

# Tracked symbols for earnings lookups
EARNINGS_SYMBOLS = [
    "WIPRO.NS", "INFY.NS", "TCS.NS", "HDFCBANK.NS", "ICICIBANK.NS",
    "SBIN.NS", "ITC.NS", "RELIANCE.NS", "LT.NS", "KOTAKBANK.NS",
    "AXISBANK.NS", "BAJAJ-AUTO.NS", "HEROMOTOCO.NS", "M&M.NS",
    "MARUTI.NS", "NTPC.NS", "COALINDIA.NS", "POWERGRID.NS",
    "ONGC.NS", "ADANIPORTS.NS", "HINDALCO.NS", "CIPLA.NS",
    "DRREDDY.NS", "SUNPHARMA.NS", "BHARTIARTL.NS", "HCLTECH.NS",
    "HAL.NS", "CANBK.NS", "ASHOKLEY.NS",
]


def safe_float(v: Any) -> float | None:
    try:
        out = float(v)
        if math.isnan(out) or math.isinf(out):
            return None
        return out
    except Exception:
        return None


def classify_impact(text: str) -> str:
    low = (text or "").lower()
    for level, keywords in ECO_KEYWORDS.items():
        for kw in keywords:
            if kw in low:
                return level
    return "low"


def fetch_economic_events() -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    seen_titles: set[str] = set()
    for feed in ECO_FEEDS:
        result = fetch_rss_entries(feed["url"], timeout=20)
        entries = result.get("entries") or []
        for entry in entries:
            title = entry.get("title") or ""
            if not title.strip() or title.strip() in seen_titles:
                continue
            seen_titles.add(title.strip())
            text = f"{title} {entry.get('summary') or ''}"
            cls = classify_text(text)
            impact = classify_impact(text)
            published_at = entry.get("published_at")
            events.append({
                "title": title.strip(),
                "source": entry.get("source", ""),
                "link": entry.get("link", ""),
                "published_at": published_at,
                "published_raw": entry.get("published_raw", ""),
                "impact": impact,
                "sentiment": cls.get("sentiment", 0.0),
                "types": cls.get("types", []),
                "region": feed["region"],
            })
    events.sort(key=lambda x: {"high": 0, "medium": 1, "low": 2}.get(x.get("impact", "low"), 2))
    return events[:50]


def normalize_earnings_date(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (list, tuple)):
        for item in value:
            normalized = normalize_earnings_date(item)
            if normalized:
                return normalized
        return None
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    if hasattr(value, "to_pydatetime"):
        try:
            return value.to_pydatetime().date().strftime("%Y-%m-%d")
        except Exception:
            pass
    if hasattr(value, "tolist") and not isinstance(value, (str, bytes, dict)):
        try:
            return normalize_earnings_date(value.tolist())
        except Exception:
            pass
    text = str(value).strip()
    if not text:
        return None
    match = re.search(r"(\d{4}-\d{2}-\d{2})", text)
    if match:
        return match.group(1)
    return text


def fetch_earnings_calendar() -> list[dict[str, Any]]:
    earnings: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for sym in EARNINGS_SYMBOLS:
        try:
            tk = yf.Ticker(sym)
            cal = tk.calendar
            if cal is None or (isinstance(cal, dict) and not cal):
                continue
            if isinstance(cal, dict):
                earnings_date = normalize_earnings_date(cal.get("Earnings Date"))
                if earnings_date:
                    clean_sym = sym.replace(".NS", "").replace(".BO", "")
                    key = (clean_sym, earnings_date)
                    if key not in seen:
                        seen.add(key)
                        earnings.append({
                            "symbol": clean_sym,
                            "earnings_date": earnings_date,
                            "type": "earnings",
                        })
        except Exception:
            continue
    earnings.sort(key=lambda row: (row.get("earnings_date") or "9999-99-99", row.get("symbol") or ""))
    return earnings[:30]


def fetch_sector_heatmap() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for spec in SECTOR_INDICES:
        ticker = spec["ticker"]
        try:
            fi = yf.Ticker(ticker).fast_info
            last = safe_float(fi.get("lastPrice") or fi.get("regularMarketPrice"))
            prev = safe_float(fi.get("previousClose"))
            change_pct = ((last / prev) - 1.0) * 100.0 if last is not None and prev not in (None, 0) else None
            rows.append({
                **spec,
                "last": round(last, 2) if last is not None else None,
                "change_pct": round(change_pct, 2) if change_pct is not None else None,
                "status": "ok",
                "fetched_at": datetime.now(UTC).isoformat(),
            })
        except Exception as exc:
            rows.append({**spec, "status": "error", "error": str(exc)})
    return rows


def main() -> int:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    eco_events = fetch_economic_events()
    earnings = fetch_earnings_calendar()
    sectors = fetch_sector_heatmap()
    payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "economic_events": eco_events,
        "earnings": earnings,
        "sectors": sectors,
    }
    OUT_PATH.write_text(json.dumps(payload, indent=2, ensure_ascii=False))
    print(str(OUT_PATH))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())