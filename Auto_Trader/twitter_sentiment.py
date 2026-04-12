from __future__ import annotations

import json
import logging
import math
import os
import re
import time
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import quote_plus

import pandas as pd
import requests

logger = logging.getLogger("Auto_Trade_Logger")

ROOT = Path(__file__).resolve().parents[1]
STATE_DIR = ROOT / "intermediary_files" / "twitter_sentiment"
REPORTS_DIR = ROOT / "reports"
SUMMARY_PATH = STATE_DIR / "latest.json"

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
        "recession",
        "global risk",
    ),
    "regulatory": (
        "sebi",
        "investigation",
        "probe",
        "raud",
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
    raw = os.getenv("AT_TWITTER_SYMBOL_ALIASES_JSON", "").strip()
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except Exception as exc:
        logger.warning("Failed to parse AT_TWITTER_SYMBOL_ALIASES_JSON: %s", exc)
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


def build_search_query(symbol: str, asset_class: Optional[str] = None, etf_theme: str = "") -> str:
    terms = _symbol_query_terms(symbol, asset_class=asset_class, etf_theme=etf_theme)
    joined = " OR ".join(f'"{term}"' if " " in term else term for term in terms)
    lang = os.getenv("AT_TWITTER_LANG", "en").strip() or "en"
    return f"({joined}) -is:retweet lang:{lang}"


def discover_symbols(limit: int = 30) -> List[str]:
    symbols: List[str] = []
    seen = set()

    extra = os.getenv("AT_TWITTER_EXTRA_SYMBOLS", "")
    for raw in extra.split(","):
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
            logger.warning("Failed to inspect Holdings.feather for Twitter symbols: %s", exc)

    instruments_path = ROOT / "intermediary_files" / "Instruments.feather"
    if instruments_path.exists() and len(symbols) < limit:
        try:
            instruments = pd.read_feather(instruments_path)
            col = instruments.get("tradingsymbol", pd.Series(dtype=str)).astype(str)
            for sym in col.head(limit * 4):
                sym_n = _normalize_symbol(sym)
                if sym_n and sym_n not in seen:
                    symbols.append(sym_n)
                    seen.add(sym_n)
                if len(symbols) >= limit:
                    break
        except Exception as exc:
            logger.warning("Failed to inspect Instruments.feather for Twitter symbols: %s", exc)

    return symbols[:limit]


def _extract_metrics(tweet: dict) -> Tuple[int, int, int, int]:
    metrics = tweet.get("public_metrics") or {}
    likes = _safe_int(metrics.get("like_count"), 0)
    retweets = _safe_int(metrics.get("retweet_count"), 0)
    replies = _safe_int(metrics.get("reply_count"), 0)
    quotes = _safe_int(metrics.get("quote_count"), 0)
    return likes, retweets, replies, quotes


def _engagement_weight(tweet: dict) -> float:
    likes, retweets, replies, quotes = _extract_metrics(tweet)
    score = likes + 2.5 * retweets + 1.5 * quotes + 0.5 * replies
    return 1.0 + min(2.0, math.log1p(max(0.0, score)) / 4.0)


def classify_tweet(text: str) -> dict:
    text = re.sub(r"\s+", " ", str(text or "").strip().lower())
    matched: Dict[str, List[str]] = {}
    score = 0.0

    for label, patterns in TYPE_PATTERNS.items():
        hits = [pat for pat in patterns if pat in text]
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


def analyze_tweets(symbol: str, tweets: Sequence[dict]) -> dict:
    if not tweets:
        return {
            "symbol": _normalize_symbol(symbol),
            "tweet_count": 0,
            "weighted_sentiment": 0.0,
            "type_counts": {},
            "dominant_types": [],
            "sample_texts": [],
            "generated_at": int(time.time()),
            "status": "no_tweets",
        }

    type_counts: Dict[str, int] = {}
    scored = []
    weighted_sum = 0.0
    total_weight = 0.0
    bullish = 0
    bearish = 0

    for tweet in tweets:
        text = str(tweet.get("text") or "")
        cls = classify_tweet(text)
        weight = _engagement_weight(tweet) * max(0.3, cls["confidence"])
        signed = cls["sentiment"] * weight
        weighted_sum += signed
        total_weight += weight
        for label in cls["types"]:
            type_counts[label] = type_counts.get(label, 0) + 1
        if any(t in NEGATIVE_TYPES for t in cls["types"]):
            bearish += 1
        if any(t in POSITIVE_TYPES for t in cls["types"]):
            bullish += 1
        scored.append(
            {
                "id": tweet.get("id"),
                "created_at": tweet.get("created_at"),
                "text": text,
                "classification": cls,
                "engagement_weight": round(weight, 4),
            }
        )

    scored.sort(key=lambda x: abs(x["classification"]["sentiment"]) * x["engagement_weight"], reverse=True)
    dominant_types = [
        label
        for label, _ in sorted(type_counts.items(), key=lambda kv: (-kv[1], kv[0]))[:4]
    ]
    weighted_sentiment = weighted_sum / total_weight if total_weight else 0.0
    summary = {
        "symbol": _normalize_symbol(symbol),
        "tweet_count": len(tweets),
        "bullish_tweets": bullish,
        "bearish_tweets": bearish,
        "weighted_sentiment": round(max(-1.0, min(1.0, weighted_sentiment)), 4),
        "type_counts": type_counts,
        "dominant_types": dominant_types,
        "sample_texts": [s["text"][:220] for s in scored[:5]],
        "top_tweets": scored[:5],
        "generated_at": int(time.time()),
        "status": "ok",
    }
    summary["trade_bias"] = infer_trade_bias(summary)
    return summary


def infer_trade_bias(analysis: dict) -> dict:
    sentiment = _safe_float(analysis.get("weighted_sentiment"), 0.0)
    tweet_count = _safe_int(analysis.get("tweet_count"), 0)
    bearish = _safe_int(analysis.get("bearish_tweets"), 0)
    bullish = _safe_int(analysis.get("bullish_tweets"), 0)
    dominant = set(analysis.get("dominant_types") or [])

    min_tweets = max(3, _safe_int(os.getenv("AT_TWITTER_MIN_TWEETS", "5"), 5))
    buy_block_threshold = _safe_float(os.getenv("AT_TWITTER_BUY_BLOCK_THRESHOLD", "-0.22"), -0.22)
    sell_force_threshold = _safe_float(os.getenv("AT_TWITTER_SELL_FORCE_THRESHOLD", "-0.40"), -0.40)
    positive_boost_threshold = _safe_float(os.getenv("AT_TWITTER_POSITIVE_BOOST_THRESHOLD", "0.28"), 0.28)

    block_buy = (
        tweet_count >= min_tweets
        and sentiment <= buy_block_threshold
        and bearish >= max(2, bullish)
        and bool(dominant & NEGATIVE_TYPES)
    )
    force_sell = (
        tweet_count >= min_tweets
        and sentiment <= sell_force_threshold
        and bearish >= max(3, bullish + 1)
        and bool(dominant & (NEGATIVE_TYPES - {"rumor"}))
    )
    positive_boost = (
        tweet_count >= min_tweets
        and sentiment >= positive_boost_threshold
        and bullish >= bearish + 2
        and bool(dominant & POSITIVE_TYPES)
    )

    reasons = []
    if block_buy:
        reasons.append(f"negative twitter flow {sentiment:.2f}")
    if force_sell:
        reasons.append(f"credible bearish twitter flow {sentiment:.2f}")
    if positive_boost:
        reasons.append(f"supportive twitter flow {sentiment:.2f}")

    return {
        "block_buy": block_buy,
        "force_sell": force_sell,
        "positive_boost": positive_boost,
        "reason": "; ".join(reasons) if reasons else "neutral",
    }


def fetch_recent_tweets(symbol: str, *, max_results: int = 25, hours_back: int = 6, asset_class: Optional[str] = None, etf_theme: str = "") -> dict:
    bearer = os.getenv("AT_TWITTER_BEARER_TOKEN", "").strip()
    if not bearer:
        return {"symbol": _normalize_symbol(symbol), "status": "disabled", "error": "AT_TWITTER_BEARER_TOKEN missing", "tweets": []}

    query = build_search_query(symbol, asset_class=asset_class, etf_theme=etf_theme)
    url = "https://api.twitter.com/2/tweets/search/recent"
    params = {
        "query": query,
        "max_results": max(10, min(100, int(max_results))),
        "tweet.fields": "created_at,lang,public_metrics",
    }
    if hours_back > 0:
        since_seconds = int(time.time()) - hours_back * 3600
        params["start_time"] = pd.Timestamp.utcfromtimestamp(since_seconds).isoformat() + "Z"

    try:
        response = requests.get(
            url,
            headers={"Authorization": f"Bearer {bearer}"},
            params=params,
            timeout=20,
        )
        response.raise_for_status()
        payload = response.json()
        tweets = payload.get("data") or []
        return {
            "symbol": _normalize_symbol(symbol),
            "status": "ok",
            "query": query,
            "tweets": tweets,
            "meta": payload.get("meta") or {},
        }
    except Exception as exc:
        logger.warning("Twitter fetch failed for %s: %s", symbol, exc)
        return {
            "symbol": _normalize_symbol(symbol),
            "status": "error",
            "query": query,
            "error": str(exc),
            "tweets": [],
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
        logger.warning("Failed to load Twitter sentiment snapshot for %s: %s", symbol, exc)
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
    summary = {
        "generated_at": int(time.time()),
        "symbols": len(analyses),
        "active": [a for a in analyses if a.get("status") == "ok"],
        "top_bullish": sorted(
            [a for a in analyses if a.get("status") == "ok"],
            key=lambda x: x.get("weighted_sentiment", 0.0),
            reverse=True,
        )[:5],
        "top_bearish": sorted(
            [a for a in analyses if a.get("status") == "ok"],
            key=lambda x: x.get("weighted_sentiment", 0.0),
        )[:5],
    }
    SUMMARY_PATH.write_text(json.dumps(summary, indent=2))
    (REPORTS_DIR / "twitter_sentiment_latest.json").write_text(json.dumps(summary, indent=2))
    return summary


def symbol_is_held(symbol: str, holdings: Optional[pd.DataFrame]) -> bool:
    if holdings is None or holdings.empty or "tradingsymbol" not in holdings.columns:
        return False
    tradingsymbols = holdings["tradingsymbol"].astype(str).str.upper()
    return _normalize_symbol(symbol) in set(tradingsymbols.tolist())


def apply_sentiment_overlay(base_decision: str, symbol: str, holdings: Optional[pd.DataFrame] = None) -> Tuple[str, Optional[dict]]:
    if not _env_flag("AT_TWITTER_SENTIMENT_ENABLED", False):
        return str(base_decision).upper(), None

    ttl_minutes = max(1, _safe_int(os.getenv("AT_TWITTER_SENTIMENT_TTL_MINUTES", "90"), 90))
    analysis = load_analysis(symbol, max_age_minutes=ttl_minutes)
    if not analysis or analysis.get("status") != "ok":
        return str(base_decision).upper(), None

    bias = analysis.get("trade_bias") or infer_trade_bias(analysis)
    decision = str(base_decision).upper()
    held = symbol_is_held(symbol, holdings)

    if decision == "BUY" and bias.get("block_buy"):
        return "HOLD", {
            "source": "TWITTER_SENTIMENT",
            "action": "blocked_buy",
            "reason": bias.get("reason"),
            "weighted_sentiment": analysis.get("weighted_sentiment"),
            "dominant_types": analysis.get("dominant_types"),
        }

    if decision == "HOLD" and held and bias.get("force_sell"):
        return "SELL", {
            "source": "TWITTER_SENTIMENT",
            "action": "forced_sell",
            "reason": bias.get("reason"),
            "weighted_sentiment": analysis.get("weighted_sentiment"),
            "dominant_types": analysis.get("dominant_types"),
        }

    if decision == "BUY" and bias.get("positive_boost"):
        return decision, {
            "source": "TWITTER_SENTIMENT",
            "action": "confirmed_buy",
            "reason": bias.get("reason"),
            "weighted_sentiment": analysis.get("weighted_sentiment"),
            "dominant_types": analysis.get("dominant_types"),
        }

    return decision, None


def fetch_and_analyze_symbol(symbol: str, *, max_results: int = 25, hours_back: int = 6, asset_class: Optional[str] = None, etf_theme: str = "") -> dict:
    fetched = fetch_recent_tweets(
        symbol,
        max_results=max_results,
        hours_back=hours_back,
        asset_class=asset_class,
        etf_theme=etf_theme,
    )
    if fetched.get("status") != "ok":
        analysis = {
            "symbol": _normalize_symbol(symbol),
            "tweet_count": 0,
            "weighted_sentiment": 0.0,
            "type_counts": {},
            "dominant_types": [],
            "sample_texts": [],
            "generated_at": int(time.time()),
            "status": fetched.get("status"),
            "error": fetched.get("error"),
            "query": fetched.get("query"),
        }
        save_analysis(analysis)
        return analysis

    analysis = analyze_tweets(symbol, fetched.get("tweets") or [])
    analysis["query"] = fetched.get("query")
    save_analysis(analysis)
    return analysis
