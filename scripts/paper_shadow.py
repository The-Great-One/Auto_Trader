#!/usr/bin/env python3
"""Paper shadow mode: compute hypothetical actions, place no orders."""

from __future__ import annotations

import json
import logging
import tempfile
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from Auto_Trader import RULE_SET_2, RULE_SET_7, RULE_SET_OPTIONS_1
from Auto_Trader import options_support as opt_support
from Auto_Trader import utils as at_utils
from Auto_Trader.news_sentiment import (
    apply_news_overlay,
    fetch_and_analyze_symbol,
    fetch_and_analyze_topics,
    latest_topic_snapshot,
    load_analysis,
)

OUT = ROOT / "reports"
OUT.mkdir(exist_ok=True)
HIST_DIR = ROOT / "intermediary_files" / "Hist_Data"
OPTIONS_OUT = OUT / "paper_shadow_options_latest.json"
logger = logging.getLogger("Auto_Trade_Logger")

KITE_FALLBACK_TOKENS = {
    "NIFTYETF": 1626369,
}


def _normalize_hist_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    use = df.copy()
    if hasattr(use.columns, "levels"):
        use.columns = [str(c[0]) for c in use.columns]
    if "Date" not in use.columns:
        use = use.reset_index()

    cols = {str(c).lower(): c for c in use.columns}
    if "date" not in cols and "datetime" in cols:
        cols["date"] = cols["datetime"]
    required = ["date", "open", "high", "low", "close"]
    if not all(k in cols for k in required):
        return pd.DataFrame()

    out = pd.DataFrame(
        {
            "Date": pd.to_datetime(use[cols["date"]], errors="coerce"),
            "Open": pd.to_numeric(use[cols["open"]], errors="coerce"),
            "High": pd.to_numeric(use[cols["high"]], errors="coerce"),
            "Low": pd.to_numeric(use[cols["low"]], errors="coerce"),
            "Close": pd.to_numeric(use[cols["close"]], errors="coerce"),
            "Volume": pd.to_numeric(use.get(cols.get("volume", "Volume"), 0), errors="coerce").fillna(0),
        }
    ).dropna(subset=["Date", "Open", "High", "Low", "Close"])
    if out.empty:
        return out
    return out.sort_values("Date").drop_duplicates(subset=["Date"], keep="last").reset_index(drop=True)



def _persist_hist_cache(cache_name: str, df: pd.DataFrame) -> Path:
    HIST_DIR.mkdir(parents=True, exist_ok=True)
    path = HIST_DIR / f"{cache_name}.feather"
    df.to_feather(path)
    return path



def _prepare_hist_df(df: pd.DataFrame) -> pd.DataFrame:
    use = _normalize_hist_df(df)
    if use.empty or len(use) < 5:
        return pd.DataFrame()
    try:
        out = at_utils.Indicators(use)
        out = out.ffill().dropna(subset=["Close"]).reset_index(drop=True)
        return out if not out.empty else pd.DataFrame()
    except Exception:
        return pd.DataFrame()



def _fetch_hist_from_yfinance(symbol: str):
    import yfinance as yf

    candidates = [
        (symbol, f"{symbol}.NS"),
        ("NIFTYBEES", "NIFTYBEES.NS"),
        ("NIFTY50_INDEX", "^NSEI"),
    ]
    for cache_name, ticker in candidates:
        try:
            raw = yf.download(ticker, period="2y", interval="1d", auto_adjust=False, progress=False)
            normalized = _normalize_hist_df(raw)
            if normalized.empty:
                continue
            path = _persist_hist_cache(cache_name, normalized)
            prepared = _prepare_hist_df(normalized)
            if not prepared.empty:
                return prepared, {"source": "yfinance", "ticker": ticker, "cache_name": cache_name, "cache_path": str(path)}
        except Exception as exc:
            logger.warning("paper shadow: yfinance fetch failed for %s: %s", ticker, exc)
    return pd.DataFrame(), None



def _fetch_hist_from_kite(symbol: str):
    token = KITE_FALLBACK_TOKENS.get(symbol)
    if not token:
        return pd.DataFrame(), None

    try:
        kite = at_utils.initialize_kite()
        end_dt = datetime.now()
        start_dt = end_dt - timedelta(days=365 * 5)
        data = kite.historical_data(token, from_date=start_dt, to_date=end_dt, interval="day", oi=False)
        normalized = _normalize_hist_df(pd.DataFrame(data))
        if normalized.empty:
            return pd.DataFrame(), None
        path = _persist_hist_cache(symbol, normalized)
        prepared = _prepare_hist_df(normalized)
        if prepared.empty:
            return pd.DataFrame(), None
        return prepared, {"source": "kite", "instrument_token": token, "cache_name": symbol, "cache_path": str(path)}
    except Exception as exc:
        logger.warning("paper shadow: Kite fallback failed for %s: %s", symbol, exc)
        return pd.DataFrame(), None



def load_hist(symbol="NIFTYETF"):
    cache_paths = [
        (symbol, HIST_DIR / f"{symbol}.feather"),
        ("NIFTYBEES", HIST_DIR / "NIFTYBEES.feather"),
        ("NIFTY50_INDEX", HIST_DIR / "NIFTY50_INDEX.feather"),
    ]

    for cache_name, path in cache_paths:
        if not path.exists():
            continue
        try:
            out = _prepare_hist_df(pd.read_feather(path))
            if not out.empty:
                return out, {"source": "cache", "cache_name": cache_name, "cache_path": str(path)}
        except Exception as exc:
            logger.warning("paper shadow: failed reading cache %s: %s", path, exc)

    out, meta = _fetch_hist_from_yfinance(symbol)
    if not out.empty:
        return out, meta

    out, meta = _fetch_hist_from_kite(symbol)
    if not out.empty:
        return out, meta

    raise SystemExit("No usable historical data found for shadow mode, cache, yfinance, and Kite fallback all failed")


def load_qty(symbol="NIFTYETF") -> int:
    h = ROOT / "intermediary_files" / "Holdings.feather"
    if not h.exists():
        return 0
    df = pd.read_feather(h)
    df = df[df["tradingsymbol"].astype(str).str.upper() == symbol.upper()]
    if df.empty:
        return 0
    return int(float(df.iloc[0].get("quantity", 0) + df.iloc[0].get("t1_quantity", 0)))


def run_equity_shadow() -> dict:
    at_utils.get_mmi_now = lambda: None
    symbol = "NIFTYETF"
    topic_summary = {"topics": []}
    try:
        fetch_and_analyze_symbol(symbol, asset_class="ETF", etf_theme="NIFTY 50")
        topic_summary = fetch_and_analyze_topics(["trump_market"])
    except Exception as exc:
        logger.warning("equity shadow: news refresh failed: %s", exc)
        topic_summary = latest_topic_snapshot(["trump_market"])

    try:
        df, price_history_meta = load_hist(symbol)
    except (SystemExit, Exception) as e:
        logger.error("equity shadow: load_hist failed: %s", e)
        return {"error": str(e), "decision": "HOLD", "mode": "failed_rc_1", "market_news_topics": topic_summary.get("topics", [])}
    if df is None or df.empty:
        logger.error("equity shadow: empty dataframe for %s", symbol)
        return {"error": "empty_dataframe", "decision": "HOLD", "mode": "failed_rc_1", "market_news_topics": topic_summary.get("topics", [])}
    row = df.iloc[-1].to_dict()
    row.setdefault("instrument_token", 1626369)

    entry_holdings = pd.DataFrame(columns=["instrument_token", "tradingsymbol", "average_price", "quantity", "t1_quantity", "bars_in_trade"])
    entry_decision, entry_details = RULE_SET_7.evaluate_signal(df, row, entry_holdings)

    qty = load_qty(symbol)
    if qty > 0:
        avg = float(df["Close"].iloc[-20:-1].mean())
        holdings = pd.DataFrame([
            {
                "instrument_token": 1626369,
                "tradingsymbol": symbol,
                "average_price": avg,
                "quantity": qty,
                "t1_quantity": 0,
                "bars_in_trade": 20,
            }
        ])
        with tempfile.TemporaryDirectory(prefix="paper_shadow_state_") as td:
            RULE_SET_2.BASE_DIR = td
            RULE_SET_2.HOLDINGS_FILE_PATH = str(Path(td) / "Holdings.json")
            RULE_SET_2.LOCK_FILE_PATH = str(Path(td) / "Holdings.lock")
            base_decision = RULE_SET_2.buy_or_sell(df, row, holdings)
        mode = "SELL_RULE_ONLY"
    else:
        holdings = entry_holdings
        base_decision = entry_decision
        mode = "BUY_RULE_ONLY"

    final_decision, news_overlay = apply_news_overlay(base_decision, symbol, holdings=holdings)
    symbol_news = load_analysis(symbol, max_age_minutes=24 * 60) or {}

    payload = {
        "generated_at": datetime.now().isoformat(),
        "paper_mode": True,
        "symbol": symbol,
        "production_rule_model": "BUY=RULE_SET_7, SELL=RULE_SET_2",
        "position_qty": qty,
        "mode": mode,
        "base_decision": str(base_decision).upper(),
        "decision": str(final_decision).upper(),
        "decision_changed_by_news": str(base_decision).upper() != str(final_decision).upper(),
        "news_overlay": news_overlay,
        "last_close": float(df.iloc[-1]["Close"]),
        "price_history": price_history_meta,
        "equity_entry_diagnostics": {
            "decision": str(entry_decision).upper(),
            "entry_gate_failures": entry_details.get("entry_gate_failures", []),
            "hard_blocks": entry_details.get("hard_blocks", []),
            "nearest_mode": entry_details.get("nearest_mode"),
            "nearest_mode_missing": entry_details.get("nearest_mode_missing", []),
            "nearest_mode_missing_count": entry_details.get("nearest_mode_missing_count"),
            "alternate_mode_missing": entry_details.get("alternate_mode_missing", []),
            "reason": entry_details.get("reason", []),
            "gate_status": entry_details.get("gate_status", {}),
            "metric_snapshot": entry_details.get("metric_snapshot", {}),
            "threshold_snapshot": entry_details.get("threshold_snapshot", {}),
            "mode_diagnostics": entry_details.get("mode_diagnostics", {}),
        },
        "symbol_news_sentiment": {
            "weighted_sentiment": symbol_news.get("weighted_sentiment"),
            "item_count": symbol_news.get("item_count"),
            "dominant_types": symbol_news.get("dominant_types"),
            "sample_headlines": symbol_news.get("sample_headlines", [])[:3],
        },
        "market_news_topics": topic_summary.get("topics", []),
    }
    (OUT / "paper_shadow_latest.json").write_text(json.dumps(payload, indent=2))
    return payload



def run_options_shadow() -> dict:
    symbols = opt_support.discover_option_symbols()
    candidates = []
    skipped = {}

    for symbol in symbols:
        path = ROOT / "intermediary_files" / "Hist_Data" / f"{symbol}.feather"
        if not path.exists():
            skipped[symbol] = "missing_file"
            continue
        try:
            df = opt_support.enrich_option_frame(pd.read_feather(path))
        except Exception as exc:
            skipped[symbol] = f"enrich_failed:{exc}"
            continue
        if df is None or df.empty or len(df) < 10:
            skipped[symbol] = "too_short"
            continue

        row = df.iloc[-1].to_dict()
        holdings = pd.DataFrame(columns=["tradingsymbol", "average_price", "quantity", "t1_quantity", "bars_in_trade"])
        decision, details = RULE_SET_OPTIONS_1.evaluate_signal(df, row, holdings)
        gate_failures = list(details.get("entry_gate_failures", []) or [])
        score_gap = float(details.get("score_gap_to_buy", 0.0) or 0.0)
        metric_snapshot = details.get("metric_snapshot", {}) or {}
        threshold_snapshot = details.get("threshold_snapshot", {}) or {}
        gate_status = details.get("gate_status", {}) or {}
        candidates.append(
            {
                "symbol": symbol,
                "decision": str(decision).upper(),
                "score": float(details.get("score", 0.0) or 0.0),
                "side": details.get("side"),
                "reason": details.get("reason", []),
                "gate_failures": gate_failures,
                "gate_failures_count": len(gate_failures),
                "score_gap_to_buy": score_gap,
                "gate_status": gate_status,
                "metric_snapshot": metric_snapshot,
                "threshold_snapshot": threshold_snapshot,
                "last_close": float(df.iloc[-1]["Close"]),
                "volume": float(df.iloc[-1].get("Volume", 0.0) or 0.0),
                "oi": float(df.iloc[-1].get("OI", 0.0) or 0.0),
                "underlying_close": float(df.iloc[-1].get("UL_Close", 0.0) or 0.0),
                "expiry": str(df.iloc[-1].get("expiry", "")),
                "strike": float(df.iloc[-1].get("strike", 0.0) or 0.0),
            }
        )

    ranked = sorted(candidates, key=lambda x: (x["decision"] == "BUY", x["score"]), reverse=True)
    buy_candidates = [x for x in ranked if x["decision"] == "BUY"]
    hold_candidates = [x for x in ranked if x["decision"] != "BUY"]
    near_miss_candidates = sorted(
        hold_candidates,
        key=lambda x: (x["gate_failures_count"], x["score_gap_to_buy"], -x["score"]),
    )[:5]
    blocker_counts = Counter()
    for candidate in hold_candidates:
        blocker_counts.update(candidate.get("gate_failures") or [])
    payload = {
        "generated_at": datetime.now().isoformat(),
        "paper_mode": True,
        "production_rule_model": "OPTIONS=RULE_SET_OPTIONS_1",
        "manifest_path": str(opt_support.OPTIONS_MANIFEST),
        "underlying_context_path": str(opt_support.HIST_DIR / "NIFTY50_INDEX.feather"),
        "universe_size": len(symbols),
        "evaluated": len(candidates),
        "skipped": skipped,
        "buy_candidates": buy_candidates[:5],
        "near_miss_candidates": near_miss_candidates,
        "near_miss_summary": {
            "hold_candidates": len(hold_candidates),
            "closest_symbol": near_miss_candidates[0].get("symbol") if near_miss_candidates else None,
            "closest_score_gap_to_buy": near_miss_candidates[0].get("score_gap_to_buy") if near_miss_candidates else None,
            "most_common_gate_failures": [
                {"gate": gate, "count": count} for gate, count in blocker_counts.most_common(8)
            ],
        },
        "top_candidate": buy_candidates[0] if buy_candidates else (ranked[0] if ranked else None),
        "all_ranked": ranked[:10],
    }
    OPTIONS_OUT.write_text(json.dumps(payload, indent=2))
    return payload



def main():
    payload = {}
    try:
        payload["equity_shadow"] = run_equity_shadow()
    except Exception as exc:
        payload["equity_shadow_error"] = str(exc)
    try:
        payload["options_shadow"] = run_options_shadow()
    except Exception as exc:
        payload["options_shadow_error"] = str(exc)
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
