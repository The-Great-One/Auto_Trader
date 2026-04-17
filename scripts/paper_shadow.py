#!/usr/bin/env python3
"""Paper shadow mode: compute hypothetical actions, place no orders."""

from __future__ import annotations

import json
import tempfile
from datetime import datetime
from pathlib import Path
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from Auto_Trader import RULE_SET_2, RULE_SET_7, RULE_SET_OPTIONS_1
from Auto_Trader import options_support as opt_support
from Auto_Trader import utils as at_utils

OUT = ROOT / "reports"
OUT.mkdir(exist_ok=True)
OPTIONS_OUT = OUT / "paper_shadow_options_latest.json"


def _prepare_hist_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    if hasattr(df.columns, "levels"):
        df.columns = [str(c[0]) for c in df.columns]
    cols = {str(c).lower(): c for c in df.columns}
    required = ["date", "open", "high", "low", "close"]
    if not all(k in cols for k in required):
        return pd.DataFrame()
    use = pd.DataFrame(
        {
            "Date": pd.to_datetime(df[cols["date"]], errors="coerce"),
            "Open": pd.to_numeric(df[cols["open"]], errors="coerce"),
            "High": pd.to_numeric(df[cols["high"]], errors="coerce"),
            "Low": pd.to_numeric(df[cols["low"]], errors="coerce"),
            "Close": pd.to_numeric(df[cols["close"]], errors="coerce"),
            "Volume": pd.to_numeric(df.get(cols.get("volume", "Volume"), 0), errors="coerce").fillna(0),
        }
    ).dropna(subset=["Date", "Open", "High", "Low", "Close"])
    if use.empty:
        return use
    use = use.sort_values("Date").drop_duplicates(subset=["Date"], keep="last").reset_index(drop=True)
    if len(use) < 5:
        return pd.DataFrame()
    try:
        out = at_utils.Indicators(use)
        out = out.ffill().dropna(subset=["Close"]).reset_index(drop=True)
        return out if not out.empty else pd.DataFrame()
    except Exception:
        return pd.DataFrame()



def load_hist(symbol="NIFTYETF"):
    cache_paths = [
        ROOT / "intermediary_files" / "Hist_Data" / f"{symbol}.feather",
        ROOT / "intermediary_files" / "Hist_Data" / "NIFTYBEES.feather",
        ROOT / "intermediary_files" / "Hist_Data" / "NIFTY50_INDEX.feather",
    ]

    for p in cache_paths:
        if not p.exists():
            continue
        try:
            out = _prepare_hist_df(pd.read_feather(p))
            if not out.empty:
                return out
        except Exception:
            pass

    # fallback: fetch fresh data if cache was cleaned by runtime or cache is bad/empty
    import yfinance as yf

    for ysym in [f"{symbol}.NS", "NIFTYBEES.NS"]:
        df = yf.download(ysym, period="2y", interval="1d", auto_adjust=False, progress=False)
        out = _prepare_hist_df(df)
        if not out.empty:
            return out

    raise SystemExit("No usable historical data found for shadow mode, local cache and yfinance fallback both failed")


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
    df = load_hist(symbol)
    row = df.iloc[-1].to_dict()
    row.setdefault("instrument_token", 1626369)

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
            decision = RULE_SET_2.buy_or_sell(df, row, holdings)
        mode = "SELL_RULE_ONLY"
    else:
        holdings = pd.DataFrame(columns=["instrument_token", "tradingsymbol", "average_price", "quantity", "t1_quantity", "bars_in_trade"])
        decision = RULE_SET_7.buy_or_sell(df, row, holdings)
        mode = "BUY_RULE_ONLY"

    payload = {
        "generated_at": datetime.now().isoformat(),
        "paper_mode": True,
        "symbol": symbol,
        "production_rule_model": "BUY=RULE_SET_7, SELL=RULE_SET_2",
        "position_qty": qty,
        "mode": mode,
        "decision": str(decision).upper(),
        "last_close": float(df.iloc[-1]["Close"]),
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
    near_miss_candidates = sorted(
        [x for x in ranked if x["decision"] != "BUY"],
        key=lambda x: (x["gate_failures_count"], x["score_gap_to_buy"], -x["score"]),
    )[:5]
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
