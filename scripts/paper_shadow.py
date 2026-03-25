#!/usr/bin/env python3
"""Paper shadow mode: compute hypothetical actions, place no orders."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from Auto_Trader import RULE_SET_2, RULE_SET_7
from Auto_Trader import utils as at_utils

OUT = ROOT / "reports"
OUT.mkdir(exist_ok=True)


def load_hist(symbol="NIFTYETF"):
    p = ROOT / "intermediary_files" / "Hist_Data" / f"{symbol}.feather"
    if not p.exists():
        p = ROOT / "intermediary_files" / "Hist_Data" / "NIFTYBEES.feather"

    if p.exists():
        df = pd.read_feather(p)
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
            df = df.sort_values("Date")
        out = at_utils.Indicators(df)
        out = out.ffill().dropna(subset=["Close"]).reset_index(drop=True)
        return out

    # fallback: fetch fresh data if cache was cleaned by runtime
    import yfinance as yf

    ysym = f"{symbol}.NS"
    df = yf.download(ysym, period="2y", interval="1d", auto_adjust=False, progress=False)
    if df is None or df.empty:
        df = yf.download("NIFTYBEES.NS", period="2y", interval="1d", auto_adjust=False, progress=False)
    if df is None or df.empty:
        raise SystemExit("No local historical data found for shadow mode and yfinance fallback failed")
    if hasattr(df.columns, "levels"):
        df.columns = [str(c[0]) for c in df.columns]
    df = df.reset_index()[["Date", "Open", "High", "Low", "Close", "Volume"]]
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    out = at_utils.Indicators(df)
    out = out.ffill().dropna(subset=["Close"]).reset_index(drop=True)
    return out


def load_qty(symbol="NIFTYETF") -> int:
    h = ROOT / "intermediary_files" / "Holdings.feather"
    if not h.exists():
        return 0
    df = pd.read_feather(h)
    df = df[df["tradingsymbol"].astype(str).str.upper() == symbol.upper()]
    if df.empty:
        return 0
    return int(float(df.iloc[0].get("quantity", 0) + df.iloc[0].get("t1_quantity", 0)))


def main():
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
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
