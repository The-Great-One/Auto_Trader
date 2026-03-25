#!/usr/bin/env python3
"""
Local-only strategy lab:
- builds strategy variants by tweaking RULE_SET_7 (BUY) + RULE_SET_2 (SELL)
- backtests variants on NIFTYBEES daily data
- compares against current baseline
- writes ranked report; DOES NOT deploy to server
"""

from __future__ import annotations

import itertools
import json
import os
import tempfile
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path

import pandas as pd
import yfinance as yf
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from Auto_Trader import RULE_SET_2, RULE_SET_7
from Auto_Trader import utils as at_utils

OUT_DIR = ROOT / "reports"
OUT_DIR.mkdir(exist_ok=True)


@dataclass
class BacktestResult:
    name: str
    final_value: float
    total_return_pct: float
    trades: int
    win_rate_pct: float
    max_drawdown_pct: float
    params: dict


def load_data() -> pd.DataFrame:
    df = yf.download("NIFTYBEES.NS", period="3y", interval="1d", auto_adjust=False, progress=False)
    if df is None or df.empty:
        raise RuntimeError("Could not fetch NIFTYBEES.NS data")
    if hasattr(df.columns, "levels"):
        df.columns = [str(c[0]) for c in df.columns]
    df = df.reset_index()
    use = pd.DataFrame({
        "Date": pd.to_datetime(df["Date"], errors="coerce"),
        "Open": pd.to_numeric(df["Open"], errors="coerce"),
        "High": pd.to_numeric(df["High"], errors="coerce"),
        "Low": pd.to_numeric(df["Low"], errors="coerce"),
        "Close": pd.to_numeric(df["Close"], errors="coerce"),
        "Volume": pd.to_numeric(df.get("Volume", 0), errors="coerce").fillna(0),
    }).dropna(subset=["Date", "Open", "High", "Low", "Close"])
    use = use.sort_values("Date").reset_index(drop=True)
    return at_utils.Indicators(use)


def _set_temp_state(rule2_module, d: str):
    rule2_module.BASE_DIR = d
    rule2_module.HOLDINGS_FILE_PATH = os.path.join(d, "Holdings.json")
    rule2_module.LOCK_FILE_PATH = os.path.join(d, "Holdings.lock")


def run_variant(name: str, df: pd.DataFrame, buy_params: dict, sell_params: dict) -> BacktestResult:
    # avoid DB dependency in RULE_SET_7 market regime check
    at_utils.get_mmi_now = lambda: None

    r2 = RULE_SET_2
    r7 = RULE_SET_7

    # patch configs for this variant
    old_r2 = dict(r2.CONFIG)
    old_r7 = dict(r7.CONFIG)
    r2.CONFIG.update(sell_params)
    r7.CONFIG.update(buy_params)

    with tempfile.TemporaryDirectory(prefix="at_state_") as td:
        _set_temp_state(r2, td)

        cash = 100000.0
        qty = 0
        avg = 0.0
        trades = 0
        wins = 0
        equity_curve = []

        for i in range(250, len(df)):
            part = df.iloc[: i + 1].copy()
            row = part.iloc[-1].to_dict()
            row.setdefault("instrument_token", 1626369)
            price = float(part.iloc[-1]["Close"])

            if qty == 0:
                hold_df = pd.DataFrame(columns=["instrument_token", "tradingsymbol", "average_price", "quantity", "t1_quantity", "bars_in_trade"])
                sig = r7.buy_or_sell(part, row, hold_df)
                if str(sig).upper() == "BUY":
                    buy_qty = int(cash // price)
                    if buy_qty > 0:
                        qty = buy_qty
                        cash -= qty * price
                        avg = price
                        trades += 1
            else:
                hold_df = pd.DataFrame([
                    {
                        "instrument_token": int(row.get("instrument_token", 1626369)),
                        "tradingsymbol": "NIFTYETF",
                        "average_price": avg,
                        "quantity": qty,
                        "t1_quantity": 0,
                        "bars_in_trade": i,
                    }
                ])
                sig = r2.buy_or_sell(part, row, hold_df)
                if str(sig).upper() == "SELL":
                    cash += qty * price
                    if price > avg:
                        wins += 1
                    qty = 0
                    avg = 0.0
                    trades += 1

            port = cash + (qty * price)
            equity_curve.append(port)

    # restore config
    r2.CONFIG.clear(); r2.CONFIG.update(old_r2)
    r7.CONFIG.clear(); r7.CONFIG.update(old_r7)

    final_val = equity_curve[-1] if equity_curve else 100000.0
    ret = (final_val / 100000.0 - 1.0) * 100.0
    win_rate = (wins / max(1, trades // 2)) * 100.0

    # max drawdown
    s = pd.Series(equity_curve if equity_curve else [100000.0], dtype=float)
    peak = s.cummax()
    dd = ((s - peak) / peak * 100.0).min()

    return BacktestResult(
        name=name,
        final_value=round(float(final_val), 2),
        total_return_pct=round(float(ret), 2),
        trades=int(trades),
        win_rate_pct=round(float(win_rate), 2),
        max_drawdown_pct=round(float(dd), 2),
        params={"buy": buy_params, "sell": sell_params},
    )


def variants() -> list[tuple[str, dict, dict]]:
    buy_grid = {
        "adx_min": [18, 20],
        "max_obv_zscore": [2.0, 2.5],
        "max_extension_atr": [2.2, 2.8],
    }
    sell_grid = {
        "momentum_exit_rsi": [42.0, 45.0, 48.0],
        "ema_break_atr_mult": [0.5, 0.7],
        "breakeven_trigger_pct": [2.5, 3.5],
    }

    out = []
    idx = 0
    for bvals in itertools.product(*buy_grid.values()):
        b = dict(zip(buy_grid.keys(), bvals))
        for svals in itertools.product(*sell_grid.values()):
            s = dict(zip(sell_grid.keys(), svals))
            idx += 1
            out.append((f"variant_{idx:02d}", b, s))

    # baseline = current configs
    out.insert(0, ("baseline_current", {}, {}))
    return out


def main():
    df = load_data()
    results = []
    for name, b, s in variants():
        results.append(run_variant(name, df, b, s))

    rank = sorted(results, key=lambda r: (r.total_return_pct, -abs(r.max_drawdown_pct), r.win_rate_pct), reverse=True)
    baseline = next(r for r in rank if r.name == "baseline_current")
    best = rank[0]

    recommendation = {
        "generated_at": datetime.now().isoformat(),
        "baseline": asdict(baseline),
        "best": asdict(best),
        "improvement_return_pct": round(best.total_return_pct - baseline.total_return_pct, 2),
        "should_promote": bool(best.name != baseline.name and best.total_return_pct > baseline.total_return_pct and abs(best.max_drawdown_pct) <= abs(baseline.max_drawdown_pct) + 2.0),
    }

    payload = {
        "recommendation": recommendation,
        "ranked": [asdict(r) for r in rank],
    }

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_json = OUT_DIR / f"strategy_lab_{ts}.json"
    out_csv = OUT_DIR / f"strategy_lab_{ts}.csv"

    out_json.write_text(json.dumps(payload, indent=2))
    pd.DataFrame([asdict(r) for r in rank]).to_csv(out_csv, index=False)

    print(json.dumps(recommendation, indent=2))
    print(f"Saved: {out_json}")
    print(f"Saved: {out_csv}")


if __name__ == "__main__":
    main()
