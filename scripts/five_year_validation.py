#!/usr/bin/env python3
"""5-year validation backtest for the vol_sizing_006 winner.

Downloads fresh 5-year history via yfinance (bypasses 3-year local cache),
runs live-parity backtest with the winning config, and reports CAGR + annual breakdown.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import yfinance as yf

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import Auto_Trader.utils as at_utils
from scripts import weekly_strategy_lab as lab
from scripts.weekly_universe_cagr_check import run_baseline_detailed

PERIOD = "5y"
MIN_HISTORY_BARS = 500
OUT_DIR = ROOT / "reports"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def _normalize_ohlcv(df: pd.DataFrame) -> pd.DataFrame | None:
    if df is None or df.empty:
        return None
    df = df.copy()
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
    col_map = {}
    for raw, std in [("Open", "Open"), ("High", "High"), ("Low", "Low"),
                     ("Close", "Close"), ("Volume", "Volume"), ("Adj Close", "Adj Close")]:
        for c in df.columns:
            if str(c).strip().lower() == raw.lower():
                col_map[c] = std
                break
    df = df.rename(columns=col_map)
    if "Date" not in df.columns and df.index.name and df.index.name.lower() == "date":
        df = df.reset_index()
    if "Date" not in df.columns:
        for c in df.columns:
            if "date" in str(c).lower():
                df = df.rename(columns={c: "Date"})
                break
    if "Date" not in df.columns:
        df = df.reset_index()
        for c in df.columns:
            if "date" in str(c).lower():
                df = df.rename(columns={c: "Date"})
                break
    df["Date"] = pd.to_datetime(df["Date"], utc=True).dt.tz_localize(None)
    if "Adj Close" in df.columns and "Close" in df.columns:
        df = df.drop(columns=["Adj Close"])
    for col in ["Open", "High", "Low", "Close", "Volume"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["Close"])
    df = df.sort_values("Date").reset_index(drop=True)
    return df if not df.empty else None


def download_5y(symbol: str) -> pd.DataFrame | None:
    y_symbols = [f"{symbol}.NS"] if "." not in symbol else [symbol]
    if symbol == "NIFTYETF":
        y_symbols.extend(["NIFTYBEES.NS", "^NSEI"])
    for y_sym in y_symbols:
        try:
            df = yf.download(y_sym, period=PERIOD, interval="1d", auto_adjust=False, progress=False)
            out = _normalize_ohlcv(df)
            if out is not None and len(out) >= MIN_HISTORY_BARS:
                return out
        except Exception:
            continue
    return None


def load_5y_data(symbols: list[str]) -> tuple[dict[str, pd.DataFrame], dict]:
    data_map = {}
    skipped = {}
    total = max(1, len(symbols))
    for idx, symbol in enumerate(symbols, 1):
        if idx % 20 == 0 or idx == total:
            print(f"  Loading {idx}/{total}: {len(data_map)} loaded so far")
        df = download_5y(symbol)
        if df is None or df.empty:
            skipped[symbol] = "missing_or_empty"
            continue
        if len(df) < MIN_HISTORY_BARS:
            skipped[symbol] = f"too_short:{len(df)}"
            continue
        try:
            ind = at_utils.Indicators(df)
            data_map[symbol] = ind
        except Exception as exc:
            skipped[symbol] = f"indicator_failed:{exc}"
    from collections import Counter
    from statistics import median
    spans = []
    for df in data_map.values():
        span = (pd.to_datetime(df["Date"].iloc[-1]) - pd.to_datetime(df["Date"].iloc[0])).days / 365.25
        spans.append(span)
    skip_reasons = Counter(r.split(":", 1)[0] for r in skipped.values())
    return data_map, {
        "loaded_symbols": list(data_map.keys()),
        "loaded_symbol_count": len(data_map),
        "skipped_symbols": skipped,
        "skip_reason_counts": dict(skip_reasons),
        "median_span_years": round(median(spans), 3) if spans else 0,
        "min_span_years": round(min(spans), 3) if spans else 0,
        "max_span_years": round(max(spans), 3) if spans else 0,
        "history_source": "yfinance_5y",
    }


def main():
    # Get symbols from the vol sizing lab
    lab_path = OUT_DIR / "volatility_sizing_lab_20260420_182753.json"
    if not lab_path.exists():
        # Fallback: find any vol sizing lab file
        lab_files = sorted(OUT_DIR.glob("volatility_sizing_lab_*.json"), reverse=True)
        lab_path = lab_files[0] if lab_files else None
    if not lab_path or not lab_path.exists():
        print("ERROR: No volatility sizing lab file found")
        return 1

    obj = json.loads(lab_path.read_text())
    syms = obj["recommendation"]["data_context"]["loaded_symbols"]
    print(f"Symbols: {len(syms)}")

    # Set winning config env
    os.environ["AT_BACKTEST_VOL_SIZING_ENABLED"] = "1"
    os.environ["AT_BACKTEST_RISK_PER_TRADE_PCT"] = "0.01"
    os.environ["AT_BACKTEST_ATR_STOP_MULT"] = "2.5"
    os.environ["AT_BACKTEST_MAX_POSITION_NOTIONAL_PCT"] = "0.25"
    os.environ["AT_LAB_RNN_ENABLED"] = "0"

    # Download 5-year data
    print(f"\nDownloading {PERIOD} history for {len(syms)} symbols...")
    data_map, data_ctx = load_5y_data(syms)
    print(f"Loaded: {len(data_map)}, Skipped: {len(data_ctx['skipped_symbols'])}")
    print(f"Span: median={data_ctx['median_span_years']}y, min={data_ctx['min_span_years']}y, max={data_ctx['max_span_years']}y")

    if not data_map:
        print("ERROR: No data loaded")
        return 1

    # Sample
    k = list(data_map.keys())[0]
    df = data_map[k]
    print(f"Sample: {k}, rows={len(df)}, range={df['Date'].min()} to {df['Date'].max()}")

    # Run vol sizing backtest
    print("\nRunning 5-year backtest with volatility sizing...")
    result, details, sim_meta = run_baseline_detailed(data_map)

    eq = sim_meta.get("portfolio_equity")
    cagr = sharpe = None
    annual_breakdown = {}
    if eq is not None and len(eq) > 20:
        total_days = (eq.index[-1] - eq.index[0]).days
        cagr = ((eq.iloc[-1] / eq.iloc[0]) ** (365.0 / max(1, total_days)) - 1) * 100
        rets = eq.pct_change().dropna()
        sharpe = rets.mean() / rets.std() * (252**0.5) if rets.std() > 0 else 0
        df_eq = pd.DataFrame({"equity": eq})
        df_eq["year"] = df_eq.index.year
        prev = None
        for year, group in df_eq.groupby("year")["equity"]:
            end = group.iloc[-1]
            if prev is not None:
                annual_breakdown[str(year)] = round((end / prev - 1) * 100, 2)
            prev = end

    sizing = sim_meta.get("curve_meta", {}).get("position_sizing", {})
    regime = sim_meta.get("curve_meta", {}).get("regime_filter", {})
    total_realized = sum(s["realized_pnl_abs"] for s in details.values())
    winners = len([s for s in details.values() if s["realized_pnl_abs"] > 0])
    losers = len([s for s in details.values() if s["realized_pnl_abs"] < 0])
    active = len([s for s in details.values() if s["trades"] > 0])

    vol_report = {
        "config": "vol_sizing_006",
        "return_pct": result.total_return_pct,
        "drawdown_pct": result.max_drawdown_pct,
        "trades": result.trades,
        "win_rate_pct": result.win_rate_pct,
        "score": result.selection_score,
        "cagr_pct": round(cagr, 2) if cagr else None,
        "sharpe": round(sharpe, 2) if sharpe else None,
        "active_symbols": active,
        "total_symbols": len(data_map),
        "winners": winners,
        "losers": losers,
        "total_realized_pnl": round(total_realized, 2),
        "sizing_buy_orders": sizing.get("buy_orders_sized"),
        "regime_blocked": regime.get("blocked_buy_signals"),
        "regime_allowed": regime.get("allowed_buy_signals"),
        "annual_breakdown": annual_breakdown,
    }

    # Now run fixed-size baseline
    print("\nRunning 5-year baseline (fixed size)...")
    os.environ["AT_BACKTEST_VOL_SIZING_ENABLED"] = "0"
    result2, details2, sim_meta2 = run_baseline_detailed(data_map)

    eq2 = sim_meta2.get("portfolio_equity")
    cagr2 = sharpe2 = None
    annual2 = {}
    if eq2 is not None and len(eq2) > 20:
        td2 = (eq2.index[-1] - eq2.index[0]).days
        cagr2 = ((eq2.iloc[-1] / eq2.iloc[0]) ** (365.0 / max(1, td2)) - 1) * 100
        r2 = eq2.pct_change().dropna()
        sharpe2 = r2.mean() / r2.std() * (252**0.5) if r2.std() > 0 else 0
        df_eq2 = pd.DataFrame({"equity": eq2})
        df_eq2["year"] = df_eq2.index.year
        prev2 = None
        for year, group in df_eq2.groupby("year")["equity"]:
            end2 = group.iloc[-1]
            if prev2 is not None:
                annual2[str(year)] = round((end2 / prev2 - 1) * 100, 2)
            prev2 = end2

    baseline_report = {
        "config": "fixed_size_baseline",
        "return_pct": result2.total_return_pct,
        "drawdown_pct": result2.max_drawdown_pct,
        "trades": result2.trades,
        "win_rate_pct": result2.win_rate_pct,
        "score": result2.selection_score,
        "cagr_pct": round(cagr2, 2) if cagr2 else None,
        "sharpe": round(sharpe2, 2) if sharpe2 else None,
        "annual_breakdown": annual2,
    }

    final = {
        "generated_at": datetime.now().astimezone().isoformat(),
        "test_period": PERIOD,
        "data_context": data_ctx,
        "vol_sizing": vol_report,
        "baseline": baseline_report,
        "improvement": {
            "return_pct": round(vol_report["return_pct"] - baseline_report["return_pct"], 2),
            "cagr_pct": round((vol_report.get("cagr_pct") or 0) - (baseline_report.get("cagr_pct") or 0), 2),
        },
    }

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = OUT_DIR / f"five_year_validation_{ts}.json"
    out_path.write_text(json.dumps(final, indent=2))

    print("\n" + "=" * 60)
    print("5-YEAR VALIDATION RESULTS")
    print("=" * 60)
    print(f"\nVOL SIZING (vol_sizing_006):")
    print(f"  Return: {vol_report['return_pct']}%")
    print(f"  CAGR: {vol_report['cagr_pct']}%")
    print(f"  Sharpe: {vol_report['sharpe']}")
    print(f"  Drawdown: {vol_report['drawdown_pct']}%")
    print(f"  Trades: {vol_report['trades']}, Win rate: {vol_report['win_rate_pct']}%")
    print(f"  Active symbols: {vol_report['active_symbols']}/{vol_report['total_symbols']}")
    print(f"  Winners/Losers: {vol_report['winners']}/{vol_report['losers']}")
    if annual_breakdown:
        print(f"  Annual:")
        for yr, ret in annual_breakdown.items():
            print(f"    {yr}: {ret:+.2f}%")

    print(f"\nBASELINE (fixed size):")
    print(f"  Return: {baseline_report['return_pct']}%")
    print(f"  CAGR: {baseline_report['cagr_pct']}%")
    print(f"  Sharpe: {baseline_report['sharpe']}")
    print(f"  Trades: {baseline_report['trades']}, Win rate: {baseline_report['win_rate_pct']}%")
    if annual2:
        print(f"  Annual:")
        for yr, ret in annual2.items():
            print(f"    {yr}: {ret:+.2f}%")

    print(f"\nSaved: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())