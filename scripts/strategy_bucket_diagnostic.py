#!/usr/bin/env python3
"""Diagnose baseline strategy performance by Nifty50 / large / mid / small cap buckets."""

from __future__ import annotations

import importlib
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from statistics import median

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / "reports"
REPORTS.mkdir(exist_ok=True)

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from Auto_Trader.StrongFundamentalsStockList import goodStocks  # noqa: E402

pack = importlib.import_module("scripts.weekly_universe_cagr_check")

BUCKET_ORDER = [
    ("nifty50", "Nifty 50"),
    ("large_cap", "Large cap"),
    ("mid_cap", "Mid cap"),
    ("small_cap", "Small cap"),
]


def build_universe_df(limit: int | None = None) -> pd.DataFrame:
    df = goodStocks().copy()
    if df is None or df.empty:
        raise RuntimeError("Strong fundamentals universe is empty")
    df["Symbol"] = df["Symbol"].astype(str).str.upper().str.strip()
    df["AssetClass"] = df["AssetClass"].astype(str).str.upper().str.strip()
    df["CapBucket"] = df["CapBucket"].astype(str).str.upper().str.strip()
    df["IsNifty50"] = df["IsNifty50"].fillna(False).astype(bool)
    if limit is not None:
        df = df.head(max(1, int(limit))).copy()
    return df


def symbol_span_years(df: pd.DataFrame) -> float:
    if df is None or df.empty or "Date" not in df.columns:
        return 0.0
    start = pd.to_datetime(df["Date"].iloc[0])
    end = pd.to_datetime(df["Date"].iloc[-1])
    return max(0.0, (end - start).days / 365.25)


def analyze_bucket(name: str, label: str, bucket_df: pd.DataFrame, data_map: dict[str, pd.DataFrame]) -> dict:
    requested_symbols = bucket_df["Symbol"].dropna().unique().tolist()
    tested_symbols = [symbol for symbol in requested_symbols if symbol in data_map]
    if not tested_symbols:
        return {
            "bucket": name,
            "label": label,
            "requested_symbols": len(requested_symbols),
            "tested_symbols": 0,
            "coverage_pct": 0.0,
            "reason": "no_symbols_with_usable_history",
        }

    subset_map = {symbol: data_map[symbol] for symbol in tested_symbols}
    subset_universe_df = bucket_df[bucket_df["Symbol"].isin(tested_symbols)].copy()
    result, details, sim_meta = pack.run_baseline_detailed(subset_map, universe_df=subset_universe_df)
    portfolio_equity, daily_returns, trades_df, curve_meta = pack.build_validation_curves(details, sim_meta=sim_meta)
    metrics = pack.compute_validation_metrics(portfolio_equity, daily_returns, trades_df)
    monthly_returns = metrics.pop("monthly_returns")
    walkforward = pack.walkforward_validation(monthly_returns)
    monte_carlo = pack.monte_carlo_validation(monthly_returns)

    symbol_rows = []
    for symbol in tested_symbols:
        stats = details[symbol]
        symbol_rows.append(
            {
                "symbol": symbol,
                "total_return_pct": float(stats.get("total_return_pct", 0.0) or 0.0),
                "closed_trades": int(len(stats.get("closed_trades", []))),
                "avg_hold_days": float(stats.get("avg_hold_days", 0.0) or 0.0),
                "exposure_pct": float(stats.get("exposure_pct", 0.0) or 0.0),
            }
        )
    symbol_df = pd.DataFrame(symbol_rows)

    spans = [symbol_span_years(subset_map[symbol]) for symbol in tested_symbols]
    median_years = float(median(spans)) if spans else 0.0
    simulation_cfg = ((getattr(result, "params", {}) or {}).get("simulation", {}) or {})
    start_capital = float(simulation_cfg.get("starting_capital", 100000.0 * max(1, len(result.symbols_tested))))
    cagr_pct = None
    if median_years > 0 and start_capital > 0 and result.final_value > 0:
        cagr_pct = round((((result.final_value / start_capital) ** (1.0 / median_years)) - 1.0) * 100.0, 2)

    symbols_with_trades = int((symbol_df["closed_trades"] > 0).sum()) if not symbol_df.empty else 0
    round_trips = int(metrics.get("closed_trades", 0) or 0)
    trade_density = round(round_trips / max(1.0, len(tested_symbols) * max(median_years, 0.01)), 3)

    return {
        "bucket": name,
        "label": label,
        "requested_symbols": len(requested_symbols),
        "tested_symbols": len(tested_symbols),
        "coverage_pct": round((len(tested_symbols) / max(1, len(requested_symbols))) * 100.0, 2),
        "median_history_years": round(median_years, 3),
        "backtest_total_return_pct": result.total_return_pct,
        "backtest_cagr_pct": cagr_pct,
        "backtest_max_drawdown_pct": result.max_drawdown_pct,
        "backtest_trades": result.trades,
        "backtest_win_rate_pct": result.win_rate_pct,
        "validation_curve_cagr_pct": metrics.get("curve_cagr_pct"),
        "validation_curve_max_drawdown_pct": metrics.get("curve_max_drawdown_pct"),
        "validation_sharpe_ratio": metrics.get("sharpe_ratio"),
        "validation_profit_factor": metrics.get("profit_factor"),
        "positive_month_pct": metrics.get("positive_month_pct"),
        "symbols_with_closed_trades": symbols_with_trades,
        "symbols_with_closed_trades_pct": round((symbols_with_trades / max(1, len(tested_symbols))) * 100.0, 2),
        "round_trips": round_trips,
        "round_trips_per_symbol_year": trade_density,
        "median_symbol_return_pct": round(float(symbol_df["total_return_pct"].median()), 2) if not symbol_df.empty else 0.0,
        "avg_symbol_return_pct": round(float(symbol_df["total_return_pct"].mean()), 2) if not symbol_df.empty else 0.0,
        "avg_symbol_hold_days": round(float(symbol_df["avg_hold_days"].mean()), 2) if not symbol_df.empty else 0.0,
        "avg_symbol_exposure_pct": round(float(symbol_df["exposure_pct"].mean()), 2) if not symbol_df.empty else 0.0,
        "walkforward": walkforward,
        "monte_carlo": monte_carlo,
    }


def diagnose_root_cause(bucket_results: list[dict]) -> dict:
    by_name = {row["bucket"]: row for row in bucket_results}
    large = by_name.get("large_cap", {})
    mid = by_name.get("mid_cap", {})
    small = by_name.get("small_cap", {})
    nifty = by_name.get("nifty50", {})

    universe_issue = False
    conditions_issue = False
    notes: list[str] = []
    recommendations: list[str] = []

    large_cagr = large.get("backtest_cagr_pct")
    small_cagr = small.get("backtest_cagr_pct")
    nifty_cagr = nifty.get("backtest_cagr_pct")
    large_trade_density = large.get("round_trips_per_symbol_year", 0)
    mid_trade_density = mid.get("round_trips_per_symbol_year", 0)
    small_trade_density = small.get("round_trips_per_symbol_year", 0)

    if large_cagr is not None and small_cagr is not None and large_cagr - small_cagr >= 8:
        universe_issue = True
        notes.append("Small cap performance materially lags large caps, so the universe is diluting edge.")
        recommendations.append("Exclude SMALL_CAP from the live universe by default and re-evaluate LARGE_CAP + MID_CAP or NIFTY50-first baskets.")

    if all(
        value is not None and value < 20.0
        for value in [nifty_cagr, large_cagr] if value is not None
    ):
        conditions_issue = True
        notes.append("Even the cleaner large-cap / Nifty buckets are below the 20% CAGR bar, so universe cleanup alone will not fix it.")
        recommendations.append("Retune entry/exit conditions on the best bucket instead of only changing the universe.")

    if max(large_trade_density, mid_trade_density, small_trade_density, 0) < 1.0:
        conditions_issue = True
        notes.append("Trade density is extremely low, which suggests the current buy logic is too restrictive for the chosen bars and universe.")
        recommendations.append("Loosen buy gates and/or hold winners longer, then rerun walk-forward and Monte Carlo on the best bucket.")

    if not notes:
        notes.append("No single smoking gun. Both universe quality and rule tuning need work.")
        recommendations.append("Compare tuned variants on Nifty50, large cap, and large+mid cap, then promote only if validation clears the 20% CAGR bar.")

    return {
        "universe_issue": universe_issue,
        "conditions_issue": conditions_issue,
        "notes": notes,
        "recommendations": recommendations,
    }


def main() -> int:
    now = datetime.now()
    limit_raw = os.getenv("AT_BUCKET_DIAG_LIMIT", "").strip()
    limit = int(limit_raw) if limit_raw else None
    min_history_bars = pack.lab.configured_min_history_bars(default=1000)

    universe_df = build_universe_df(limit=limit)
    data_map, data_context = pack.load_data(universe_df["Symbol"].tolist(), min_history_bars=min_history_bars)

    buckets = []
    buckets.append(analyze_bucket("nifty50", "Nifty 50", universe_df[universe_df["IsNifty50"]].copy(), data_map))
    buckets.append(analyze_bucket("large_cap", "Large cap", universe_df[universe_df["CapBucket"] == "LARGE_CAP"].copy(), data_map))
    buckets.append(analyze_bucket("mid_cap", "Mid cap", universe_df[universe_df["CapBucket"] == "MID_CAP"].copy(), data_map))
    buckets.append(analyze_bucket("small_cap", "Small cap", universe_df[universe_df["CapBucket"] == "SMALL_CAP"].copy(), data_map))

    diagnosis = diagnose_root_cause(buckets)
    payload = {
        "generated_at": now.isoformat(),
        "history_period": os.getenv("AT_LAB_HISTORY_PERIOD", "5y"),
        "min_history_bars": min_history_bars,
        "universe_summary": {
            "requested_symbols": int(len(universe_df)),
            "asset_class_counts": universe_df["AssetClass"].value_counts(dropna=False).to_dict(),
            "cap_bucket_counts": universe_df["CapBucket"].value_counts(dropna=False).to_dict(),
            "nifty50_symbols_in_universe": int(universe_df["IsNifty50"].sum()),
            "loaded_symbols": len(data_map),
            "skip_reason_counts": data_context.get("skip_reason_counts", {}),
        },
        "bucket_results": buckets,
        "diagnosis": diagnosis,
    }

    ts = now.strftime("%Y%m%d_%H%M%S")
    out_json = REPORTS / f"strategy_bucket_diagnostic_{ts}.json"
    out_md = REPORTS / f"strategy_bucket_diagnostic_{ts}.md"
    out_json.write_text(json.dumps(payload, indent=2))

    lines = [
        f"# Strategy bucket diagnostic, {ts}",
        "",
        f"- Universe symbols: **{payload['universe_summary']['requested_symbols']}**",
        f"- Loaded symbols: **{payload['universe_summary']['loaded_symbols']}**",
        f"- Cap buckets: **{payload['universe_summary']['cap_bucket_counts']}**",
        f"- Nifty 50 symbols in universe: **{payload['universe_summary']['nifty50_symbols_in_universe']}**",
        "",
        "## Buckets",
    ]
    for row in buckets:
        lines.extend(
            [
                f"### {row['label']}",
                f"- Requested/tested: **{row['requested_symbols']} / {row['tested_symbols']}**",
                f"- Backtest CAGR: **{row.get('backtest_cagr_pct')}%**",
                f"- Max drawdown: **{row.get('backtest_max_drawdown_pct')}%**",
                f"- Trades: **{row.get('backtest_trades')}**",
                f"- Symbols with trades: **{row.get('symbols_with_closed_trades_pct')}%**",
                f"- Round trips per symbol-year: **{row.get('round_trips_per_symbol_year')}**",
                "",
            ]
        )
    lines.extend([
        "## Diagnosis",
        *[f"- {note}" for note in diagnosis["notes"]],
        "",
        "## Recommendations",
        *[f"- {note}" for note in diagnosis["recommendations"]],
    ])
    out_md.write_text("\n".join(lines) + "\n")

    print(json.dumps(payload, indent=2))
    print(f"Saved: {out_json}")
    print(f"Saved: {out_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
