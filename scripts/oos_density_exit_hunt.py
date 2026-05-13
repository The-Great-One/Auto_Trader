#!/usr/bin/env python3
"""OOS Density + Exit Hunt — targets the two core bottlenecks:

1. TRADE DENSITY: buy_adx_strong_min_18 only fires on 3-4% of bars. This
   variant tests relaxed entry gates (ADX >= 14 instead of 18, no ich filter,
   RSI>30 instead of >40) to increase trade frequency while maintaining edge.

2. EXIT CAPTURE: Telegram audit shows 94.4% max-20d favorable rate but only
   53.8% 5d close-positive rate (FinanceWithSunil). The signal has momentum
   but bleeds. Tests trailing BEP + tighter time stops to capture MFE.

3. UNIVERSE CONCENTRATION: Instead of 250+ symbols, test Nifty-50 heavy
   universe (higher liquidity, tighter spreads, more data quality).

Key constraint: only candidates that show >= 4/5 positive WF folds AND
mean OOS >= 10% are considered credible. Target 30% headline.

Uses weekly_strategy_lab framework for pipeline consistency.
"""

from __future__ import annotations

import json
import os
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

os.environ.setdefault("AT_DISABLE_FILE_LOGGING", "1")
os.environ["AT_RESEARCH_MODE"] = "1"
os.environ["AT_LAB_PRECACHE"] = "0"
os.environ["AT_LAB_CACHE_ONLY"] = "1"
os.environ["AT_LAB_MODE"] = "1"
os.environ["AT_BACKTEST_VOL_SIZING_ENABLED"] = "1"
os.environ["AT_BACKTEST_RISK_PER_TRADE_PCT"] = "0.02"
os.environ["AT_BACKTEST_ATR_STOP_MULT"] = "2.5"
os.environ["AT_BACKTEST_MAX_POSITION_NOTIONAL_PCT"] = "0.10"
os.environ["AT_MAX_SINGLE_SYMBOL_WEIGHT"] = "0.15"
os.environ["AT_PORTFOLIO_BAND"] = "0.10"
os.environ["AT_TARGET_EQUITY"] = "1.0"
os.environ["AT_TARGET_ETF"] = "0.0"
os.environ["AT_LAB_MATCH_LIVE"] = "0"

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from Auto_Trader.utils import Indicators
from scripts.weekly_strategy_lab import load_data, run_variant, run_walk_forward_validation

HIST_DIR = ROOT / "intermediary_files" / "Hist_Data"
OUT_DIR = ROOT / "reports"
OUT_DIR.mkdir(exist_ok=True)

# Nifty-50-ish heavy liquid universe (hand-picked for data quality)
NIFTY50_HEAVY = [
    "RELIANCE", "TCS", "HDFCBANK", "INFY", "ICICIBANK", "HINDUNILVR",
    "SBIN", "BHARTIARTL", "ITC", "KOTAKBANK", "LT", "AXISBANK",
    "BAJFINANCE", "ASIANPAINT", "MARUTI", "HCLTECH", "TITAN", "SUNPHARMA",
    "WIPRO", "ULTRACEMCO", "TATAMOTORS", "TATASTEEL", "POWERGRID",
    "NTPC", "ONGC", "COALINDIA", "ADANIENT", "ADANIPORTS", "BAJAJFINSV",
    "TECHM", "HDFCLIFE", "TATACONSUM", "HEROMOTOCO", "BPCL", "DRREDDY",
    "CIPLA", "EICHERMOT", "DIVISLAB", "TRENT", "M&M", "JSWSTEEL",
    "GRASIM", "INDUSINDBK", "HINDALCO", "APOLLOHOSP", "BRITANNIA",
    "SHRIRAMFIN", "DMART", "SIEMENS", "ABB", "PIDILITIND",
]

# =============================================================================
# VARIANT DEFINITIONS
# =============================================================================
# Group 1: Relaxed entry gates (increase trade density from ~3% to ~8-15%)
DENSITY_VARIANTS = [
    {"name": "dens_adx14_nofilter", "buy": {"adx_strong_min": 14}, "sell": {}},
    {"name": "dens_adx14_tim10", "buy": {"adx_strong_min": 14}, "sell": {"equity_time_stop_bars": 10}},
    {"name": "dens_adx14_tim15", "buy": {"adx_strong_min": 14}, "sell": {"equity_time_stop_bars": 15}},
    {"name": "dens_adx15_nofilter", "buy": {"adx_strong_min": 15}, "sell": {}},
    {"name": "dens_adx15_tim10", "buy": {"adx_strong_min": 15}, "sell": {"equity_time_stop_bars": 10}},
    {"name": "dens_adx15_tim15", "buy": {"adx_strong_min": 15}, "sell": {"equity_time_stop_bars": 15}},
    {"name": "dens_adx16_nofilter", "buy": {"adx_strong_min": 16}, "sell": {}},
    {"name": "dens_adx16_tim10", "buy": {"adx_strong_min": 16}, "sell": {"equity_time_stop_bars": 10}},
    {"name": "dens_adx16_tim15", "buy": {"adx_strong_min": 16}, "sell": {"equity_time_stop_bars": 15}},
]

# Group 2: Exit capture variants (profit-taking inspired by Telegram MFE pattern)
EXIT_VARIANTS = [
    {"name": "exit_adx18_bep3_ts8", "buy": {"adx_strong_min": 18}, "sell": {"equity_trail_bep_pct": 3, "equity_time_stop_bars": 8}},
    {"name": "exit_adx18_bep3_ts10", "buy": {"adx_strong_min": 18}, "sell": {"equity_trail_bep_pct": 3, "equity_time_stop_bars": 10}},
    {"name": "exit_adx18_bep4_ts8", "buy": {"adx_strong_min": 18}, "sell": {"equity_trail_bep_pct": 4, "equity_time_stop_bars": 8}},
    {"name": "exit_adx18_bep5_ts10", "buy": {"adx_strong_min": 18}, "sell": {"equity_trail_bep_pct": 5, "equity_time_stop_bars": 10}},
    {"name": "exit_adx18_bep5_ts12", "buy": {"adx_strong_min": 18}, "sell": {"equity_trail_bep_pct": 5, "equity_time_stop_bars": 12}},
    {"name": "exit_adx14_bep3_ts8", "buy": {"adx_strong_min": 14}, "sell": {"equity_trail_bep_pct": 3, "equity_time_stop_bars": 8}},
    {"name": "exit_adx14_bep4_ts10", "buy": {"adx_strong_min": 14}, "sell": {"equity_trail_bep_pct": 4, "equity_time_stop_bars": 10}},
    {"name": "exit_adx14_bep5_ts10", "buy": {"adx_strong_min": 14}, "sell": {"equity_trail_bep_pct": 5, "equity_time_stop_bars": 10}},
]

# Group 3: Telegram confluence overlay
# Uses learned channel confidence as a watchlist boost filter
CONFLUENCE_VARIANTS = [
    {"name": "conf_adx18_teleboost", "buy": {"adx_strong_min": 18, "telegram_watchlist_boost": 1}, "sell": {}},
    {"name": "conf_adx18_teleboost_tim10", "buy": {"adx_strong_min": 18, "telegram_watchlist_boost": 1}, "sell": {"equity_time_stop_bars": 10}},
    {"name": "conf_adx14_teleboost_bep3", "buy": {"adx_strong_min": 14, "telegram_watchlist_boost": 1}, "sell": {"equity_trail_bep_pct": 3, "equity_time_stop_bars": 10}},
    {"name": "conf_adx14_teleboost_bep4", "buy": {"adx_strong_min": 14, "telegram_watchlist_boost": 1}, "sell": {"equity_trail_bep_pct": 4, "equity_time_stop_bars": 8}},
]

ALL_VARIANTS = DENSITY_VARIANTS + EXIT_VARIANTS + CONFLUENCE_VARIANTS

def load_kite_symbols(min_rows=1000, min_span_days=1200):
    """Load all Kite cached feather data for 5Y window."""
    data_map = {}
    skipped = 0
    for fp in sorted(HIST_DIR.glob("*.feather")):
        try:
            df = pd.read_feather(fp)
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
            df = df.dropna(subset=["Date"]).sort_values("Date").reset_index(drop=True)
            if len(df) < min_rows:
                skipped += 1
                continue
            span = (df.iloc[-1]["Date"] - df.iloc[0]["Date"]).days
            if span < min_span_days:
                skipped += 1
                continue
            enriched = Indicators(df)
            if enriched is not None and len(enriched) >= min_rows:
                symbol = fp.stem.replace(".NS", "").replace(".feather", "")
                data_map[symbol] = enriched
        except Exception:
            skipped += 1
            continue
    print(f"[LOAD] Loaded {len(data_map)} symbols, skipped {skipped}")
    return data_map


def load_nifty50_subset(data_map: dict) -> dict:
    """Filter to only Nifty-50 heavy liquid symbols present in data_map."""
    subset = {}
    for sym in NIFTY50_HEAVY:
        if sym in data_map:
            subset[sym] = data_map[sym]
        elif sym + ".NS" in data_map:
            subset[sym] = data_map[sym + ".NS"]
    print(f"[LOAD] Nifty-50 subset: {len(subset)}/{len(NIFTY50_HEAVY)} symbols available")
    return subset


def run_variant_on_data(name: str, data_map: dict, buy_params: dict, sell_params: dict, 
                         rnn_params: dict | None = None) -> dict:
    """Run a single variant on the full data map."""
    try:
        result = run_variant(name, data_map, buy_params, sell_params, rnn_params=rnn_params)
        if result is None:
            return {"name": name, "error": "no_result"}
        return {"name": name, **result}
    except Exception as e:
        return {"name": name, "error": str(e)}


def main():
    start_time = time.time()
    print(f"[DENSITY_EXIT_HUNT] Starting at {datetime.now().isoformat()}")
    print(f"[DENSITY_EXIT_HUNT] {len(ALL_VARIANTS)} variants to test")

    data_map = load_kite_symbols()
    if not data_map:
        print("[DENSITY_EXIT_HUNT] ERROR: No data loaded")
        return 1

    # Also prepare Nifty-50 subset
    nifty50_map = load_nifty50_subset(data_map)
    
    results = []
    wf_results = []

    # Phase 1: Full universe scan
    print(f"\n{'='*80}")
    print("PHASE 1: Full universe scan ({0} symbols, {1} variants)".format(
        len(data_map), len(ALL_VARIANTS)))
    print(f"{'='*80}")

    for i, variant in enumerate(ALL_VARIANTS):
        v_name = variant["name"]
        buy_params = variant["buy"]
        sell_params = variant["sell"]
        print(f"\n[{i+1}/{len(ALL_VARIANTS)}] Running {v_name} (buy={buy_params}, sell={sell_params})...")

        result = run_variant_on_data(v_name, data_map, buy_params, sell_params)
        results.append(result)
        headline = result.get("total_return_pct", 0)
        trades = result.get("trades", 0)
        print(f"  Result: ret={headline:.2f}% trades={trades}")

    # Phase 2: Walk-forward validate top candidates (headline >= 10% and trades >= 100)
    candidates = [r for r in results
                  if not r.get("error") and r.get("total_return_pct", 0) >= 10 and r.get("trades", 0) >= 100]
    candidates.sort(key=lambda x: x.get("total_return_pct", 0), reverse=True)
    top_n = min(5, len(candidates))

    if top_n > 0:
        print(f"\n{'='*80}")
        print(f"PHASE 2: Walk-forward validation of top {top_n} candidates")
        print(f"{'='*80}")
        for i, cand in enumerate(candidates[:top_n]):
            v_name = cand["name"]
            v_def = next(v for v in ALL_VARIANTS if v["name"] == v_name)
            print(f"\n[{i+1}/{top_n}] Validating {v_name}...")

            try:
                wf = run_walk_forward_validation(
                    data_map, v_def["buy"], v_def["sell"],
                    n_splits=5, min_train_days=500
                )
                wf_entry = {
                    "name": v_name,
                    "headline_return_pct": cand.get("total_return_pct", 0),
                    "headline_trades": cand.get("trades", 0),
                    "walk_forward": wf
                }
                wf_results.append(wf_entry)

                mean_oos = wf.get("mean_oos_return_pct", 0)
                pos_folds = wf.get("positive_folds", 0)
                print(f"  WF: mean_oos={mean_oos:.2f}% +ve={pos_folds}/5")

            except Exception as e:
                print(f"  WF error: {e}")
                wf_results.append({"name": v_name, "error": str(e)})
    else:
        print("\n[PHASE 2] No candidates met 10%+ headline threshold. Skipping WF validation.")

    # Phase 3: Nifty-50 concentrated universe test on best variant
    if nifty50_map and len(nifty50_map) >= 20:
        print(f"\n{'='*80}")
        print(f"PHASE 3: Nifty-50 concentrated universe ({len(nifty50_map)} symbols)")
        print(f"{'='*80}")

        # Test top 3 density variants on concentrated universe
        density_variants = DENSITY_VARIANTS[:3]
        for i, variant in enumerate(density_variants):
            v_name = f"nifty50_{variant['name']}"
            print(f"\n[{i+1}/{len(density_variants)}] Running {v_name} on Nifty-50...")
            result = run_variant_on_data(v_name, nifty50_map, variant["buy"], variant["sell"])
            results.append(result)
            headline = result.get("total_return_pct", 0)
            trades = result.get("trades", 0)
            print(f"  Result: ret={headline:.2f}% trades={trades}")

        # WF validate best nifty50 variant
        nifty50_candidates = [r for r in results if r["name"].startswith("nifty50_")
                              and not r.get("error") and r.get("total_return_pct", 0) >= 10]
        if nifty50_candidates:
            nifty50_candidates.sort(key=lambda x: x.get("total_return_pct", 0), reverse=True)
            best_n50 = nifty50_candidates[0]
            v_def = next(v for v in DENSITY_VARIANTS if v["name"] == best_n50["name"].replace("nifty50_", ""))
            print(f"\n  WF validating best Nifty-50 variant: {best_n50['name']}...")

            try:
                wf = run_walk_forward_validation(
                    nifty50_map, v_def["buy"], v_def["sell"],
                    n_splits=5, min_train_days=500
                )
                wf_entry = {
                    "name": best_n50["name"],
                    "headline_return_pct": best_n50.get("total_return_pct", 0),
                    "headline_trades": best_n50.get("trades", 0),
                    "universe": "nifty50_heavy",
                    "walk_forward": wf
                }
                wf_results.append(wf_entry)
                mean_oos = wf.get("mean_oos_return_pct", 0)
                pos_folds = wf.get("positive_folds", 0)
                print(f"  WF: mean_oos={mean_oos:.2f}% +ve={pos_folds}/5")
            except Exception as e:
                print(f"  WF error: {e}")

    elapsed = time.time() - start_time
    print(f"\n{'='*80}")
    print(f"COMPLETED in {elapsed:.0f}s ({elapsed/60:.1f}min)")
    print(f"{'='*80}")

    # Summary table
    print(f"\n{'Name':<40} {'Ret%':>7} {'Trades':>7} {'WR%':>6} {'DD%':>7} {'OOS_Mean':>9} {'OOS_+ve':>7}")
    print("-" * 90)
    for r in results:
        if not r.get("error"):
            name = r.get("name", "?")
            ret = r.get("total_return_pct", 0)
            trades = r.get("trades", 0)
            wr = r.get("win_rate_pct", 0)
            dd = r.get("max_drawdown_pct", 0)
            # Find WF result if exists
            wf = next((w for w in wf_results if w.get("name") == name), None)
            oos_mean = wf.get("walk_forward", {}).get("mean_oos_return_pct", 0) if wf else "—"
            oos_pos = wf.get("walk_forward", {}).get("positive_folds", "—") if wf else "—"
            print(f"{name:<40} {ret:>7.2f} {trades:>7} {wr:>6.1f} {dd:>7.2f} {oos_mean:>9} {oos_pos:>7}")

    # Save report
    report = {
        "generated_at": datetime.now().isoformat(),
        "label": "oos_density_exit_hunt",
        "full_universe_size": len(data_map),
        "nifty50_universe_size": len(nifty50_map),
        "variants_total": len(ALL_VARIANTS),
        "variants_done": len([r for r in results if not r.get("error")]),
        "wf_validated": len(wf_results),
        "results": results,
        "wf_results": wf_results,
        "elapsed_seconds": elapsed,
    }

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = OUT_DIR / f"oos_density_exit_hunt_{ts}.json"
    with open(out_path, "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"\nReport saved to {out_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())