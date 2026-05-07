#!/usr/bin/env python3
"""
Focused walk-forward validation of 30% CAGR candidate variants.
Tests the focus_combo_169 params and variants that might improve early-period trade density.
Uses expanding-window OOS validation on Kite 5Y cached data.
"""
from __future__ import annotations
import json, os, sys
from datetime import datetime
from pathlib import Path
import pandas as pd

os.environ.setdefault("AT_RESEARCH_MODE", "1")
os.environ.setdefault("AT_LAB_PRECACHE", "0")
os.environ.setdefault("AT_LAB_CACHE_ONLY", "1")
os.environ.setdefault("AT_DISABLE_FILE_LOGGING", "1")
os.environ["AT_LAB_MODE"] = "1"
os.environ["AT_BACKTEST_VOL_SIZING_ENABLED"] = "1"
os.environ["AT_BACKTEST_RISK_PER_TRADE_PCT"] = "0.02"
os.environ["AT_BACKTEST_ATR_STOP_MULT"] = "2.5"
os.environ["AT_BACKTEST_MAX_POSITION_NOTIONAL_PCT"] = "0.10"
os.environ["AT_MAX_SINGLE_SYMBOL_WEIGHT"] = "0.15"
os.environ["AT_PORTFOLIO_BAND"] = "0.10"
os.environ["AT_TARGET_EQUITY"] = "1.0"
os.environ["AT_TARGET_ETF"] = "0.0"
os.environ["AT_LAB_MATCH_LIVE"] = "1"

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from Auto_Trader.utils import Indicators
from scripts.weekly_strategy_lab import load_data, run_variant, run_walk_forward_validation
# rule_sets imported by run_variant / run_walk_forward_validation internally

OUT_DIR = ROOT / "reports"

CANDIDATES = [
    {"name": "focus_combo_169_ich_tim15", "buy": {"ich_cloud_bull": 1}, "sell": {"equity_time_stop_bars": 15}},
    {"name": "focus_combo_ich_only", "buy": {"ich_cloud_bull": 1}, "sell": {}},
    {"name": "focus_combo_tim15_only", "buy": {}, "sell": {"equity_time_stop_bars": 15}},
    {"name": "baseline_no_ich", "buy": {"ich_cloud_bull": 0}, "sell": {"equity_time_stop_bars": 15}},
    {"name": "adx18_tim15", "buy": {"adx_strong_min": 18}, "sell": {"equity_time_stop_bars": 15}},
    {"name": "adx18_baseline", "buy": {"adx_strong_min": 18}, "sell": {}},
    {"name": "adx18_ich", "buy": {"adx_strong_min": 18, "ich_cloud_bull": 1}, "sell": {}},
    {"name": "loose_adx10_tim15", "buy": {"adx_min": 10}, "sell": {"equity_time_stop_bars": 15}},
    {"name": "adx15_tim15_ich", "buy": {"adx_strong_min": 15, "ich_cloud_bull": 1}, "sell": {"equity_time_stop_bars": 15}},
    {"name": "baseline_current", "buy": {}, "sell": {}},
]


def selected_candidates() -> list[dict]:
    """Return candidates, optionally limited for smoke tests."""
    candidates = list(CANDIDATES)
    limit_raw = os.getenv("AT_FOCUSED_WF_CANDIDATE_LIMIT", "").strip()
    if limit_raw:
        try:
            limit = max(1, int(limit_raw))
            candidates = candidates[:limit]
        except ValueError:
            pass
    return candidates

def load_kite_symbols(min_rows=260):
    hist_dir = ROOT / "intermediary_files" / "Hist_Data"
    data_map = {}
    skipped = 0
    for fp in sorted(hist_dir.glob("*.feather")):
        try:
            df = pd.read_feather(fp)
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
            df = df.dropna(subset=["Date"]).sort_values("Date").reset_index(drop=True)
            if len(df) < min_rows:
                skipped += 1
                continue
            enriched = Indicators(df)
            if enriched is not None and len(enriched) >= min_rows:
                data_map[fp.stem] = enriched
        except Exception:
            skipped += 1
    print(f"Loaded {len(data_map)} symbols (skipped {skipped})")
    return data_map

def main():
    candidates = selected_candidates()
    n_splits = int(os.getenv("AT_FOCUSED_WF_SPLITS", "5"))
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] Starting focused walk-forward validation of {len(candidates)} 30% candidates")
    
    # Load data
    # Use load_data from weekly_strategy_lab for full pipeline parity
    try:
        data_map, data_context = load_data({}, {})
        print(f"Loaded {len(data_map)} symbols via load_data()")
    except Exception as e:
        print(f"load_data failed ({e}), falling back to load_kite_symbols()")
        data_map = load_kite_symbols()
    
    if not data_map:
        print("ERROR: No data loaded")
        return 1

    symbol_limit_raw = os.getenv("AT_FOCUSED_WF_SYMBOL_LIMIT", "").strip()
    if symbol_limit_raw:
        try:
            symbol_limit = max(1, int(symbol_limit_raw))
            data_map = dict(list(sorted(data_map.items()))[:symbol_limit])
            print(f"Limited universe to {len(data_map)} symbols for smoke test")
        except ValueError:
            pass
    
    # Get date range
    all_dates = []
    for df in data_map.values():
        all_dates.extend(df["Date"].tolist())
    min_date = min(all_dates)
    max_date = max(all_dates)
    total_days = (max_date - min_date).days
    print(f"Data range: {min_date.date()} to {max_date.date()} ({total_days} days)")
    
    results = []
    for i, candidate in enumerate(candidates):
        name = candidate["name"]
        buy_params = candidate["buy"]
        sell_params = candidate["sell"]
        print(f"\n[{i+1}/{len(candidates)}] Validating {name} (buy={buy_params}, sell={sell_params})...")
        
        # Run in-sample (full data)
        try:
            is_result = run_variant(name, data_map, buy_params, sell_params, {"enabled": False}, {})
            headline_ret = is_result.total_return_pct if hasattr(is_result, 'total_return_pct') else 0
            headline_trades = is_result.trades if hasattr(is_result, 'trades') else 0
            headline_wr = is_result.win_rate_pct if hasattr(is_result, 'win_rate_pct') else 0
            headline_dd = is_result.max_drawdown_pct if hasattr(is_result, 'max_drawdown_pct') else 0
            print(f"  In-sample: ret={headline_ret:.2f}% trades={headline_trades} wr={headline_wr:.1f}% dd={headline_dd:.1f}%")
        except Exception as e:
            print(f"  In-sample FAILED: {e}")
            headline_ret = headline_trades = headline_wr = headline_dd = 0
        
        # Walk-forward folds: preserve full warmup history through each test_end,
        # while run_walk_forward_validation gates signals with
        # AT_BACKTEST_SIGNAL_START_DATE/END_DATE. Slicing directly to the OOS
        # window starves indicators and was producing artificial zero-trade folds.
        try:
            wf_summary = run_walk_forward_validation(data_map, buy_params, sell_params, n_splits=n_splits)
        except Exception as e:
            print(f"  WF FAILED: {e}")
            wf_summary = {"error": str(e), "n_folds": 0, "mean_oos_return_pct": 0, "positive_folds": 0, "folds": []}

        for fold in wf_summary.get("folds", []):
            print(
                f"    Fold {fold.get('fold')}: ret={fold.get('return_pct', 0):.2f}% "
                f"trades={fold.get('trades', 0)} dd={fold.get('max_drawdown_pct', 0):.1f}% "
                f"sym={fold.get('symbols_tested', 0)}"
            )
        print(f"  WF: mean_oos={wf_summary.get('mean_oos_return_pct', 0)}% +ve={wf_summary.get('positive_folds', 0)}/{wf_summary.get('n_folds', 0)}")
        
        results.append({
            "name": name,
            "buy": buy_params,
            "sell": sell_params,
            "headline_return_pct": headline_ret,
            "headline_trades": headline_trades,
            "headline_win_rate_pct": headline_wr,
            "headline_max_drawdown_pct": headline_dd,
            "walk_forward": wf_summary,
        })
    
    # Save report
    report = {
        "generated_at": datetime.now().isoformat(),
        "label": "focused_wf_validate_30_target",
        "n_candidates": len(candidates),
        "universe_size": len(data_map),
        "validation_method": "scripts.weekly_strategy_lab.run_walk_forward_validation",
        "n_splits": n_splits,
        "results": results,
    }
    
    out_path = OUT_DIR / f"focused_wf_validate_30_target_{datetime.now():%Y%m%d_%H%M%S}.json"
    with open(out_path, "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"\nReport saved to {out_path}")
    
    # Summary table
    print("\n" + "="*110)
    print(f"{'Name':<35} {'IS Ret':>8} {'Trades':>7} {'WR%':>6} {'DD%':>7} {'OOS Mean':>9} {'OOS Min':>8} {'OOS Max':>8} {'+ve':>4}")
    print("-"*110)
    for r in results:
        wf = r["walk_forward"]
        print(f"{r['name']:<35} {r['headline_return_pct']:>8.2f} {r['headline_trades']:>7} {r['headline_win_rate_pct']:>6.1f} {r['headline_max_drawdown_pct']:>7.1f} {wf.get('mean_oos_return_pct',0):>9.2f} {wf.get('min_oos_return_pct',0):>8.2f} {wf.get('max_oos_return_pct',0):>8.2f} {wf.get('positive_folds',0):>4}")
    print("="*110)
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
