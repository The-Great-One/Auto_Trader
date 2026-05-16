#!/usr/bin/env python3
"""
Walk-forward validation of top 5 OOS hunt v2 candidates.
Uses the same framework as focused_wf_validate_30_target.py but with the best OOS variants.
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

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from Auto_Trader.utils import Indicators
from scripts.weekly_strategy_lab import load_data, run_variant, run_walk_forward_validation

OUT_DIR = ROOT / "reports"
HIST_DIR = ROOT / "intermediary_files" / "Hist_Data"

MIN_ROWS = 400
MIN_SPAN_DAYS = 500

# Top 5 from OOS hunt v2 secondary results
CANDIDATES = [
    {"name": "oos_adx18_loose_telegram_boost", "buy": {"adx_strong_min": 18, "volume_confirm_mult": 0.85, "ich_cloud_bull": 0, "rsi_floor": 38, "stoch_pull_max": 95, "max_extension_atr": 3.5, "telegram_watchlist_boost": 0.15}, "sell": {"momentum_exit_rsi": 38, "equity_review_rsi": 45}},
    {"name": "oos_regime30_150_adx18_bep3_ts6", "buy": {"regime_filter_enabled": 1, "regime_ema_fast": 30, "regime_ema_slow": 150, "adx_strong_min": 18, "volume_confirm_mult": 0.85}, "sell": {"breakeven_trigger_pct": 3.0, "equity_time_stop_bars": 6}},
    {"name": "oos_adx15_strong18_loose", "buy": {"adx_min": 15, "adx_strong_min": 18, "volume_confirm_mult": 0.85}, "sell": {}},
    {"name": "oos_combo263_tight_ts8", "buy": {"sr_bounce_enabled": 1, "sr_vpoc_reclaim_enabled": 1, "sr_near_support_pct": 0.02, "volume_confirm_mult": 0.75, "rsi_floor": 38, "ich_cloud_bull": 0, "vwap_buy_above": 0}, "sell": {"momentum_exit_rsi": 38, "equity_review_rsi": 45, "equity_time_stop_bars": 8}},
    {"name": "oos_adx18_trail_bep4_ts8", "buy": {"adx_strong_min": 18, "volume_confirm_mult": 0.85}, "sell": {"breakeven_trigger_pct": 4.0, "equity_time_stop_bars": 8}},
]

def load_kite_symbols(min_rows=MIN_ROWS, min_span=MIN_SPAN_DAYS):
    data_map = {}
    for fp in sorted(HIST_DIR.glob("*.feather")):
        sym = fp.stem
        try:
            df = pd.read_feather(fp)
            if len(df) >= min_rows and (df.index.max() - df.index.min()).days >= min_span:
                data_map[sym] = df
        except Exception:
            continue
    return data_map

def main():
    print(f"[oos_top5_wf] Loading Kite cached data...", flush=True)
    data_map = load_kite_symbols()
    print(f"[oos_top5_wf] Loaded {len(data_map)} symbols", flush=True)

    results = []
    for cand in CANDIDATES:
        name = cand["name"]
        buy = cand.get("buy", {})
        sell = cand.get("sell", {})
        print(f"[oos_top5_wf] Running {name}...", flush=True)
        try:
            wf_result = run_walk_forward_validation(
                data_map=data_map,
                buy_params=buy,
                sell_params=sell,
                n_splits=5,
            )
            # Also get headline
            is_result = run_variant(
                name=name,
                data_map=data_map,
                buy_params=buy,
                sell_params=sell,
            )
            headline = {
                "name": name,
                "total_return_pct": is_result.total_return_pct if is_result else None,
                "trades": is_result.trades if is_result else None,
                "win_rate_pct": is_result.win_rate_pct if is_result else None,
                "max_drawdown_pct": is_result.max_drawdown_pct if is_result else None,
            }
            result = {
                "name": name,
                "headline": headline,
                "walk_forward": wf_result,
            }
            results.append(result)
            print(f"  {name}: headline={headline.get('total_return_pct')}%, mean_OOS={wf_result.get('mean_oos_return_pct')}%, +folds={wf_result.get('positive_folds')}", flush=True)
        except Exception as e:
            print(f"  {name}: ERROR: {e}", flush=True)
            results.append({"name": name, "error": str(e)})

    out = {
        "generated_at": datetime.now().astimezone().isoformat(),
        "label": "oos_top5_wf_validate",
        "n_candidates": len(CANDIDATES),
        "universe_size": len(data_map),
        "results": results,
    }
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = OUT_DIR / f"oos_top5_wf_validate_{ts}.json"
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(out, indent=2, default=str))
    print(f"[oos_top5_wf] Saved: {path}", flush=True)
    return out

if __name__ == "__main__":
    main()
