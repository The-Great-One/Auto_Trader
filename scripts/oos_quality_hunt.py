#!/usr/bin/env python3
"""OOS Quality Hunt — targets walk-forward OOS robustness, not headline CAGR.

Key insight from prior iterations:
- buy_adx_strong_min_18: 25.63% headline, 3.23% mean OOS, 3/5 positive folds (BEST OOS so far)
- curated_combo_263: 53.4% headline but only 2/5 positive OOS, 2.0% mean (OOS collapse)
- curated_combo_282: 38.12% headline but -0.14% mean OOS (FAILS)

Strategy: Test variants that specifically target:
1. More consistent OOS by reducing overfitting — simpler, fewer gates
2. Telegram confluence overlay using learned channel weights
3. Trailing profit-taking (inspired by high max_favorable / weak close pattern)
4. Regime-aware entry timing (avoid first/last 30min, lunch chop)
5. Trade density improvements — the buy_window_rate is only 3-4%, way too restrictive

Uses the weekly_strategy_lab framework for consistency with existing pipeline.
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

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import weekly_strategy_lab as lab
from scripts.weekly_universe_cagr_check import run_baseline_detailed
from Auto_Trader.utils import Indicators

HIST_DIR = ROOT / "intermediary_files" / "Hist_Data"
OUT_DIR = ROOT / "reports"
OUT_DIR.mkdir(exist_ok=True)


@dataclass
class VariantResult:
    name: str = ""
    total_return_pct: float = 0.0
    cagr_pct: float = 0.0
    max_drawdown_pct: float = 0.0
    trades: int = 0
    win_rate_pct: float = 0.0
    sharpe: float = 0.0
    active_symbols: int = 0
    selection_score: float = 0.0


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
                data_map[fp.stem] = enriched
        except Exception:
            skipped += 1
    print(f"[OOS_HUNT] Loaded {len(data_map)} symbols (skipped {skipped})")
    return data_map


# --- Variant definitions ---
# Focus on OOS-robust variants. The prior best OOS result was buy_adx_strong_min_18
# (3/5 positive, 3.23% mean). We need to IMPROVE OOS while keeping headline >=30%.
#
# Key observations:
# 1. Overly complex buy gates (macd_signal_cross, supertrend, cci_oversold, rsi_floor)
#    block most signals (buy_window_rate 3-4%). Reducing gate strictness
#    improves trade density which stabilizes OOS.
# 2. Telegram channels show high max_favorable (11-16%) but weak close returns (3-5% avg).
#    This means trades go in our favor but we don't capture enough. Better exits needed.
# 3. The "ultra-loose" variants that hit 50%+ headline are OVERFITTING — they
#    trade everything and collapse OOS. Need a middle ground.

OOS_HUNT_VARIANTS = [
    # === GROUP 1: Relaxed buy gates (improve trade density for OOS stability) ===
    # Current best OOS uses adx_strong_min=18. Try loosening surrounding gates
    # while keeping ADX quality filter.
    {
        "name": "oos_adx18_loosevol_rsi38",
        "buy": {"adx_strong_min": 18, "volume_confirm_mult": 0.7, "rsi_floor": 38, "ich_cloud_bull": 0, "vwap_buy_above": 0},
        "sell": {"momentum_exit_rsi": 40.0, "equity_review_rsi": 48.0, "breakeven_trigger_pct": 3.0},
    },
    {
        "name": "oos_adx18_loosevol_rsi36",
        "buy": {"adx_strong_min": 18, "volume_confirm_mult": 0.65, "rsi_floor": 36, "ich_cloud_bull": 0, "vwap_buy_above": 0},
        "sell": {"momentum_exit_rsi": 38.0, "equity_review_rsi": 45.0, "breakeven_trigger_pct": 3.5},
    },
    {
        "name": "oos_adx18_loosevol_norstoch",
        "buy": {"adx_strong_min": 18, "volume_confirm_mult": 0.6, "rsi_floor": 36, "stoch_pull_max": 95, "ich_cloud_bull": 0, "vwap_buy_above": 0},
        "sell": {"momentum_exit_rsi": 38.0, "equity_review_rsi": 45.0, "breakeven_trigger_pct": 3.0},
    },
    {
        "name": "oos_adx15_strong18_loose",
        "buy": {"adx_min": 15, "adx_strong_min": 18, "volume_confirm_mult": 0.7, "rsi_floor": 36, "stoch_pull_max": 95, "ich_cloud_bull": 0, "vwap_buy_above": 0},
        "sell": {"momentum_exit_rsi": 38.0, "equity_review_rsi": 45.0, "breakeven_trigger_pct": 3.0},
    },

    # === GROUP 2: Trailing stop / fast exit variants (capture max favorable) ===
    # Telegram data: avg max_favorable_20d = 11.24% but close returns only 3.95%.
    # These variants aim to capture more of the favorable move.
    {
        "name": "oos_adx18_trail_bep3_ts10",
        "buy": {"adx_strong_min": 18, "volume_confirm_mult": 0.85, "ich_cloud_bull": 0},
        "sell": {"breakeven_trigger_pct": 3.0, "equity_time_stop_bars": 10, "momentum_exit_rsi": 38.0},
    },
    {
        "name": "oos_adx18_trail_bep4_ts8",
        "buy": {"adx_strong_min": 18, "volume_confirm_mult": 0.85, "ich_cloud_bull": 0},
        "sell": {"breakeven_trigger_pct": 4.0, "equity_time_stop_bars": 8, "momentum_exit_rsi": 40.0},
    },
    {
        "name": "oos_adx18_trail_bep5_ts12",
        "buy": {"adx_strong_min": 18, "volume_confirm_mult": 0.85, "ich_cloud_bull": 0},
        "sell": {"breakeven_trigger_pct": 5.0, "equity_time_stop_bars": 12, "momentum_exit_rsi": 35.0},
    },
    {
        "name": "oos_adx18_partial_exit_ladder",
        "buy": {"adx_strong_min": 18, "volume_confirm_mult": 0.85, "ich_cloud_bull": 0},
        "sell": {"partial_exit_enabled": 1, "partial_exit_ladder": [(5.0, 0.5, 1.005), (10.0, 0.3, 1.03)], "breakeven_trigger_pct": 3.0, "equity_time_stop_bars": 15},
    },

    # === GROUP 3: SR structure + ADX quality (best of both worlds) ===
    # The curated_combo_263 (best headline) uses sr_bounce + sr_vpoc_reclaim.
    # Combine that with ADX quality to improve OOS.
    {
        "name": "oos_srbounce_adx18_loose",
        "buy": {"sr_bounce_enabled": 1, "sr_vpoc_reclaim_enabled": 1, "sr_near_support_pct": 0.02, "volume_confirm_mult": 0.75, "rsi_floor": 38, "ich_cloud_bull": 0, "adx_strong_min": 18},
        "sell": {"momentum_exit_rsi": 35.0, "equity_review_rsi": 42.0, "equity_time_stop_bars": 15},
    },
    {
        "name": "oos_srbounce_adx18_tight",
        "buy": {"sr_bounce_enabled": 1, "sr_vpoc_reclaim_enabled": 1, "sr_near_support_pct": 0.02, "volume_confirm_mult": 0.8, "rsi_floor": 40, "ich_cloud_bull": 0, "adx_strong_min": 18},
        "sell": {"momentum_exit_rsi": 38.0, "equity_review_rsi": 45.0, "breakeven_trigger_pct": 4.0},
    },
    {
        "name": "oos_srbreakout_adx18_v070",
        "buy": {"sr_breakout_enabled": 1, "sr_breakout_buffer_pct": 0.005, "volume_confirm_mult": 0.7, "adx_strong_min": 18, "ich_cloud_bull": 0},
        "sell": {"momentum_exit_rsi": 38.0, "equity_review_rsi": 45.0, "breakeven_trigger_pct": 3.0},
    },

    # === GROUP 4: Mean-reversion variants (OOS-differentiated regime) ===
    # Mean-rev behaves differently in OOS because it's counter-cyclical.
    {
        "name": "oos_meanrev_rsi35_adx25_tsexit",
        "buy": {"meanrev_enabled": 1, "meanrev_rsi_oversold": 35, "meanrev_adx_max": 25, "meanrev_bb_pctb_max": 0.3, "rsi_floor": 45, "adx_min": 10, "ich_cloud_bull": 0},
        "sell": {"meanrev_exit_rsi": 60, "meanrev_exit_bb_pctb": 0.8, "meanrev_exit_bars": 5, "equity_time_stop_bars": 20},
    },
    {
        "name": "oos_meanrev_rsi40_adx28",
        "buy": {"meanrev_enabled": 1, "meanrev_rsi_oversold": 40, "meanrev_adx_max": 28, "meanrev_bb_pctb_max": 0.35, "meanrev_cci_min": -100, "meanrev_stoch_k_max": 35, "rsi_floor": 45, "adx_min": 10, "ich_cloud_bull": 0},
        "sell": {"meanrev_exit_rsi": 55, "meanrev_exit_bb_pctb": 0.7, "meanrev_exit_bars": 8, "equity_time_stop_bars": 20},
    },

    # === GROUP 5: Telegram confluence overlay ===
    # Use telegram_watchlist_boost to add signal confidence for symbols
    # that Telegram channels have identified, without blindly following them.
    {
        "name": "oos_adx18_telegram_boost",
        "buy": {"adx_strong_min": 18, "volume_confirm_mult": 0.8, "ich_cloud_bull": 0},
        "sell": {"momentum_exit_rsi": 38.0, "equity_review_rsi": 45.0},
        "rnn": {"enabled": False, "telegram_overlay": True, "telegram_watchlist_boost": 0.1},
    },
    {
        "name": "oos_adx18_loose_telegram_boost",
        "buy": {"adx_strong_min": 18, "volume_confirm_mult": 0.65, "rsi_floor": 36, "stoch_pull_max": 95, "ich_cloud_bull": 0, "vwap_buy_above": 0},
        "sell": {"momentum_exit_rsi": 38.0, "equity_review_rsi": 45.0, "breakeven_trigger_pct": 3.0},
        "rnn": {"enabled": False, "telegram_overlay": True, "telegram_watchlist_boost": 0.1},
    },

    # === GROUP 6: Hybrid — combine best OOS patterns ===
    # The buy_adx_strong_min_18 baseline had the best OOS. What if we combine
    # it with the sr_bounce structure AND trailing exits?
    {
        "name": "oos_hybrid_adx18_srbounce_trail",
        "buy": {"adx_strong_min": 18, "sr_bounce_enabled": 1, "sr_near_support_pct": 0.02, "volume_confirm_mult": 0.75, "rsi_floor": 38, "ich_cloud_bull": 0},
        "sell": {"momentum_exit_rsi": 38.0, "equity_review_rsi": 45.0, "breakeven_trigger_pct": 3.0, "equity_time_stop_bars": 12},
    },
    {
        "name": "oos_hybrid_adx18_loose_srbounce_trail",
        "buy": {"adx_strong_min": 18, "sr_bounce_enabled": 1, "sr_near_support_pct": 0.02, "volume_confirm_mult": 0.7, "rsi_floor": 36, "stoch_pull_max": 95, "ich_cloud_bull": 0},
        "sell": {"momentum_exit_rsi": 38.0, "equity_review_rsi": 45.0, "breakeven_trigger_pct": 3.5, "equity_time_stop_bars": 10},
    },
    {
        "name": "oos_hybrid_adx18_srbreakout_trail",
        "buy": {"adx_strong_min": 18, "sr_breakout_enabled": 1, "sr_breakout_buffer_pct": 0.005, "volume_confirm_mult": 0.7, "ich_cloud_bull": 0},
        "sell": {"momentum_exit_rsi": 38.0, "equity_review_rsi": 45.0, "breakeven_trigger_pct": 3.0, "equity_time_stop_bars": 10},
    },

    # === GROUP 7: Ultra-simple variants (reduce overfitting) ===
    # The fewer parameters, the more likely OOS holds. Test minimal param sets.
    {
        "name": "oos_simple_adx18_only",
        "buy": {"adx_strong_min": 18, "ich_cloud_bull": 0},
        "sell": {},
    },
    {
        "name": "oos_simple_adx18_bep5",
        "buy": {"adx_strong_min": 18, "ich_cloud_bull": 0},
        "sell": {"breakeven_trigger_pct": 5.0},
    },
    {
        "name": "oos_simple_adx18_bep4_ts15",
        "buy": {"adx_strong_min": 18, "ich_cloud_bull": 0},
        "sell": {"breakeven_trigger_pct": 4.0, "equity_time_stop_bars": 15},
    },
]


def run_variant_on_data(name: str, data_map: dict, buy_params: dict, sell_params: dict, rnn_params: dict | None = None) -> dict:
    """Run a single variant using the lab's simulation framework."""
    from scripts.weekly_strategy_lab import run_variant
    result = run_variant(name, data_map, buy_params, sell_params, rnn_params=rnn_params)
    if result is None:
        return {"name": name, "error": "no_result"}
    return {
        "name": name,
        "total_return_pct": result.total_return_pct,
        "cagr_pct": result.cagr_pct,
        "max_drawdown_pct": result.max_drawdown_pct,
        "trades": result.trades,
        "win_rate_pct": result.win_rate_pct,
        "sharpe": getattr(result, "sharpe", 0.0),
        "active_symbols": getattr(result, "active_symbols", 0),
        "selection_score": getattr(result, "selection_score", 0.0),
    }


def main():
    start_time = time.time()
    print(f"[OOS_HUNT] Starting OOS Quality Hunt at {datetime.now().isoformat()}")

    data_map = load_kite_symbols()
    if not data_map:
        print("[OOS_HUNT] ERROR: No data loaded. Exiting.")
        return

    # Run all variants
    results = []
    for i, variant in enumerate(OOS_HUNT_VARIANTS):
        v_name = variant["name"]
        buy_params = variant["buy"]
        sell_params = variant["sell"]
        rnn_params = variant.get("rnn")

        print(f"[OOS_HUNT] Running variant {i+1}/{len(OOS_HUNT_VARIANTS)}: {v_name}")
        t0 = time.time()

        try:
            result = run_variant_on_data(v_name, data_map, buy_params, sell_params, rnn_params)
            elapsed = time.time() - t0
            print(f"  -> {v_name}: ret={result.get('total_return_pct','ERR')}% trades={result.get('trades','ERR')} DD={result.get('max_drawdown_pct','ERR')}% in {elapsed:.1f}s")
            results.append(result)
        except Exception as e:
            elapsed = time.time() - t0
            print(f"  -> {v_name}: ERROR: {e} ({elapsed:.1f}s)")
            results.append({"name": v_name, "error": str(e)})

    # Sort by total return
    valid_results = [r for r in results if "error" not in r]
    valid_results.sort(key=lambda r: r.get("total_return_pct", 0), reverse=True)

    print(f"\n[OOS_HUNT] === FULL HISTORY RESULTS (sorted by return) ===")
    for r in valid_results:
        print(f"  {r['name']}: ret={r.get('total_return_pct',0):.2f}% trades={r.get('trades',0)} DD={r.get('max_drawdown_pct',0):.2f}% wr={r.get('win_rate_pct',0):.1f}% score={r.get('selection_score',0):.3f}")

    # Run walk-forward validation on top candidates (top 5)
    top_candidates = valid_results[:5]
    wf_results = []

    if top_candidates:
        print(f"\n[OOS_HUNT] === WALK-FORWARD VALIDATION (top {len(top_candidates)}) ===")
        from scripts.weekly_strategy_lab import run_walk_forward_validation

        for candidate in top_candidates:
            v_name = candidate["name"]
            # Find matching variant definition
            v_def = next(v for v in OOS_HUNT_VARIANTS if v["name"] == v_name)
            buy_params = v_def["buy"]
            sell_params = v_def["sell"]
            rnn_params = v_def.get("rnn")

            print(f"[OOS_HUNT] Walk-forward: {v_name}")
            try:
                wf_result = run_walk_forward_validation(
                    v_name, data_map, buy_params, sell_params, rnn_params=rnn_params
                )
                if wf_result:
                    mean_oos = wf_result.get("mean_oos_return_pct", 0)
                    pos_folds = wf_result.get("positive_folds", 0)
                    min_oos = wf_result.get("min_oos_return_pct", -999)
                    max_oos = wf_result.get("max_oos_return_pct", -999)
                    print(f"  -> WF mean_OOS={mean_oos:.2f}% min={min_oos:.2f}% max={max_oos:.2f}% pos_folds={pos_folds}/5")
                    wf_results.append({
                        "name": v_name,
                        "walk_forward": wf_result,
                        "full_return": candidate.get("total_return_pct", 0),
                    })
            except Exception as e:
                print(f"  -> WF ERROR: {e}")
                wf_results.append({"name": v_name, "error": str(e)})

    # Save report
    report = {
        "generated_at": datetime.now().isoformat(),
        "label": "oos_quality_hunt",
        "data_source": "kite_feather_cache",
        "symbols_loaded": len(data_map),
        "variants_tested": len(OOS_HUNT_VARIANTS),
        "results": results,
        "ranked": valid_results,
        "walk_forward_results": wf_results,
        "elapsed_s": time.time() - start_time,
    }

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = OUT_DIR / f"oos_quality_hunt_{ts}.json"
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"\n[OOS_HUNT] Saved report: {report_path}")

    # Print final summary
    print(f"\n[OOS_HUNT] === FINAL SUMMARY ===")
    print(f"Variants tested: {len(OOS_HUNT_VARIANTS)}")
    print(f"Valid results: {len(valid_results)}")
    if valid_results:
        best = valid_results[0]
        print(f"Best headline: {best['name']} = {best.get('total_return_pct',0):.2f}%")
    if wf_results:
        # Find best OOS result
        best_oos = None
        best_oos_mean = -999
        for wfr in wf_results:
            if "walk_forward" in wfr and "error" not in wfr:
                mean_oos = wfr["walk_forward"].get("mean_oos_return_pct", -999)
                if mean_oos > best_oos_mean:
                    best_oos_mean = mean_oos
                    best_oos = wfr
        if best_oos:
            print(f"Best OOS: {best_oos['name']} = mean_OOS {best_oos_mean:.2f}% (headline {best_oos.get('full_return','?')}%)")

    print(f"[OOS_HUNT] Total elapsed: {time.time() - start_time:.1f}s")


if __name__ == "__main__":
    main()