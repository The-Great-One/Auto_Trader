#!/usr/bin/env python3
"""
CAGR hunt for RULE_SET_8 (Adaptive Regime-Switching).
Tests combinations of regime parameters + entry/sell params.
"""

from __future__ import annotations

import json
import os
import sys
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

os.environ.setdefault("AT_DISABLE_FILE_LOGGING", "1")

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from Auto_Trader.utils import Indicators
from scripts import weekly_strategy_lab as lab
from scripts.weekly_universe_cagr_check import run_baseline_detailed

OUT_DIR = ROOT / "reports"
STATUS_FILE = OUT_DIR / "rs8_hunt_status.json"
HISTORY_FILE = OUT_DIR / "rs8_cagr_hunt_history.jsonl"

# ── Variant Blueprints ──

RS8_VARIANTS = []

# Pass 1: Regime detection thresholds
for adx_thresh, adx_sideways in [(15, 20), (20, 25), (25, 30), (15, 25), (20, 30)]:
    RS8_VARIANTS.append({
        "name": f"regime_adx{adx_thresh}_sideways{adx_sideways}",
        "buy": {"regime_adx_threshold": adx_thresh, "regime_adx_sideways_max": adx_sideways},
        "sell": {},
    })

# Pass 2: Bull regime — loosen gates
for adx_min, vol_mult, rsi_floor in [
    (5, 0.3, 30), (8, 0.5, 35), (5, 0.5, 30), (8, 0.3, 35), (10, 0.75, 40),
]:
    RS8_VARIANTS.append({
        "name": f"bull_a{adx_min}_v{vol_mult}_r{rsi_floor}",
        "buy": {"bull_adx_min": adx_min, "bull_volume_confirm_mult": vol_mult, "bull_rsi_floor": rsi_floor},
        "sell": {},
    })

# Pass 3: Sideways regime — loosen mean-reversion gates
for rsi_os, stoch_os, cmf_min in [
    (30, 20, -0.15), (35, 25, -0.10), (40, 30, -0.15), (35, 25, -0.05),
    (30, 20, -0.05), (40, 20, -0.10),
]:
    RS8_VARIANTS.append({
        "name": f"side_rsi{rsi_os}_stoch{stoch_os}_cmf{cmf_min}",
        "buy": {"side_rsi_oversold": rsi_os, "side_stoch_k_oversold": stoch_os, "side_cmf_min": cmf_min},
        "sell": {},
    })

# Pass 4: Bull + sideways combined
for bull_adx, side_rsi in [(5, 30), (8, 35), (5, 40), (10, 30)]:
    RS8_VARIANTS.append({
        "name": f"combo_ba{bull_adx}_sr{side_rsi}",
        "buy": {
            "bull_adx_min": bull_adx, "bull_volume_confirm_mult": 0.5, "bull_rsi_floor": 35,
            "side_rsi_oversold": side_rsi, "side_stoch_k_oversold": 25, "side_cmf_min": -0.10,
        },
        "sell": {},
    })

# Pass 5: Exit strategy variations
EXIT_VARIANTS = [
    {"name": "bep3_ts15", "sell": {"breakeven_trigger_pct": 3.0, "time_stop_bars": 15}},
    {"name": "bep2_ts10", "sell": {"breakeven_trigger_pct": 2.0, "time_stop_bars": 10}},
    {"name": "trail3", "sell": {"trailing_stop_atr_mult": 3.0}},
    {"name": "trail4", "sell": {"trailing_stop_atr_mult": 4.0}},
    {"name": "bep0_ts20", "sell": {"breakeven_trigger_pct": 0.0, "time_stop_bars": 20}},
]

# Pair top combos with exit strategies
TOP_COMBO_NAMES = [
    "combo_ba5_sr30",
    "combo_ba8_sr35",
    "combo_ba5_sr40",
    "combo_ba10_sr30",
    "bull_a5_v0.3_r30",
    "bull_a8_v0.5_r35",
]

for combo_name in TOP_COMBO_NAMES:
    combo_bp = next(v for v in RS8_VARIANTS if v["name"] == combo_name)
    for exit_bp in EXIT_VARIANTS:
        RS8_VARIANTS.append({
            "name": f"{combo_name}__{exit_bp['name']}",
            "buy": {**combo_bp["buy"]},
            "sell": {**exit_bp["sell"]},
        })

# Pass 6: Risk management variations
for max_loss, sector_cap in [(5.0, 25.0), (3.0, 20.0), (7.0, 30.0), (5.0, 20.0)]:
    RS8_VARIANTS.append({
        "name": f"risk_ml{max_loss}_sc{sector_cap}",
        "buy": {},
        "sell": {"max_position_loss_pct": max_loss, "sector_cap_pct": sector_cap},
    })

print(f"Total RS8 variants: {len(RS8_VARIANTS)}")


# ── Data Loading ──

def load_kite_symbols(min_rows: int = 260) -> dict[str, pd.DataFrame]:
    hist_dir = ROOT / "intermediary_files" / "Hist_Data"
    data_map: dict[str, pd.DataFrame] = {}
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


# ── Backtest Runner ──

@dataclass
class VariantResult:
    name: str
    total_return_pct: float = 0.0
    cagr_pct: float = 0.0
    max_drawdown_pct: float = 0.0
    trades: int = 0
    win_rate_pct: float = 0.0
    sharpe: float = 0.0
    active_symbols: int = 0
    selection_score: float = 0.0
    error: str = ""


def _compute_curve_metrics(equity_curve: list[float]) -> dict:
    if len(equity_curve) < 20:
        return {"cagr_pct": 0.0, "sharpe": 0.0, "max_dd_pct": 0.0}
    s = pd.Series(equity_curve, dtype=float)
    final = s.iloc[-1]
    years = len(s) / 252.0
    cagr = ((final / s.iloc[0]) ** (1.0 / max(years, 0.01)) - 1.0) * 100.0
    peak = s.cummax()
    dd = ((s - peak) / peak * 100.0).min()
    rets = s.pct_change().dropna()
    sharpe = float(rets.mean() / rets.std() * np.sqrt(252)) if len(rets) > 5 and rets.std() > 0 else 0.0
    return {"cagr_pct": round(cagr, 2), "sharpe": round(sharpe, 2), "max_dd_pct": round(float(dd), 2)}


def run_variant(data_map: dict[str, pd.DataFrame], buy: dict, sell: dict) -> VariantResult:
    """Run variant with RS8 by temporarily patching it as the active rule set."""
    # We need to make the lab use RULE_SET_8 instead of RULE_SET_7
    # The lab's run_baseline_detailed calls buy_or_sell from the active rule set
    # So we temporarily swap the module reference
    
    import Auto_Trader.RULE_SET_8 as rs8
    
    old_r2 = dict(lab.RULE_SET_2.CONFIG)
    old_r7 = dict(lab.RULE_SET_7.CONFIG)
    old_buy_or_sell = lab.RULE_SET_7.buy_or_sell
    old_evaluate = lab.RULE_SET_7.evaluate_signal
    
    try:
        # Patch RS8 config
        rs8.CONFIG.update(buy)
        rs8.CONFIG.update(sell)
        
        # Swap active rule set
        lab.RULE_SET_7.buy_or_sell = rs8.buy_or_sell
        lab.RULE_SET_7.evaluate_signal = rs8.evaluate_signal
        lab.RULE_SET_2.CONFIG.clear()
        lab.RULE_SET_2.CONFIG.update(old_r2)
        lab.RULE_SET_2.CONFIG.update(sell)
        
        result, details, sim_meta = run_baseline_detailed(data_map)
        eq = sim_meta.get("portfolio_equity")
        curve = _compute_curve_metrics(eq) if eq is not None and len(eq) > 20 else {"cagr_pct": 0.0, "sharpe": 0.0}
        
        return VariantResult(
            name="",
            total_return_pct=round(result.total_return_pct, 2),
            cagr_pct=curve["cagr_pct"],
            max_drawdown_pct=round(result.max_drawdown_pct, 2),
            trades=result.trades,
            win_rate_pct=round(result.win_rate_pct, 1),
            sharpe=curve["sharpe"],
            active_symbols=sum(1 for v in details.values() if int(v.get("trades", 0) or 0) > 0),
            selection_score=round(result.selection_score, 3),
        )
    except Exception as e:
        return VariantResult(name="", error=str(e)[:200])
    finally:
        # Restore
        lab.RULE_SET_7.buy_or_sell = old_buy_or_sell
        lab.RULE_SET_7.evaluate_signal = old_evaluate
        lab.RULE_SET_2.CONFIG.clear()
        lab.RULE_SET_2.CONFIG.update(old_r2)
        # Reset RS8 config to defaults
        import importlib
        importlib.reload(rs8)


# ── Main ──

def save_status(status: dict):
    STATUS_FILE.write_text(json.dumps(status, indent=2))


def append_history(entry: dict):
    # Convert numpy types to native Python for JSON serialization
    import math
    def _convert(obj):
        if isinstance(obj, (np.integer,)):
            return int(obj)
        if isinstance(obj, (np.floating,)):
            return float(obj)
        if isinstance(obj, (np.bool_,)):
            return bool(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return obj
    cleaned = {k: _convert(v) for k, v in entry.items()}
    with open(HISTORY_FILE, "a") as f:
        f.write(json.dumps(cleaned) + "\n")


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--shard", type=str, default="")
    parser.add_argument("--train-start", type=str, default=None, help="Walk-forward: train start date")
    parser.add_argument("--train-end", type=str, default=None, help="Walk-forward: train end (also test start)")
    parser.add_argument("--test-end", type=str, default=None, help="Walk-forward: test end date")
    args = parser.parse_args()

    variants = RS8_VARIANTS
    if args.limit > 0:
        variants = variants[args.offset:args.offset + args.limit]

    shard_label = args.shard or f"offset{args.offset}"

    print("=" * 60)
    print(f"RS8 CAGR HUNT — Adaptive Regime — Shard: {shard_label}")
    print(f"Variants: {len(variants)}")
    if args.train_end:
        print(f"Walk-forward: train={args.train_start}→{args.train_end}, test={args.train_end}→{args.test_end}")
    print("=" * 60)

    # Load symbols
    data_map = load_kite_symbols()
    if not data_map:
        print("ERROR: No symbols loaded")
        return 1

    # Walk-forward: split data if requested
    if args.train_end:
        full_map = data_map
        train_map = {}
        test_map = {}
        train_start = pd.Timestamp(args.train_start) if args.train_start else None
        train_end = pd.Timestamp(args.train_end)
        test_end = pd.Timestamp(args.test_end) if args.test_end else None
        
        for sym, df in full_map.items():
            tdf = df.copy()
            if train_start:
                tdf = tdf[tdf["Date"] >= train_start]
            train_df = tdf[tdf["Date"] < train_end].reset_index(drop=True)
            test_df = tdf[tdf["Date"] >= train_end].reset_index(drop=True)
            if test_end:
                test_df = test_df[test_df["Date"] <= test_end].reset_index(drop=True)
            if len(train_df) >= 260:
                train_map[sym] = train_df
            if len(test_df) >= 260:
                test_map[sym] = test_df
        
        print(f"Walk-forward split: train={len(train_map)} sym, test={len(test_map)} sym")
        
        # Run on both periods
        best_cagr = -999
        best_name = ""
        best_result = None
        
        for i, variant in enumerate(variants, 1):
            name = variant["name"]
            buy = variant["buy"]
            sell = variant["sell"]
            
            print(f"\n[{i}/{len(variants)}] {name}")
            
            # Full 5Y
            full_result = run_variant(data_map, buy, sell)
            if full_result.error:
                print(f"  Full: ERROR - {full_result.error}")
                continue
            print(f"  Full 5Y: CAGR={full_result.cagr_pct:.2f}%  trades={full_result.trades}")
            
            # Train
            train_result = run_variant(train_map, buy, sell)
            if train_result.error:
                print(f"  Train: ERROR - {train_result.error}")
                continue
            print(f"  Train:  CAGR={train_result.cagr_pct:.2f}%  trades={train_result.trades}")
            
            # Test
            test_result = run_variant(test_map, buy, sell)
            if test_result.error:
                print(f"  Test: ERROR - {test_result.error}")
                continue
            print(f"  Test:   CAGR={test_result.cagr_pct:.2f}%  trades={test_result.trades}")
            
            # Degradation
            full_cagr = full_result.cagr_pct
            test_cagr = test_result.cagr_pct
            degradation = ((full_cagr - test_cagr) / max(abs(full_cagr), 0.01)) * 100 if full_cagr != 0 else 0
            holds_up = test_cagr > 0 and degradation < 50
            
            verdict = "✅ HOLDS UP" if holds_up else "❌ DEGRADED"
            print(f"  {verdict}  (degradation: {degradation:.1f}%)")
            
            entry = {
                "name": name,
                "full_5y_cagr": full_cagr,
                "full_5y_trades": full_result.trades,
                "train_cagr": train_result.cagr_pct,
                "train_trades": train_result.trades,
                "test_cagr": test_cagr,
                "test_trades": test_result.trades,
                "test_return_pct": test_result.total_return_pct,
                "test_max_dd_pct": test_result.max_drawdown_pct,
                "test_win_rate": test_result.win_rate_pct,
                "test_sharpe": test_result.sharpe,
                "degradation_pct": round(degradation, 1),
                "holds_up": holds_up,
                "shard": shard_label,
            }
            append_history(entry)
            
            if test_cagr > best_cagr:
                best_cagr = test_cagr
                best_name = name
                best_result = entry
            
            save_status({
                "generated_at": datetime.now().isoformat(),
                "status": "running",
                "phase": "walkforward",
                "shard": shard_label,
                "message": f"variant {i}/{len(variants)} (shard {shard_label})",
                "variants_total": len(variants),
                "variants_done": i,
                "best_variant": best_name,
                "best_cagr_pct": round(best_cagr, 2),
            })
        
        print("\n" + "=" * 60)
        print("BEST OUT-OF-SAMPLE:")
        print(f"  {best_name}: test CAGR={best_cagr:.2f}%")
        print("=" * 60)
        
        return 0

    # Standard full-period hunt
    os.environ["AT_BACKTEST_VOL_SIZING_ENABLED"] = "1"
    os.environ["AT_BACKTEST_RISK_PER_TRADE_PCT"] = "0.02"
    os.environ["AT_BACKTEST_ATR_STOP_MULT"] = "2.5"
    os.environ["AT_BACKTEST_MAX_POSITION_NOTIONAL_PCT"] = "0.10"
    os.environ["AT_MAX_SINGLE_SYMBOL_WEIGHT"] = "0.15"
    os.environ["AT_PORTFOLIO_BAND"] = "0.10"
    os.environ["AT_TARGET_EQUITY"] = "1.0"
    os.environ["AT_TARGET_ETF"] = "0.0"

    best_cagr = -999
    best_name = ""

    for i, variant in enumerate(variants, 1):
        name = variant["name"]
        buy = variant["buy"]
        sell = variant["sell"]

        print(f"\n[{i}/{len(variants)}] {name}")
        result = run_variant(data_map, buy, sell)

        if result.error:
            print(f"  ERROR: {result.error[:100]}")
            entry = {"name": name, "error": result.error, "cagr_pct": 0, "shard": shard_label}
            append_history(entry)
            continue

        print(f"  CAGR={result.cagr_pct:.2f}%  ret={result.total_return_pct:.2f}%  "
              f"dd={result.max_drawdown_pct:.2f}%  trades={result.trades}  "
              f"win={result.win_rate_pct:.1f}%  sharpe={result.sharpe:.2f}  "
              f"sym={result.active_symbols}")

        entry = {
            "name": name,
            **asdict(result),
            "shard": shard_label,
            "global_idx": args.offset + i,
        }
        append_history(entry)

        if result.cagr_pct > best_cagr:
            best_cagr = result.cagr_pct
            best_name = name

        save_status({
            "generated_at": datetime.now().isoformat(),
            "status": "running",
            "phase": "sweeping",
            "shard": shard_label,
            "message": f"variant {i}/{len(variants)} (shard {shard_label})",
            "variants_total": len(variants),
            "variants_done": i,
            "best_variant": best_name,
            "best_cagr_pct": round(best_cagr, 2),
        })

    save_status({
        "generated_at": datetime.now().isoformat(),
        "status": "complete",
        "phase": "done",
        "shard": shard_label,
        "message": f"completed {len(variants)} variants",
        "variants_total": len(variants),
        "variants_done": len(variants),
        "best_variant": best_name,
        "best_cagr_pct": round(best_cagr, 2),
    })

    print(f"\n{'='*60}")
    print(f"BEST: {best_name} at {best_cagr:.2f}% CAGR")
    print(f"{'='*60}")
    return 0


if __name__ == "__main__":
    sys.exit(main())