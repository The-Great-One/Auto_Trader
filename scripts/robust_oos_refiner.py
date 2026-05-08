#!/usr/bin/env python3
"""Robust OOS refiner for the 30% Auto_Trader hunt.

Goal: stop optimizing only headline CAGR. This script tests variants around the
best targeted-hunt family and scores them on both full-history CAGR and recent
OOS proxy performance using the same Kite/cache + live-parity simulation path.

It is intentionally research-only and never promotes.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

os.environ.setdefault("AT_DISABLE_FILE_LOGGING", "1")
os.environ.setdefault("AT_RESEARCH_MODE", "1")
os.environ.setdefault("AT_LAB_PRECACHE", "0")
os.environ.setdefault("AT_LAB_CACHE_ONLY", "1")
os.environ["AT_BACKTEST_VOL_SIZING_ENABLED"] = "1"
os.environ["AT_BACKTEST_RISK_PER_TRADE_PCT"] = "0.05"
os.environ["AT_BACKTEST_ATR_STOP_MULT"] = "2.0"
os.environ["AT_BACKTEST_MAX_POSITION_NOTIONAL_PCT"] = "1.0"
os.environ["AT_MAX_SINGLE_SYMBOL_WEIGHT"] = "0.65"
os.environ["AT_TARGET_EQUITY"] = "1.0"
os.environ["AT_TARGET_ETF"] = "0.0"

from scripts.cagr_hunt_30_targeted import (  # noqa: E402
    ULTRA_LOOSE_BASE,
    ULTRA_LOOSE_REGIME_30_150,
    load_kite_symbols,
    run_variant,
)

OUT_DIR = ROOT / "reports"
OUT_DIR.mkdir(exist_ok=True)


def patch(base: dict, **kwargs) -> dict:
    out = dict(base)
    out.update(kwargs)
    return out


def build_variants() -> list[dict]:
    """Variants chosen to fix the validated failure mode: weak late OOS and DD."""
    variants: list[dict] = []
    # Around the current rejected best: adx15/strong23/ts4.
    for adx, strong in [(14, 24), (15, 25), (16, 25), (18, 26), (20, 28), (22, 30)]:
        for ts in [3, 4, 5]:
            variants.append({
                "name": f"oos_adx{adx}_strong{strong}_ts{ts}",
                "buy": patch(ULTRA_LOOSE_REGIME_30_150, adx_min=adx, adx_strong_min=strong),
                "sell": {"breakeven_trigger_pct": 1.5, "equity_time_stop_bars": ts},
            })
    # Add quality gates to reduce late-period churn/drawdown.
    quality_sets = [
        ("vol05", {"volume_confirm_mult": 0.5}),
        ("cmf0", {"cmf_base_min": 0.0, "cmf_strong_min": 0.0, "cmf_weak_min": -0.02}),
        ("obv0", {"obv_min_zscore": 0.0}),
        ("ext3", {"max_extension_atr": 3.0}),
        ("rsi30", {"rsi_floor": 30}),
        ("vol05_cmf0", {"volume_confirm_mult": 0.5, "cmf_base_min": 0.0, "cmf_strong_min": 0.0, "cmf_weak_min": -0.02}),
    ]
    for label, q in quality_sets:
        variants.append({
            "name": f"oos_adx15_strong23_ts4_{label}",
            "buy": patch(ULTRA_LOOSE_REGIME_30_150, adx_min=15, adx_strong_min=23, **q),
            "sell": {"breakeven_trigger_pct": 1.5, "equity_time_stop_bars": 4},
        })
    # Regime-pair search; the rejected best used 30/150 and decayed late.
    for fast, slow in [(20, 100), (20, 150), (30, 100), (30, 200), (50, 150), (50, 200)]:
        variants.append({
            "name": f"oos_regime{fast}_{slow}_adx15_ts4",
            "buy": patch(ULTRA_LOOSE_BASE, regime_filter_enabled=1, regime_ema_fast=fast, regime_ema_slow=slow, adx_min=15, adx_strong_min=23),
            "sell": {"breakeven_trigger_pct": 1.5, "equity_time_stop_bars": 4},
        })
    return variants


def subset_data(data_map: dict, limit: int) -> dict:
    if not limit or limit >= len(data_map):
        return data_map
    # Prefer deterministic broad sample while preserving enough symbols.
    items = sorted(data_map.items())
    step = max(1, len(items) // limit)
    picked = items[::step][:limit]
    return dict(picked)


def run_with_window(data_map: dict, name: str, buy: dict, sell: dict, start: str | None, end: str | None):
    old_start = os.environ.get("AT_BACKTEST_SIGNAL_START_DATE")
    old_end = os.environ.get("AT_BACKTEST_SIGNAL_END_DATE")
    try:
        if start:
            os.environ["AT_BACKTEST_SIGNAL_START_DATE"] = start
        else:
            os.environ.pop("AT_BACKTEST_SIGNAL_START_DATE", None)
        if end:
            os.environ["AT_BACKTEST_SIGNAL_END_DATE"] = end
        else:
            os.environ.pop("AT_BACKTEST_SIGNAL_END_DATE", None)
        return run_variant(data_map, buy, sell)
    finally:
        if old_start is None:
            os.environ.pop("AT_BACKTEST_SIGNAL_START_DATE", None)
        else:
            os.environ["AT_BACKTEST_SIGNAL_START_DATE"] = old_start
        if old_end is None:
            os.environ.pop("AT_BACKTEST_SIGNAL_END_DATE", None)
        else:
            os.environ["AT_BACKTEST_SIGNAL_END_DATE"] = old_end


def score(full, recent) -> float:
    # Reward full CAGR, but heavily punish recent weakness and drawdown.
    full_cagr = full.cagr_pct or 0.0
    recent_ret = recent.total_return_pct or 0.0
    recent_dd = abs(recent.max_drawdown_pct or 0.0)
    trades = recent.trades or 0
    trade_penalty = 8.0 if trades < 50 else 0.0
    dd_penalty = max(0.0, recent_dd - 12.0) * 0.9
    return round(full_cagr + 2.0 * recent_ret - dd_penalty - trade_penalty, 3)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol-limit", type=int, default=120)
    ap.add_argument("--offset", type=int, default=0)
    ap.add_argument("--limit", type=int, default=0)
    # Use the last ~2 WF folds as a recent robustness proxy. A 2025-only window
    # can be too sparse on sampled universes and hide useful-but-weak candidates.
    ap.add_argument("--recent-start", default="2024-08-20")
    ap.add_argument("--label", default="robust_oos_refiner")
    args = ap.parse_args()

    all_variants = build_variants()
    variants = all_variants[args.offset:]
    if args.limit:
        variants = variants[: args.limit]

    print(
        f"[{datetime.now():%Y-%m-%d %H:%M:%S}] Robust OOS refiner: "
        f"variants={len(variants)} offset={args.offset} total={len(all_variants)} "
        f"symbol_limit={args.symbol_limit}",
        flush=True,
    )
    data_map = subset_data(load_kite_symbols(), args.symbol_limit)
    print(f"Using {len(data_map)} symbols", flush=True)

    rows = []
    history_path = OUT_DIR / f"{args.label}_history.jsonl"
    for idx, bp in enumerate(variants, 1):
        t0 = time.time()
        full = run_with_window(data_map, bp["name"], bp["buy"], bp.get("sell", {}), None, None)
        recent = run_with_window(data_map, bp["name"] + "_recent", bp["buy"], bp.get("sell", {}), args.recent_start, None)
        row = {
            "name": bp["name"],
            "buy": bp["buy"],
            "sell": bp.get("sell", {}),
            "full": asdict(full),
            "recent": asdict(recent),
            "robust_score": score(full, recent),
            "elapsed_s": round(time.time() - t0, 1),
        }
        rows.append(row)
        with history_path.open("a") as f:
            f.write(json.dumps({"generated_at": datetime.now().isoformat(), **row}) + "\n")
        print(
            f"{idx}/{len(variants)} {bp['name']} full_cagr={full.cagr_pct}% full_dd={full.max_drawdown_pct}% "
            f"recent_ret={recent.total_return_pct}% recent_dd={recent.max_drawdown_pct}% trades={recent.trades} score={row['robust_score']} [{row['elapsed_s']}s]",
            flush=True,
        )

    ranked = sorted(rows, key=lambda r: r["robust_score"], reverse=True)
    out = {
        "generated_at": datetime.now().isoformat(),
        "label": args.label,
        "data_source": "kite_feather_cache",
        "universe_size": len(data_map),
        "recent_start": args.recent_start,
        "ranked": ranked,
    }
    out_path = OUT_DIR / f"{args.label}_{datetime.now():%Y%m%d_%H%M%S}.json"
    out_path.write_text(json.dumps(out, indent=2, default=str))
    print(f"REPORT {out_path}")
    print("TOP")
    for r in ranked[:10]:
        print(r["name"], r["robust_score"], r["full"].get("cagr_pct"), r["recent"].get("total_return_pct"), r["recent"].get("max_drawdown_pct"), r["recent"].get("trades"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
