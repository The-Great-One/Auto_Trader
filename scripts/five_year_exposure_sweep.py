#!/usr/bin/env python3
"""Coarse 5-year exposure/risk sweep for an existing report winner."""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import weekly_strategy_lab as lab
from scripts.five_year_validation import PERIOD, load_5y_data
from scripts.five_year_validate_report_winner import _compute_curve_metrics, _load_report, _pick_variant
from scripts.weekly_universe_cagr_check import run_baseline_detailed

OUT_DIR = ROOT / "reports"
OUT_DIR.mkdir(parents=True, exist_ok=True)

CONFIGS = [
    {"name": "current_1x", "risk_per_trade_pct": 0.01, "atr_stop_mult": 2.5, "max_position_notional_pct": 0.25, "target_equity": 0.75, "max_single_symbol_weight": 0.15},
    {"name": "scale_2x", "risk_per_trade_pct": 0.02, "atr_stop_mult": 2.5, "max_position_notional_pct": 0.40, "target_equity": 0.90, "max_single_symbol_weight": 0.25},
    {"name": "scale_3x", "risk_per_trade_pct": 0.03, "atr_stop_mult": 2.5, "max_position_notional_pct": 0.60, "target_equity": 1.00, "max_single_symbol_weight": 0.35},
    {"name": "scale_4x", "risk_per_trade_pct": 0.04, "atr_stop_mult": 2.5, "max_position_notional_pct": 0.80, "target_equity": 1.00, "max_single_symbol_weight": 0.50},
    {"name": "tight_stop_3x", "risk_per_trade_pct": 0.03, "atr_stop_mult": 2.0, "max_position_notional_pct": 0.60, "target_equity": 1.00, "max_single_symbol_weight": 0.35},
    {"name": "tight_stop_4x", "risk_per_trade_pct": 0.04, "atr_stop_mult": 2.0, "max_position_notional_pct": 0.80, "target_equity": 1.00, "max_single_symbol_weight": 0.50},
]


def _apply_env(cfg: dict, base_env: dict[str, str]) -> dict[str, str]:
    applied = dict(base_env)
    applied.update(
        {
            "AT_BACKTEST_VOL_SIZING_ENABLED": "1",
            "AT_BACKTEST_RISK_PER_TRADE_PCT": str(cfg["risk_per_trade_pct"]),
            "AT_BACKTEST_ATR_STOP_MULT": str(cfg["atr_stop_mult"]),
            "AT_BACKTEST_MAX_POSITION_NOTIONAL_PCT": str(cfg["max_position_notional_pct"]),
            "AT_TARGET_EQUITY": str(cfg["target_equity"]),
            "AT_TARGET_ETF": str(max(0.0, 1.0 - float(cfg["target_equity"]))),
            "AT_MAX_SINGLE_SYMBOL_WEIGHT": str(cfg["max_single_symbol_weight"]),
            "AT_LAB_MATCH_LIVE": "1",
            "AT_LAB_RNN_ENABLED": "0",
        }
    )
    for key, value in applied.items():
        os.environ[str(key)] = str(value)
    return applied


def _run_variant(data_map: dict[str, pd.DataFrame], buy: dict, sell: dict) -> tuple:
    old_r2 = dict(lab.RULE_SET_2.CONFIG)
    old_r7 = dict(lab.RULE_SET_7.CONFIG)
    try:
        lab.RULE_SET_2.CONFIG.update(sell)
        lab.RULE_SET_7.CONFIG.update(buy)
        return run_baseline_detailed(data_map)
    finally:
        lab.RULE_SET_2.CONFIG.clear()
        lab.RULE_SET_2.CONFIG.update(old_r2)
        lab.RULE_SET_7.CONFIG.clear()
        lab.RULE_SET_7.CONFIG.update(old_r7)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--report", default=str(OUT_DIR / "sizing_exit_sweep_latest.json"))
    parser.add_argument("--variant")
    args = parser.parse_args()

    report_path = Path(args.report)
    report_obj = _load_report(report_path)
    variant = _pick_variant(report_obj, args.variant)
    rec = report_obj.get("recommendation", {})
    symbols = list((rec.get("data_context") or {}).get("loaded_symbols") or variant.get("symbols_tested") or [])
    if not symbols:
        raise ValueError("no symbols found in report")

    params = dict(variant.get("params") or {})
    buy = dict(params.get("buy") or {})
    sell = dict(params.get("sell") or {})
    base_env = dict((((params.get("simulation") or {}).get("sizing_exit_sweep_env")) or {}))

    print(f"Variant: {variant.get('name')}")
    print(f"Symbols: {len(symbols)}")
    print(f"Downloading fresh {PERIOD} history once for the sweep...")
    data_map, data_ctx = load_5y_data(symbols)
    print(f"Loaded: {len(data_map)}, skipped: {len(data_ctx.get('skipped_symbols', {}))}")
    if not data_map:
        raise ValueError("no 5y data loaded")

    rows = []
    for cfg in CONFIGS:
        applied_env = _apply_env(cfg, base_env)
        result, details, sim_meta = _run_variant(data_map, buy, sell)
        curve = _compute_curve_metrics(sim_meta.get("portfolio_equity"))
        row = {
            "name": cfg["name"],
            "config": cfg,
            "env": applied_env,
            "return_pct": result.total_return_pct,
            "drawdown_pct": result.max_drawdown_pct,
            "trades": result.trades,
            "win_rate_pct": result.win_rate_pct,
            "score": result.selection_score,
            **curve,
        }
        rows.append(row)
        print(json.dumps({k: row[k] for k in ["name", "return_pct", "cagr_pct", "drawdown_pct", "trades", "win_rate_pct", "sharpe"]}, indent=2))

    ranked = sorted(rows, key=lambda row: ((row.get("cagr_pct") or -999), row.get("return_pct") or -999, -(abs(row.get("drawdown_pct") or 0))), reverse=True)
    payload = {
        "generated_at": datetime.now().astimezone().isoformat(),
        "source_report": str(report_path),
        "variant": {"name": variant.get("name"), "params": variant.get("params")},
        "test_period": PERIOD,
        "data_context": data_ctx,
        "ranked": ranked,
        "best": ranked[0] if ranked else None,
    }

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = OUT_DIR / f"five_year_exposure_sweep_{variant.get('name')}_{ts}.json"
    out_path.write_text(json.dumps(payload, indent=2))
    print(f"Wrote {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
