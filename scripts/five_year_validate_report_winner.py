#!/usr/bin/env python3
"""Validate a report winner on fresh 5-year data using the live-parity path."""
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
from scripts.weekly_universe_cagr_check import run_baseline_detailed

OUT_DIR = ROOT / "reports"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def _load_report(path: Path) -> dict:
    obj = json.loads(path.read_text())
    if "output_json" in obj:
        obj = json.loads(Path(obj["output_json"]).read_text())
    return obj


def _pick_variant(report_obj: dict, variant_name: str | None) -> dict:
    ranked = report_obj.get("ranked", [])
    if not ranked:
        raise ValueError("report has no ranked variants")
    if variant_name:
        match = next((row for row in ranked if row.get("name") == variant_name), None)
        if not match:
            raise ValueError(f"variant not found: {variant_name}")
        return match
    rec = report_obj.get("recommendation", {})
    best_name = (((rec.get("best") or {}).get("name")) or report_obj.get("best_variant"))
    match = next((row for row in ranked if row.get("name") == best_name), None)
    return match or ranked[0]


def _compute_curve_metrics(eq: pd.Series | None) -> dict:
    if eq is None or len(eq) <= 20:
        return {"cagr_pct": None, "sharpe": None, "annual_breakdown": {}}
    eq = eq.astype(float)
    total_days = int((eq.index[-1] - eq.index[0]).days)
    cagr = ((eq.iloc[-1] / max(eq.iloc[0], 1e-9)) ** (365.0 / max(1, total_days)) - 1.0) * 100.0
    rets = eq.pct_change().dropna()
    sharpe = (rets.mean() / rets.std() * np.sqrt(252.0)) if len(rets) > 5 and rets.std() > 0 else 0.0
    annual = {}
    prev = None
    df_eq = pd.DataFrame({"equity": eq})
    df_eq["year"] = df_eq.index.year
    for year, group in df_eq.groupby("year")["equity"]:
        end = float(group.iloc[-1])
        if prev is not None:
            annual[str(year)] = round((end / max(prev, 1e-9) - 1.0) * 100.0, 2)
        prev = end
    return {
        "cagr_pct": round(float(cagr), 2),
        "sharpe": round(float(sharpe), 2),
        "annual_breakdown": annual,
    }


def _summarize_symbols(details: dict[str, dict], limit: int = 20) -> dict:
    rows = []
    for symbol, stats in details.items():
        pnl = float(stats.get("realized_pnl_abs", 0.0) or 0.0) + float(stats.get("unrealized_pnl_abs", 0.0) or 0.0)
        rows.append({
            "symbol": symbol,
            "trades": int(stats.get("trades", 0) or 0),
            "wins": int(stats.get("wins", 0) or 0),
            "pnl_abs": round(pnl, 2),
            "return_pct": float(stats.get("total_return_pct", 0.0) or 0.0),
            "exposure_pct": float(stats.get("exposure_pct", 0.0) or 0.0),
            "avg_hold_days": float(stats.get("avg_hold_days", 0.0) or 0.0),
        })
    rows.sort(key=lambda row: (row["pnl_abs"], row["return_pct"]), reverse=True)
    active = [row for row in rows if row["trades"] > 0]
    return {
        "active_symbols": len(active),
        "top_positive": active[:limit],
        "top_negative": sorted(active, key=lambda row: (row["pnl_abs"], row["return_pct"]))[:limit],
    }


def _apply_sim_env(simulation: dict, *, starting_capital: float | None = None) -> dict:
    applied = {}
    env_map = (((simulation or {}).get("simulation") or {}).get("sizing_exit_sweep_env")) or simulation.get("env") or {}
    for key, value in env_map.items():
        os.environ[str(key)] = str(value)
        applied[str(key)] = str(value)
    if starting_capital is not None:
        os.environ["AT_BACKTEST_STARTING_CAPITAL"] = str(starting_capital)
        applied["AT_BACKTEST_STARTING_CAPITAL"] = str(starting_capital)
    os.environ["AT_LAB_MATCH_LIVE"] = "1"
    os.environ["AT_LAB_RNN_ENABLED"] = "0"
    applied["AT_LAB_MATCH_LIVE"] = "1"
    applied["AT_LAB_RNN_ENABLED"] = "0"
    return applied


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--report", default=str(OUT_DIR / "sizing_exit_sweep_latest.json"))
    parser.add_argument("--variant")
    parser.add_argument("--starting-capital", type=float, default=100000.0)
    args = parser.parse_args()

    report_path = Path(args.report)
    report_obj = _load_report(report_path)
    variant = _pick_variant(report_obj, args.variant)

    rec = report_obj.get("recommendation", {})
    data_context = rec.get("data_context", {}) or report_obj.get("data_context", {}) or {}
    symbols = list(data_context.get("loaded_symbols") or variant.get("symbols_tested") or [])
    if not symbols:
        raise ValueError("no symbols found in report")

    variant_params = dict((variant.get("params") or {}))
    buy = dict(variant_params.get("buy") or variant.get("buy") or {})
    sell = dict(variant_params.get("sell") or variant.get("sell") or {})
    rnn = dict(variant_params.get("rnn") or {"enabled": False})
    applied_env = _apply_sim_env(variant_params or variant, starting_capital=args.starting_capital)

    print(f"Variant: {variant.get('name')}")
    print(f"Symbols: {len(symbols)}")
    print(f"Downloading fresh {PERIOD} history...")
    data_map, data_ctx = load_5y_data(symbols)
    print(f"Loaded: {len(data_map)}, skipped: {len(data_ctx.get('skipped_symbols', {}))}")
    if not data_map:
        raise ValueError("no 5y data loaded")

    old_r2 = dict(lab.RULE_SET_2.CONFIG)
    old_r7 = dict(lab.RULE_SET_7.CONFIG)
    try:
        lab.RULE_SET_2.CONFIG.update(sell)
        lab.RULE_SET_7.CONFIG.update(buy)
        result, details, sim_meta = run_baseline_detailed(data_map)
    finally:
        lab.RULE_SET_2.CONFIG.clear()
        lab.RULE_SET_2.CONFIG.update(old_r2)
        lab.RULE_SET_7.CONFIG.clear()
        lab.RULE_SET_7.CONFIG.update(old_r7)

    portfolio_equity = sim_meta.get("portfolio_equity")
    curve = _compute_curve_metrics(portfolio_equity)
    final_equity = float(portfolio_equity.iloc[-1]) if portfolio_equity is not None and len(portfolio_equity) else float(args.starting_capital)
    symbol_summary = _summarize_symbols(details)
    sizing = ((sim_meta.get("curve_meta") or {}).get("position_sizing") or {})
    regime = ((sim_meta.get("curve_meta") or {}).get("regime_filter") or {})

    payload = {
        "generated_at": datetime.now().astimezone().isoformat(),
        "source_report": str(report_path),
        "variant": {
            "name": variant.get("name"),
            "params": variant_params or None,
            "buy": buy,
            "sell": sell,
            "env": variant.get("env") or (((variant_params.get("simulation") or {}).get("sizing_exit_sweep_env")) or {}),
            "source_backtest_total_return_pct": variant.get("total_return_pct", variant.get("return_pct")),
            "source_backtest_drawdown_pct": variant.get("max_drawdown_pct", variant.get("drawdown_pct")),
            "source_backtest_trades": variant.get("trades"),
            "source_backtest_win_rate_pct": variant.get("win_rate_pct"),
        },
        "test_period": PERIOD,
        "data_context": data_ctx,
        "simulation_env": applied_env,
        "validation": {
            "starting_capital": float(args.starting_capital),
            "final_equity": round(final_equity, 2),
            "return_pct": result.total_return_pct,
            "drawdown_pct": result.max_drawdown_pct,
            "trades": result.trades,
            "win_rate_pct": result.win_rate_pct,
            "score": result.selection_score,
            **curve,
            "active_symbols": symbol_summary["active_symbols"],
            "sizing_buy_orders": sizing.get("buy_orders_sized"),
            "regime_blocked": regime.get("blocked_buy_signals"),
            "regime_allowed": regime.get("allowed_buy_signals"),
        },
        "symbol_summary": symbol_summary,
    }

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = OUT_DIR / f"five_year_validate_{variant.get('name')}_{ts}.json"
    out_path.write_text(json.dumps(payload, indent=2))
    print(f"Wrote {out_path}")
    print(json.dumps(payload["validation"], indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
