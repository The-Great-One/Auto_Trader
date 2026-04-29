#!/usr/bin/env python3
"""Symbol-selection risk sweep for breakout+ATR structural candidates.

Purpose: the breakout family has positive OOS CAGR but edge is not broad enough.
This script tests whether selecting symbols using train-period evidence improves
OOS robustness without lookahead.
"""
from __future__ import annotations

import json
import os
import sys
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

os.environ.setdefault("AT_DISABLE_FILE_LOGGING", "1")
os.environ.setdefault("AT_RESEARCH_MODE", "1")
os.environ.setdefault("AT_LAB_PRECACHE", "0")

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import explore_strategies as ex  # noqa: E402
from scripts import validate_breakout_atr_pipeline as val  # noqa: E402

REPORTS = ROOT / "reports"
REPORTS.mkdir(exist_ok=True)


def _period_dates(data_map: dict[str, pd.DataFrame]) -> tuple[str, str]:
    starts, ends = [], []
    for df in data_map.values():
        d = pd.to_datetime(df["Date"])
        starts.append(d.min())
        ends.append(d.max())
    return min(starts).strftime("%Y-%m-%d"), max(ends).strftime("%Y-%m-%d")


def _per_symbol_period(precomputed: dict[str, pd.DataFrame], params: dict[str, Any], start: str, end: str, trend_filter: bool, atr_pct_max: float | None, max_hold: int | None) -> dict[str, dict[str, Any]]:
    entry = val._entry_fn(params, trend_filter=trend_filter, atr_pct_max=atr_pct_max)
    exit_ = val._exit_fn(params, max_hold=max_hold)
    out = {}
    for sym, df in precomputed.items():
        r = val._simulate_symbol_period(df, entry, exit_, pd.Timestamp(start), pd.Timestamp(end))
        if not r.get("skip"):
            out[sym] = r
    return out


def _aggregate(rows: dict[str, dict[str, Any]], start: str, end: str) -> dict[str, Any]:
    if not rows:
        return {"symbols": 0, "active_symbols": 0, "profitable_symbols": 0, "trades": 0, "total_return_pct": 0, "cagr_pct": 0, "max_drawdown_pct": 0, "win_rate_pct": 0, "sharpe_ratio": 0}
    symbols = len(rows)
    total_start = 100000.0 * symbols
    total_final = sum(float(r["final_equity"]) for r in rows.values())
    ret = (total_final / total_start - 1) * 100.0
    years = max((pd.Timestamp(end) - pd.Timestamp(start)).days / 365.25, 0.1)
    cagr = ((total_final / total_start) ** (1.0 / years) - 1.0) * 100.0 if total_final > 0 else -100.0
    trades = sum(int(r["trades"]) for r in rows.values())
    wins = sum(int(r["wins"]) for r in rows.values())
    active = sum(1 for r in rows.values() if int(r["trades"]) > 0)
    prof = sum(1 for r in rows.values() if float(r["total_return_pct"]) > 0)
    return {
        "symbols": symbols,
        "active_symbols": active,
        "profitable_symbols": prof,
        "trades": trades,
        "win_rate_pct": round((wins / max(1, trades)) * 100.0, 2),
        "total_return_pct": round(float(ret), 2),
        "cagr_pct": round(float(cagr), 2),
        "max_drawdown_pct": round(float(np.mean([float(r["max_drawdown_pct"]) for r in rows.values()])), 2),
        "sharpe_ratio": round(float(np.mean([float(r["sharpe_ratio"]) for r in rows.values()])), 2),
    }


def main() -> int:
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--split-date", default="2024-04-24")
    p.add_argument("--recent-start", default="2025-04-24")
    p.add_argument("--min-train-return", type=float, default=0.0)
    p.add_argument("--min-train-trades", type=int, default=3)
    args = p.parse_args()

    data_map = ex._load_data()
    start, end = _period_dates(data_map)
    train_end = (pd.Timestamp(args.split_date) - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    precomputed = {s: ex._compute_indicators(df) for s, df in data_map.items()}

    base_candidates = [
        {"lookback": 10, "atr_trail": 3.0, "vol_mult": 1.5, "trend_filter": True, "atr_pct_max": None, "max_hold": None},
        {"lookback": 10, "atr_trail": 3.0, "vol_mult": 1.2, "trend_filter": True, "atr_pct_max": None, "max_hold": None},
        {"lookback": 10, "atr_trail": 3.0, "vol_mult": 1.0, "trend_filter": True, "atr_pct_max": None, "max_hold": None},
        {"lookback": 10, "atr_trail": 2.5, "vol_mult": 1.2, "trend_filter": True, "atr_pct_max": None, "max_hold": None},
    ]
    selection_rules = []
    for min_ret in [0, 5, 10, 20]:
        for min_trades in [2, 3, 5, 8]:
            selection_rules.append({"min_train_return_pct": min_ret, "min_train_trades": min_trades})

    ranked = []
    for ci, cand in enumerate(base_candidates, 1):
        params = {k: cand[k] for k in ["lookback", "atr_trail", "vol_mult"]}
        tf = bool(cand["trend_filter"])
        train_rows = _per_symbol_period(precomputed, params, start, train_end, tf, cand["atr_pct_max"], cand["max_hold"])
        test_rows_all = _per_symbol_period(precomputed, params, args.split_date, end, tf, cand["atr_pct_max"], cand["max_hold"])
        recent_rows_all = _per_symbol_period(precomputed, params, args.recent_start, end, tf, cand["atr_pct_max"], cand["max_hold"])
        for rule in selection_rules:
            selected = [
                s for s, r in train_rows.items()
                if float(r["total_return_pct"]) >= rule["min_train_return_pct"] and int(r["trades"]) >= rule["min_train_trades"]
            ]
            if len(selected) < 15:
                continue
            test_rows = {s: test_rows_all[s] for s in selected if s in test_rows_all}
            recent_rows = {s: recent_rows_all[s] for s in selected if s in recent_rows_all}
            test = _aggregate(test_rows, args.split_date, end)
            recent = _aggregate(recent_rows, args.recent_start, end)
            score = test["cagr_pct"] * 2 + recent["cagr_pct"] + min(test["trades"] / 100, 2) + (test["profitable_symbols"] / max(1, test["symbols"])) * 5 + max(test["max_drawdown_pct"], -60) * 0.15
            verdict = "promote_symbol_filtered_breakout" if test["cagr_pct"] >= 12 and recent["cagr_pct"] >= 8 and test["max_drawdown_pct"] >= -30 and test["profitable_symbols"] / max(1, test["symbols"]) >= 0.55 and test["trades"] >= 100 else "needs_more_filtering"
            ranked.append({
                "candidate": cand,
                "selection_rule": rule,
                "selected_symbols": selected,
                "selected_symbol_count": len(selected),
                "test": test,
                "recent": recent,
                "score": round(float(score), 3),
                "verdict": verdict,
            })
            print(f"cand {ci} rule {rule}: n={len(selected)} test_cagr={test['cagr_pct']} dd={test['max_drawdown_pct']} prof={test['profitable_symbols']}/{test['symbols']} verdict={verdict}", flush=True)

    ranked.sort(key=lambda r: (r["verdict"] == "promote_symbol_filtered_breakout", r["score"], r["test"]["cagr_pct"]), reverse=True)
    best = ranked[0] if ranked else None
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    payload = {
        "generated_at": datetime.now().isoformat(),
        "source": "breakout_atr_symbol_filter_sweep",
        "data_context": {"symbols_loaded": len(data_map), "date_start": start, "date_end": end, "split_date": args.split_date, "recent_start": args.recent_start},
        "best": best,
        "ranked": ranked,
        "pipeline_decision": best["verdict"] if best else "no_candidate",
        "next_action": "Implement structural Optuna/risk-size sweep on selected-symbol breakout" if best and best["verdict"] == "promote_symbol_filtered_breakout" else "Broaden selection/risk filters before promotion",
    }
    out = REPORTS / f"breakout_atr_symbol_filter_sweep_{ts}.json"
    latest = REPORTS / "breakout_atr_symbol_filter_latest.json"
    out.write_text(json.dumps(payload, indent=2, default=str))
    latest.write_text(json.dumps(payload, indent=2, default=str))
    print(json.dumps({"path": str(out), "decision": payload["pipeline_decision"], "best": best}, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
