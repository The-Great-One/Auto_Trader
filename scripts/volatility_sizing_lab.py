#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys
from contextlib import contextmanager
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import weekly_strategy_lab as lab

OUT_DIR = ROOT / "reports"
STATUS_PATH = OUT_DIR / "volatility_sizing_lab_latest.json"
HISTORY_PATH = OUT_DIR / "volatility_sizing_lab_history.jsonl"
ANCHOR_PATH = OUT_DIR / "regime_filter_lab_20260420_155011.json"

DEFAULT_BUY = {
    "adx_min": 10,
    "volume_confirm_mult": 0.85,
    "ich_cloud_bull": 0,
    "regime_filter_enabled": 1,
    "regime_ema_fast": 50,
    "regime_ema_slow": 200,
}
DEFAULT_SELL = {"breakeven_trigger_pct": 4.0}

SIZING_VARIANTS = [
    {"AT_BACKTEST_VOL_SIZING_ENABLED": "0"},
    {"AT_BACKTEST_VOL_SIZING_ENABLED": "1", "AT_BACKTEST_RISK_PER_TRADE_PCT": "0.005", "AT_BACKTEST_ATR_STOP_MULT": "1.5", "AT_BACKTEST_MAX_POSITION_NOTIONAL_PCT": "0.15"},
    {"AT_BACKTEST_VOL_SIZING_ENABLED": "1", "AT_BACKTEST_RISK_PER_TRADE_PCT": "0.0075", "AT_BACKTEST_ATR_STOP_MULT": "1.5", "AT_BACKTEST_MAX_POSITION_NOTIONAL_PCT": "0.2"},
    {"AT_BACKTEST_VOL_SIZING_ENABLED": "1", "AT_BACKTEST_RISK_PER_TRADE_PCT": "0.0075", "AT_BACKTEST_ATR_STOP_MULT": "2.0", "AT_BACKTEST_MAX_POSITION_NOTIONAL_PCT": "0.2"},
    {"AT_BACKTEST_VOL_SIZING_ENABLED": "1", "AT_BACKTEST_RISK_PER_TRADE_PCT": "0.01", "AT_BACKTEST_ATR_STOP_MULT": "2.0", "AT_BACKTEST_MAX_POSITION_NOTIONAL_PCT": "0.2"},
    {"AT_BACKTEST_VOL_SIZING_ENABLED": "1", "AT_BACKTEST_RISK_PER_TRADE_PCT": "0.01", "AT_BACKTEST_ATR_STOP_MULT": "2.5", "AT_BACKTEST_MAX_POSITION_NOTIONAL_PCT": "0.25"},
]


def now_iso() -> str:
    return datetime.now().astimezone().isoformat()


def write_status(payload: dict) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    STATUS_PATH.write_text(json.dumps(payload, indent=2))
    with HISTORY_PATH.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload) + "\n")


def load_anchor() -> tuple[str, dict, dict, list[str]]:
    if ANCHOR_PATH.exists():
        payload = json.loads(ANCHOR_PATH.read_text())
        rec = payload.get("recommendation", {}) or {}
        best = rec.get("best", {}) or {}
        params = best.get("params", {}) or {}
        loaded = (rec.get("data_context", {}) or {}).get("loaded_symbols", []) or []
        return (
            str(best.get("name") or "regime_filter_007"),
            dict(params.get("buy", {}) or DEFAULT_BUY),
            dict(params.get("sell", {}) or DEFAULT_SELL),
            [str(x).upper() for x in loaded if str(x).strip()],
        )
    return "regime_filter_007", dict(DEFAULT_BUY), dict(DEFAULT_SELL), []


@contextmanager
def env_overrides(patch: dict[str, str]):
    old = {k: os.environ.get(k) for k in patch.keys()}
    try:
        for k, v in patch.items():
            os.environ[k] = str(v)
        yield
    finally:
        for k, prev in old.items():
            if prev is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = prev


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    anchor_name, anchor_buy, anchor_sell, anchor_symbols = load_anchor()
    if anchor_symbols:
        os.environ["AT_LAB_SYMBOLS"] = ",".join(anchor_symbols)
        os.environ.setdefault("AT_LAB_USE_APPROVED_UNIVERSE", "0")
    os.environ.setdefault("AT_LAB_MATCH_LIVE", "1")
    os.environ.setdefault("AT_LAB_RNN_ENABLED", "0")

    score_context = lab.load_scorecard_context()
    trade_context = lab.load_tradebook_context()
    fundamental_context = lab.load_fundamental_context()
    data_map, data_context = lab.load_data(trade_context, fundamental_context)

    total = len(SIZING_VARIANTS)
    results = []
    best_so_far = None

    write_status({
        "generated_at": now_iso(),
        "status": "running",
        "phase": "initializing",
        "message": "starting volatility sizing lab",
        "anchor_variant": anchor_name,
        "variants_total": total,
        "variants_done": 0,
    })

    for idx, patch in enumerate(SIZING_VARIANTS, start=1):
        name = "regime_anchor_fixed_size" if patch.get("AT_BACKTEST_VOL_SIZING_ENABLED") == "0" else f"vol_sizing_{idx:03d}"
        with env_overrides(patch):
            result = lab.run_variant(name, data_map, anchor_buy, anchor_sell, rnn_params={"enabled": False}, rnn_models={})
        result.params.setdefault("simulation", {})
        result.params["simulation"]["volatility_sizing_env"] = dict(patch)
        results.append(result)
        if best_so_far is None or (result.selection_score, result.total_return_pct) > (best_so_far.selection_score, best_so_far.total_return_pct):
            best_so_far = result
        write_status({
            "generated_at": now_iso(),
            "status": "running",
            "phase": "evaluating_variants",
            "message": "running volatility sizing variants",
            "anchor_variant": anchor_name,
            "variants_total": total,
            "variants_done": idx,
            "current_variant": name,
            "best_variant": best_so_far.name,
            "best_return_pct": best_so_far.total_return_pct,
            "best_score": best_so_far.selection_score,
            "progress_pct": round((idx / total) * 100.0, 1),
        })

    ranked = sorted(results, key=lambda r: (r.selection_score, r.total_return_pct, -abs(r.max_drawdown_pct), r.win_rate_pct), reverse=True)
    baseline = next(r for r in ranked if r.name == "regime_anchor_fixed_size")
    best = ranked[0]
    payload = {
        "recommendation": {
            "generated_at": now_iso(),
            "lab_type": "volatility_sizing_followup",
            "anchor_variant": anchor_name,
            "production_rule_model": "BUY=RULE_SET_7, SELL=RULE_SET_2",
            "data_context": data_context,
            "scorecard_context": score_context,
            "tradebook_context": trade_context,
            "baseline": asdict(baseline),
            "best": asdict(best),
            "tested_variants": len(ranked),
            "improvement_return_pct": round(best.total_return_pct - baseline.total_return_pct, 2),
            "improvement_score": round(best.selection_score - baseline.selection_score, 3),
        },
        "ranked": [asdict(r) for r in ranked],
    }

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = OUT_DIR / f"volatility_sizing_lab_{ts}.json"
    csv_path = OUT_DIR / f"volatility_sizing_lab_{ts}.csv"
    json_path.write_text(json.dumps(payload, indent=2))
    pd.DataFrame([asdict(r) for r in ranked]).to_csv(csv_path, index=False)

    write_status({
        "generated_at": now_iso(),
        "status": "completed",
        "phase": "done",
        "message": "volatility sizing lab complete",
        "anchor_variant": anchor_name,
        "best_variant": best.name,
        "best_return_pct": best.total_return_pct,
        "best_score": best.selection_score,
        "variants_total": total,
        "variants_done": total,
        "output_json": str(json_path),
        "output_csv": str(csv_path),
    })
    print(json.dumps(payload["recommendation"], indent=2))
    print(f"Saved: {json_path}")
    print(f"Saved: {csv_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
