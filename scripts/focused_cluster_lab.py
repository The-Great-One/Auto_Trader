#!/usr/bin/env python3
from __future__ import annotations

import json
import os
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

import pandas as pd

from scripts import weekly_strategy_lab as lab

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "reports"
STATUS_PATH = OUT_DIR / "focused_cluster_lab_latest.json"
HISTORY_PATH = OUT_DIR / "focused_cluster_lab_history.jsonl"

BUY_PATCHES = [
    {"adx_min": 10, "volume_confirm_mult": 0.85, "ich_cloud_bull": 0},
    {"adx_min": 10, "volume_confirm_mult": 0.9, "ich_cloud_bull": 0},
    {"adx_min": 12, "volume_confirm_mult": 0.8, "ich_cloud_bull": 0},
    {"adx_min": 12, "volume_confirm_mult": 0.85, "ich_cloud_bull": 0},
    {"adx_min": 12, "volume_confirm_mult": 0.9, "ich_cloud_bull": 0},
    {"adx_min": 12, "volume_confirm_mult": 0.9, "ich_cloud_bull": 0, "rsi_floor": 40},
    {"adx_min": 12, "volume_confirm_mult": 0.9, "ich_cloud_bull": 0, "stoch_pull_max": 85, "stoch_momo_max": 90},
    {"adx_min": 12, "volume_confirm_mult": 0.9, "ich_cloud_bull": 0, "max_extension_atr": 3.5},
    {"adx_min": 12, "volume_confirm_mult": 0.9, "ich_cloud_bull": 0, "vwap_buy_above": 0},
    {"adx_min": 14, "volume_confirm_mult": 0.9, "ich_cloud_bull": 0},
]

SELL_PATCHES = [
    {},
    {"breakeven_trigger_pct": 3.5},
    {"breakeven_trigger_pct": 4.0},
    {"breakeven_trigger_pct": 4.5},
    {"breakeven_trigger_pct": 5.0},
    {"breakeven_trigger_pct": 4.0, "fund_time_stop_bars": 18},
    {"breakeven_trigger_pct": 4.0, "equity_time_stop_bars": 12},
    {"breakeven_trigger_pct": 5.0, "equity_time_stop_bars": 15},
]


def now_iso() -> str:
    return datetime.now().astimezone().isoformat()


def write_status(payload: dict) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    STATUS_PATH.write_text(json.dumps(payload, indent=2))
    with HISTORY_PATH.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload) + "\n")


def build_grid() -> list[tuple[str, dict, dict]]:
    combos: list[tuple[str, dict, dict]] = []
    idx = 0
    for buy_patch in BUY_PATCHES:
        for sell_patch in SELL_PATCHES:
            idx += 1
            combos.append((f"focused_cluster_{idx:03d}", buy_patch, sell_patch))
    offset = max(0, int(os.getenv("AT_FOCUSED_CLUSTER_OFFSET", "0") or "0"))
    limit_raw = os.getenv("AT_FOCUSED_CLUSTER_LIMIT", "0") or "0"
    limit = int(limit_raw)
    if limit > 0:
        combos = combos[offset : offset + limit]
    elif offset > 0:
        combos = combos[offset:]
    return combos


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    score_context = lab.load_scorecard_context()
    trade_context = lab.load_tradebook_context()
    fundamental_context = lab.load_fundamental_context()
    data_map, data_context = lab.load_data(trade_context, fundamental_context)
    rnn_models = {}

    variants = build_grid()
    total = len(variants) + 1
    results = []

    write_status(
        {
            "generated_at": now_iso(),
            "status": "running",
            "phase": "initializing",
            "message": "starting focused cluster lab",
            "variants_total": total,
            "variants_done": 0,
            "anchor_variant": "curated_combo_032",
        }
    )

    baseline = lab.run_variant(
        "baseline_current",
        data_map,
        {},
        {},
        rnn_params={"enabled": False},
        rnn_models=rnn_models,
    )
    results.append(baseline)
    write_status(
        {
            "generated_at": now_iso(),
            "status": "running",
            "phase": "evaluating_variants",
            "message": "baseline complete",
            "variants_total": total,
            "variants_done": 1,
            "current_variant": "baseline_current",
            "anchor_variant": "curated_combo_032",
            "best_variant": baseline.name,
            "best_return_pct": baseline.total_return_pct,
            "best_score": baseline.selection_score,
        }
    )

    best_so_far = baseline
    for idx, (name, buy_patch, sell_patch) in enumerate(variants, start=2):
        result = lab.run_variant(
            name,
            data_map,
            buy_patch,
            sell_patch,
            rnn_params={"enabled": False},
            rnn_models=rnn_models,
        )
        results.append(result)
        if (result.selection_score, result.total_return_pct) > (
            best_so_far.selection_score,
            best_so_far.total_return_pct,
        ):
            best_so_far = result
        write_status(
            {
                "generated_at": now_iso(),
                "status": "running",
                "phase": "evaluating_variants",
                "message": "running focused cluster variants",
                "variants_total": total,
                "variants_done": idx,
                "current_variant": name,
                "anchor_variant": "curated_combo_032",
                "best_variant": best_so_far.name,
                "best_return_pct": best_so_far.total_return_pct,
                "best_score": best_so_far.selection_score,
                "progress_pct": round((idx / total) * 100, 1),
            }
        )

    ranked = sorted(
        results,
        key=lambda r: (r.selection_score, r.total_return_pct, -abs(r.max_drawdown_pct), r.win_rate_pct),
        reverse=True,
    )
    baseline = next(r for r in ranked if r.name == "baseline_current")
    best = ranked[0]
    batch = {
        "offset": max(0, int(os.getenv("AT_FOCUSED_CLUSTER_OFFSET", "0") or "0")),
        "limit": int(os.getenv("AT_FOCUSED_CLUSTER_LIMIT", "0") or "0"),
        "tested_variants": len(ranked),
        "full_variant_count": 1 + len(BUY_PATCHES) * len(SELL_PATCHES),
    }
    payload = {
        "recommendation": {
            "generated_at": now_iso(),
            "lab_type": "focused_cluster_followup",
            "anchor_variant": "curated_combo_032",
            "production_rule_model": "BUY=RULE_SET_7, SELL=RULE_SET_2",
            "data_context": data_context,
            "scorecard_context": score_context,
            "tradebook_context": trade_context,
            "baseline": asdict(baseline),
            "best": asdict(best),
            "tested_variants": len(ranked),
            "improvement_return_pct": round(best.total_return_pct - baseline.total_return_pct, 2),
            "improvement_score": round(best.selection_score - baseline.selection_score, 3),
            "batch": batch,
        },
        "ranked": [asdict(r) for r in ranked],
    }

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = OUT_DIR / f"focused_cluster_lab_{ts}.json"
    csv_path = OUT_DIR / f"focused_cluster_lab_{ts}.csv"
    json_path.write_text(json.dumps(payload, indent=2))
    pd.DataFrame([asdict(r) for r in ranked]).to_csv(csv_path, index=False)

    write_status(
        {
            "generated_at": now_iso(),
            "status": "completed",
            "phase": "done",
            "message": "focused cluster lab complete",
            "variants_total": total,
            "variants_done": total,
            "anchor_variant": "curated_combo_032",
            "best_variant": best.name,
            "best_return_pct": best.total_return_pct,
            "best_score": best.selection_score,
            "output_json": str(json_path),
            "output_csv": str(csv_path),
            "batch": batch,
        }
    )
    print(json.dumps(payload["recommendation"], indent=2))
    print(f"Saved: {json_path}")
    print(f"Saved: {csv_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
