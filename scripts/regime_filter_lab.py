#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import weekly_strategy_lab as lab

OUT_DIR = ROOT / "reports"
STATUS_PATH = OUT_DIR / "regime_filter_lab_latest.json"
HISTORY_PATH = OUT_DIR / "regime_filter_lab_history.jsonl"
ANCHOR_PATH = OUT_DIR / "focused_cluster_lab_latest.json"

DEFAULT_BUY = {"adx_min": 10, "volume_confirm_mult": 0.85, "ich_cloud_bull": 0}
DEFAULT_SELL = {"breakeven_trigger_pct": 4.0}

REGIME_PATCHES = [
    {},
    {"regime_filter_enabled": 1, "regime_ema_fast": 20, "regime_ema_slow": 50},
    {"regime_filter_enabled": 1, "regime_ema_fast": 20, "regime_ema_slow": 50, "regime_atr_pct_max": 0.05},
    {"regime_filter_enabled": 1, "regime_ema_fast": 20, "regime_ema_slow": 50, "regime_atr_pct_max": 0.04},
    {"regime_filter_enabled": 1, "regime_ema_fast": 20, "regime_ema_slow": 200},
    {"regime_filter_enabled": 1, "regime_ema_fast": 20, "regime_ema_slow": 200, "regime_atr_pct_max": 0.04},
    {"regime_filter_enabled": 1, "regime_ema_fast": 50, "regime_ema_slow": 200},
    {"regime_filter_enabled": 1, "regime_ema_fast": 50, "regime_ema_slow": 200, "regime_atr_pct_max": 0.05},
    {"regime_filter_enabled": 1, "regime_ema_fast": 50, "regime_ema_slow": 200, "regime_atr_pct_max": 0.04},
    {"regime_filter_enabled": 1, "regime_ema_fast": 50, "regime_ema_slow": 200, "regime_atr_pct_max": 0.03},
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
        try:
            payload = json.loads(ANCHOR_PATH.read_text())
            rec = payload.get("recommendation", {}) or {}
            best = rec.get("best", {}) or {}
            params = best.get("params", {}) or {}
            loaded = (rec.get("data_context", {}) or {}).get("loaded_symbols", []) or []
            name = str(best.get("name") or rec.get("anchor_variant") or "focused_cluster_003")
            buy = dict(params.get("buy", {}) or DEFAULT_BUY)
            sell = dict(params.get("sell", {}) or DEFAULT_SELL)
            symbols = [str(x).upper() for x in loaded if str(x).strip()]
            if buy and sell and symbols:
                return name, buy, sell, symbols
        except Exception:
            pass
    return "focused_cluster_003", dict(DEFAULT_BUY), dict(DEFAULT_SELL), []


def build_variants(anchor_buy: dict, anchor_sell: dict) -> list[tuple[str, dict, dict]]:
    variants: list[tuple[str, dict, dict]] = [("anchor_cluster_current", dict(anchor_buy), dict(anchor_sell))]
    for idx, patch in enumerate(REGIME_PATCHES, start=1):
        if not patch:
            continue
        buy = dict(anchor_buy)
        buy.update(patch)
        variants.append((f"regime_filter_{idx:03d}", buy, dict(anchor_sell)))
    offset = max(0, int(os.getenv("AT_REGIME_FILTER_OFFSET", "0") or "0"))
    limit = int(os.getenv("AT_REGIME_FILTER_LIMIT", "0") or "0")
    if limit > 0:
        variants = variants[offset : offset + limit]
    elif offset > 0:
        variants = variants[offset:]
    return variants


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    anchor_name, anchor_buy, anchor_sell, anchor_symbols = load_anchor()
    if anchor_symbols:
        os.environ["AT_LAB_SYMBOLS"] = ",".join(anchor_symbols)
        os.environ.setdefault("AT_LAB_USE_APPROVED_UNIVERSE", "0")
    os.environ.setdefault("AT_LAB_MATCH_LIVE", "1")

    score_context = lab.load_scorecard_context()
    trade_context = lab.load_tradebook_context()
    fundamental_context = lab.load_fundamental_context()
    data_map, data_context = lab.load_data(trade_context, fundamental_context)
    variants = build_variants(anchor_buy, anchor_sell)
    total = len(variants)
    results = []

    write_status(
        {
            "generated_at": now_iso(),
            "status": "running",
            "phase": "initializing",
            "message": "starting regime filter lab",
            "variants_total": total,
            "variants_done": 0,
            "anchor_variant": anchor_name,
            "anchor_buy": anchor_buy,
            "anchor_sell": anchor_sell,
            "symbols_loaded": len(data_context.get("loaded_symbols", [])),
        }
    )

    best_so_far = None
    for idx, (name, buy_patch, sell_patch) in enumerate(variants, start=1):
        result = lab.run_variant(
            name,
            data_map,
            buy_patch,
            sell_patch,
            rnn_params={"enabled": False},
            rnn_models={},
        )
        results.append(result)
        if best_so_far is None or (result.selection_score, result.total_return_pct) > (
            best_so_far.selection_score,
            best_so_far.total_return_pct,
        ):
            best_so_far = result
        write_status(
            {
                "generated_at": now_iso(),
                "status": "running",
                "phase": "evaluating_variants",
                "message": "running regime filter variants",
                "variants_total": total,
                "variants_done": idx,
                "current_variant": name,
                "anchor_variant": anchor_name,
                "best_variant": best_so_far.name,
                "best_return_pct": best_so_far.total_return_pct,
                "best_score": best_so_far.selection_score,
                "progress_pct": round((idx / max(total, 1)) * 100, 1),
            }
        )

    ranked = sorted(
        results,
        key=lambda r: (r.selection_score, r.total_return_pct, -abs(r.max_drawdown_pct), r.win_rate_pct),
        reverse=True,
    )
    baseline = next(r for r in ranked if r.name == "anchor_cluster_current")
    best = ranked[0]
    batch = {
        "offset": max(0, int(os.getenv("AT_REGIME_FILTER_OFFSET", "0") or "0")),
        "limit": int(os.getenv("AT_REGIME_FILTER_LIMIT", "0") or "0"),
        "tested_variants": len(ranked),
        "full_variant_count": 1 + sum(1 for patch in REGIME_PATCHES if patch),
    }
    payload = {
        "recommendation": {
            "generated_at": now_iso(),
            "lab_type": "regime_filter_followup",
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
            "batch": batch,
        },
        "ranked": [asdict(r) for r in ranked],
    }

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = OUT_DIR / f"regime_filter_lab_{ts}.json"
    csv_path = OUT_DIR / f"regime_filter_lab_{ts}.csv"
    json_path.write_text(json.dumps(payload, indent=2))
    pd.DataFrame([asdict(r) for r in ranked]).to_csv(csv_path, index=False)

    write_status(
        {
            "generated_at": now_iso(),
            "status": "completed",
            "phase": "done",
            "message": "regime filter lab complete",
            "variants_total": total,
            "variants_done": total,
            "anchor_variant": anchor_name,
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
