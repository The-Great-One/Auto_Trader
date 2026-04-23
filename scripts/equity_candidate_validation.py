#!/usr/bin/env python3
from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

from scripts import weekly_strategy_lab as w

OUT_DIR = Path(__file__).resolve().parents[1] / "reports"
OUT_DIR.mkdir(exist_ok=True)

CANDIDATES = [
    ("baseline_current", {}, {}),
    (
        "looser_entry_a",
        {
            "adx_min": 8,
            "volume_confirm_mult": 0.75,
            "rsi_floor": 38,
            "stoch_pull_max": 90,
            "ich_cloud_bull": 0,
            "vwap_buy_above": 0,
        },
        {},
    ),
    (
        "looser_entry_b",
        {
            "adx_min": 8,
            "volume_confirm_mult": 0.75,
            "max_extension_atr": 3.2,
            "obv_min_zscore": 0.0,
            "ich_cloud_bull": 0,
        },
        {},
    ),
    (
        "looser_entry_c",
        {
            "adx_min": 6,
            "volume_confirm_mult": 0.7,
            "rsi_floor": 36,
            "stoch_pull_max": 95,
            "cci_buy_min": -150,
            "vwap_buy_above": 0,
        },
        {},
    ),
]


def main() -> int:
    scorecard_context = w.load_scorecard_context()
    tradebook_context = w.load_tradebook_context()
    fundamental_context = w.load_fundamental_context()
    data_map, data_context = w.load_data(tradebook_context, fundamental_context)

    results = []
    for name, buy_params, sell_params in CANDIDATES:
        result = w.run_variant(name, data_map, buy_params, sell_params, rnn_params={"enabled": False}, rnn_models={})
        results.append(result)

    ranked = sorted(
        results,
        key=lambda r: (r.selection_score, r.total_return_pct, -abs(r.max_drawdown_pct), r.win_rate_pct),
        reverse=True,
    )
    baseline = next(r for r in results if r.name == "baseline_current")
    best = ranked[0]

    payload = {
        "generated_at": datetime.now().isoformat(),
        "production_rule_model": "BUY=RULE_SET_7, SELL=RULE_SET_2",
        "scorecard_context": scorecard_context,
        "tradebook_context": tradebook_context,
        "fundamental_context": {
            k: v for k, v in fundamental_context.items() if k != "sector_map"
        },
        "data_context": data_context,
        "baseline": asdict(baseline),
        "best": asdict(best),
        "improvement_return_pct": round(best.total_return_pct - baseline.total_return_pct, 2),
        "improvement_score": round(best.selection_score - baseline.selection_score, 3),
        "ranked": [asdict(r) for r in ranked],
        "notes": [
            "Targeted same-loader equity validation for the looser-entry candidates added on 2026-04-23.",
            "Uses weekly_strategy_lab loaders and run_variant for loader/history/universe parity.",
        ],
    }

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = OUT_DIR / f"equity_candidate_validation_{ts}.json"
    path.write_text(json.dumps(payload, indent=2))
    print(json.dumps({
        "path": str(path),
        "baseline_return_pct": payload["baseline"]["total_return_pct"],
        "best_variant": payload["best"]["name"],
        "best_return_pct": payload["best"]["total_return_pct"],
        "improvement_return_pct": payload["improvement_return_pct"],
        "improvement_score": payload["improvement_score"],
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
