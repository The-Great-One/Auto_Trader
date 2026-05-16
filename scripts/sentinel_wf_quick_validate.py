#!/usr/bin/env python3
"""
Quick walk-forward validation of sentinel_30 best candidates.
Validates combo263 variants on full universe with chronological 5-fold WF.
"""
from __future__ import annotations
import json, os, sys, time
from pathlib import Path

os.environ.setdefault("AT_DISABLE_FILE_LOGGING", "1")
os.environ["AT_RESEARCH_MODE"] = "1"
os.environ["AT_LAB_PRECACHE"] = "0"
os.environ["AT_LAB_CACHE_ONLY"] = "1"

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Import the sentinel module's core functions
import numpy as np
import pandas as pd
from scripts.sentinel_30_targeted_v2 import (
    load_kite_symbols,
    VariantResult,
    run_variant,
    VARIANTS,
)
from Auto_Trader.utils import Indicators

OUT_DIR = ROOT / "reports"

# Best candidates from full universe sweep
# Pick top 3 combo263 variants from VARIANTS
CANDIDATES = [v for v in VARIANTS if v["name"] in ("combo263_bep2.0_ts4", "combo263_bep2.5_ts4", "combo263_bep1.5_ts4")]
if not CANDIDATES:
    CANDIDATES = [v for v in VARIANTS if "combo263" in v["name"]][:3]


def main():
    print("=" * 60)
    print("SENTINEL WF QUICK VALIDATE — combo263 variants")
    print("=" * 60)

    data_map = load_kite_symbols()
    print(f"Loaded {len(data_map)} symbols")

    results = []
    for cand in CANDIDATES:
        name = cand["name"]
        buy_cfg = cand["buy"]
        sell_cfg = cand["sell"]
        print(f"\nRunning full backtest for {name}...")
        full_result = run_variant(name, buy_cfg, sell_cfg, data_map)
        # Convert dataclass to dict
        if hasattr(full_result, '__dataclass_fields__'):
            from dataclasses import asdict
            full_result = asdict(full_result)
        print(f"  Full: cagr={full_result.get('cagr_pct', '?')}%, ret={full_result.get('total_return_pct', '?')}%, dd={full_result.get('max_drawdown_pct', '?')}%, trades={full_result.get('trades', '?')}")

        # Walk-forward
        N_FOLDS = 5
        print(f"  Running {N_FOLDS}-fold WF validation...")
        all_dates = sorted(data_map[list(data_map.keys())[0]].index)
        total_days = len(all_dates)
        fold_size = total_days // N_FOLDS
        wf_folds = []
        for fold_i in range(N_FOLDS):
            train_end = (fold_i + 1) * fold_size
            test_start = train_end
            test_end = min((fold_i + 2) * fold_size, total_days)
            if test_end <= test_start:
                continue

            train_cutoff = all_dates[train_end - 1]
            test_cutoff = all_dates[min(test_end - 1, total_days - 1)]

            train_map = {s: df[df.index <= train_cutoff] for s, df in data_map.items()}
            test_map = {s: df[(df.index > train_cutoff) & (df.index <= test_cutoff)] for s, df in data_map.items()}

            test_result = run_variant(name, buy_cfg, sell_cfg, test_map)
            if hasattr(test_result, '__dataclass_fields__'):
                from dataclasses import asdict
                test_result = asdict(test_result)
            wf_folds.append({
                "fold": fold_i + 1,
                "train_end": str(train_cutoff.date()) if hasattr(train_cutoff, 'date') else str(train_cutoff),
                "test_end": str(test_cutoff.date()) if hasattr(test_cutoff, 'date') else str(test_cutoff),
                "oos_return_pct": test_result.get("total_return_pct", 0),
                "oos_trades": test_result.get("trades", 0),
                "oos_dd_pct": test_result.get("max_drawdown_pct", 0),
            })
            print(f"    Fold {fold_i+1}: OOS ret={test_result.get('total_return_pct', 0)}%, trades={test_result.get('trades', 0)}, dd={test_result.get('max_drawdown_pct', 0)}%")

        positive_folds = sum(1 for f in wf_folds if f["oos_return_pct"] > 0)
        mean_oos = np.mean([f["oos_return_pct"] for f in wf_folds]) if wf_folds else 0
        min_oos = min([f["oos_return_pct"] for f in wf_folds]) if wf_folds else 0

        result = {
            "name": name,
            "full": full_result,
            "wf_folds": wf_folds,
            "n_folds": len(wf_folds),
            "mean_oos_return_pct": round(mean_oos, 2),
            "min_oos_return_pct": round(min_oos, 2),
            "positive_folds": positive_folds,
        }
        results.append(result)
        print(f"  Summary: mean_OOS={mean_oos:.2f}%, min_OOS={min_oos:.2f}%, positive_folds={positive_folds}/{len(wf_folds)}")

    report = {
        "generated_at": pd.Timestamp.now().isoformat(),
        "hunt_label": "Sentinel WF Quick Validate — combo263",
        "data_source": "kite_feather_5y",
        "results": results,
    }

    out_path = OUT_DIR / f"sentinel_wf_quick_validate_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(out_path, "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"\nSaved: {out_path}")


if __name__ == "__main__":
    main()