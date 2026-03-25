#!/usr/bin/env python3
"""Walk-forward validation for strategy variants (local only).
Uses the same production model structure: BUY=RULE_SET_7, SELL=RULE_SET_2.
"""

from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

import pandas as pd

import weekly_strategy_lab as lab

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "reports"
OUT.mkdir(exist_ok=True)


def split_windows(df: pd.DataFrame, train=504, test=126, step=126):
    i = 0
    n = len(df)
    while i + train + test <= n:
        yield df.iloc[i : i + train].copy(), df.iloc[i + train : i + train + test].copy()
        i += step


def main():
    df = lab.load_data()
    candidates = lab.variants()

    windows = list(split_windows(df, train=504, test=126, step=126))
    if not windows:
        raise SystemExit("Not enough data for walk-forward windows")

    wf_rows = []
    for widx, (train_df, test_df) in enumerate(windows, 1):
        # pick best on train
        train_res = []
        for name, b, s in candidates:
            r = lab.run_variant(name, train_df, b, s)
            train_res.append((name, b, s, r.total_return_pct, r.max_drawdown_pct))
        train_res.sort(key=lambda x: (x[3], -abs(x[4])), reverse=True)
        best_name, best_b, best_s, best_train_ret, _ = train_res[0]

        # evaluate chosen on test
        best_test = lab.run_variant(best_name, test_df, best_b, best_s)
        base_test = lab.run_variant("baseline_current", test_df, {}, {})

        wf_rows.append(
            {
                "window": widx,
                "selected": best_name,
                "train_return_pct": round(best_train_ret, 2),
                "test_return_pct": best_test.total_return_pct,
                "test_baseline_return_pct": base_test.total_return_pct,
                "test_alpha_pct": round(best_test.total_return_pct - base_test.total_return_pct, 2),
            }
        )

    wf = pd.DataFrame(wf_rows)
    summary = {
        "generated_at": datetime.now().isoformat(),
        "production_rule_model": "BUY=RULE_SET_7, SELL=RULE_SET_2",
        "windows": len(wf_rows),
        "avg_test_return_pct": round(float(wf["test_return_pct"].mean()), 2),
        "avg_baseline_test_return_pct": round(float(wf["test_baseline_return_pct"].mean()), 2),
        "avg_alpha_pct": round(float(wf["test_alpha_pct"].mean()), 2),
        "win_windows": int((wf["test_alpha_pct"] > 0).sum()),
        "lose_windows": int((wf["test_alpha_pct"] <= 0).sum()),
    }

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    (OUT / f"walkforward_{ts}.json").write_text(
        json.dumps({"summary": summary, "windows": wf_rows}, indent=2)
    )
    wf.to_csv(OUT / f"walkforward_{ts}.csv", index=False)

    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
