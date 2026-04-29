#!/usr/bin/env python3
"""
Walk-forward validation for CAGR hunt variants.
Trains on the first N years, tests on the remaining years.
Reports whether in-sample parameter choices hold up out-of-sample.
"""

from __future__ import annotations

import json
import os
import sys
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

os.environ.setdefault("AT_DISABLE_FILE_LOGGING", "1")

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import weekly_strategy_lab as lab
from scripts.weekly_universe_cagr_check import run_baseline_detailed
from Auto_Trader.utils import Indicators
import Auto_Trader.utils as at_utils

OUT_DIR = ROOT / "reports"


def load_kite_symbols(min_rows: int = 260) -> dict[str, pd.DataFrame]:
    """Load all Kite feather files with sufficient history."""
    hist_dir = ROOT / "intermediary_files" / "Hist_Data"
    data_map: dict[str, pd.DataFrame] = {}
    skipped = 0
    for fp in sorted(hist_dir.glob("*.feather")):
        try:
            df = pd.read_feather(fp)
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
            df = df.dropna(subset=["Date"]).sort_values("Date").reset_index(drop=True)
            if len(df) < min_rows:
                skipped += 1
                continue
            enriched = Indicators(df)
            if enriched is not None and len(enriched) >= min_rows:
                data_map[fp.stem] = enriched
        except Exception:
            skipped += 1

    print(f"Loaded {len(data_map)} symbols (skipped {skipped})")
    return data_map


def split_data_by_date(data_map: dict[str, pd.DataFrame], split_date: str) -> tuple[dict, dict]:
    """Split data into train (before split_date) and test (from split_date onward)."""
    split_ts = pd.Timestamp(split_date)
    train_map = {}
    test_map = {}
    for symbol, df in data_map.items():
        train_df = df[df["Date"] < split_ts].copy().reset_index(drop=True)
        test_df = df[df["Date"] >= split_ts].copy().reset_index(drop=True)
        # Need warmup bars for indicators
        min_warmup = 250
        if len(train_df) >= min_warmup:
            train_map[symbol] = train_df
        if len(test_df) >= min_warmup:
            test_map[symbol] = test_df
    return train_map, test_map


def run_variant(data_map: dict[str, pd.DataFrame], buy: dict, sell: dict) -> dict:
    """Run a single variant and return results dict."""
    at_utils.get_mmi_now = lambda: None
    old_r2 = dict(lab.RULE_SET_2.CONFIG)
    old_r7 = dict(lab.RULE_SET_7.CONFIG)
    try:
        lab.RULE_SET_2.CONFIG.clear()
        lab.RULE_SET_2.CONFIG.update(old_r2)
        lab.RULE_SET_2.CONFIG.update(sell)
        lab.RULE_SET_7.CONFIG.clear()
        lab.RULE_SET_7.CONFIG.update(old_r7)
        lab.RULE_SET_7.CONFIG.update(buy)
        result, details, sim_meta = run_baseline_detailed(data_map)
        eq = sim_meta.get("portfolio_equity")
        if eq is not None and len(eq) > 20:
            s = pd.Series(eq, dtype=float) if not isinstance(eq, pd.Series) else eq
            final = s.iloc[-1]
            years = len(s) / 252.0
            cagr = ((final / s.iloc[0]) ** (1.0 / max(years, 0.01)) - 1.0) * 100.0 if years > 0 else 0.0
            peak = s.cummax()
            dd = ((s - peak) / peak * 100.0).min()
            rets = s.pct_change().dropna()
            sharpe = float(rets.mean() / rets.std() * np.sqrt(252)) if len(rets) > 5 and rets.std() > 0 else 0.0
        else:
            cagr = 0.0
            dd = 0.0
            sharpe = 0.0

        return {
            "trades": result.trades,
            "win_rate_pct": round(result.win_rate_pct, 1),
            "total_return_pct": round(result.total_return_pct, 2),
            "max_drawdown_pct": round(result.max_drawdown_pct, 2),
            "cagr_pct": round(cagr, 2),
            "sharpe": round(sharpe, 2),
            "active_symbols": sum(1 for v in details.values() if int(v.get("trades", 0) or 0) > 0),
            "selection_score": round(result.selection_score, 3),
        }
    except Exception as e:
        return {"error": str(e)[:200], "trades": 0, "cagr_pct": 0.0}
    finally:
        lab.RULE_SET_2.CONFIG.clear()
        lab.RULE_SET_2.CONFIG.update(old_r2)
        lab.RULE_SET_7.CONFIG.clear()
        lab.RULE_SET_7.CONFIG.update(old_r7)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Walk-forward validation for CAGR hunt variants")
    parser.add_argument("--train-years", type=float, default=3.0, help="Training period in years")
    parser.add_argument("--top-n", type=int, default=10, help="Validate top N variants from hunt history")
    parser.add_argument("--split-date", type=str, default=None, help="Explicit split date (YYYY-MM-DD)")
    parser.add_argument("--output", type=str, default=None, help="Output JSON path")
    args = parser.parse_args()

    # Load sizing env
    os.environ["AT_BACKTEST_VOL_SIZING_ENABLED"] = "1"
    os.environ["AT_BACKTEST_RISK_PER_TRADE_PCT"] = "0.02"
    os.environ["AT_BACKTEST_ATR_STOP_MULT"] = "2.5"
    os.environ["AT_BACKTEST_MAX_POSITION_NOTIONAL_PCT"] = "0.10"
    os.environ["AT_MAX_SINGLE_SYMBOL_WEIGHT"] = "0.15"
    os.environ["AT_PORTFOLIO_BAND"] = "0.10"
    os.environ["AT_TARGET_EQUITY"] = "1.0"
    os.environ["AT_TARGET_ETF"] = "0.0"

    # Load hunt results
    history_file = OUT_DIR / "kite_cagr_hunt_history.jsonl"
    if not history_file.exists():
        print(f"ERROR: No hunt history at {history_file}")
        return 1

    lines = [json.loads(l) for l in history_file.read_text().strip().split("\n") if l.strip()]
    lines.sort(key=lambda x: x.get("cagr_pct", 0), reverse=True)

    # Deduplicate by name (keep best CAGR)
    seen = {}
    for l in lines:
        name = l.get("name", "")
        if name not in seen or l.get("cagr_pct", 0) > seen[name].get("cagr_pct", 0):
            seen[name] = l

    top_variants = sorted(seen.values(), key=lambda x: x.get("cagr_pct", 0), reverse=True)[:args.top_n]

    print(f"Top {len(top_variants)} variants to validate:")
    for v in top_variants:
        print(f"  {v['name']:45s} CAGR={v['cagr_pct']:6.2f}%  trades={v['trades']:4d}")

    # Load full data
    data_map = load_kite_symbols()
    if not data_map:
        print("ERROR: No data loaded")
        return 1

    # Determine split date
    all_dates = []
    for df in data_map.values():
        all_dates.extend(df["Date"].tolist())
    if not all_dates:
        print("ERROR: No dates found")
        return 1

    min_date = min(all_dates)
    max_date = max(all_dates)
    total_years = (max_date - min_date).days / 365.25

    if args.split_date:
        split_date = args.split_date
    else:
        split_ts = min_date + pd.Timedelta(days=int(args.train_years * 365.25))
        split_date = split_ts.strftime("%Y-%m-%d")

    print(f"\nData range: {min_date.strftime('%Y-%m-%d')} to {max_date.strftime('%Y-%m-%d')} ({total_years:.1f} years)")
    print(f"Train/test split: {split_date}")
    print(f"Train: {min_date.strftime('%Y-%m-%d')} to {split_date} ({args.train_years:.1f} years)")
    print(f"Test:  {split_date} to {max_date.strftime('%Y-%m-%d')} ({total_years - args.train_years:.1f} years)")

    train_map, test_map = split_data_by_date(data_map, split_date)
    print(f"Train symbols: {len(train_map)}, Test symbols: {len(test_map)}")

    # Rebuild variant blueprints from hunt results
    from scripts.kite_cagr_hunt import FULL_VARIANT_BLUEPRINTS
    variant_map = {bp["name"]: bp for bp in FULL_VARIANT_BLUEPRINTS}

    results = []
    for i, v in enumerate(top_variants, 1):
        name = v["name"]
        bp = variant_map.get(name)
        if not bp:
            print(f"\n{i}/{len(top_variants)} {name}: SKIPPED (blueprint not found)")
            continue

        buy = bp.get("buy", {})
        sell = bp.get("sell", {})

        print(f"\n{i}/{len(top_variants)} {name}")
        print(f"  In-sample CAGR: {v['cagr_pct']:.2f}%  (full 5Y)")

        # Train period
        print(f"  Running train ({len(train_map)} symbols)...", end=" ", flush=True)
        train_result = run_variant(train_map, buy, sell)
        if "error" in train_result:
            print(f"ERROR: {train_result['error']}")
            continue
        print(f"CAGR={train_result['cagr_pct']:.2f}%  trades={train_result['trades']}")

        # Test period
        print(f"  Running test  ({len(test_map)} symbols)...", end=" ", flush=True)
        test_result = run_variant(test_map, buy, sell)
        if "error" in test_result:
            print(f"ERROR: {test_result['error']}")
            continue
        print(f"CAGR={test_result['cagr_pct']:.2f}%  trades={test_result['trades']}")

        # Degradation
        is_cagr = v["cagr_pct"]
        train_cagr = train_result["cagr_pct"]
        test_cagr = test_result["cagr_pct"]
        degradation = ((is_cagr - test_cagr) / max(abs(is_cagr), 0.01)) * 100 if is_cagr != 0 else 0

        result = {
            "name": name,
            "full_5y_cagr": is_cagr,
            "full_5y_trades": v["trades"],
            "train_cagr": train_cagr,
            "train_trades": train_result["trades"],
            "train_return_pct": train_result["total_return_pct"],
            "train_max_dd_pct": train_result["max_drawdown_pct"],
            "train_win_rate": train_result["win_rate_pct"],
            "train_sharpe": train_result.get("sharpe", 0),
            "test_cagr": test_cagr,
            "test_trades": test_result["trades"],
            "test_return_pct": test_result["total_return_pct"],
            "test_max_dd_pct": test_result["max_drawdown_pct"],
            "test_win_rate": test_result["win_rate_pct"],
            "test_sharpe": test_result.get("sharpe", 0),
            "degradation_pct": round(degradation, 1),
            "holds_up": test_cagr > 0 and degradation < 50,
        }
        results.append(result)

        verdict = "✅ HOLDS UP" if result["holds_up"] else "❌ DEGRADED"
        print(f"  Verdict: {verdict}  (degradation: {degradation:.1f}%)")

    # Summary
    print("\n" + "=" * 80)
    print("WALK-FORWARD VALIDATION SUMMARY")
    print("=" * 80)
    print(f"Train: {min_date.strftime('%Y-%m-%d')} → {split_date} ({args.train_years:.1f}Y)")
    print(f"Test:  {split_date} → {max_date.strftime('%Y-%m-%d')} ({total_years - args.train_years:.1f}Y)")
    print(f"Split date: {split_date}")
    print()

    for r in results:
        verdict = "✅" if r["holds_up"] else "❌"
        print(f"{verdict} {r['name']:45s}  IS={r['full_5y_cagr']:6.2f}%  Train={r['train_cagr']:6.2f}%  Test={r['test_cagr']:6.2f}%  Deg={r['degradation_pct']:5.1f}%")

    holds = sum(1 for r in results if r["holds_up"])
    print(f"\n{holds}/{len(results)} variants hold up in walk-forward")

    # Save results
    output_path = args.output or str(OUT_DIR / f"walkforward_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
    output = {
        "generated_at": datetime.now().isoformat(),
        "split_date": split_date,
        "train_years": args.train_years,
        "test_years": round(total_years - args.train_years, 2),
        "train_symbols": len(train_map),
        "test_symbols": len(test_map),
        "variants_tested": len(results),
        "variants_hold_up": holds,
        "results": results,
    }
    Path(output_path).write_text(json.dumps(output, indent=2))
    print(f"\nSaved to {output_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())