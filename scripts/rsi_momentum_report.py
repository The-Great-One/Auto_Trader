#!/usr/bin/env python3
"""RSI 22/44/66 + 1-Month Momentum rotation strategy — official full report.

Extends the RSI rotation lab with a momentum overlay: only stocks with
positive 1-month returns are eligible for the RSI top-N ranking.
Produces both in-sample headline and walk-forward validation.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.rsi_224466_rotation_lab import (
    load_prices as lab_load_prices,
    rsi_dataframe as lab_rsi,
    rebalance_dates as lab_rebalance_dates,
    find_hist_dir as lab_find_hist_dir,
)

OUT_DIR = ROOT / "reports"
OUT_DIR.mkdir(exist_ok=True)


def find_hist_dir(override: str = "") -> Path:
    return lab_find_hist_dir(override)


@dataclass
class WFFold:
    fold: int
    train_start: str
    train_end: str
    test_start: str
    test_end: str
    train_cagr_pct: float
    test_cagr_pct: float
    test_return_pct: float
    test_max_drawdown_pct: float
    test_sharpe: float
    test_trades: int
    test_positive: bool = False


@dataclass
class Report:
    generated_at: str
    strategy_name: str
    params: dict
    is_headline: dict
    wf_folds: list[WFFold] = field(default_factory=list)
    wf_summary: dict = field(default_factory=dict)

    def finalize_wf(self):
        if not self.wf_folds:
            return
        pos = sum(1 for f in self.wf_folds if f.test_positive)
        cagrs = [f.test_cagr_pct for f in self.wf_folds]
        returns = [f.test_return_pct for f in self.wf_folds]
        dds = [f.test_max_drawdown_pct for f in self.wf_folds]
        self.wf_summary = {
            "fold_count": len(self.wf_folds),
            "positive_folds": pos,
            "positive_fold_pct": round(pos / len(self.wf_folds) * 100, 1),
            "mean_test_cagr_pct": round(float(np.mean(cagrs)), 1),
            "median_test_cagr_pct": round(float(np.median(cagrs)), 1),
            "std_test_cagr_pct": round(float(np.std(cagrs)), 1),
            "best_test_cagr_pct": round(float(np.max(cagrs)), 1),
            "worst_test_cagr_pct": round(float(np.min(cagrs)), 1),
            "mean_test_return_pct": round(float(np.mean(returns)), 1),
            "mean_test_max_dd_pct": round(float(np.mean(dds)), 1),
        }


def run_is_headline(prices: pd.DataFrame, top_n: int, cost_bps: float = 10.0) -> dict:
    """In-sample full-period metrics for the RSI+momentum strategy."""
    rsi_score = (lab_rsi(prices, 22) + lab_rsi(prices, 44) + lab_rsi(prices, 66)) / 3.0
    mom_1m = prices.pct_change(21, fill_method=None)
    returns = prices.pct_change(fill_method=None).fillna(0)

    dates = lab_rebalance_dates(prices.index, "ME")
    weights = pd.DataFrame(0.0, index=prices.index, columns=prices.columns)
    turnover_l = pd.Series(0.0, index=prices.index)
    prev = pd.Series(0.0, index=prices.columns)

    for i, d in enumerate(dates):
        pos = prices.index.get_loc(d)
        if pos + 1 >= len(prices.index):
            continue
        td = prices.index[pos + 1]
        ed = dates[i + 1] if i + 1 < len(dates) else prices.index[-1]
        target = pd.Series(0.0, index=prices.columns)
        rsi_at = rsi_score.loc[d].copy()
        mom_at = mom_1m.loc[d].copy()
        combined = rsi_at.where(mom_at > 0, 0)
        sc = combined.dropna().sort_values(ascending=False)
        picks = [s for s in sc.index if s in prices.columns and pd.notna(prices.loc[d, s]) and sc[s] > 0][:top_n]
        if picks:
            target.loc[picks] = 1.0 / len(picks)
        turnover_l.loc[td] = abs(target - prev).sum()
        prev = target
        mask = (prices.index >= td) & (prices.index <= ed)
        weights.loc[mask, :] = target.values

    gross = (weights * returns).sum(axis=1)
    net = gross - turnover_l * (cost_bps / 10000.0)

    active = weights.sum(axis=1) > 0
    if not active.any():
        raise ValueError("No active periods")

    r = net.loc[active]
    eq = (1 + r).cumprod()
    years = len(r) / 252
    cagr = eq.iloc[-1] ** (1 / years) - 1 if years > 0 else 0
    dd = float((eq / eq.cummax() - 1).min())
    elapsed = net.loc[r.index[0]:]
    elapsed_eq = (1 + elapsed).cumprod()
    elapsed_y = len(elapsed) / 252
    xirr = elapsed_eq.iloc[-1] ** (1 / elapsed_y) - 1 if elapsed_y > 0 else 0
    vol = r.std() * math.sqrt(252)
    sharpe = (r.mean() * 252) / vol if vol > 0 else 0
    yearly = r.groupby(r.index.year).apply(lambda x: (1 + x).prod() - 1)

    return {
        "name": f"rsi+momentum_ME_top{top_n}",
        "top_n": top_n,
        "cost_bps": cost_bps,
        "start": str(r.index[0].date()),
        "end": str(r.index[-1].date()),
        "days": int(len(r)),
        "total_return_pct": round(float((eq.iloc[-1] - 1) * 100), 1),
        "cagr_pct": round(float(cagr * 100), 2),
        "xirr_pct": round(float(xirr * 100), 2),
        "max_drawdown_pct": round(float(dd * 100), 2),
        "vol_pct": round(float(vol * 100), 1),
        "sharpe_like": round(float(sharpe), 3),
        "worst_year_pct": round(float(yearly.min() * 100), 1),
        "positive_years": int((yearly > 0).sum()),
        "total_years": int(len(yearly)),
    }


def run_walkforward(prices: pd.DataFrame, top_n: int, cost_bps: float = 10.0,
                    test_months: int = 6, min_train_years: int = 2) -> list[WFFold]:
    """Expanding-window walk-forward validation."""
    rsi_score = (lab_rsi(prices, 22) + lab_rsi(prices, 44) + lab_rsi(prices, 66)) / 3.0
    mom_1m = prices.pct_change(21, fill_method=None)
    returns = prices.pct_change(fill_method=None).fillna(0)
    all_dates = prices.index

    start = all_dates[0]
    end_dt = all_dates[-1]
    train_end = all_dates[all_dates >= start + pd.DateOffset(years=min_train_years) - pd.Timedelta(days=1)][0]
    folds: list[WFFold] = []
    fi = 0

    while True:
        test_start = train_end + pd.Timedelta(days=1)
        test_end = min(test_start + pd.DateOffset(months=test_months) - pd.Timedelta(days=1), end_dt)
        if test_end <= test_start or test_end - test_start < pd.Timedelta(days=60):
            break

        test_mask = (all_dates >= test_start) & (all_dates <= test_end)
        if test_mask.sum() < 30:
            train_end = test_end
            continue

        test_dates = all_dates[test_mask]
        rb_dates = lab_rebalance_dates(test_dates, "ME")
        if len(rb_dates) < 2:
            train_end = test_end
            continue

        fi += 1
        weights = pd.DataFrame(0.0, index=test_dates, columns=prices.columns)
        turnover_l = pd.Series(0.0, index=test_dates)
        prev = pd.Series(0.0, index=prices.columns)

        for i, d in enumerate(rb_dates):
            pos_idx = test_dates.get_loc(d)
            if pos_idx + 1 >= len(test_dates):
                continue
            td = test_dates[pos_idx + 1]
            ed = test_dates[test_dates.get_loc(rb_dates[i + 1])] if i + 1 < len(rb_dates) else test_dates[-1]
            target = pd.Series(0.0, index=prices.columns)
            rsi_at = rsi_score.loc[d].copy()
            mom_at = mom_1m.loc[d].copy()
            combined = rsi_at.where(mom_at > 0, 0)
            sc = combined.dropna().sort_values(ascending=False)
            picks = [s for s in sc.index if s in prices.columns and pd.notna(prices.loc[d, s]) and sc[s] > 0][:top_n]
            if picks:
                target.loc[picks] = 1.0 / len(picks)
            turnover_l.loc[td] = abs(target - prev).sum()
            prev = target
            mask = (test_dates >= td) & (test_dates <= ed)
            weights.loc[mask, :] = target.values

        gross = (weights * returns.loc[test_dates]).sum(axis=1).fillna(0)
        net = gross - turnover_l * (cost_bps / 10000.0)
        eq = (1 + net).cumprod()

        if eq.iloc[-1] > 0 and len(net) > 30:
            y = len(net) / 252
            c = eq.iloc[-1] ** (1 / y) - 1
            d = float((eq / eq.cummax() - 1).min())
            ret = float(eq.iloc[-1] - 1)
            vol = net.std() * math.sqrt(252)
            sh = float((net.mean() * 252) / vol) if vol > 0 else 0.0
            pos = c > 0
        else:
            c = d = ret = sh = 0.0
            pos = False

        train_win = all_dates[(all_dates >= start) & (all_dates <= train_end)]
        if len(train_win) > 252:
            train_eq = (1 + returns.loc[train_win].mean(axis=1).fillna(0)).cumprod()
            train_y = len(train_win) / 252
            train_cagr = train_eq.iloc[-1] ** (1 / train_y) - 1
        else:
            train_cagr = 0.0

        folds.append(WFFold(
            fold=fi,
            train_start=str(start.date()),
            train_end=str(train_end.date()),
            test_start=str(test_start.date()),
            test_end=str(test_end.date()),
            train_cagr_pct=round(float(train_cagr * 100), 1),
            test_cagr_pct=round(float(c * 100), 1),
            test_return_pct=round(float(ret * 100), 1),
            test_max_drawdown_pct=round(float(d * 100), 1),
            test_sharpe=round(float(sh), 3),
            test_trades=len(rb_dates),
            test_positive=bool(pos),
        ))
        train_end = test_end

    return folds


def main() -> int:
    parser = argparse.ArgumentParser(description="RSI + Momentum rotation full report")
    parser.add_argument("--hist-dir", default="")
    parser.add_argument("--top-n", type=int, nargs="+", default=[10])
    parser.add_argument("--cost-bps", type=float, default=10.0)
    parser.add_argument("--test-months", type=int, default=6)
    parser.add_argument("--min-train-years", type=int, default=2)
    parser.add_argument("--skip-wf", action="store_true", help="Skip walk-forward (IS only)")
    parser.add_argument("--skip-is", action="store_true", help="Skip in-sample")
    args = parser.parse_args()

    hist_dir = find_hist_dir(args.hist_dir)
    print(f"Loading data from {hist_dir}...")
    prices_raw, ctx = lab_load_prices(hist_dir, min_rows=700, min_end_date="2026-04-17", symbols=set(), max_symbols=0)
    prices = prices_raw.ffill(limit=3)
    print(f"Loaded {prices.shape[1]} symbols, {prices.shape[0]} days ({prices.index[0].date()} to {prices.index[-1].date()})")

    all_reports = []

    for top_n in args.top_n:
        print(f"\n{'='*60}")
        print(f"  RSI + Momentum ME_top{top_n}")

        report = Report(
            generated_at=datetime.now().isoformat(),
            strategy_name=f"rsi+momentum_ME_top{top_n}",
            params={"top_n": top_n, "cost_bps": args.cost_bps, "momentum_period": 21, "momentum_filter": "positive_only"},
            is_headline={},
        )

        if not args.skip_is:
            print("\n  --- IN-SAMPLE HEADLINE ---")
            headline = run_is_headline(prices, top_n, args.cost_bps)
            report.is_headline = headline
            print(f"  CAGR  = {headline['cagr_pct']:.2f}%")
            print(f"  XIRR  = {headline['xirr_pct']:.2f}%")
            print(f"  MaxDD = {headline['max_drawdown_pct']:.1f}%")
            print(f"  Sharpe= {headline['sharpe_like']:.3f}")
            print(f"  PosYrs= {headline['positive_years']}/{headline['total_years']}")

        if not args.skip_wf:
            print("\n  --- WALK-FORWARD ---")
            folds = run_walkforward(prices, top_n, args.cost_bps, args.test_months, args.min_train_years)
            report.wf_folds = folds
            report.finalize_wf()

            for f in folds:
                sym = "✅" if f.test_positive else "❌"
                print(f"  {sym} {f.test_start} → {f.test_end}: "
                      f"CAGR={f.test_cagr_pct:+.1f}% Return={f.test_return_pct:+.1f}% DD={f.test_max_drawdown_pct:+.1f}% Sharpe={f.test_sharpe:.3f}")

            ws = report.wf_summary
            print(f"\n  WF Summary: {ws['positive_folds']}/{ws['fold_count']} positive folds")
            print(f"  Mean CAGR: {ws['mean_test_cagr_pct']:+.1f}% | Worst: {ws['worst_test_cagr_pct']:+.1f}%")

        all_reports.append(report)

    # Write report output
    if all_reports:
        best = all_reports[0]
        payload = {
            "generated_at": best.generated_at,
            "strategy_name": best.strategy_name,
            "params": best.params,
            "in_sample": best.is_headline,
            "walk_forward": {
                "folds": [asdict(f) for f in best.wf_folds],
                "summary": best.wf_summary,
            } if best.wf_folds else None,
            "data_context": {
                "symbols": prices.shape[1],
                "days": prices.shape[0],
                "start": str(prices.index[0].date()),
                "end": str(prices.index[-1].date()),
            },
        }

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        json_path = OUT_DIR / f"rsi_momentum_report_{ts}.json"
        latest_path = OUT_DIR / "rsi_momentum_latest.json"
        json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        latest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        print(f"\nSaved: {json_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
