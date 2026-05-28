#!/usr/bin/env python3
"""RSI + momentum robustness validation report.

Builds on the official RSI+momentum rotation logic and adds:
- cost sensitivity
- top-N sensitivity
- rolling-window robustness
- Monte Carlo bootstrap on monthly returns

Writes a timestamped JSON plus reports/rsi_momentum_robustness_latest.json.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.rsi_momentum_report import find_hist_dir
from scripts.rsi_224466_rotation_lab import (
    load_prices as lab_load_prices,
    rebalance_dates as lab_rebalance_dates,
    rsi_dataframe as lab_rsi,
)

OUT_DIR = ROOT / "reports"
OUT_DIR.mkdir(exist_ok=True)


def load_research_prices(hist_dir_override: str = "") -> tuple[pd.DataFrame, dict]:
    hist_dir = find_hist_dir(hist_dir_override)
    prices_raw, ctx = lab_load_prices(
        hist_dir,
        min_rows=700,
        min_end_date="2026-04-17",
        symbols=set(),
        max_symbols=0,
    )
    return prices_raw.ffill(limit=3), ctx


def strategy_daily_returns(
    prices: pd.DataFrame,
    top_n: int = 10,
    cost_bps: float = 10.0,
    momentum_period: int = 21,
) -> tuple[pd.Series, list[dict]]:
    rsi_score = (lab_rsi(prices, 22) + lab_rsi(prices, 44) + lab_rsi(prices, 66)) / 3.0
    mom_1m = prices.pct_change(momentum_period, fill_method=None)
    returns = prices.pct_change(fill_method=None).fillna(0)
    dates = lab_rebalance_dates(prices.index, "ME")
    weights = pd.DataFrame(0.0, index=prices.index, columns=prices.columns)
    turnover_l = pd.Series(0.0, index=prices.index)
    prev = pd.Series(0.0, index=prices.columns)
    pick_log: list[dict] = []

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
        scored = combined.dropna().sort_values(ascending=False)
        picks = [
            s
            for s in scored.index
            if s in prices.columns and pd.notna(prices.loc[d, s]) and scored[s] > 0
        ][:top_n]
        if picks:
            target.loc[picks] = 1.0 / len(picks)
        turnover_l.loc[td] = abs(target - prev).sum()
        prev = target
        mask = (prices.index >= td) & (prices.index <= ed)
        weights.loc[mask, :] = target.values
        pick_log.append(
            {
                "signal_date": str(d.date()),
                "trade_date": str(td.date()),
                "pick_count": int(len(picks)),
                "picks": picks,
            }
        )

    gross = (weights * returns).sum(axis=1)
    net = gross - turnover_l * (cost_bps / 10000.0)
    active = weights.sum(axis=1) > 0
    return net.loc[active].copy(), pick_log


def metrics_from_returns(r: pd.Series) -> dict:
    if r.empty:
        raise ValueError("empty return series")
    eq = (1 + r).cumprod()
    years = len(r) / 252
    cagr = eq.iloc[-1] ** (1 / years) - 1 if years > 0 else 0.0
    dd = float((eq / eq.cummax() - 1).min())
    vol = float(r.std() * math.sqrt(252)) if len(r) > 1 else 0.0
    sharpe = float((r.mean() * 252) / vol) if vol > 0 else 0.0
    yearly = r.groupby(r.index.year).apply(lambda x: (1 + x).prod() - 1)
    return {
        "days": int(len(r)),
        "start": str(r.index[0].date()),
        "end": str(r.index[-1].date()),
        "total_return_pct": round(float((eq.iloc[-1] - 1) * 100), 2),
        "cagr_pct": round(float(cagr * 100), 2),
        "xirr_pct": round(float(cagr * 100), 2),
        "max_drawdown_pct": round(float(dd * 100), 2),
        "vol_pct": round(float(vol * 100), 2),
        "sharpe_like": round(sharpe, 3),
        "worst_year_pct": round(float(yearly.min() * 100), 2),
        "positive_years": int((yearly > 0).sum()),
        "total_years": int(len(yearly)),
    }


def rolling_window_checks(r: pd.Series, years_list: list[int]) -> dict:
    out = {}
    for yrs in years_list:
        win = int(252 * yrs)
        cagrs = []
        dds = []
        for i in range(0, len(r) - win + 1, 21):
            s = r.iloc[i : i + win]
            m = metrics_from_returns(s)
            cagrs.append(m["cagr_pct"])
            dds.append(m["max_drawdown_pct"])
        arr = np.array(cagrs)
        out[f"{yrs}y"] = {
            "window_count": int(len(cagrs)),
            "min_cagr_pct": round(float(np.min(arr)), 2),
            "median_cagr_pct": round(float(np.median(arr)), 2),
            "max_cagr_pct": round(float(np.max(arr)), 2),
            "pct_windows_above_30": round(float(np.mean(arr > 30.0) * 100), 1),
            "worst_max_dd_pct": round(float(np.min(dds)), 2),
        }
    return out


def monte_carlo_monthly_bootstrap(r: pd.Series, simulations: int, seed: int) -> dict:
    monthly = (1 + r).groupby(pd.Grouper(freq="ME")).prod() - 1
    vals = monthly.values
    n_months = len(vals)
    rng = np.random.default_rng(seed)
    mc = []
    for _ in range(simulations):
        sample = rng.choice(vals, size=n_months, replace=True)
        eq = np.cumprod(1 + sample)
        total = eq[-1]
        cagr = total ** (12 / n_months) - 1 if n_months else 0.0
        dd = np.min(eq / np.maximum.accumulate(eq) - 1) if len(eq) else 0.0
        mc.append((cagr * 100, dd * 100, (total - 1) * 100))
    mc_arr = np.array(mc)
    return {
        "simulations": int(len(mc_arr)),
        "cagr_pct_p5": round(float(np.percentile(mc_arr[:, 0], 5)), 2),
        "cagr_pct_p25": round(float(np.percentile(mc_arr[:, 0], 25)), 2),
        "cagr_pct_p50": round(float(np.percentile(mc_arr[:, 0], 50)), 2),
        "cagr_pct_p75": round(float(np.percentile(mc_arr[:, 0], 75)), 2),
        "cagr_pct_p95": round(float(np.percentile(mc_arr[:, 0], 95)), 2),
        "pct_sims_above_30_cagr": round(float(np.mean(mc_arr[:, 0] > 30.0) * 100), 1),
        "max_dd_pct_p50": round(float(np.percentile(mc_arr[:, 1], 50)), 2),
        "max_dd_pct_p95_worse": round(float(np.percentile(mc_arr[:, 1], 5)), 2),
        "total_return_pct_p50": round(float(np.percentile(mc_arr[:, 2], 50)), 2),
    }


def top_symbol_frequency(pick_log: list[dict], limit: int = 10) -> dict:
    picks = [sym for row in pick_log for sym in row["picks"]]
    if not picks:
        return {}
    freq = pd.Series(picks).value_counts().head(limit)
    return {str(k): int(v) for k, v in freq.to_dict().items()}


def main() -> int:
    parser = argparse.ArgumentParser(description="RSI + momentum robustness validation report")
    parser.add_argument("--hist-dir", default="")
    parser.add_argument("--top-n", type=int, default=10)
    parser.add_argument("--cost-bps", type=float, default=10.0)
    parser.add_argument("--momentum-period", type=int, default=21)
    parser.add_argument("--cost-grid", default="0,10,25,50,100")
    parser.add_argument("--topn-grid", default="6,8,10,12,15")
    parser.add_argument("--rolling-years", default="1,2,3")
    parser.add_argument("--simulations", type=int, default=5000)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    prices, data_ctx = load_research_prices(args.hist_dir)
    base_returns, pick_log = strategy_daily_returns(
        prices,
        top_n=args.top_n,
        cost_bps=args.cost_bps,
        momentum_period=args.momentum_period,
    )
    cost_grid = [float(x) for x in args.cost_grid.split(",") if x.strip()]
    topn_grid = [int(x) for x in args.topn_grid.split(",") if x.strip()]
    rolling_years = [int(x) for x in args.rolling_years.split(",") if x.strip()]

    cost_sensitivity = {}
    for bps in cost_grid:
        r, _ = strategy_daily_returns(prices, top_n=args.top_n, cost_bps=bps, momentum_period=args.momentum_period)
        m = metrics_from_returns(r)
        cost_sensitivity[str(int(bps) if float(bps).is_integer() else bps)] = {
            k: m[k]
            for k in ["cagr_pct", "xirr_pct", "total_return_pct", "max_drawdown_pct", "sharpe_like"]
        }

    topn_sensitivity = {}
    for top_n in topn_grid:
        r, _ = strategy_daily_returns(prices, top_n=top_n, cost_bps=args.cost_bps, momentum_period=args.momentum_period)
        m = metrics_from_returns(r)
        topn_sensitivity[str(top_n)] = {
            k: m[k]
            for k in ["cagr_pct", "xirr_pct", "total_return_pct", "max_drawdown_pct", "sharpe_like"]
        }

    pick_counts = pd.Series([row["pick_count"] for row in pick_log])
    report = {
        "generated_at": datetime.utcnow().isoformat(),
        "candidate": f"rsi+momentum_ME_top{args.top_n}",
        "params": {
            "top_n": args.top_n,
            "cost_bps": args.cost_bps,
            "momentum_period": args.momentum_period,
            "simulations": args.simulations,
            "seed": args.seed,
        },
        "data_context": {
            "symbols_loaded": int(prices.shape[1]),
            "days": int(prices.shape[0]),
            "date_start": str(prices.index.min().date()),
            "date_end": str(prices.index.max().date()),
            "loader_context": data_ctx,
        },
        "base_metrics": metrics_from_returns(base_returns),
        "cost_sensitivity": cost_sensitivity,
        "top_n_sensitivity": topn_sensitivity,
        "rolling_window_checks": rolling_window_checks(base_returns, rolling_years),
        "monte_carlo_monthly_bootstrap": monte_carlo_monthly_bootstrap(base_returns, args.simulations, args.seed),
        "trade_shape": {
            "rebalance_count": int(len(pick_log)),
            "avg_pick_count": round(float(pick_counts.mean()), 2),
            "min_pick_count": int(pick_counts.min()),
            "max_pick_count": int(pick_counts.max()),
            "top_symbol_frequency": top_symbol_frequency(pick_log),
        },
    }

    latest = OUT_DIR / "rsi_momentum_robustness_latest.json"
    stamped = OUT_DIR / f"rsi_momentum_robustness_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
    latest.write_text(json.dumps(report, indent=2))
    stamped.write_text(json.dumps(report, indent=2))

    print(f"Saved: {latest}")
    print(f"Saved: {stamped}")
    print(
        f"Base XIRR={report['base_metrics']['xirr_pct']:.2f}% | "
        f"MC p50 CAGR={report['monte_carlo_monthly_bootstrap']['cagr_pct_p50']:.2f}% | "
        f"MC p5 CAGR={report['monte_carlo_monthly_bootstrap']['cagr_pct_p5']:.2f}%"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
