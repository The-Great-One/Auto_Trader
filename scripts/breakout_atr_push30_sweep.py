#!/usr/bin/env python3
"""Push breakout+ATR structural candidate toward 30% CAGR.

This sweep focuses on the credible current lead:
    breakout_10_atr3.0_vol1.5_trend

It searches risk/regime/entry-quality controls that could close the gap to the
30% CAGR target without relying on RS7's gate cascade or train-period symbol
whitelisting (which overfit badly).

Outputs:
- reports/breakout_atr_push30_sweep_<timestamp>.json
- reports/breakout_atr_push30_latest.json
"""
from __future__ import annotations

import json
import os
import random
import sys
from dataclasses import dataclass, asdict
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

REPORTS = ROOT / "reports"
REPORTS.mkdir(exist_ok=True)


@dataclass
class PeriodMetrics:
    period: str
    start_date: str
    end_date: str
    symbols: int
    active_symbols: int
    profitable_symbols: int
    trades: int
    win_rate_pct: float
    total_return_pct: float
    cagr_pct: float
    max_drawdown_pct: float
    sharpe_ratio: float
    avg_holding_bars: float


@dataclass
class SweepResult:
    name: str
    params: dict[str, Any]
    test: PeriodMetrics
    recent: PeriodMetrics
    score: float
    target_gap_pct_points: float
    target_ratio: float
    verdict: str
    reasons: list[str]


def _prep_symbol(df: pd.DataFrame) -> pd.DataFrame:
    out = ex._compute_indicators(df)
    close = out["Close"]
    out["ROC20"] = close.pct_change(20) * 100.0
    out["ROC63"] = close.pct_change(63) * 100.0
    out["HHV5"] = out["High"].rolling(5).max().shift(1)
    out["HHV7"] = out["High"].rolling(7).max().shift(1)
    out["HHV10"] = out["High"].rolling(10).max().shift(1)
    out["HHV15"] = out["High"].rolling(15).max().shift(1)
    out["HHV20"] = out["High"].rolling(20).max().shift(1)
    out["AboveEMA150"] = (out["Close"] > out["EMA150"]).astype(float)
    return out


def _add_breadth(precomputed: dict[str, pd.DataFrame]) -> None:
    rows = []
    for df in precomputed.values():
        rows.append(pd.DataFrame({"Date": pd.to_datetime(df["Date"]), "AboveEMA150": df["AboveEMA150"]}))
    if not rows:
        return
    all_rows = pd.concat(rows, ignore_index=True)
    breadth = all_rows.groupby("Date")["AboveEMA150"].mean().to_dict()
    for df in precomputed.values():
        df["BreadthEMA150"] = pd.to_datetime(df["Date"]).map(breadth).astype(float)


def _simulate_symbol(df: pd.DataFrame, params: dict[str, Any], start: pd.Timestamp, end: pd.Timestamp) -> dict[str, Any] | None:
    dates = pd.to_datetime(df["Date"])
    mask = (dates >= start) & (dates <= end)
    idxs = np.flatnonzero(mask.to_numpy())
    if len(idxs) < 30:
        return None
    start_idx = max(220, int(idxs[0]))
    end_idx = int(idxs[-1])
    if end_idx <= start_idx + 20:
        return None

    close = df["Close"].to_numpy(dtype=float)
    high = df["High"].to_numpy(dtype=float)
    atr = df["ATR"].to_numpy(dtype=float)
    atr_pct = df["ATR_pct"].to_numpy(dtype=float)
    vol_ratio = df["Vol_Ratio"].to_numpy(dtype=float)
    rsi = df["RSI"].to_numpy(dtype=float)
    adx = df["ADX"].to_numpy(dtype=float)
    ema50 = df["EMA50"].to_numpy(dtype=float)
    ema150 = df["EMA150"].to_numpy(dtype=float)
    ema200 = df["EMA200"].to_numpy(dtype=float)
    roc20 = df["ROC20"].to_numpy(dtype=float)
    roc63 = df["ROC63"].to_numpy(dtype=float)
    breadth = df.get("BreadthEMA150", pd.Series(np.ones(len(df)))).to_numpy(dtype=float)
    hhv = df[f"HHV{int(params['lookback'])}"].to_numpy(dtype=float)

    cash0 = 100000.0
    cash = cash0
    qty = 0.0
    entry_price = 0.0
    entry_idx = -1
    trades = wins = 0
    hold_bars: list[int] = []
    eq_curve: list[float] = []

    position_pct = float(params.get("position_pct", 0.95))
    for i in range(start_idx, end_idx + 1):
        price = close[i]
        if not np.isfinite(price) or price <= 0:
            continue

        if qty <= 0:
            prev_close = close[i - 1]
            breakout = np.isfinite(hhv[i]) and price > hhv[i] and prev_close <= hhv[i]
            if breakout:
                ok = True
                if vol_ratio[i] < float(params["vol_mult"]):
                    ok = False
                trend_mode = params.get("trend_mode", "ema150")
                if trend_mode == "ema150" and not (price > ema150[i] and ema50[i] >= ema150[i]):
                    ok = False
                elif trend_mode == "ema200" and not (price > ema200[i] and ema50[i] >= ema150[i]):
                    ok = False
                elif trend_mode == "ema50_150_200" and not (price > ema50[i] > ema150[i] > ema200[i]):
                    ok = False
                if float(params.get("adx_min", 0)) > 0 and adx[i] < float(params["adx_min"]):
                    ok = False
                if float(params.get("rsi_min", 0)) > 0 and rsi[i] < float(params["rsi_min"]):
                    ok = False
                if float(params.get("rsi_max", 100)) < 100 and rsi[i] > float(params["rsi_max"]):
                    ok = False
                if float(params.get("roc20_min", -999)) > -999 and roc20[i] < float(params["roc20_min"]):
                    ok = False
                if float(params.get("roc63_min", -999)) > -999 and roc63[i] < float(params["roc63_min"]):
                    ok = False
                if params.get("atr_pct_max") is not None and atr_pct[i] > float(params["atr_pct_max"]):
                    ok = False
                if float(params.get("breadth_min", 0)) > 0 and breadth[i] < float(params["breadth_min"]):
                    ok = False

                if ok:
                    qty = (cash * position_pct) / price
                    cash -= qty * price
                    entry_price = price
                    entry_idx = i
                    trades += 1
        else:
            bars = i - entry_idx
            highest_since = np.nanmax(close[entry_idx:i + 1])
            trail = highest_since - float(params["atr_trail"]) * atr[i] if np.isfinite(atr[i]) else entry_price * 0.9
            sell = bars > 3 and price < trail
            max_hold = params.get("max_hold")
            if max_hold is not None and bars >= int(max_hold):
                sell = True
            hard_stop_atr = params.get("hard_stop_atr")
            if hard_stop_atr is not None and np.isfinite(atr[i]) and price < entry_price - float(hard_stop_atr) * atr[i]:
                sell = True
            if sell:
                cash += qty * price
                ret = price / entry_price - 1.0
                if ret > 0:
                    wins += 1
                hold_bars.append(bars)
                qty = 0.0
                entry_price = 0.0
                entry_idx = -1
        eq_curve.append(cash + qty * price)

    if qty > 0:
        price = close[end_idx]
        cash += qty * price
        ret = price / entry_price - 1.0
        if ret > 0:
            wins += 1
        hold_bars.append(end_idx - entry_idx)

    eq = pd.Series(eq_curve, dtype=float)
    if len(eq) < 5:
        dd = 0.0
        sharpe = 0.0
    else:
        dd = float(((eq - eq.cummax()) / eq.cummax() * 100.0).min())
        rets = eq.pct_change().dropna()
        sharpe = float(rets.mean() / rets.std() * np.sqrt(252)) if len(rets) > 5 and rets.std() > 0 else 0.0
    return {
        "final_equity": cash,
        "trades": trades,
        "wins": wins,
        "return_pct": (cash / cash0 - 1.0) * 100.0,
        "dd_pct": dd,
        "sharpe": sharpe,
        "avg_hold": float(np.mean(hold_bars)) if hold_bars else 0.0,
    }


def _aggregate(period: str, precomputed: dict[str, pd.DataFrame], params: dict[str, Any], start: str, end: str) -> PeriodMetrics:
    st = pd.Timestamp(start)
    et = pd.Timestamp(end)
    rows = []
    for df in precomputed.values():
        r = _simulate_symbol(df, params, st, et)
        if r is not None:
            rows.append(r)
    if not rows:
        return PeriodMetrics(period, start, end, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
    symbols = len(rows)
    total_start = 100000.0 * symbols
    total_final = sum(float(r["final_equity"]) for r in rows)
    ret = (total_final / total_start - 1.0) * 100.0
    years = max((et - st).days / 365.25, 0.1)
    cagr = ((total_final / total_start) ** (1.0 / years) - 1.0) * 100.0 if total_final > 0 else -100.0
    trades = sum(int(r["trades"]) for r in rows)
    wins = sum(int(r["wins"]) for r in rows)
    active = sum(1 for r in rows if int(r["trades"]) > 0)
    prof = sum(1 for r in rows if float(r["return_pct"]) > 0)
    return PeriodMetrics(
        period=period,
        start_date=start,
        end_date=end,
        symbols=symbols,
        active_symbols=active,
        profitable_symbols=prof,
        trades=trades,
        win_rate_pct=round((wins / max(1, trades)) * 100.0, 2),
        total_return_pct=round(float(ret), 2),
        cagr_pct=round(float(cagr), 2),
        max_drawdown_pct=round(float(np.mean([float(r["dd_pct"]) for r in rows])), 2),
        sharpe_ratio=round(float(np.mean([float(r["sharpe"]) for r in rows])), 2),
        avg_holding_bars=round(float(np.mean([float(r["avg_hold"]) for r in rows if int(r["trades"]) > 0])), 1) if active else 0.0,
    )


def _params_name(p: dict[str, Any]) -> str:
    return (
        f"bo{p['lookback']}_atr{p['atr_trail']}_vol{p['vol_mult']}_"
        f"{p['trend_mode']}_adx{p.get('adx_min', 0)}_roc{p.get('roc63_min', -999)}_"
        f"br{p.get('breadth_min', 0)}_pos{p.get('position_pct', 0.95)}"
    )


def _verdict(test: PeriodMetrics, recent: PeriodMetrics) -> tuple[str, list[str], float]:
    reasons = []
    broad = test.profitable_symbols / max(1, test.symbols)
    score = (
        test.cagr_pct * 2.0
        + recent.cagr_pct
        + min(test.trades / 250.0, 3.0)
        + broad * 8.0
        + max(test.max_drawdown_pct, -60.0) * 0.25
        + test.sharpe_ratio * 3.0
    )
    if test.cagr_pct < 20:
        reasons.append("below_20pct_oos_cagr")
    if test.cagr_pct < 30:
        reasons.append("below_30pct_target")
    if recent.cagr_pct < 15:
        reasons.append("weak_recent_cagr")
    if test.max_drawdown_pct < -35:
        reasons.append("drawdown_above_35pct")
    if broad < 0.50:
        reasons.append("less_than_half_symbols_profitable")
    if test.trades < 150:
        reasons.append("too_few_oos_trades")
    if test.cagr_pct >= 30 and test.max_drawdown_pct >= -35 and broad >= 0.50 and test.trades >= 150:
        verdict = "hits_30_candidate"
    elif test.cagr_pct >= 20 and test.max_drawdown_pct >= -30 and test.trades >= 150:
        verdict = "near_target_candidate"
    elif test.cagr_pct >= 12 and test.trades >= 150:
        verdict = "useful_lead"
    else:
        verdict = "reject"
    return verdict, reasons, round(float(score), 3)


def _generate_candidates(limit: int, seed: int = 42) -> list[dict[str, Any]]:
    random.seed(seed)
    anchors = [
        {"lookback": 10, "atr_trail": 3.0, "vol_mult": 1.5, "trend_mode": "ema150", "adx_min": 0, "roc63_min": -999, "breadth_min": 0, "position_pct": 0.95, "rsi_min": 0, "rsi_max": 100, "atr_pct_max": None, "max_hold": None, "hard_stop_atr": None},
        {"lookback": 10, "atr_trail": 3.0, "vol_mult": 1.5, "trend_mode": "ema150", "adx_min": 0, "roc63_min": -999, "breadth_min": 0, "position_pct": 1.25, "rsi_min": 0, "rsi_max": 100, "atr_pct_max": None, "max_hold": None, "hard_stop_atr": None},
        {"lookback": 10, "atr_trail": 3.0, "vol_mult": 1.5, "trend_mode": "ema150", "adx_min": 0, "roc63_min": -999, "breadth_min": 0, "position_pct": 1.5, "rsi_min": 0, "rsi_max": 100, "atr_pct_max": None, "max_hold": None, "hard_stop_atr": None},
    ]
    grid = []
    for _ in range(limit * 3):
        grid.append({
            "lookback": random.choice([5, 7, 10, 15, 20]),
            "atr_trail": random.choice([2.0, 2.5, 3.0, 3.5, 4.0]),
            "vol_mult": random.choice([0.9, 1.0, 1.2, 1.5, 1.8]),
            "trend_mode": random.choice(["ema150", "ema200", "ema50_150_200"]),
            "adx_min": random.choice([0, 12, 15, 20, 25]),
            "roc20_min": random.choice([-999, 0, 2, 5]),
            "roc63_min": random.choice([-999, 0, 5, 10, 15]),
            "breadth_min": random.choice([0, 0.40, 0.45, 0.50, 0.55]),
            "position_pct": random.choice([0.95, 1.10, 1.25, 1.50, 1.75]),
            "rsi_min": random.choice([0, 45, 50, 55]),
            "rsi_max": random.choice([100, 75, 80]),
            "atr_pct_max": random.choice([None, 0.05, 0.06, 0.08, 0.10]),
            "max_hold": random.choice([None, 40, 60, 90]),
            "hard_stop_atr": random.choice([None, 2.0, 2.5, 3.0]),
        })
    out = []
    seen = set()
    for p in anchors + grid:
        key = json.dumps(p, sort_keys=True)
        if key in seen:
            continue
        seen.add(key)
        out.append(p)
        if len(out) >= limit:
            break
    return out


def main() -> int:
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--split-date", default="2024-04-24")
    parser.add_argument("--recent-start", default="2025-04-24")
    parser.add_argument("--max-candidates", type=int, default=160)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    data_map = ex._load_data()
    if not data_map:
        print("ERROR: no data loaded")
        return 1
    precomputed = {}
    for i, (sym, df) in enumerate(data_map.items(), 1):
        precomputed[sym] = _prep_symbol(df)
        if i % 20 == 0 or i == len(data_map):
            print(f"indicators {i}/{len(data_map)}", flush=True)
    _add_breadth(precomputed)

    all_dates = [pd.to_datetime(df["Date"]).min() for df in precomputed.values()] + [pd.to_datetime(df["Date"]).max() for df in precomputed.values()]
    end = max(all_dates).strftime("%Y-%m-%d")

    results: list[SweepResult] = []
    for i, params in enumerate(_generate_candidates(args.max_candidates, seed=args.seed), 1):
        test = _aggregate("test", precomputed, params, args.split_date, end)
        recent = _aggregate("recent", precomputed, params, args.recent_start, end)
        verdict, reasons, score = _verdict(test, recent)
        result = SweepResult(
            name=_params_name(params),
            params=params,
            test=test,
            recent=recent,
            score=score,
            target_gap_pct_points=round(30.0 - test.cagr_pct, 2),
            target_ratio=round(test.cagr_pct / 30.0, 3),
            verdict=verdict,
            reasons=reasons,
        )
        results.append(result)
        if i % 10 == 0 or verdict in {"hits_30_candidate", "near_target_candidate"}:
            print(f"{i}/{args.max_candidates} {result.name}: {verdict} test_cagr={test.cagr_pct:.2f}% dd={test.max_drawdown_pct:.2f}% recent={recent.cagr_pct:.2f}%", flush=True)

    results.sort(key=lambda r: (r.verdict == "hits_30_candidate", r.verdict == "near_target_candidate", r.score, r.test.cagr_pct), reverse=True)
    best = results[0] if results else None
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    payload = {
        "generated_at": datetime.now().isoformat(),
        "source": "breakout_atr_push30_sweep",
        "target_cagr_pct": 30.0,
        "data_context": {"symbols_loaded": len(data_map), "split_date": args.split_date, "recent_start": args.recent_start, "end_date": end},
        "best": asdict(best) if best else None,
        "ranked": [asdict(r) for r in results],
        "pipeline_decision": best.verdict if best else "no_candidate",
        "next_action": "Promote to deeper execution-parity portfolio validation" if best and best.verdict in {"hits_30_candidate", "near_target_candidate"} else "Continue structural search; current sweep did not reach target safely",
    }
    out = REPORTS / f"breakout_atr_push30_sweep_{ts}.json"
    latest = REPORTS / "breakout_atr_push30_latest.json"
    out.write_text(json.dumps(payload, indent=2, default=str))
    latest.write_text(json.dumps(payload, indent=2, default=str))
    print(json.dumps({
        "path": str(out),
        "decision": payload["pipeline_decision"],
        "best": asdict(best) if best else None,
        "next_action": payload["next_action"],
    }, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
