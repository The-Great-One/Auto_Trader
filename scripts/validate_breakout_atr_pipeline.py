#!/usr/bin/env python3
"""Validate breakout+ATR structural strategy candidates and publish pipeline output.

This is the promotion step after scripts/explore_strategies.py finds promising
non-RS7 candidates. It deliberately does NOT use RULE_SET_7/2. The goal is to
validate whether the clean breakout family survives out-of-sample and can be
fed into the next research iteration.

Outputs:
- reports/breakout_atr_validation_<timestamp>.json
- reports/breakout_atr_validation_<timestamp>.md
- reports/breakout_atr_pipeline_candidate_latest.json
"""

from __future__ import annotations

import json
import os
import re
import sys
from dataclasses import asdict, dataclass
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
class PeriodResult:
    period: str
    start_date: str
    end_date: str
    symbols_tested: int
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
class CandidateValidation:
    name: str
    params: dict[str, Any]
    full: PeriodResult
    train: PeriodResult
    test: PeriodResult
    recent: PeriodResult
    verdict: str
    reasons: list[str]
    promotion_score: float


def _default_candidates() -> list[dict[str, Any]]:
    """Seed around exploration's winning breakout family."""
    return [
        {"lookback": 10, "atr_trail": 3.0, "vol_mult": 1.0},
        {"lookback": 10, "atr_trail": 3.0, "vol_mult": 1.2},
        {"lookback": 10, "atr_trail": 3.0, "vol_mult": 1.5},
        {"lookback": 10, "atr_trail": 2.5, "vol_mult": 1.0},
        {"lookback": 20, "atr_trail": 3.0, "vol_mult": 1.0},
    ]


def _parse_breakout_name(name: str) -> dict[str, Any] | None:
    m = re.match(r"breakout_(\d+)_atr([0-9.]+)_vol([0-9.]+)$", name)
    if not m:
        return None
    return {"lookback": int(m.group(1)), "atr_trail": float(m.group(2)), "vol_mult": float(m.group(3))}


def _load_exploration_candidates(top_n: int = 8) -> list[dict[str, Any]]:
    path = REPORTS / "exploration_results.jsonl"
    rows: list[dict[str, Any]] = []
    if path.exists():
        for line in path.read_text(errors="ignore").splitlines():
            try:
                row = json.loads(line)
            except Exception:
                continue
            if str(row.get("name", "")).startswith("breakout_"):
                rows.append(row)
    rows.sort(key=lambda r: float(r.get("cagr_pct", -999)), reverse=True)
    candidates: list[dict[str, Any]] = []
    seen = set()
    for row in rows[:top_n]:
        params = row.get("params") or _parse_breakout_name(str(row.get("name", "")))
        if not params:
            continue
        key = (int(params["lookback"]), float(params["atr_trail"]), float(params["vol_mult"]))
        if key in seen:
            continue
        seen.add(key)
        candidates.append(dict(params))
    for params in _default_candidates():
        key = (int(params["lookback"]), float(params["atr_trail"]), float(params["vol_mult"]))
        if key not in seen:
            candidates.append(params)
            seen.add(key)
    return candidates


def _entry_fn(params: dict[str, Any], trend_filter: bool, atr_pct_max: float | None):
    lookback = int(params["lookback"])
    vol_mult = float(params["vol_mult"])

    def entry(df: pd.DataFrame, i: int) -> bool:
        if i < max(lookback + 5, 200):
            return False
        row = df.iloc[i]
        prev = df.iloc[i - 1]
        close = float(row["Close"])
        hhv = float(df["High"].iloc[max(0, i - lookback):i].max())
        breakout = close > hhv and float(prev["Close"]) <= hhv
        if not breakout:
            return False
        if float(row.get("Vol_Ratio", 1.0)) < vol_mult:
            return False
        if trend_filter:
            ema50 = float(row.get("EMA50", close))
            ema150 = float(row.get("EMA150", close))
            if not (close > ema150 and ema50 >= ema150):
                return False
        if atr_pct_max is not None and float(row.get("ATR_pct", 0.0)) > atr_pct_max:
            return False
        return True

    return entry


def _exit_fn(params: dict[str, Any], max_hold: int | None):
    atr_trail = float(params["atr_trail"])

    def exit_(df: pd.DataFrame, i: int, entry_price: float, entry_idx: int, bars_held: int) -> bool:
        atr = float(df.iloc[i].get("ATR", 0.0) or 0.0)
        close = float(df.iloc[i]["Close"])
        if atr > 0 and entry_idx is not None:
            highest_since = float(df.iloc[entry_idx:i + 1]["Close"].max())
            trail = highest_since - atr_trail * atr
            if close < trail and bars_held > 3:
                return True
        if max_hold is not None and bars_held >= max_hold:
            return True
        return False

    return exit_


def _simulate_symbol_period(
    df: pd.DataFrame,
    entry_fn,
    exit_fn,
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
    cash: float = 100000.0,
    position_pct: float = 0.95,
) -> dict[str, Any]:
    dates = pd.to_datetime(df["Date"])
    eligible = np.where((dates >= start_ts) & (dates <= end_ts))[0]
    if len(eligible) == 0:
        return {"skip": True}
    start_idx = max(200, int(eligible[0]))
    end_idx = int(eligible[-1])
    if end_idx <= start_idx + 20:
        return {"skip": True}

    capital = cash
    qty = 0
    entry_price = 0.0
    entry_idx: int | None = None
    trades = wins = 0
    trade_returns: list[float] = []
    holding_bars: list[int] = []
    equity_curve: list[float] = []

    for i in range(start_idx, end_idx + 1):
        price = float(df.iloc[i]["Close"])
        if qty == 0:
            if entry_fn(df, i):
                buy_qty = int((capital * position_pct) // price)
                if buy_qty > 0:
                    qty = buy_qty
                    capital -= qty * price
                    entry_price = price
                    entry_idx = i
                    trades += 1
        else:
            bars = i - int(entry_idx)
            if exit_fn(df, i, entry_price, int(entry_idx), bars):
                capital += qty * price
                ret = (price / entry_price - 1.0) * 100.0
                trade_returns.append(ret)
                if ret > 0:
                    wins += 1
                holding_bars.append(bars)
                qty = 0
                entry_price = 0.0
                entry_idx = None
        equity_curve.append(capital + qty * price)

    if qty > 0:
        price = float(df.iloc[end_idx]["Close"])
        capital += qty * price
        ret = (price / entry_price - 1.0) * 100.0
        trade_returns.append(ret)
        if ret > 0:
            wins += 1
        if entry_idx is not None:
            holding_bars.append(end_idx - int(entry_idx))

    eq = pd.Series(equity_curve, dtype=float)
    if len(eq) > 2:
        peak = eq.cummax()
        dd = float(((eq - peak) / peak * 100.0).min())
        rets = eq.pct_change().dropna()
        sharpe = float(rets.mean() / rets.std() * np.sqrt(252)) if len(rets) > 5 and rets.std() > 0 else 0.0
    else:
        dd = 0.0
        sharpe = 0.0
    return {
        "skip": False,
        "final_equity": capital,
        "total_return_pct": (capital / cash - 1.0) * 100.0,
        "trades": trades,
        "wins": wins,
        "trade_returns": trade_returns,
        "avg_holding_bars": float(np.mean(holding_bars)) if holding_bars else 0.0,
        "max_drawdown_pct": dd,
        "sharpe_ratio": sharpe,
    }


def _run_period(
    period: str,
    data_map: dict[str, pd.DataFrame],
    precomputed: dict[str, pd.DataFrame],
    params: dict[str, Any],
    start: str,
    end: str,
    trend_filter: bool,
    atr_pct_max: float | None,
    max_hold: int | None,
) -> PeriodResult:
    start_ts = pd.Timestamp(start)
    end_ts = pd.Timestamp(end)
    entry = _entry_fn(params, trend_filter=trend_filter, atr_pct_max=atr_pct_max)
    exit_ = _exit_fn(params, max_hold=max_hold)
    rows = []
    for sym in data_map:
        df = precomputed.get(sym)
        if df is None:
            continue
        res = _simulate_symbol_period(df, entry, exit_, start_ts, end_ts)
        if not res.get("skip"):
            rows.append(res)
    if not rows:
        return PeriodResult(period, start, end, 0, 0, 0, 0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
    symbols_tested = len(rows)
    active = sum(1 for r in rows if int(r["trades"]) > 0)
    profitable = sum(1 for r in rows if float(r["total_return_pct"]) > 0)
    total_start = 100000.0 * symbols_tested
    total_final = sum(float(r["final_equity"]) for r in rows)
    ret = (total_final / total_start - 1.0) * 100.0
    years = max((end_ts - start_ts).days / 365.25, 0.1)
    cagr = ((total_final / total_start) ** (1.0 / years) - 1.0) * 100.0 if total_final > 0 else -100.0
    trades = sum(int(r["trades"]) for r in rows)
    wins = sum(int(r["wins"]) for r in rows)
    dd = float(np.mean([float(r["max_drawdown_pct"]) for r in rows]))
    sharpe = float(np.mean([float(r["sharpe_ratio"]) for r in rows]))
    hold = float(np.mean([float(r["avg_holding_bars"]) for r in rows if int(r["trades"]) > 0])) if active else 0.0
    return PeriodResult(
        period=period,
        start_date=start,
        end_date=end,
        symbols_tested=symbols_tested,
        active_symbols=active,
        profitable_symbols=profitable,
        trades=trades,
        win_rate_pct=round((wins / max(1, trades)) * 100.0, 2),
        total_return_pct=round(float(ret), 2),
        cagr_pct=round(float(cagr), 2),
        max_drawdown_pct=round(dd, 2),
        sharpe_ratio=round(sharpe, 2),
        avg_holding_bars=round(hold, 1),
    )


def _date_range(data_map: dict[str, pd.DataFrame]) -> tuple[str, str]:
    starts, ends = [], []
    for df in data_map.values():
        d = pd.to_datetime(df["Date"])
        starts.append(d.min())
        ends.append(d.max())
    return min(starts).strftime("%Y-%m-%d"), max(ends).strftime("%Y-%m-%d")


def _verdict(full: PeriodResult, train: PeriodResult, test: PeriodResult, recent: PeriodResult) -> tuple[str, list[str], float]:
    reasons: list[str] = []
    score = 0.0
    # Reward test/recent edge, penalize drawdown and fragility.
    score += test.cagr_pct * 2.0
    score += recent.cagr_pct * 1.0
    score += min(test.trades / 200.0, 2.0)
    score += (test.profitable_symbols / max(1, test.symbols_tested)) * 5.0
    score += max(test.max_drawdown_pct, -60.0) * 0.15  # drawdown is negative
    score += test.sharpe_ratio * 2.0

    if test.trades < 100:
        reasons.append("too_few_oos_trades")
    if test.cagr_pct <= 0:
        reasons.append("negative_or_flat_oos_cagr")
    if recent.cagr_pct <= 0:
        reasons.append("negative_or_flat_recent_cagr")
    if test.max_drawdown_pct < -35:
        reasons.append("oos_drawdown_above_35pct")
    if test.profitable_symbols / max(1, test.symbols_tested) < 0.55:
        reasons.append("not_broad_enough_symbol_edge")
    if train.cagr_pct > 0 and test.cagr_pct < 0.4 * train.cagr_pct:
        reasons.append("large_train_to_test_degradation")

    if not reasons and test.cagr_pct >= 8 and recent.cagr_pct >= 4 and test.max_drawdown_pct >= -35:
        verdict = "promote_to_structural_optuna"
    elif test.cagr_pct > 0 and test.trades >= 100:
        verdict = "needs_risk_tuning"
    else:
        verdict = "reject_for_now"
    return verdict, reasons, round(float(score), 3)


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Validate breakout+ATR pipeline candidates")
    parser.add_argument("--split-date", default="2024-04-24", help="OOS test start date")
    parser.add_argument("--recent-start", default="2025-04-24", help="Recent stress period start date")
    parser.add_argument("--max-hold", type=int, default=0, help="Optional max holding bars; 0 disables")
    parser.add_argument("--trend-filter", action="store_true", help="Require close>EMA150 and EMA50>=EMA150 at entry")
    parser.add_argument("--atr-pct-max", type=float, default=0.0, help="Optional ATR% cap; 0 disables")
    parser.add_argument("--top-n", type=int, default=5, help="Top exploration breakout candidates to validate")
    parser.add_argument("--risk-variants", choices=["minimal", "standard", "full"], default="standard", help="Risk-control variant breadth")
    args = parser.parse_args()

    data_map = ex._load_data()
    if not data_map:
        print("ERROR: no data loaded")
        return 1
    start, end = _date_range(data_map)
    split = args.split_date
    recent_start = args.recent_start
    max_hold = args.max_hold or None
    atr_pct_max = args.atr_pct_max or None

    precomputed: dict[str, pd.DataFrame] = {}
    for sym, df in data_map.items():
        try:
            precomputed[sym] = ex._compute_indicators(df)
        except Exception:
            pass

    candidates = _load_exploration_candidates(top_n=args.top_n)
    validations: list[CandidateValidation] = []
    for base_idx, base in enumerate(candidates, start=1):
        # Test base plus a small set of risk-control variants. Keep search interpretable.
        if args.risk_variants == "minimal":
            variants = [
                {**base, "trend_filter": args.trend_filter, "atr_pct_max": atr_pct_max, "max_hold": max_hold},
                {**base, "trend_filter": True, "atr_pct_max": atr_pct_max, "max_hold": max_hold},
            ]
        elif args.risk_variants == "full":
            variants = [
                {**base, "trend_filter": args.trend_filter, "atr_pct_max": atr_pct_max, "max_hold": max_hold},
                {**base, "trend_filter": True, "atr_pct_max": atr_pct_max, "max_hold": max_hold},
                {**base, "trend_filter": True, "atr_pct_max": 0.08, "max_hold": max_hold},
                {**base, "trend_filter": True, "atr_pct_max": 0.06, "max_hold": max_hold},
                {**base, "trend_filter": True, "atr_pct_max": 0.08, "max_hold": 60},
            ]
        else:
            variants = [
                {**base, "trend_filter": args.trend_filter, "atr_pct_max": atr_pct_max, "max_hold": max_hold},
                {**base, "trend_filter": True, "atr_pct_max": atr_pct_max, "max_hold": max_hold},
                {**base, "trend_filter": True, "atr_pct_max": 0.08, "max_hold": max_hold},
            ]
        seen = set()
        for params in variants:
            key = json.dumps(params, sort_keys=True)
            if key in seen:
                continue
            seen.add(key)
            tf = bool(params.pop("trend_filter"))
            apm = params.pop("atr_pct_max")
            mh = params.pop("max_hold")
            name = f"breakout_{params['lookback']}_atr{params['atr_trail']}_vol{params['vol_mult']}"
            if tf:
                name += "_trend"
            if apm:
                name += f"_atrpct{apm}"
            if mh:
                name += f"_maxh{mh}"
            run_params = {**params, "trend_filter": tf, "atr_pct_max": apm, "max_hold": mh}
            full = _run_period("full", data_map, precomputed, params, start, end, tf, apm, mh)
            train = _run_period("train", data_map, precomputed, params, start, (pd.Timestamp(split) - pd.Timedelta(days=1)).strftime("%Y-%m-%d"), tf, apm, mh)
            test = _run_period("test", data_map, precomputed, params, split, end, tf, apm, mh)
            recent = _run_period("recent", data_map, precomputed, params, recent_start, end, tf, apm, mh)
            verdict, reasons, score = _verdict(full, train, test, recent)
            validations.append(CandidateValidation(name, run_params, full, train, test, recent, verdict, reasons, score))
            print(
                f"[{base_idx}/{len(candidates)}] {name}: verdict={verdict} "
                f"test_cagr={test.cagr_pct:.2f}% test_dd={test.max_drawdown_pct:.2f}% "
                f"recent_cagr={recent.cagr_pct:.2f}% trades={test.trades}",
                flush=True,
            )

    validations.sort(key=lambda v: (v.verdict == "promote_to_structural_optuna", v.promotion_score, v.test.cagr_pct), reverse=True)
    best = validations[0] if validations else None
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    payload = {
        "generated_at": datetime.now().isoformat(),
        "source": "breakout_atr_structural_exploration",
        "data_context": {
            "symbols_loaded": len(data_map),
            "symbols_with_indicators": len(precomputed),
            "date_start": start,
            "date_end": end,
            "split_date": split,
            "recent_start": recent_start,
            "history_source": "Kite feather cache",
        },
        "best": asdict(best) if best else None,
        "ranked": [asdict(v) for v in validations],
        "pipeline_decision": (best.verdict if best else "no_candidate"),
        "next_action": (
            "Add breakout_atr to structural Optuna/risk-tuning sweep" if best and best.verdict == "promote_to_structural_optuna"
            else "Run risk-control sweep before live consideration" if best and best.verdict == "needs_risk_tuning"
            else "Do not promote; explore different structural families"
        ),
        "notes": [
            "This is a structural strategy validation, not an RS7 threshold variant.",
            "Test period starts at split_date and uses prior bars only for indicator warmup.",
            "Live deployment remains blocked until OOS risk gates pass and execution parity is implemented.",
        ],
    }
    out_json = REPORTS / f"breakout_atr_validation_{ts}.json"
    out_md = REPORTS / f"breakout_atr_validation_{ts}.md"
    latest = REPORTS / "breakout_atr_pipeline_candidate_latest.json"
    out_json.write_text(json.dumps(payload, indent=2, default=str))
    latest.write_text(json.dumps(payload, indent=2, default=str))

    lines = [
        f"# Breakout ATR Validation {ts}",
        "",
        f"- Symbols: {len(data_map)}",
        f"- Date range: {start} to {end}",
        f"- Split date: {split}",
        f"- Best: **{best.name if best else 'none'}**",
        f"- Decision: **{payload['pipeline_decision']}**",
        "",
        "## Top candidates",
    ]
    for v in validations[:10]:
        lines.append(
            f"- **{v.name}** — score {v.promotion_score}, verdict `{v.verdict}`; "
            f"test CAGR {v.test.cagr_pct:.2f}%, ret {v.test.total_return_pct:.2f}%, "
            f"DD {v.test.max_drawdown_pct:.2f}%, trades {v.test.trades}, "
            f"recent CAGR {v.recent.cagr_pct:.2f}%"
        )
    out_md.write_text("\n".join(lines) + "\n")

    print(json.dumps({
        "path": str(out_json),
        "latest": str(latest),
        "best": best.name if best else None,
        "decision": payload["pipeline_decision"],
        "test_cagr_pct": best.test.cagr_pct if best else None,
        "test_return_pct": best.test.total_return_pct if best else None,
        "test_max_drawdown_pct": best.test.max_drawdown_pct if best else None,
        "recent_cagr_pct": best.recent.cagr_pct if best else None,
        "next_action": payload["next_action"],
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
