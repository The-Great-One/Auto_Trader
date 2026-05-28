#!/usr/bin/env python3
"""Kite-only RSI 22/44/66 momentum rotation lab.

Researches the YouTube/GUI scanner idea without yfinance or GUI code:
  1. load Kite feather OHLCV from intermediary_files/Hist_Data
  2. compute RSI(22), RSI(44), RSI(66), average them
  3. rank symbols at each rebalance date
  4. hold equal-weight top-N until next rebalance
  5. report transaction-cost-adjusted performance and yearly OOS-style folds

This is a research lab only. It intentionally writes ignored reports and does not
change live Auto_Trader rules.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import sys
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "reports"
DEFAULT_HIST_DIRS = [
    ROOT / "intermediary_files" / "Hist_Data",
    ROOT.parent / "Stocks" / "intermediary_files" / "Hist_Data",
]


@dataclass
class RotationResult:
    name: str
    rebalance: str
    top_n: int
    cost_bps: float
    regime: str
    start: str
    end: str
    days: int
    symbols_loaded: int
    avg_positions: float
    total_return_pct: float
    cagr_pct: float
    xirr_pct: float
    max_drawdown_pct: float
    vol_pct: float
    sharpe_like: float
    turnover_monthly_equiv: float
    worst_year_pct: float
    positive_years: int
    total_years: int
    selection_score: float


def now_iso() -> str:
    return datetime.now().astimezone().isoformat()


def parse_symbols(raw: str | None) -> set[str] | None:
    if not raw:
        return None
    vals = {x.strip().upper() for x in raw.split(",") if x.strip()}
    return vals or None


def is_derivative_symbol(symbol: str) -> bool:
    s = symbol.upper()
    return any(tag in s for tag in ("CE", "PE", "FUT")) or any(ch.isdigit() for ch in s[-8:])


def find_hist_dir(value: str | None) -> Path:
    if value:
        p = Path(value).expanduser()
        if p.exists():
            return p
        raise SystemExit(f"hist dir does not exist: {p}")
    for p in DEFAULT_HIST_DIRS:
        if p.exists():
            return p
    raise SystemExit("No Hist_Data directory found")


def load_prices(
    hist_dir: Path,
    min_rows: int,
    min_end_date: str,
    symbols: set[str] | None,
    max_symbols: int,
) -> tuple[pd.DataFrame, dict]:
    min_end = pd.Timestamp(min_end_date) if min_end_date else None
    loaded: list[pd.Series] = []
    skipped: dict[str, int] = {"derivative": 0, "not_requested": 0, "too_short": 0, "stale": 0, "read_error": 0}
    summaries: list[dict] = []

    files = sorted(hist_dir.glob("*.feather"))
    for fp in files:
        symbol = fp.stem.upper()
        if symbols and symbol not in symbols:
            skipped["not_requested"] += 1
            continue
        if is_derivative_symbol(symbol):
            skipped["derivative"] += 1
            continue
        try:
            df = pd.read_feather(fp)
            cmap = {str(c).lower(): c for c in df.columns}
            if "date" not in cmap or "close" not in cmap:
                skipped["read_error"] += 1
                continue
            s = pd.DataFrame(
                {
                    "date": pd.to_datetime(df[cmap["date"]], errors="coerce"),
                    "close": pd.to_numeric(df[cmap["close"]], errors="coerce"),
                }
            ).dropna()
            s = s.drop_duplicates("date").sort_values("date")
            if len(s) < min_rows:
                skipped["too_short"] += 1
                continue
            if min_end is not None and s["date"].max() < min_end:
                skipped["stale"] += 1
                continue
            loaded.append(s.set_index("date")["close"].rename(symbol))
            summaries.append(
                {
                    "symbol": symbol,
                    "rows": int(len(s)),
                    "start": str(s["date"].min().date()),
                    "end": str(s["date"].max().date()),
                }
            )
            if max_symbols and len(loaded) >= max_symbols:
                break
        except Exception:
            skipped["read_error"] += 1

    if not loaded:
        raise SystemExit(f"No usable symbols loaded from {hist_dir}")
    prices = pd.concat(loaded, axis=1).sort_index().dropna(how="all")
    context = {
        "hist_dir": str(hist_dir),
        "symbols_loaded": len(loaded),
        "skipped": skipped,
        "date_range": [str(prices.index.min().date()), str(prices.index.max().date())],
        "min_rows": min_rows,
        "min_end_date": min_end_date,
        "loaded_symbols": [x["symbol"] for x in summaries],
        "symbol_summaries_sample": summaries[:50],
    }
    return prices, context


def rsi_dataframe(prices: pd.DataFrame, period: int) -> pd.DataFrame:
    delta = prices.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.rolling(window=period, min_periods=period).mean()
    avg_loss = loss.rolling(window=period, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    out = 100 - (100 / (1 + rs))
    # Require a real continuous-ish window; no synthetic score on missing prices.
    valid = prices.notna().rolling(window=period + 1, min_periods=period + 1).sum() >= period + 1
    return out.where(valid)


def rebalance_dates(index: pd.DatetimeIndex, freq: str) -> list[pd.Timestamp]:
    out: list[pd.Timestamp] = []
    for d in pd.Series(index=index, dtype=float).resample(freq).last().index:
        loc = index[index <= d]
        if len(loc):
            out.append(loc[-1])
    return sorted(set(out))


def build_regime_mask(prices: pd.DataFrame, mode: str) -> pd.Series:
    if mode == "none":
        return pd.Series(True, index=prices.index)
    # Synthetic universe index avoids external/yfinance index dependency.
    eq_index = prices.ffill(limit=3).pct_change(fill_method=None).mean(axis=1).fillna(0).add(1).cumprod()
    if mode == "universe_sma50":
        return eq_index > eq_index.rolling(50, min_periods=50).mean()
    if mode == "universe_sma100":
        return eq_index > eq_index.rolling(100, min_periods=100).mean()
    if mode == "universe_sma200":
        return eq_index > eq_index.rolling(200, min_periods=200).mean()
    raise ValueError(f"unknown regime mode: {mode}")


def metrics(name: str, returns: pd.Series, weights: pd.DataFrame, turnover: pd.Series, params: dict) -> RotationResult:
    active = weights.sum(axis=1) > 0
    r = returns.loc[active].copy()
    if r.empty:
        raise ValueError(f"empty active returns for {name}")
    eq = (1 + r).cumprod()
    years = len(r) / 252
    cagr = eq.iloc[-1] ** (1 / years) - 1 if years > 0 else np.nan
    # Investor XIRR/CAGR should include cash/out-of-market days after the first
    # active allocation. Keep active-only CAGR for strategy selection continuity,
    # but report xirr_pct on the elapsed investor calendar.
    elapsed = returns.loc[r.index[0] : returns.index[-1]].copy()
    elapsed_eq = (1 + elapsed).cumprod()
    elapsed_years = len(elapsed) / 252
    xirr = elapsed_eq.iloc[-1] ** (1 / elapsed_years) - 1 if elapsed_years > 0 else np.nan
    dd = eq / eq.cummax() - 1
    vol = r.std() * math.sqrt(252)
    sharpe = (r.mean() * 252) / vol if vol and not np.isnan(vol) else np.nan
    yearly = r.groupby(r.index.year).apply(lambda x: (1 + x).prod() - 1)
    avg_positions = weights.loc[active].astype(bool).sum(axis=1).mean()
    turnover_monthly_equiv = turnover.sum() / max(len(r) / 21, 1)
    # Penalize drawdown/turnover so pure headline CAGR does not dominate ranking.
    selection_score = (cagr * 100) + (dd.min() * 35) - (turnover_monthly_equiv * 1.5)
    return RotationResult(
        name=name,
        rebalance=params["rebalance"],
        top_n=int(params["top_n"]),
        cost_bps=float(params["cost_bps"]),
        regime=params["regime"],
        start=str(r.index[0].date()),
        end=str(r.index[-1].date()),
        days=int(len(r)),
        symbols_loaded=int(params["symbols_loaded"]),
        avg_positions=round(float(avg_positions), 2),
        total_return_pct=round((eq.iloc[-1] - 1) * 100, 2),
        cagr_pct=round(cagr * 100, 2),
        xirr_pct=round(xirr * 100, 2),
        max_drawdown_pct=round(dd.min() * 100, 2),
        vol_pct=round(vol * 100, 2),
        sharpe_like=round(float(sharpe), 3) if not np.isnan(sharpe) else 0.0,
        turnover_monthly_equiv=round(float(turnover_monthly_equiv), 3),
        worst_year_pct=round(float(yearly.min() * 100), 2),
        positive_years=int((yearly > 0).sum()),
        total_years=int(len(yearly)),
        selection_score=round(float(selection_score), 3),
    )


def run_rotation(
    prices_raw: pd.DataFrame,
    score: pd.DataFrame,
    rebalance: str,
    top_n: int,
    cost_bps: float,
    regime: str,
    ffill_limit: int,
) -> tuple[RotationResult, dict]:
    prices = prices_raw.ffill(limit=ffill_limit)
    returns = prices.pct_change(fill_method=None).fillna(0)
    dates = rebalance_dates(prices.index, rebalance)
    regime_mask = build_regime_mask(prices, regime).fillna(False)

    weights = pd.DataFrame(0.0, index=prices.index, columns=prices.columns)
    turnover = pd.Series(0.0, index=prices.index)
    previous = pd.Series(0.0, index=prices.columns)
    picks_log: list[dict] = []

    for i, d in enumerate(dates):
        pos = prices.index.get_loc(d)
        if pos + 1 >= len(prices.index):
            continue
        trade_date = prices.index[pos + 1]
        end_date = dates[i + 1] if i + 1 < len(dates) else prices.index[-1]
        target = pd.Series(0.0, index=prices.columns)
        if bool(regime_mask.loc[d]):
            sc = score.loc[d].dropna().sort_values(ascending=False)
            picks = [s for s in sc.index if pd.notna(prices.loc[d, s])][:top_n]
            if picks:
                target.loc[picks] = 1.0 / len(picks)
                picks_log.append(
                    {
                        "signal_date": str(d.date()),
                        "trade_date": str(trade_date.date()),
                        "picks": picks,
                        "scores": {s: round(float(sc.loc[s]), 2) for s in picks[:20]},
                    }
                )
        turnover.loc[trade_date] = abs(target - previous).sum()
        previous = target
        weights.loc[(prices.index >= trade_date) & (prices.index <= end_date), :] = target.values

    gross = (weights * returns).sum(axis=1)
    net = gross - turnover * (cost_bps / 10000.0)
    name = f"rsi224466_{rebalance}_top{top_n}_{regime}"
    result = metrics(
        name,
        net,
        weights,
        turnover,
        {
            "rebalance": rebalance,
            "top_n": top_n,
            "cost_bps": cost_bps,
            "regime": regime,
            "symbols_loaded": prices.shape[1],
        },
    )
    diagnostics = {
        "latest_picks": picks_log[-1] if picks_log else {},
        "rebalance_count": len(picks_log),
        "cash_rebalance_count": int(sum(1 for d in dates if d in regime_mask.index and not bool(regime_mask.loc[d]))),
    }
    return result, diagnostics


def equal_weight_baseline(prices_raw: pd.DataFrame, cost_bps: float, ffill_limit: int) -> RotationResult:
    prices = prices_raw.ffill(limit=ffill_limit)
    returns = prices.pct_change(fill_method=None).fillna(0)
    weights = prices.notna().astype(float)
    weights = weights.div(weights.sum(axis=1), axis=0).fillna(0)
    turnover = weights.diff().abs().sum(axis=1).fillna(weights.sum(axis=1))
    net = (weights * returns).sum(axis=1) - turnover * (cost_bps / 10000.0)
    return metrics(
        "equal_weight_available_universe",
        net,
        weights,
        turnover,
        {
            "rebalance": "daily_available",
            "top_n": prices.shape[1],
            "cost_bps": cost_bps,
            "regime": "none",
            "symbols_loaded": prices.shape[1],
        },
    )


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Kite-only RSI 22/44/66 rotation strategy lab")
    parser.add_argument("--hist-dir", default=os.getenv("AT_RSI_ROTATION_HIST_DIR", ""))
    parser.add_argument("--symbols", default=os.getenv("AT_RSI_ROTATION_SYMBOLS", ""), help="Comma-separated symbols")
    parser.add_argument("--max-symbols", type=int, default=int(os.getenv("AT_RSI_ROTATION_MAX_SYMBOLS", "0") or "0"))
    parser.add_argument("--min-rows", type=int, default=int(os.getenv("AT_RSI_ROTATION_MIN_ROWS", "700") or "700"))
    parser.add_argument("--min-end-date", default=os.getenv("AT_RSI_ROTATION_MIN_END_DATE", "2026-04-17"))
    parser.add_argument("--top-n", default=os.getenv("AT_RSI_ROTATION_TOP_N", "10,20,30"))
    parser.add_argument("--rebalance", default=os.getenv("AT_RSI_ROTATION_REBALANCE", "ME,W-FRI"))
    parser.add_argument("--regime", default=os.getenv("AT_RSI_ROTATION_REGIME", "none,universe_sma100,universe_sma200"))
    parser.add_argument("--cost-bps", type=float, default=float(os.getenv("AT_RSI_ROTATION_COST_BPS", "10") or "10"))
    parser.add_argument("--ffill-limit", type=int, default=int(os.getenv("AT_RSI_ROTATION_FFILL_LIMIT", "3") or "3"))
    return parser


def main() -> int:
    args = build_arg_parser().parse_args()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    hist_dir = find_hist_dir(args.hist_dir)
    prices, data_context = load_prices(
        hist_dir=hist_dir,
        min_rows=args.min_rows,
        min_end_date=args.min_end_date,
        symbols=parse_symbols(args.symbols),
        max_symbols=args.max_symbols,
    )
    score = (rsi_dataframe(prices, 22) + rsi_dataframe(prices, 44) + rsi_dataframe(prices, 66)) / 3.0

    top_ns = [int(x.strip()) for x in str(args.top_n).split(",") if x.strip()]
    rebalances = [x.strip() for x in str(args.rebalance).split(",") if x.strip()]
    regimes = [x.strip() for x in str(args.regime).split(",") if x.strip()]

    results: list[RotationResult] = []
    diagnostics: dict[str, dict] = {}
    baseline = equal_weight_baseline(prices, args.cost_bps, args.ffill_limit)
    results.append(baseline)

    for reb in rebalances:
        for top_n in top_ns:
            for regime in regimes:
                try:
                    res, diag = run_rotation(prices, score, reb, top_n, args.cost_bps, regime, args.ffill_limit)
                    results.append(res)
                    diagnostics[res.name] = diag
                except Exception as exc:
                    diagnostics[f"{reb}_top{top_n}_{regime}"] = {"error": str(exc)}

    ranked = sorted(results, key=lambda r: (r.selection_score, r.cagr_pct, r.max_drawdown_pct), reverse=True)
    best = ranked[0]
    recommendation = {
        "generated_at": now_iso(),
        "lab_type": "rsi_224466_rotation",
        "source_idea": "RSI Momentum Scanner V6.7 / RSI 22-44-66 average rank momentum rotation",
        "data_context": data_context,
        "params": {
            "top_n": top_ns,
            "rebalance": rebalances,
            "regime": regimes,
            "cost_bps": args.cost_bps,
            "ffill_limit": args.ffill_limit,
        },
        "baseline": asdict(baseline),
        "best": asdict(best),
        "verdict": "research_candidate" if best.xirr_pct >= 30 and best.positive_years >= max(2, best.total_years - 1) else "needs_more_validation",
        "promotion_note": "Do not promote from this report alone; run survivorship-controlled walk-forward and compare against live RS7/RS2 baseline.",
        "diagnostics_for_best": diagnostics.get(best.name, {}),
    }
    payload = {
        "recommendation": recommendation,
        "ranked": [asdict(r) for r in ranked],
        "diagnostics": diagnostics,
    }
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = OUT_DIR / f"rsi_224466_rotation_lab_{ts}.json"
    csv_path = OUT_DIR / f"rsi_224466_rotation_lab_{ts}.csv"
    latest_path = OUT_DIR / "rsi_224466_rotation_lab_latest.json"
    json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    latest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    pd.DataFrame([asdict(r) for r in ranked]).to_csv(csv_path, index=False)
    print(json.dumps(recommendation, indent=2))
    print(f"Saved: {json_path}")
    print(f"Saved: {csv_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
