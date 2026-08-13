#!/usr/bin/env python3
"""RSI + Momentum Rotation Paper Shadow — champion config.

Runs the auto-iteration champion strategy (see Trader_Labs auto_iteration_lab):
  rsi_periods [22,44,66], momentum_period 63, blend_weight 0.3,
  regime sma100, MACD filter, top_n 8, 3W-FRI rebalance, vol_weight,
  max_per_sector 3. Publishes paper decision to paper_shadow_rsi_momentum_latest.json.
No real orders placed.
"""

from __future__ import annotations

import json
import os
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
    build_regime_mask,
)

OUT_DIR = ROOT / "reports"
HIST_DIR = ROOT / "intermediary_files" / "Hist_Data"
OUT_DIR.mkdir(exist_ok=True)

# Champion params (matches Trader_Labs auto_iteration_lab BASELINE + champion
# vol_weight/lookback that qualified: CAGR 77%, Sharpe 2.32, worst year +27%).
PARAMS = {
    "rsi_periods": [22, 44, 66],
    "momentum_period": 63,
    "regime_mode": "sma100",
    "use_macd": True,
    "top_n": 8,
    "rebalance_freq": "3W-FRI",
    "cost_bps": 10.0,
    "max_per_sector": 3,
    "blend_weight": 0.3,
    "vol_weight": True,
    "vol_lookback": 10,
    "rsi_min": 0,
    "rsi_max": 100,
}

# Env overrides for cron tuning
MIN_ROWS = int(os.getenv("RSI_MOM_MIN_ROWS", "700"))
MIN_END_DATE = os.getenv("RSI_MOM_MIN_END_DATE", "2026-04-17")
INSTRUMENTS_FILE = ROOT / "intermediary_files" / "Instruments.feather"


def load_instruments() -> pd.DataFrame:
    try:
        df = pd.read_feather(INSTRUMENTS_FILE)
        df["Symbol"] = df["Symbol"].astype(str).str.upper()
        return df
    except Exception:
        return pd.DataFrame()


def filter_universe(prices: pd.DataFrame, instruments: pd.DataFrame,
                    min_vol: float = 50000.0, min_mcap: float = 500.0) -> pd.DataFrame:
    """Mirror auto_iteration_lab._filter_universe so the paper shadow backtest
    runs on the SAME universe the lab uses (min 20d avg volume, min mcap)."""
    keep = []
    for col in prices.columns:
        sym = str(col).strip().upper()
        f = HIST_DIR / f"{sym}.feather"
        if min_vol > 0 and f.exists():
            try:
                df = pd.read_feather(f)
                vc = "volume" if "volume" in df.columns else ("Volume" if "Volume" in df.columns else None)
                if vc and df[vc].tail(20).mean() < min_vol:
                    continue
            except Exception:
                pass
        if min_mcap > 0 and not instruments.empty:
            m = instruments[instruments["Symbol"] == sym]
            if not m.empty and "MarketCapCr" in m.columns:
                mc = m.iloc[0]["MarketCapCr"]
                if pd.notna(mc) and float(mc) < min_mcap:
                    continue
        keep.append(col)
    return prices[keep]


def load_hist(hist_dir: Path) -> pd.DataFrame:
    """Load the same research-grade price matrix used by the official validator."""
    if not hist_dir.is_dir():
        return pd.DataFrame()
    prices_raw, _ctx = lab_load_prices(
        hist_dir,
        min_rows=MIN_ROWS,
        min_end_date=MIN_END_DATE,
        symbols=set(),
        max_symbols=0,
    )
    return prices_raw.ffill(limit=3)


def _sector_of(instruments: pd.DataFrame, symbol: str) -> str:
    if instruments.empty:
        return "Unknown"
    m = instruments[instruments["Symbol"] == symbol]
    if m.empty or "Sector" not in m.columns:
        return "Unknown"
    return str(m.iloc[0].get("Sector", "Unknown"))


def signal_data_quality_error(
    prices: pd.DataFrame,
    picks: list[str] | None,
    top_n: int,
    min_fresh_symbols: int = 50,
    min_fresh_coverage: float = 0.8,
    max_data_age_days: int = 5,
    as_of: pd.Timestamp | None = None,
) -> str | None:
    """Return a fail-closed reason when the latest signal data is unsafe."""
    if prices.empty or len(prices.columns) == 0:
        return "no price data available for signal publication"

    latest_date = pd.Timestamp(prices.index[-1]).tz_localize(None).normalize()
    check_date = (
        pd.Timestamp.now().tz_localize(None).normalize()
        if as_of is None
        else pd.Timestamp(as_of).tz_localize(None).normalize()
    )
    age_days = int((check_date - latest_date).days)
    if age_days > max_data_age_days:
        return (
            f"latest price date {latest_date.date()} is stale by {age_days} days "
            f"(maximum {max_data_age_days})"
        )

    latest = pd.to_numeric(prices.iloc[-1], errors="coerce")
    valid = latest.apply(
        lambda value: pd.notna(value) and np.isfinite(float(value)) and float(value) > 0
    )
    fresh_count = int(valid.sum())
    total_count = len(prices.columns)
    coverage = fresh_count / total_count
    required_symbols = max(top_n, min_fresh_symbols)
    if fresh_count < required_symbols or coverage < min_fresh_coverage:
        return (
            f"latest-date fresh symbols {fresh_count}/{total_count} below safety threshold "
            f"(need >= {required_symbols} and coverage >= {min_fresh_coverage:.0%})"
        )

    if picks is not None:
        unique_picks = list(dict.fromkeys(picks))
        if len(picks) != top_n or len(unique_picks) != top_n:
            return (
                f"signal must contain exactly {top_n} unique picks; "
                f"received {len(picks)} picks ({len(unique_picks)} unique)"
            )
    return None


def compute_rotation(prices: pd.DataFrame) -> dict:
    """Compute latest champion-config rotation picks and publish paper decision."""
    p = PARAMS
    top_n = int(p["top_n"])
    if prices.empty or len(prices.columns) < max(3, top_n):
        return {"error": "insufficient symbols", "symbols_loaded": len(prices.columns)}

    pf = filter_universe(prices, load_instruments())
    if pf.empty or len(pf.columns) < max(3, top_n):
        return {"error": "insufficient symbols after universe filter", "symbols_loaded": len(prices.columns), "universe": len(pf.columns)}

    rsi_periods = p["rsi_periods"]
    score = sum(lab_rsi(pf, per) for per in rsi_periods) / len(rsi_periods)

    mom_period = int(p["momentum_period"])
    mom = pf.pct_change(mom_period, fill_method=None)

    blend_w = float(p["blend_weight"])
    if blend_w > 0:
        mom_rank = mom.rank(axis=1, pct=True)
        score = (1 - blend_w) * score + blend_w * (mom_rank * 100)

    regime_mode = p["regime_mode"]
    if regime_mode == "none":
        regime = (pf > 0).astype(float)
    else:
        rw = int(regime_mode.replace("sma", ""))
        regime = (pf > pf.rolling(rw, min_periods=rw).mean()).astype(float)

    if p.get("use_macd", True):
        ema_f = pf.ewm(span=12, min_periods=12).mean()
        ema_s = pf.ewm(span=26, min_periods=26).mean()
        macd_line = ema_f - ema_s
        macd_filter = (macd_line > macd_line.ewm(span=9, min_periods=9).mean()).astype(float)
    else:
        macd_filter = (pf > 0).astype(float)

    dates = lab_rebalance_dates(pf.index, p["rebalance_freq"])
    if len(dates) < 1:
        return {"error": "no rebalance dates"}
    actionable_dates = [d for d in dates if pf.index.get_loc(d) + 1 < len(pf.index)]
    if not actionable_dates:
        return {"error": "no actionable rebalance dates"}

    latest_date = actionable_dates[-1]
    instruments = load_instruments()

    # ---- Latest signal selection (mirrors auto_iteration_lab._simulate) ----
    rsi_at = score.loc[latest_date].copy()
    mom_at = mom.loc[latest_date].copy()
    combined = rsi_at.where(mom_at > 0, 0)
    if latest_date in regime.index:
        combined = combined.where(regime.loc[latest_date] > 0, 0)
    if latest_date in macd_filter.index:
        combined = combined.where(macd_filter.loc[latest_date] > 0, 0)
    rsi_min = float(p.get("rsi_min", 0))
    rsi_max = float(p.get("rsi_max", 100))
    combined = combined.where(rsi_at >= rsi_min, 0)
    combined = combined.where(rsi_at <= rsi_max, 0)

    scored = combined.dropna().sort_values(ascending=False)
    n_bullish = int((combined > 0).sum())
    raw_picks = [s for s in scored.index if scored[s] > 0][: top_n * 2]

    max_sec = int(p.get("max_per_sector", 0))
    picks: list[str] = []
    if max_sec > 0 and not instruments.empty:
        sector_counts: dict[str, int] = {}
        for sym in raw_picks:
            sec = _sector_of(instruments, sym)
            if sector_counts.get(sec, 0) < max_sec:
                picks.append(sym)
                sector_counts[sec] = sector_counts.get(sec, 0) + 1
            if len(picks) >= top_n:
                break
        picks = picks[:top_n]
    else:
        picks = raw_picks[:top_n]

    pick_scores = {s: round(float(score.loc[latest_date, s]), 2) for s in picks}
    latest_screened_count = int((combined > 0).sum())

    # Volatility weights for the picks (inverse vol over vol_lookback)
    weights: dict[str, float] = {}
    if p.get("vol_weight", False):
        vol_lb = int(p.get("vol_lookback", 20))
        vols = {}
        for s in picks:
            col = pf[s].loc[:latest_date].tail(vol_lb)
            vols[s] = float(col.pct_change().std()) if len(col) > 5 else 1.0
        iv = {s: 1.0 / (v + 1e-9) for s, v in vols.items()}
        ti = sum(iv.values())
        weights = {s: iv[s] / ti for s in picks}
    else:
        weights = {s: 1.0 / len(picks) for s in picks} if picks else {}

    # ---- Historical backtest with champion config (for the report) ----
    returns = pf.pct_change(fill_method=None).fillna(0)
    cost_rate = float(p["cost_bps"]) / 10000.0
    port_rets: list[float] = []
    port_dates: list[pd.Timestamp] = []
    prev_picks: set[str] = set()
    turnover_total = 0.0
    rebalance_count = 0

    for i, d in enumerate(actionable_dates):
        ed = actionable_dates[i + 1] if i + 1 < len(actionable_dates) else pf.index[-1]
        rsi_d = score.loc[d]
        mom_d = mom.loc[d]
        comb = rsi_d.where(mom_d > 0, 0)
        if d in regime.index:
            comb = comb.where(regime.loc[d] > 0, 0)
        if d in macd_filter.index:
            comb = comb.where(macd_filter.loc[d] > 0, 0)
        comb = comb.where(rsi_d >= rsi_min, 0).where(rsi_d <= rsi_max, 0)
        sc = comb.dropna().sort_values(ascending=False)
        raw = [s for s in sc.index if sc[s] > 0][: top_n * 2]
        period_picks: list[str] = []
        if max_sec > 0 and not instruments.empty:
            scnt: dict[str, int] = {}
            for sym in raw:
                sec = _sector_of(instruments, sym)
                if scnt.get(sec, 0) < max_sec:
                    period_picks.append(sym)
                    scnt[sec] = scnt.get(sec, 0) + 1
                if len(period_picks) >= top_n:
                    break
            period_picks = period_picks[:top_n]
        else:
            period_picks = raw[:top_n]

        if not period_picks:
            mask = (returns.index > d) & (returns.index <= ed)
            port_rets.extend([0.0] * int(mask.sum()))
            port_dates.extend(returns.index[mask].tolist())
            prev_picks = set()
            continue

        rebalance_count += 1
        new_set = set(period_picks)
        turnover_total += len(new_set.symmetric_difference(prev_picks)) / 2

        if p.get("vol_weight", False):
            pvols = {}
            for s in period_picks:
                col = pf[s].loc[:d].tail(int(p.get("vol_lookback", 20)))
                pvols[s] = float(col.pct_change().std()) if len(col) > 5 else 1.0
            piv = {s: 1.0 / (v + 1e-9) for s, v in pvols.items()}
            pti = sum(piv.values())
            w = {s: piv[s] / pti for s in period_picks}
        else:
            w = {s: 1.0 / len(period_picks) for s in period_picks}

        mask = (returns.index > d) & (returns.index <= ed)
        period_days = returns.loc[mask]
        n_buy = len(new_set - prev_picks) if i > 0 else len(period_picks)
        n_sell = len(prev_picks - new_set) if i > 0 else 0
        tc = (n_buy + n_sell) * cost_rate / 2
        daily = []
        for idx_date in period_days.index:
            day_ret = sum(w.get(s, 0) * (period_days.loc[idx_date, s] if s in period_days.columns else 0.0) for s in period_picks)
            daily.append(day_ret - (tc / max(len(period_days), 1)))
        port_rets.extend(daily)
        port_dates.extend(period_days.index.tolist())
        prev_picks = new_set

    if not port_rets:
        return {"error": "no active periods in backtest"}

    r_series = pd.Series(port_rets, index=pd.DatetimeIndex(port_dates), dtype=float).sort_index()
    eq = (1 + r_series).cumprod()
    years = len(r_series) / 252
    cagr = eq.iloc[-1] ** (1 / years) - 1 if years > 0 else 0.0
    dd = eq / eq.cummax() - 1
    vol = r_series.std() * np.sqrt(252) if len(r_series) > 1 else 0.0
    sharpe = (r_series.mean() * 252) / vol if vol and not np.isnan(vol) else 0.0
    yearly = r_series.groupby(r_series.index.year).apply(lambda x: (1 + x).prod() - 1)
    ret_12m = 0.0
    if len(r_series) > 252:
        last_start = r_series.index[-1] - pd.Timedelta(days=365)
        eq_12m = (1 + r_series.loc[r_series.index >= last_start]).cumprod()
        ret_12m = eq_12m.iloc[-1] - 1 if len(eq_12m) > 0 else 0.0

    # Fail closed: do not publish a signal built on stale or thin data.
    quality_error = signal_data_quality_error(prices, picks=picks, top_n=top_n)
    if quality_error:
        return {"error": quality_error, "symbols_loaded": len(prices.columns)}

    return {
        "generated_at": datetime.now().isoformat(),
        "strategy": "rsi_momentum_rotation_champion",
        "params": {k: v for k, v in PARAMS.items()},
        "latest_signal": {
            "date": str(latest_date.date()),
            "picks": picks,
            "scores": pick_scores,
            "weights": {s: round(wv, 4) for s, wv in weights.items()},
            "symbols_screened": latest_screened_count,
            "sectors": {s: _sector_of(instruments, s) for s in picks} if not instruments.empty else {},
        },
        "backtest_metrics": {
            "symbols_loaded": len(pf.columns),
            "date_range": [str(r_series.index[0].date()), str(r_series.index[-1].date())],
            "days": int(len(r_series)),
            "years": round(years, 2),
            "rebalance_count": rebalance_count,
            "avg_turnover": round(turnover_total / max(rebalance_count, 1), 1),
            "cagr_pct": round(cagr * 100, 2),
            "total_return_pct": round((eq.iloc[-1] - 1) * 100, 2),
            "max_drawdown_pct": round(dd.min() * 100, 2),
            "vol_pct": round(vol * 100, 2),
            "sharpe": round(float(sharpe), 3),
            "positive_years": int((yearly > 0).sum()),
            "total_years": int(len(yearly)),
            "return_12m_pct": round(float(ret_12m * 100), 1),
        },
    }


def main() -> int:
    hist_dir = Path(os.getenv("RSI_MOM_HIST_DIR", str(find_hist_dir(""))))
    if not hist_dir.is_dir():
        print(f"ERROR: Hist_Data dir not found at {hist_dir}")
        return 1

    print(f"Loading {hist_dir}...")
    prices = load_hist(hist_dir)
    print(f"Loaded {len(prices.columns)} symbols, {len(prices)} days")

    result = compute_rotation(prices)
    if "error" in result:
        print(f"ERROR: {result['error']}")
        return 1

    output_path = OUT_DIR / "paper_shadow_rsi_momentum_latest.json"
    output_path.write_text(json.dumps(result, indent=2), encoding="utf-8")

    picks = result["latest_signal"]["picks"]
    scores = result["latest_signal"]["scores"]
    bm = result["backtest_metrics"]

    print(f"\n=== RSI + Momentum Rotation Paper Shadow (champion config) ===")
    print(f"Signal date: {result['latest_signal']['date']} | Rebalance: {PARAMS['rebalance_freq']} | Top {PARAMS['top_n']}")
    print(f"Top {PARAMS['top_n']} picks:")
    for s in picks:
        print(f"  {s:<15s} RSI score: {scores.get(s, 'N/A')}")
    print(f"\nBacktest: {bm['cagr_pct']:.2f}% CAGR, {bm['max_drawdown_pct']:.1f}% MaxDD, "
          f"Sharpe {bm['sharpe']:.3f}, {bm['positive_years']}/{bm['total_years']} pos years")
    print(f"12-month return: {bm['return_12m_pct']:+.1f}%")
    print(f"\nSaved: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
