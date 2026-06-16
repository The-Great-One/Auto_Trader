#!/usr/bin/env python3
"""RSI + Momentum Rotation Paper Shadow.

Rotation paper trader: ranks stocks by RSI(22,44,66) average,
filters to positive 1-month momentum, holds top-N equal-weight.
Publishes paper decision to paper_shadow_rsi_momentum_latest.json.
No real orders placed.
"""

from __future__ import annotations

import json
import os
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.rsi_momentum_report import find_hist_dir, run_is_headline
from scripts.rsi_momentum_robustness_report import strategy_daily_returns
from scripts.rsi_224466_rotation_lab import (
    load_prices as lab_load_prices,
    rebalance_dates as lab_rebalance_dates,
    rsi_dataframe as lab_rsi,
)

OUT_DIR = ROOT / "reports"
HIST_DIR = ROOT / "intermediary_files" / "Hist_Data"
OUT_DIR.mkdir(exist_ok=True)

# Default params — override via env
TOP_N = int(os.getenv("RSI_MOM_TOP_N", "8"))
REBALANCE_FREQ = os.getenv("RSI_MOM_REBALANCE_FREQ", "2W-FRI")
REGIME_MODE = os.getenv("RSI_MOM_REGIME_MODE", "sma100")  # sma100, sma200, none
COST_BPS = float(os.getenv("RSI_MOM_COST_BPS", "10"))
MOMENTUM_PERIOD = int(os.getenv("RSI_MOM_MOMENTUM_PERIOD", "63"))
MIN_ROWS = int(os.getenv("RSI_MOM_MIN_ROWS", "700"))
MIN_END_DATE = os.getenv("RSI_MOM_MIN_END_DATE", (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"))

MIN_AVG_VOLUME = int(os.getenv("RSI_MOM_MIN_AVG_VOLUME", "50000"))
MIN_MARKET_CAP_CR = float(os.getenv("RSI_MOM_MIN_MARKET_CAP_CR", "500"))
MAX_PER_SECTOR = int(os.getenv("RSI_MOM_MAX_PER_SECTOR", "3"))


def _load_instruments_master():
    """Load Instruments.feather for market cap and sector lookups."""
    path = ROOT / "intermediary_files" / "Instruments.feather"
    if path.exists():
        return pd.read_feather(path)
    return pd.DataFrame()


def _filter_universe(prices):
    """Apply volume, market cap, and sector filters to the price universe."""
    hist_dir = ROOT / "intermediary_files" / "Hist_Data"
    instruments = _load_instruments_master()

    keep = []
    for col in prices.columns:
        sym = str(col).strip().upper()
        f = hist_dir / f"{sym}.feather"

        # Volume filter
        if MIN_AVG_VOLUME > 0 and f.exists():
            try:
                df = pd.read_feather(f)
                vol_col = "volume" if "volume" in df.columns else ("Volume" if "Volume" in df.columns else None)
                if vol_col:
                    avg_vol = df[vol_col].tail(20).mean()
                    if avg_vol < MIN_AVG_VOLUME:
                        continue
            except Exception:
                pass

        # Market cap filter (only for stocks in watchlist)
        if MIN_MARKET_CAP_CR > 0 and not instruments.empty:
            match = instruments[instruments["Symbol"].str.upper() == sym]
            if not match.empty and "MarketCapCr" in match.columns:
                mc = match.iloc[0]["MarketCapCr"]
                if pd.notna(mc) and float(mc) < MIN_MARKET_CAP_CR:
                    continue

        keep.append(col)

    filtered = prices[keep]
    dropped = len(prices.columns) - len(keep)
    if dropped > 0:
        print(f"Universe filtered: {len(prices.columns)} -> {len(keep)} (vol>={MIN_AVG_VOLUME}, mcap>={MIN_MARKET_CAP_CR}Cr, regime={REGIME_MODE}, freq={REBALANCE_FREQ})")
    return filtered


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


def compute_rotation(prices: pd.DataFrame, top_n: int = TOP_N) -> dict:
    """Compute latest monthly rotation picks and publish paper decision."""
    if prices.empty or len(prices.columns) < top_n:
        return {"error": "insufficient symbols", "symbols_loaded": len(prices.columns)}

    # Apply quality filters to the universe
    prices = _filter_universe(prices)
    if prices.empty or len(prices.columns) < top_n:
        return {"error": f"insufficient symbols after filtering ({len(prices.columns)} left)", "symbols_loaded": len(prices.columns)}

    instruments = _load_instruments_master()

    prices_ffill = prices

    # SMA200 filter: price must be above 200-day moving average
    # Regime filter: configurable SMA (sma100 best in backtest sweep)
    regime_window = 100 if REGIME_MODE == "sma100" else 200
    regime_ma = prices_ffill.rolling(regime_window, min_periods=regime_window).mean()
    regime_filter = (prices_ffill > regime_ma).astype(float) if REGIME_MODE != "none" else (prices_ffill > 0).astype(float)

    # MACD filter: MACD line must be above signal line
    ema_fast = prices_ffill.ewm(span=12, min_periods=12).mean()
    ema_slow = prices_ffill.ewm(span=26, min_periods=26).mean()
    macd_line = ema_fast - ema_slow
    macd_signal = macd_line.ewm(span=9, min_periods=9).mean()
    macd_filter = (macd_line > macd_signal).astype(float)
    mom_1m = prices_ffill.pct_change(MOMENTUM_PERIOD, fill_method=None)

    # RSI composite score
    rsi22 = lab_rsi(prices_ffill, 22)
    rsi44 = lab_rsi(prices_ffill, 44)
    rsi66 = lab_rsi(prices_ffill, 66)
    score = (rsi22 + rsi44 + rsi66) / 3.0

    # Rebalance dates (configurable, default bi-weekly Friday)
    dates = lab_rebalance_dates(prices_ffill.index, REBALANCE_FREQ)
    if len(dates) < 1:
        return {"error": "no rebalance dates"}
    actionable_dates = []
    for d in dates:
        pos = prices_ffill.index.get_loc(d)
        if pos + 1 < len(prices_ffill.index):
            actionable_dates.append(d)
    if not actionable_dates:
        return {"error": "no actionable rebalance dates"}

    # Latest actionable signal (must have a next trading day for execution parity)
    latest_date = actionable_dates[-1]
    latest_picks: list[str] = []
    latest_pick_scores: dict[str, float] = {}
    latest_screened_count = 0

    # Historical backtest simulation — reuse the validated helper to avoid drift.
    r, pick_log = strategy_daily_returns(
        prices_ffill,
        top_n=top_n,
        cost_bps=COST_BPS,
        momentum_period=MOMENTUM_PERIOD,
        rebalance_freq=REBALANCE_FREQ,
    )
    if r.empty or not pick_log:
        return {"error": "no active periods in backtest"}
    eq = (1 + r).cumprod()

    for row in pick_log:
        if row["signal_date"] == str(latest_date.date()):
            raw_picks_full = list(row["picks"])
            # Apply SMA200+MACD filter to raw picks
            raw_picks = []
            for p in raw_picks_full:
                passes = True
                if latest_date in regime_filter.index and p in regime_filter.columns:
                    if regime_filter.loc[latest_date, p] <= 0:
                        passes = False
                if latest_date in macd_filter.index and p in macd_filter.columns:
                    if macd_filter.loc[latest_date, p] <= 0:
                        passes = False
                if passes:
                    raw_picks.append(p)
            # Compute screened count (positive momentum + SMA200 + MACD)
            combined = score.loc[latest_date].where(mom_1m.loc[latest_date] > 0, 0)
            if latest_date in regime_filter.index:
                combined = combined.where(regime_filter.loc[latest_date] > 0, 0)
            if latest_date in macd_filter.index:
                combined = combined.where(macd_filter.loc[latest_date] > 0, 0)
            latest_screened_count = int((combined > 0).sum())

            # Apply sector concentration cap, filling gaps from ranked list
            if MAX_PER_SECTOR > 0 and not instruments.empty:
                sector_count: dict[str, int] = {}
                latest_picks = []
                for pick in raw_picks:
                    sector_row = instruments[instruments["Symbol"].str.upper() == pick.upper()]
                    sec = str(sector_row.iloc[0]["Sector"]) if not sector_row.empty and "Sector" in sector_row.columns else "Unknown"
                    if sector_count.get(sec, 0) < MAX_PER_SECTOR:
                        latest_picks.append(pick)
                        sector_count[sec] = sector_count.get(sec, 0) + 1
                    if len(latest_picks) >= top_n:
                        break
                # Fill remaining slots from ranked list when sector cap excludes picks
                if len(latest_picks) < top_n:
                    fill_combined = combined.copy()
                    if latest_date in regime_filter.index:
                        fill_combined = fill_combined.where(regime_filter.loc[latest_date] > 0, 0)
                    if latest_date in macd_filter.index:
                        fill_combined = fill_combined.where(macd_filter.loc[latest_date] > 0, 0)
                    ranked = fill_combined[fill_combined > 0].sort_values(ascending=False)
                    for sym in ranked.index:
                        sym_str = str(sym)
                        if sym_str in latest_picks:
                            continue
                        sector_row = instruments[instruments["Symbol"].str.upper() == sym_str.upper()]
                        sec = str(sector_row.iloc[0]["Sector"]) if not sector_row.empty and "Sector" in sector_row.columns else "Unknown"
                        if sector_count.get(sec, 0) < MAX_PER_SECTOR:
                            latest_picks.append(sym_str)
                            sector_count[sec] = sector_count.get(sec, 0) + 1
                        if len(latest_picks) >= top_n:
                            break
            else:
                latest_picks = raw_picks
            latest_pick_scores = {s: round(float(score.loc[latest_date, s]), 2) for s in latest_picks}
            break

    # Last 12 months performance
    if len(r) > 252:
        last_start = r.index[-1] - pd.Timedelta(days=365)
        last_12m = r.loc[r.index >= last_start]
    else:
        last_12m = r
    eq_12m = (1 + last_12m).cumprod()
    ret_12m = eq_12m.iloc[-1] - 1 if len(eq_12m) > 0 else 0.0

    headline = run_is_headline(prices_ffill, top_n=top_n, cost_bps=COST_BPS)

    return {
        "generated_at": datetime.now().isoformat(),
        "strategy": "rsi_momentum_rotation",
        "params": {
            "top_n": top_n,
            "momentum_period": MOMENTUM_PERIOD,
            "cost_bps": COST_BPS,
        },
        "latest_signal": {
            "date": str(latest_date.date()),
            "picks": latest_picks,
            "scores": latest_pick_scores,
            "symbols_screened": latest_screened_count,
        },
        "backtest_metrics": {
            "symbols_loaded": len(prices_ffill.columns),
            "date_range": [headline["start"], headline["end"]],
            "days": int(headline["days"]),
            "years": round(len(r) / 252, 2),
            "cagr_pct": headline["cagr_pct"],
            "xirr_pct": headline["xirr_pct"],
            "total_return_pct": headline["total_return_pct"],
            "max_drawdown_pct": headline["max_drawdown_pct"],
            "vol_pct": headline["vol_pct"],
            "sharpe": headline["sharpe_like"],
            "positive_years": headline["positive_years"],
            "total_years": headline["total_years"],
            "return_12m_pct": round(float(ret_12m * 100), 1),
        },
    }


def main():
    import os

    hist_dir = Path(os.getenv("RSI_MOM_HIST_DIR", str(find_hist_dir(""))))
    if not hist_dir.is_dir():
        print(f"ERROR: Hist_Data dir not found at {hist_dir}")
        return 1

    print(f"Loading {hist_dir}...")
    prices = load_hist(hist_dir)
    print(f"Loaded {len(prices.columns)} symbols, {len(prices)} days")

    result = compute_rotation(prices, top_n=TOP_N)
    if "error" in result:
        print(f"ERROR: {result['error']}")
        return 1

    # Write output
    output_path = OUT_DIR / "paper_shadow_rsi_momentum_latest.json"
    output_path.write_text(json.dumps(result, indent=2), encoding="utf-8")

    # Print summary
    picks = result["latest_signal"]["picks"]
    scores = result["latest_signal"]["scores"]
    bm = result["backtest_metrics"]

    print(f"\n=== RSI + Momentum Rotation Paper Shadow ===")
    print(f"Signal date: {result['latest_signal']['date']}")
    print(f"Top {TOP_N} picks:")
    for s in picks:
        print(f"  {s:<15s} RSI score: {scores.get(s, 'N/A')}")
    print(f"\nBacktest: {bm['cagr_pct']:.2f}% CAGR, {bm['max_drawdown_pct']:.1f}% MaxDD, "
          f"Sharpe {bm['sharpe']:.3f}, {bm['positive_years']}/{bm['total_years']} pos years")
    print(f"12-month return: {bm['return_12m_pct']:+.1f}%")
    print(f"\nSaved: {output_path}")
    return 0


if __name__ == "__main__":
    import os
    raise SystemExit(main())
