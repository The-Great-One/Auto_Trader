#!/usr/bin/env python3
"""Explore fundamentally different equity strategies beyond RULE_SET_7 threshold tuning.

While Optuna hunts within the RS7 parameter space, this script tests structural alternatives:
1. Pure momentum (trend-following with NO gate cascade)
2. Mean-reversion with RSI/BB extremes
3. Dual MA crossover systems
4. Breakout + ATR trailing stop
5. Volatility-breakout (range expansion)
6. RSI divergence (momentum exhaustion)

Each strategy computes its own indicators from raw OHLCV data and runs a simple
backtest engine. Results are appended to reports/exploration_results.jsonl.

Usage:
    AT_RESEARCH_MODE=1 AT_LAB_PRECACHE=0 python3 scripts/explore_strategies.py
"""

from __future__ import annotations

import json
import os
import sys
import warnings
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

os.environ.setdefault("AT_DISABLE_FILE_LOGGING", "1")
os.environ.setdefault("AT_LAB_PRECACHE", "0")
os.environ.setdefault("AT_RESEARCH_MODE", "1")

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

HIST_DIR = REPO / "intermediary_files" / "Hist_Data"
RESULTS_PATH = REPO / "reports" / "exploration_results.jsonl"
STATUS_PATH = REPO / "intermediary_files" / "lab_status" / "exploration_status.json"


@dataclass
class StrategyResult:
    name: str
    total_return_pct: float = 0.0
    cagr_pct: float = 0.0
    max_drawdown_pct: float = 0.0
    trades: int = 0
    win_rate_pct: float = 0.0
    avg_trade_return_pct: float = 0.0
    profit_factor: float = 0.0
    sharpe_ratio: float = 0.0
    symbols_tested: int = 0
    symbols_profitable: int = 0
    avg_holding_bars: float = 0.0
    params: dict = field(default_factory=dict)
    per_symbol: dict = field(default_factory=dict)


def _load_data() -> dict[str, pd.DataFrame]:
    """Load all feather files with >260 rows of OHLCV data."""
    if not HIST_DIR.exists():
        return {}
    data = {}
    for f in sorted(HIST_DIR.glob("*.feather")):
        if f.stat().st_size < 1024:
            continue
        try:
            df = pd.read_feather(f)
            if len(df) < 260:
                continue
            df["Date"] = pd.to_datetime(df["Date"])
            df = df.sort_values("Date").reset_index(drop=True)
            for col in ["Open", "High", "Low", "Close", "Volume"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            df = df.dropna(subset=["Close", "Volume"])
            if len(df) < 260:
                continue
            data[f.stem.upper()] = df
        except Exception:
            continue
    return data


def _compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Compute common indicators from raw OHLCV."""
    df = df.copy()
    c = df["Close"]
    h = df["High"]
    l = df["Low"]
    v = df["Volume"]

    # EMAs
    for p in [9, 20, 50, 100, 150, 200]:
        df[f"EMA{p}"] = c.ewm(span=p, adjust=False).mean()

    # SMAs
    for p in [20, 50]:
        df[f"SMA{p}"] = c.rolling(p).mean()

    # RSI
    delta = c.diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = (-delta.clip(upper=0)).rolling(14).mean()
    rs = gain / loss.replace(0, 1e-10)
    df["RSI"] = 100 - (100 / (1 + rs))

    # ATR
    tr = pd.concat([h - l, (h - c.shift(1)).abs(), (l - c.shift(1)).abs()], axis=1).max(axis=1)
    df["ATR"] = tr.rolling(14).mean()
    df["ATR_pct"] = df["ATR"] / c.replace(0, 1)

    # Bollinger Bands
    bb_sma = c.rolling(20).mean()
    bb_std = c.rolling(20).std()
    df["BB_upper"] = bb_sma + 2 * bb_std
    df["BB_lower"] = bb_sma - 2 * bb_std
    df["BB_pct"] = (c - df["BB_lower"]) / (df["BB_upper"] - df["BB_lower"]).replace(0, 1)

    # MACD
    macd_line = c.ewm(span=12, adjust=False).mean() - c.ewm(span=26, adjust=False).mean()
    df["MACD"] = macd_line
    df["MACD_Signal"] = macd_line.ewm(span=9, adjust=False).mean()
    df["MACD_Hist"] = df["MACD"] - df["MACD_Signal"]

    # ADX
    plus_dm = (h - h.shift(1)).clip(lower=0)
    minus_dm = (l.shift(1) - l).clip(lower=0)
    plus_dm[plus_dm < minus_dm] = 0
    minus_dm[minus_dm < plus_dm] = 0
    atr14 = tr.rolling(14).mean()
    plus_di = 100 * plus_dm.rolling(14).mean() / atr14.replace(0, 1)
    minus_di = 100 * minus_dm.rolling(14).mean() / atr14.replace(0, 1)
    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, 1)
    df["ADX"] = dx.rolling(14).mean()
    df["PLUS_DI"] = plus_di
    df["MINUS_DI"] = minus_di

    # Volume ratio
    df["Vol_SMA20"] = v.rolling(20).mean()
    df["Vol_Ratio"] = v / df["Vol_SMA20"].replace(0, 1)

    # Supertrend
    hl2 = (h + l) / 2
    for mult in [2.5, 3.0, 3.5]:
        ub = hl2 + mult * df["ATR"]
        lb = hl2 - mult * df["ATR"]
        st = ub.copy()
        st.iloc[0] = ub.iloc[0]
        direction = pd.Series(1, index=df.index)
        for i in range(1, len(df)):
            if c.iloc[i] > ub.iloc[i - 1]:
                direction.iloc[i] = 1
            elif c.iloc[i] < lb.iloc[i - 1]:
                direction.iloc[i] = -1
            else:
                direction.iloc[i] = direction.iloc[i - 1]
            if direction.iloc[i] == 1:
                st.iloc[i] = min(ub.iloc[i], st.iloc[i - 1]) if c.iloc[i - 1] > st.iloc[i - 1] else ub.iloc[i]
            else:
                st.iloc[i] = max(lb.iloc[i], st.iloc[i - 1]) if c.iloc[i - 1] < st.iloc[i - 1] else lb.iloc[i]
        df[f"ST_{mult}"] = st
        df[f"ST_dir_{mult}"] = direction

    # Stochastic
    low14 = l.rolling(14).min()
    high14 = h.rolling(14).max()
    df["Stoch_K"] = 100 * (c - low14) / (high14 - low14).replace(0, 1)
    df["Stoch_D"] = df["Stoch_K"].rolling(3).mean()

    # OBV
    obv_dir = np.sign(c.diff())
    df["OBV"] = (obv_dir * v).cumsum()

    return df


def _backtest_symbol(df: pd.DataFrame, entry_fn, exit_fn, name: str,
                      start_idx: int = 200, cash: float = 100000.0,
                      position_pct: float = 0.95) -> dict:
    """Generic backtest engine: entry_fn(df, i) -> bool, exit_fn(df, i, entry_price, entry_idx, bars_held) -> bool"""
    capital = cash
    qty = 0
    entry_price = 0.0
    entry_idx = None
    trades = 0
    wins = 0
    total_pnl = 0.0
    equity_curve = []
    holding_bars = []
    peak_equity = cash
    max_dd = 0.0

    for i in range(start_idx, len(df)):
        price = float(df.iloc[i]["Close"])

        if qty == 0:
            if entry_fn(df, i):
                invest = capital * position_pct
                buy_qty = int(invest // price)
                if buy_qty > 0:
                    qty = buy_qty
                    capital -= qty * price
                    entry_price = price
                    entry_idx = i
                    trades += 1
        else:
            bars_held = i - entry_idx
            if exit_fn(df, i, entry_price, entry_idx, bars_held):
                capital += qty * price
                pnl = (price - entry_price) * qty
                total_pnl += pnl
                if price > entry_price:
                    wins += 1
                holding_bars.append(bars_held)
                qty = 0
                entry_price = 0.0
                entry_idx = None

        current_equity = capital + qty * price
        equity_curve.append(current_equity)
        if current_equity > peak_equity:
            peak_equity = current_equity
        dd = (peak_equity - current_equity) / peak_equity * 100
        if dd > max_dd:
            max_dd = dd

    # Close any open position at end
    if qty > 0:
        price = float(df.iloc[-1]["Close"])
        capital += qty * price
        pnl = (price - entry_price) * qty
        total_pnl += pnl
        if price > entry_price:
            wins += 1
        if entry_idx is not None:
            holding_bars.append(len(df) - 1 - entry_idx)

    final_equity = capital
    total_return = (final_equity - cash) / cash * 100

    # CAGR
    days = (df.iloc[-1]["Date"] - df.iloc[start_idx]["Date"]).days
    years = max(days / 365.25, 0.1)
    cagr = ((final_equity / cash) ** (1 / years) - 1) * 100

    # Sharpe approximation (daily returns from equity curve)
    eq = np.array(equity_curve)
    if len(eq) > 20:
        daily_ret = np.diff(eq) / eq[:-1]
        sharpe = daily_ret.mean() / (daily_ret.std() + 1e-9) * np.sqrt(252)
    else:
        sharpe = 0.0

    # Profit factor
    # Approximate: we only track total_pnl, not individual wins/losses
    avg_win = total_pnl / max(wins, 1)
    avg_loss = total_pnl / max(trades - wins, 1) if trades > wins else 1.0
    profit_factor = abs(avg_win * wins / (avg_loss * max(trades - wins, 1) + 1e-9))

    return {
        "total_return_pct": round(total_return, 2),
        "cagr_pct": round(cagr, 2),
        "max_drawdown_pct": round(-max_dd, 2),
        "trades": trades,
        "win_rate_pct": round(wins / max(trades, 1) * 100, 2),
        "avg_trade_return_pct": round(total_pnl / max(trades, 1) / cash * 100, 4),
        "profit_factor": round(profit_factor, 2),
        "sharpe_ratio": round(sharpe, 2),
        "avg_holding_bars": round(np.mean(holding_bars), 1) if holding_bars else 0,
        "final_equity": round(final_equity, 2),
    }


# ─── Strategy 1: Pure EMA Crossover ────────────────────────────────────────────

def strategy_ema_crossover(df_map: dict, fast: int = 20, slow: int = 50, atr_trail: float = 3.0) -> StrategyResult:
    name = f"ema_crossover_{fast}_{slow}_atr{atr_trail}"

    def entry(df, i):
        if i < slow + 5:
            return False
        fast_ema = df.iloc[i][f"EMA{fast}"]
        prev_fast = df.iloc[i - 1][f"EMA{fast}"]
        slow_ema = df.iloc[i][f"EMA{slow}"]
        prev_slow = df.iloc[i - 1][f"EMA{slow}"]
        return prev_fast <= prev_slow and fast_ema > slow_ema

    def exit(df, i, ep, eidx, bars):
        fast_ema = df.iloc[i][f"EMA{fast}"]
        slow_ema = df.iloc[i][f"EMA{slow}"]
        # Also trail stop with ATR
        atr = df.iloc[i].get("ATR", 0)
        trail = ep + atr_trail * atr if atr > 0 else ep * 1.05
        return fast_ema < slow_ema or (df.iloc[i]["Close"] < trail and bars > 5)

    return _run_multi(name, df_map, entry, exit, {"fast": fast, "slow": slow, "atr_trail": atr_trail})


# ─── Strategy 2: RSI Mean Reversion ────────────────────────────────────────────

def strategy_rsi_mean_reversion(df_map: dict, rsi_low: int = 25, rsi_high: int = 75,
                                  bb_exit: float = 0.8, max_hold: int = 30) -> StrategyResult:
    name = f"rsi_mr_{rsi_low}_{rsi_high}_bb{bb_exit}_h{max_hold}"

    def entry(df, i):
        rsi = df.iloc[i]["RSI"]
        bb_pct = df.iloc[i].get("BB_pct", 0.5)
        adx = df.iloc[i].get("ADX", 50)
        # RSI oversold + in lower BB + trend still intact (ADX > 15)
        return rsi < rsi_low and bb_pct < 0.2 and adx > 15

    def exit(df, i, ep, eidx, bars):
        rsi = df.iloc[i]["RSI"]
        bb_pct = df.iloc[i].get("BB_pct", 0.5)
        # Exit on RSI overbought or BB upper touch or time stop
        return rsi > rsi_high or bb_pct > bb_exit or bars > max_hold

    return _run_multi(name, df_map, entry, exit, {"rsi_low": rsi_low, "rsi_high": rsi_high,
                                                    "bb_exit": bb_exit, "max_hold": max_hold})


# ─── Strategy 3: Supertrend Only ──────────────────────────────────────────────

def strategy_supertrend(df_map: dict, atr_mult: float = 3.0, adx_filter: int = 0) -> StrategyResult:
    name = f"supertrend_{atr_mult}_adx{adx_filter}"
    st_col = f"ST_dir_{atr_mult}"
    st_price = f"ST_{atr_mult}"

    def entry(df, i):
        if st_col not in df.columns:
            return False
        direction = df.iloc[i].get(st_col, 0)
        prev_dir = df.iloc[i - 1].get(st_col, 0) if i > 0 else 0
        adx = df.iloc[i].get("ADX", 50)
        adx_ok = adx >= adx_filter if adx_filter > 0 else True
        return direction == 1 and prev_dir == -1 and adx_ok

    def exit(df, i, ep, eidx, bars):
        if st_col not in df.columns:
            return True
        direction = df.iloc[i].get(st_col, 0)
        prev_dir = df.iloc[i - 1].get(st_col, 0) if i > 0 else 0
        return direction == -1 and prev_dir == 1

    return _run_multi(name, df_map, entry, exit, {"atr_mult": atr_mult, "adx_filter": adx_filter})


# ─── Strategy 4: Breakout + ATR Trail ──────────────────────────────────────────

def strategy_breakout_atr(df_map: dict, lookback: int = 20, atr_trail: float = 2.5,
                           vol_mult: float = 1.2) -> StrategyResult:
    name = f"breakout_{lookback}_atr{atr_trail}_vol{vol_mult}"

    def entry(df, i):
        if i < lookback + 5:
            return False
        close = df.iloc[i]["Close"]
        prev_close = df.iloc[i - 1]["Close"]
        high_n = df.iloc[i - 1]["High"]  # yesterday's high
        # 20-bar high breakout
        recent_high = df.iloc[i - 1:i].iloc[-1]["High"]  # same as high_n
        hhv = df["High"].iloc[max(0, i - lookback):i].max()
        breakout = close > hhv and prev_close <= hhv
        vol_ok = df.iloc[i].get("Vol_Ratio", 1.0) >= vol_mult
        return breakout and vol_ok

    def exit(df, i, ep, eidx, bars):
        atr = df.iloc[i].get("ATR", 0)
        close = df.iloc[i]["Close"]
        # Trailing stop: highest close since entry - N*ATR
        if atr > 0 and eidx is not None:
            highest_since = df.iloc[eidx:i + 1]["Close"].max()
            trail = highest_since - atr_trail * atr
            return close < trail and bars > 3
        return False

    return _run_multi(name, df_map, entry, exit, {"lookback": lookback, "atr_trail": atr_trail, "vol_mult": vol_mult})


# ─── Strategy 5: Volatility Squeeze Breakout ───────────────────────────────────

def strategy_vol_squeeze(df_map: dict, bb_squeeze_periods: int = 5,
                          atr_trail: float = 3.0) -> StrategyResult:
    name = f"vol_squeeze_{bb_squeeze_periods}_atr{atr_trail}"

    def entry(df, i):
        if i < 25:
            return False
        # BB width at historical low → squeeze
        bb_width = (df.iloc[i]["BB_upper"] - df.iloc[i]["BB_lower"]) / df.iloc[i].get("SMA20", df.iloc[i]["Close"])
        bb_widths = ((df.iloc[max(0, i - 50):i + 1]["BB_upper"] - df.iloc[max(0, i - 50):i + 1]["BB_lower"]) /
                     df.iloc[max(0, i - 50):i + 1].get("SMA20", df.iloc[max(0, i - 50):i + 1]["Close"])).dropna()
        if len(bb_widths) < 10:
            return False
        pct_rank = (bb_widths < bb_width).sum() / len(bb_widths)
        # Price above SMA20 + BB width in bottom 20% (squeeze)
        close = df.iloc[i]["Close"]
        sma20 = df.iloc[i].get("SMA20", close)
        return pct_rank < 0.2 and close > sma20

    def exit(df, i, ep, eidx, bars):
        atr = df.iloc[i].get("ATR", 0)
        close = df.iloc[i]["Close"]
        if atr > 0 and eidx is not None:
            highest_since = df.iloc[eidx:i + 1]["Close"].max()
            trail = highest_since - atr_trail * atr
            return close < trail and bars > 5
        return bars > 30

    return _run_multi(name, df_map, entry, exit, {"bb_squeeze_periods": bb_squeeze_periods, "atr_trail": atr_trail})


# ─── Strategy 6: RSI Divergence ────────────────────────────────────────────────

def strategy_rsi_divergence(df_map: dict, lookback: int = 20, rsi_oversold: int = 35,
                            rsi_overbought_exit: int = 65, max_hold: int = 25) -> StrategyResult:
    name = f"rsi_div_{lookback}_rsi{rsi_oversold}_{rsi_overbought_exit}"

    def entry(df, i):
        if i < lookback + 5:
            return False
        rsi = df.iloc[i]["RSI"]
        close = df.iloc[i]["Close"]
        # Price making lower low but RSI making higher low = bullish divergence
        price_low = df.iloc[max(0, i - lookback):i + 1]["Low"].min()
        rsi_min = df.iloc[max(0, i - lookback):i + 1]["RSI"].min()
        prev_price_low = df.iloc[max(0, i - 2 * lookback):max(0, i - lookback + 1)]["Low"].min() if i > 2 * lookback else price_low
        prev_rsi_min = df.iloc[max(0, i - 2 * lookback):max(0, i - lookback + 1)]["RSI"].min() if i > 2 * lookback else rsi_min
        bullish_div = price_low < prev_price_low and rsi_min > prev_rsi_min and rsi < rsi_oversold
        # Also accept: RSI bouncing from oversold with MACD confirmation
        macd_hist = df.iloc[i].get("MACD_Hist", 0)
        prev_macd_hist = df.iloc[i - 1].get("MACD_Hist", 0)
        rsi_bounce = rsi < rsi_oversold and rsi > df.iloc[i - 1]["RSI"] and macd_hist > prev_macd_hist
        return bullish_div or rsi_bounce

    def exit(df, i, ep, eidx, bars):
        rsi = df.iloc[i]["RSI"]
        return rsi > rsi_overbought_exit or bars > max_hold

    return _run_multi(name, df_map, entry, exit, {"lookback": lookback, "rsi_oversold": rsi_oversold,
                                                    "rsi_overbought_exit": rsi_overbought_exit, "max_hold": max_hold})


# ─── Strategy 7: MACD Zero-Line Re-entry ──────────────────────────────────────

def strategy_macd_zero(df_map: dict, adx_min: int = 20, atr_trail: float = 3.0) -> StrategyResult:
    name = f"macd_zero_adx{adx_min}_atr{atr_trail}"

    def entry(df, i):
        if i < 35:
            return False
        macd = df.iloc[i]["MACD"]
        prev_macd = df.iloc[i - 1]["MACD"]
        macd_sig = df.iloc[i]["MACD_Signal"]
        adx = df.iloc[i].get("ADX", 0)
        # MACD crosses above signal from below zero
        return macd > macd_sig and prev_macd <= macd_sig and macd < 0 and adx >= adx_min

    def exit(df, i, ep, eidx, bars):
        atr = df.iloc[i].get("ATR", 0)
        close = df.iloc[i]["Close"]
        macd = df.iloc[i]["MACD"]
        # Exit on MACD cross down + ATR trail
        if atr > 0 and eidx is not None:
            highest_since = df.iloc[eidx:i + 1]["Close"].max()
            trail = highest_since - atr_trail * atr
            if close < trail and bars > 3:
                return True
        return macd < df.iloc[i]["MACD_Signal"] and bars > 5

    return _run_multi(name, df_map, entry, exit, {"adx_min": adx_min, "atr_trail": atr_trail})


# ─── Strategy 8: Multi-timeframe EMA (trend + pullback) ───────────────────────

def strategy_mtf_ema_pullback(df_map: dict, trend_ema: int = 150, pullback_ema: int = 20,
                               rsi_pull: int = 45, atr_trail: float = 3.0) -> StrategyResult:
    name = f"mtf_ema_{trend_ema}_{pullback_ema}_rsi{rsi_pull}"

    def entry(df, i):
        if i < trend_ema + 5:
            return False
        close = df.iloc[i]["Close"]
        trend_ema_val = df.iloc[i][f"EMA{trend_ema}"]
        pb_ema = df.iloc[i][f"EMA{pullback_ema}"]
        rsi = df.iloc[i]["RSI"]
        # Uptrend: close above 150 EMA, pullback to 20 EMA, RSI resetting
        return (close > trend_ema_val and
                abs(close - pb_ema) / close < 0.02 and  # within 2% of pullback EMA
                rsi < rsi_pull and rsi > 30 and
                close > pb_ema)

    def exit(df, i, ep, eidx, bars):
        atr = df.iloc[i].get("ATR", 0)
        close = df.iloc[i]["Close"]
        # Trail + trend break
        if atr > 0 and eidx is not None:
            highest_since = df.iloc[eidx:i + 1]["Close"].max()
            trail = highest_since - atr_trail * atr
            if close < trail and bars > 3:
                return True
        # Exit if close drops below trend EMA
        if f"EMA{trend_ema}" in df.columns:
            return close < df.iloc[i][f"EMA{trend_ema}"] and bars > 5
        return bars > 40

    return _run_multi(name, df_map, entry, exit, {"trend_ema": trend_ema, "pullback_ema": pullback_ema,
                                                    "rsi_pull": rsi_pull, "atr_trail": atr_trail})


# ─── Runner ───────────────────────────────────────────────────────────────────

def _run_multi(name: str, df_map: dict, entry_fn, exit_fn, params: dict) -> StrategyResult:
    per_symbol = {}
    for sym, df in df_map.items():
        try:
            df_ind = _compute_indicators(df)
            res = _backtest_symbol(df_ind, entry_fn, exit_fn, name)
            per_symbol[sym] = res
        except Exception:
            continue

    if not per_symbol:
        return StrategyResult(name=name, params=params, symbols_tested=0)

    # Aggregate
    all_returns = [v["total_return_pct"] for v in per_symbol.values()]
    all_trades = [v["trades"] for v in per_symbol.values()]
    all_dd = [v["max_drawdown_pct"] for v in per_symbol.values()]
    all_cagr = [v["cagr_pct"] for v in per_symbol.values()]
    all_wr = [v["win_rate_pct"] for v in per_symbol.values() if v["trades"] > 0]
    all_sharpe = [v["sharpe_ratio"] for v in per_symbol.values()]
    all_holding = [v["avg_holding_bars"] for v in per_symbol.values() if v["trades"] > 0]
    total_trades = sum(all_trades)
    profitable = sum(1 for r in all_returns if r > 0)

    # Portfolio-level CAGR: average of per-symbol CAGR (equal weight)
    avg_cagr = np.mean(all_cagr) if all_cagr else 0

    # Weighted average return by trade count
    if total_trades > 0:
        wt_return = sum(r * t for r, t in zip(all_returns, all_trades)) / total_trades
    else:
        wt_return = 0

    result = StrategyResult(
        name=name,
        total_return_pct=round(wt_return, 2),
        cagr_pct=round(avg_cagr, 2),
        max_drawdown_pct=round(np.mean(all_dd), 2),
        trades=total_trades,
        win_rate_pct=round(np.mean(all_wr), 2) if all_wr else 0,
        avg_trade_return_pct=round(np.mean([v["avg_trade_return_pct"] for v in per_symbol.values()]), 4),
        profit_factor=round(np.mean([v["profit_factor"] for v in per_symbol.values()]), 2),
        sharpe_ratio=round(np.mean(all_sharpe), 2),
        symbols_tested=len(per_symbol),
        symbols_profitable=profitable,
        avg_holding_bars=round(np.mean(all_holding), 1) if all_holding else 0,
        params=params,
        per_symbol={k: v for k, v in per_symbol.items() if v["trades"] > 0},
    )
    return result


def _run_all_variations(df_map: dict) -> list[StrategyResult]:
    """Run multiple param variations of each strategy type."""
    results = []

    print(f"\n{'='*60}")
    print(f"EXPLORATION: {len(df_map)} symbols loaded")
    print(f"{'='*60}\n")

    # 1. EMA Crossover grid
    for fast in [9, 20]:
        for slow in [50, 100, 150]:
            for atr in [2.0, 3.0, 4.0]:
                r = strategy_ema_crossover(df_map, fast, slow, atr)
                results.append(r)
                _print_result(r)

    # 2. RSI Mean Reversion grid
    for rsi_low in [20, 25, 30]:
        for rsi_high in [65, 70, 75]:
            for max_hold in [20, 30, 40]:
                r = strategy_rsi_mean_reversion(df_map, rsi_low, rsi_high, 0.8, max_hold)
                results.append(r)

    # 3. Supertrend grid
    for mult in [2.5, 3.0, 3.5]:
        for adx in [0, 15, 20, 25]:
            r = strategy_supertrend(df_map, mult, adx)
            results.append(r)

    # 4. Breakout + ATR Trail
    for lb in [10, 20, 30]:
        for atr in [2.0, 2.5, 3.0]:
            for vol in [1.0, 1.2, 1.5]:
                r = strategy_breakout_atr(df_map, lb, atr, vol)
                results.append(r)

    # 5. Vol Squeeze
    for squeeze in [3, 5, 7]:
        for atr in [2.5, 3.0, 3.5]:
            r = strategy_vol_squeeze(df_map, squeeze, atr)
            results.append(r)

    # 6. RSI Divergence
    for lb in [15, 20, 30]:
        for rsi_os in [30, 35]:
            for rsi_exit in [60, 65, 70]:
                r = strategy_rsi_divergence(df_map, lb, rsi_os, rsi_exit, 25)
                results.append(r)

    # 7. MACD Zero-Line
    for adx in [15, 20, 25]:
        for atr in [2.5, 3.0, 4.0]:
            r = strategy_macd_zero(df_map, adx, atr)
            results.append(r)

    # 8. Multi-timeframe EMA Pullback
    for trend in [100, 150, 200]:
        for pb in [20, 50]:
            for rsi in [40, 45, 50]:
                r = strategy_mtf_ema_pullback(df_map, trend, pb, rsi, 3.0)
                results.append(r)

    return results


def _print_result(r: StrategyResult):
    print(f"  {r.name}: CAGR={r.cagr_pct:+.1f}% | Ret={r.total_return_pct:+.1f}% | "
          f"DD={r.max_drawdown_pct:.1f}% | Trades={r.trades} | WR={r.win_rate_pct:.1f}% | "
          f"Sharpe={r.sharpe_ratio:.2f} | Profitable={r.symbols_profitable}/{r.symbols_tested} | "
          f"AvgHold={r.avg_holding_bars:.0f}b")


def main():
    print("Loading data...")
    df_map = _load_data()
    if not df_map:
        print("ERROR: No data loaded from feather files")
        return

    _write_status({"status": "running", "phase": "computing_indicators", "symbols_loaded": len(df_map),
                    "started_at": datetime.now().isoformat()})

    results = _run_all_variations(df_map)

    # Sort by CAGR
    results.sort(key=lambda r: r.cagr_pct, reverse=True)

    # Save results
    RESULTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with RESULTS_PATH.open("w") as f:
        for r in results:
            f.write(json.dumps({
                "ts": datetime.now().isoformat(),
                "name": r.name,
                "cagr_pct": r.cagr_pct,
                "total_return_pct": r.total_return_pct,
                "max_drawdown_pct": r.max_drawdown_pct,
                "trades": r.trades,
                "win_rate_pct": r.win_rate_pct,
                "sharpe_ratio": r.sharpe_ratio,
                "profit_factor": r.profit_factor,
                "symbols_tested": r.symbols_tested,
                "symbols_profitable": r.symbols_profitable,
                "avg_holding_bars": r.avg_holding_bars,
                "params": r.params,
            }, default=str) + "\n")

    # Print top 20
    print(f"\n{'='*60}")
    print(f"TOP 20 STRATEGIES BY CAGR ({len(results)} total)")
    print(f"{'='*60}")
    for r in results[:20]:
        _print_result(r)

    # Top by Sharpe
    by_sharpe = sorted(results, key=lambda r: r.sharpe_ratio, reverse=True)
    print(f"\nTOP 10 BY SHARPE:")
    for r in by_sharpe[:10]:
        _print_result(r)

    # Top by profit factor
    by_pf = sorted(results, key=lambda r: r.profit_factor, reverse=True)
    print(f"\nTOP 10 BY PROFIT FACTOR:")
    for r in by_pf[:10]:
        _print_result(r)

    # Also output strategies with CAGR > 10%
    good = [r for r in results if r.cagr_pct > 10 and r.trades > 50]
    print(f"\nSTRATEGIES WITH CAGR > 10% AND >50 TRADES: {len(good)}")
    for r in good:
        _print_result(r)

    _write_status({
        "status": "complete",
        "phase": "done",
        "total_strategies": len(results),
        "strategies_above_10pct_cagr": len(good),
        "best_cagr": results[0].cagr_pct if results else 0,
        "best_name": results[0].name if results else "",
        "completed_at": datetime.now().isoformat(),
    })

    print(f"\nResults saved to {RESULTS_PATH}")


def _write_status(data: dict):
    STATUS_PATH.parent.mkdir(parents=True, exist_ok=True)
    current = {}
    if STATUS_PATH.exists():
        try:
            current = json.loads(STATUS_PATH.read_text())
        except Exception:
            pass
    current.update(data)
    current["updated_at"] = datetime.now().isoformat()
    STATUS_PATH.write_text(json.dumps(current, indent=2, default=str))


if __name__ == "__main__":
    main()