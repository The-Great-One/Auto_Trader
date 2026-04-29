#!/usr/bin/env python3
"""Diagnose why walk-forward test period produces zero/negative CAGR."""
import os, sys, json
from pathlib import Path

os.environ["AT_LAB_REGIME_FILTER_ENABLED"] = "0"
os.environ["AT_LAB_HISTORY_PERIOD"] = "5y"
sys.path.insert(0, "/home/ubuntu/Auto_Trader")

import pandas as pd
import numpy as np
from Auto_Trader.utils import Indicators
from scripts.weekly_universe_cagr_check import run_baseline_detailed
from scripts import weekly_strategy_lab as lab

ROOT = Path("/home/ubuntu/Auto_Trader")
HIST_DIR = ROOT / "intermediary_files" / "Hist_Data"
test_start = pd.Timestamp("2024-04-11")

# ── 1. NIFTYETF regime in test period ──
nifty = pd.read_feather(HIST_DIR / "NIFTYETF.feather")
nifty["Date"] = pd.to_datetime(nifty["Date"])
nifty = nifty.sort_values("Date").reset_index(drop=True)
test_nifty = nifty[nifty["Date"] >= test_start].copy()
enriched = Indicators(test_nifty)
close = enriched["Close"]
ema50 = enriched["EMA50"]
ema200 = enriched["EMA200"]
bullish = (ema50 > ema200).sum()
bearish = (ema50 <= ema200).sum()
total = len(enriched)
last_date = enriched["Date"].iloc[-1].strftime("%Y-%m-%d")

print("=" * 60)
print("1. NIFTYETF REGIME IN TEST PERIOD (2024-04-11 to {})".format(last_date))
print("=" * 60)
print("  Total days: {}".format(total))
print("  EMA50 > EMA200 (bullish): {} ({:.1f}%)".format(bullish, bullish/total*100))
print("  EMA50 <= EMA200 (bearish): {} ({:.1f}%)".format(bearish, bearish/total*100))
print("  Price: {:.2f} -> {:.2f} ({:.1f}% return)".format(close.iloc[0], close.iloc[-1], (close.iloc[-1]/close.iloc[0]-1)*100))

# ── 2. Symbol data availability ──
all_files = list(HIST_DIR.glob("*.feather"))
test_syms = 0
train_syms = 0
total_syms = 0
for fp in all_files:
    try:
        df = pd.read_feather(fp)
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        df = df.dropna(subset=["Date"]).sort_values("Date")
        total_syms += 1
        if len(df[df["Date"] >= test_start]) >= 250:
            test_syms += 1
        if len(df[df["Date"] < test_start]) >= 250:
            train_syms += 1
    except:
        pass

print("\n" + "=" * 60)
print("2. DATA AVAILABILITY")
print("=" * 60)
print("  Train symbols (>=250 bars pre-2024): {}/{}".format(train_syms, total_syms))
print("  Test symbols (>=250 bars post-2024): {}/{}".format(test_syms, total_syms))

# ── 3. Load test data and run with regime OFF ──
data_map = {}
for fp in sorted(HIST_DIR.glob("*.feather")):
    try:
        df = pd.read_feather(fp)
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        df = df.dropna(subset=["Date"]).sort_values("Date").reset_index(drop=True)
        test = df[df["Date"] >= test_start].reset_index(drop=True)
        if len(test) >= 260:
            enriched = Indicators(test)
            if enriched is not None and len(enriched) >= 260:
                data_map[fp.stem] = enriched
    except:
        pass

print("\n" + "=" * 60)
print("3. BACKTEST WITH REGIME OFF ON TEST PERIOD ONLY")
print("=" * 60)
print("  Loaded {} symbols".format(len(data_map)))

os.environ["AT_BACKTEST_VOL_SIZING_ENABLED"] = "1"
os.environ["AT_BACKTEST_RISK_PER_TRADE_PCT"] = "0.05"
os.environ["AT_BACKTEST_ATR_STOP_MULT"] = "2.0"
os.environ["AT_BACKTEST_MAX_POSITION_NOTIONAL_PCT"] = "1.0"
os.environ["AT_MAX_SINGLE_SYMBOL_WEIGHT"] = "0.65"
os.environ["AT_TARGET_EQUITY"] = "1.0"
os.environ["AT_TARGET_ETF"] = "0.0"

buy = {
    "adx_min": 5, "volume_confirm_mult": 0.3, "rsi_floor": 25,
    "cmf_base_min": -0.1, "cmf_strong_min": -0.1, "cmf_weak_min": -0.1,
    "obv_min_zscore": -2.0,
    "min_atr_pct": 0.0, "max_atr_pct": 0.15, "max_extension_atr": 5.0,
    "ich_cloud_bull": 0, "vwap_buy_above": 0, "sar_buy_enabled": 0,
    "di_cross_enabled": 0, "di_plus_min": 0, "cci_buy_min": -100,
    "willr_oversold_max": -20, "mmi_risk_off": 100,
    "regime_filter_enabled": 0,
}
sell = {"breakeven_trigger_pct": 0.0, "equity_time_stop_bars": 12}

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

    sym_with_trades = 0
    total_trades = 0
    trade_returns = []
    for sym, d in details.items():
        t = d.get("trades", 0)
        if t and t > 0:
            sym_with_trades += 1
            total_trades += t

    if eq is not None and len(eq) > 20:
        s = pd.Series(eq, dtype=float)
        final = s.iloc[-1]
        years = len(s) / 252.0
        cagr = ((final / s.iloc[0]) ** (1.0 / max(years, 0.01)) - 1.0) * 100.0
        peak = s.cummax()
        dd = ((s - peak) / peak * 100.0).min()
        print("  CAGR: {:.2f}%".format(cagr))
        print("  Total return: {:.2f}%".format(result.total_return_pct))
        print("  Max drawdown: {:.2f}%".format(dd))
        print("  Trades: {}".format(result.trades))
        print("  Win rate: {:.1f}%".format(result.win_rate_pct))
        print("  Symbols with trades: {}/{}".format(sym_with_trades, len(data_map)))
        print("  Equity curve: {:.4f} -> {:.4f} ({} bars)".format(s.iloc[0], final, len(s)))
        
        # Monthly breakdown
        s.index = pd.date_range(start="2024-04-11", periods=len(s), freq="B")
        monthly = s.resample("M").last()
        monthly_ret = monthly.pct_change().dropna()
        print("\n  Monthly returns (test period):")
        for date, ret in monthly_ret.items():
            print("    {}: {:.1f}%".format(date.strftime("%Y-%m"), ret*100))
    else:
        print("  No equity curve produced!")
        print("  Trades: {}".format(result.trades))
finally:
    lab.RULE_SET_2.CONFIG.clear()
    lab.RULE_SET_2.CONFIG.update(old_r2)
    lab.RULE_SET_7.CONFIG.clear()
    lab.RULE_SET_7.CONFIG.update(old_r7)

# ── 4. Check: what happens with just buy-and-hold NIFTYETF ──
print("\n" + "=" * 60)
print("4. BENCHMARK: NIFTYETF BUY-AND-HOLD IN TEST PERIOD")
print("=" * 60)
nifty_test = nifty[nifty["Date"] >= test_start].copy()
nifty_test = nifty_test.sort_values("Date").reset_index(drop=True)
start_price = nifty_test["Close"].iloc[0]
end_price = nifty_test["Close"].iloc[-1]
bh_return = (end_price / start_price - 1) * 100
bh_years = len(nifty_test) / 252.0
bh_cagr = ((end_price / start_price) ** (1/max(bh_years, 0.01)) - 1) * 100
print("  Start: {:.2f}, End: {:.2f}".format(start_price, end_price))
print("  Return: {:.1f}%, CAGR: {:.1f}%".format(bh_return, bh_cagr))

# ── 5. Per-symbol trade count in test period ──
print("\n" + "=" * 60)
print("5. TOP 15 SYMBOLS BY TRADE COUNT (test period, regime OFF)")
print("=" * 60)
sym_trades = [(sym, d.get("trades", 0), d.get("total_return_pct", 0)) for sym, d in details.items() if d.get("trades", 0) > 0]
sym_trades.sort(key=lambda x: x[1], reverse=True)
for sym, trades, ret in sym_trades[:15]:
    print("  {:20s} {:4d} trades  ret={:.1f}%".format(sym, trades, ret))
print("  ... {} symbols with trades out of {}".format(len(sym_trades), len(details)))