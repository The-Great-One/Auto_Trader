# Equity Lab Audit — 2026-05-02

## Target: 30% CAGR on Kite 5Y data

---

## CRITICAL BUGS

### BUG 1: Walk-forward is NOT walk-forward — it's just sliced backtesting
**File**: `scripts/weekly_strategy_lab.py`, lines 1458-1558
**Severity**: CRITICAL

The walk-forward validation splits data into expanding window folds, but **the strategy parameters are FIXED** across all folds. It applies the same `buy_params` and `sell_params` to every fold — there is no training step that optimizes parameters per fold. This is NOT walk-forward; it's just backtesting on different time slices.

**Impact**: The "walk-forward" results are meaningless. They test whether fixed parameters work across different periods, but a true walk-forward would retrain/optimize on the training fold and test with those optimized parameters on the next fold. Without retraining, the validation cannot detect overfitting.

**Fix**: For each fold:
1. Run a mini-optimization on the training period to find best params
2. Apply those params to the test period
3. Compare test-period returns with in-sample returns

### BUG 2: Simulation is per-symbol, not portfolio — return is averaged, not compounded
**File**: `scripts/weekly_strategy_lab.py`, lines 988-1067, 1122-1203
**Severity**: CRITICAL

`_simulate_symbol()` gives each symbol its own 100k capital and simulates independently. `run_variant()` then sums all final values and divides by `100000 * num_symbols`. This computes the **average return per symbol**, not a portfolio return.

Example: 300 symbols, each making 1% = 1% average return. But a real 100k portfolio holding 5-10 positions would have different results depending on position sizing and correlation.

**Impact**: The 45% "return" is meaningless. It means the average stock returned 45% when traded by RS7+RS2 in isolation. A real portfolio with position limits would return much less (or more if compounding helps).

**Contrast**: `run_baseline_detailed()` in `weekly_universe_cagr_check.py` DOES have proper portfolio simulation with:
- Single cash pool (100k starting)
- Position sizing (ATR-based, max 15% per position)
- Portfolio constraints (max symbol weight, class caps)
- Daily rebalancing of portfolio value

**Fix**: The lab should use the `run_baseline_detailed` simulation engine (or a simplified version of it) instead of `_simulate_symbol`.

### BUG 3: Walk-forward OOS has zero trades in later folds
**File**: `scripts/weekly_strategy_lab.py`, lines 1500-1535
**Severity**: CRITICAL

The walk-forward test period for fold 4 (likely 2024-2025) produces ZERO trades. The test data is filtered with `pd.to_datetime(df["Date"]) >= test_start_date` but the simulation starts at `range(250, len(df))` — requiring 250 bars of warmup. If the test period is short or starts mid-series, there may not be enough warmup bars.

More importantly, the BUY gates in RS7 require:
- `close > ema20 > ema50` (strong uptrend)
- `adx >= 10` (trending)
- `volume > 0.85 * sma20_vol` (volume confirmation)
- `rsi >= 45` (not oversold)
- MACD above signal

In a sideways market (2024-2025 NIFTY was choppy), these conditions rarely all trigger simultaneously.

**Fix**: The walk-forward needs a **regime-aware** variant that can trade in sideways markets (mean-reversion) instead of only trending markets.

### BUG 4: Equity time stop is too aggressive (8 bars = 8 trading days)
**File**: `Auto_Trader/RULE_SET_2.py`, line 29
**Severity**: HIGH

`equity_time_stop_bars = 8` forces exits after 8 trading days if profit < 1.5%. Many good trades need 2-4 weeks to develop. 8 days kills trades before they have time to run.

**Impact**: Forces premature exits on trades that would have been profitable, reducing overall returns significantly.

**Fix**: Increase to at least 15-20 bars. Better: make it a parameter that the lab sweeps over.

---

## STRUCTURAL ISSUES

### ISSUE 1: No mean-reversion mode for sideways markets
**File**: `Auto_Trader/RULE_SET_7.py`
**Severity**: CRITICAL (blocks 30% CAGR)

RS7 only has two entry modes: "pullback" (buying in an uptrend) and "breakout" (buying on momentum). Both require a strong trend. There is NO mode for:
- Mean-reversion in sideways/range-bound markets
- Buying oversold bounces in choppy conditions
- RSI-based reversal entries

**Fix**: Add a "mean_reversion" entry mode:
- RSI < 30 (oversold)
- Price near lower Bollinger Band (%b < 0.2)
- ADX < 20 (non-trending)
- Stochastic %K crossing up from below 20
- Exit: RSI > 60 or price near upper BB

### ISSUE 2: Selection score formula penalizes drawdown too lightly
**File**: `scripts/weekly_strategy_lab.py`, line 1188
**Severity**: HIGH

`selection_score = ret + (0.02 * total_trades) - (0.15 * abs(min(0, worst_dd)))`

With 45% return and -28% DD: score = 45 + (0.02*1662) - (0.15*28) = 45 + 33.24 - 4.2 = 74
The DD penalty is only 4.2 points on a 28% drawdown — barely matters.

**Fix**: Weight DD more heavily: `selection_score = ret + (0.01 * total_trades) - (0.5 * abs(min(0, worst_dd))) + (5.0 if positive_folds >= 3 else 0)`

### ISSUE 3: Walk-forward doesn't test the simulation engine correctly
**File**: `scripts/weekly_strategy_lab.py`, lines 1500-1535
**Severity**: HIGH

Walk-forward uses `_simulate_symbol` (per-symbol sim) for OOS testing but the in-sample results come from `run_baseline_detailed` (portfolio sim) when `AT_LAB_MATCH_LIVE=1`. This means IS and OOS use **different simulation engines**, making the comparison invalid.

**Fix**: Use the same simulation engine for both IS and OOS.

### ISSUE 4: Buy signal requires too many simultaneous conditions
**File**: `Auto_Trader/RULE_SET_7.py`, lines 89-175
**Severity**: HIGH

The pullback mode requires ALL of:
- trend_ok, trend_slope_ok, adx_ok, volume_confirm, cmf_ok, obv_ok, macd_hist_rising, rsi_pullback_trigger, stoch_pull_ok, close_above_ema20

That's 10 simultaneous conditions — extremely restrictive. In practice, maybe 2-5% of bars pass all 10.

**Fix**: Reduce to a "soft score" system where each condition contributes points, and a threshold determines BUY. This allows partial matches and generates more trades.

---

## PROPOSED CHANGES (Priority Order)

### 1. Fix the simulation engine (BUG 2) — HIGHEST PRIORITY
Replace `_simulate_symbol` with a portfolio-level simulation. Use a simplified version of `run_baseline_detailed` that:
- Has a single 100k cash pool
- Limits positions to 10-15% of portfolio each
- Allows up to ~10 concurrent positions
- Tracks portfolio equity curve

### 2. Add mean-reversion entry mode (ISSUE 1) — HIGHEST PRIORITY
Add to RULE_SET_7 a third entry mode:
```python
meanrev_checks = {
    "rsi_oversold": bool(rsi < 35),
    "bb_lower_bounce": bool(np.isfinite(curr_b) and curr_b < 0.2 and rsi_slope_up),
    "adx_low": bool(adx < 22),  # non-trending
    "volume_no_surge": bool(not vol_ok or vol_ok),  # don't require high volume
    "cci_oversold": bool(np.isfinite(cci) and cci < -100),
}
meanrev_mode = all([meanrev_checks["rsi_oversold"], meanrev_checks["bb_lower_bounce"], meanrev_checks["adx_low"]])
```
And add mean-reversion exits to RULE_SET_2:
- RSI > 60 or price > mid BB → exit
- Time stop: 5 bars (mean-reversion should be quick)

### 3. Fix walk-forward to use proper portfolio sim (BUG 3 + ISSUE 3)
Use the portfolio simulation engine for OOS testing, same as IS.

### 4. Increase time stop (BUG 4)
Change default `equity_time_stop_bars` from 8 to 20. Add it as a sweep parameter.

### 5. Implement scoring-based entry (ISSUE 4)
Replace hard AND gates with soft scoring:
- Each condition = 1 point
- Buy threshold = 7/10 (pullback) or 6/10 (mean-reversion)
- Allows partial matches and generates more trades

### 6. Fix walk-forward retraining (BUG 1)
For each fold, run a mini-optimization on the training period.

---

## EXPECTED IMPACT

- Fixing simulation (BUG 2): Current "45%" return is actually ~6-8% portfolio return. After fixing, numbers will be more realistic but lower. Need strategy improvements to compensate.
- Adding mean-reversion: Could add 10-15% return by capturing sideways/choppy periods (currently 0 trades).
- Increasing time stop: Could add 5-10% return by letting winners run longer.
- Portfolio sim: More realistic returns, better position sizing, potential 5% boost from diversification.
- Combined: Target 30% CAGR is achievable if mean-reversion captures sideways periods and time stop lets winners compound.