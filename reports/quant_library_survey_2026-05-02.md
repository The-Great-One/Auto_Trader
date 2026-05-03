# Quant library survey — awesome-quant + pytrade.org

Checked sources:
- https://github.com/wilsonfreitas/awesome-quant
- https://github.com/PFund-Software-Ltd/pytrade.org

Context/rules:
- Keep Auto_Trader training/execution on Kite data only.
- Do not replace the live-parity simulator with a toy/vectorized result for final claims.
- Use secondary for installs/heavy experiments; avoid mutating active research venv while the structural lab is running.

## Already wired into current secondary lab

| Library | Status | Auto_Trader use |
|---|---:|---|
| skfolio | installed | Portfolio optimization variants via HRP / portfolio weighting. |
| PyPortfolioOpt | installed | Fallback efficient-frontier / max-Sharpe optimizer. |
| cvxpy | installed | Solver dependency for optimization. |

Current lab includes portfolio optimizer variants (`priority_po_meanrev_007`, `priority_po_meanrev_008`) and will tell us whether these help versus live-parity mean-reversion.

## Best next candidates

### 1. Riskfolio-Lib — high priority
- Purpose: portfolio optimization / risk allocation.
- Why useful: richer risk measures than the current HRP/max-Sharpe path; good fit for capping drawdown and concentration.
- Test idea: add variants for HRP/HERC/CVaR/min-drawdown weighting, with max 15% symbol cap and walk-forward scoring.
- Caution: install in isolated/secondary environment first; do not disrupt active lab venv mid-run.

### 2. quantstats + empyrical-reloaded + ffn — high priority for reporting/scoring
- Purpose: Sharpe/Sortino/CAGR/max-DD/calmar/rolling stats.
- Why useful: selection_score currently over-rewards trade count and under-penalizes OOS/drawdown; these libraries can standardize risk metrics.
- Test idea: compute richer metrics from portfolio equity curve and use them in report ranking.

### 3. alphalens-reloaded — medium/high priority
- Purpose: predictive factor analysis.
- Why useful: can test whether RULE_SET_7 factors actually predict forward returns before running expensive portfolio sims.
- Test idea: build factor tearsheets for RSI/ADX/CCI/BB%B/volume/momentum signals using Kite-cache universe.

### 4. vectorbt / pybroker — medium priority
- Purpose: fast signal sweeps and walk-forward research.
- Why useful: could speed exploratory parameter screens.
- Limitation: final results must still be verified through Auto_Trader live-parity simulator; vectorized numbers cannot be quoted as final live-parity CAGR.

### 5. arch / pyod / stumpy / tsfresh — experimental
- Purpose: regime, volatility, anomaly, and time-series feature extraction.
- Why useful: current RS7/RS8 issue is structural/regime failure, not just parameter tuning.
- Test idea: build regime/anomaly filters and only then validate through live-parity walk-forward.

## Deprioritized / not immediate
- Lean, Nautilus, Backtrader, Backtesting.py, QSTrader, Zipline: full backtesting frameworks; useful generally, but replacing our engine risks breaking Kite/live-parity assumptions.
- Qlib: powerful but heavy; likely a separate research project, not a quick improvement.
- Crypto-first bots/frameworks: not relevant to NSE/Kite equity execution.
- yfinance/data libs: not acceptable for final training/execution under current standard.

## Current secondary availability check

Installed in active secondary venv:
- skfolio: yes
- PyPortfolioOpt: yes
- cvxpy: yes

Not installed yet:
- Riskfolio-Lib, quantstats, empyrical-reloaded, ffn, alphalens-reloaded, vectorbt, pybroker, backtesting.py, arch, tsfresh, stumpy, pyod, polars, duckdb.

## Recommended next action
1. Let the active structural lab reach the existing portfolio-optimizer variants first.
2. In parallel or immediately after, create an isolated secondary test venv for Riskfolio/metrics libs to avoid another NumPy/TA-Lib ABI break.
3. If imports/smoke tests pass, patch `weekly_strategy_lab.py` with explicit optimizer method variants: `skfolio_hrp`, `pypfopt_max_sharpe`, `riskfolio_hrp`, `riskfolio_cvar`, `inverse_volatility`.
4. Add quantstats/empyrical metrics to reports and tighten `selection_score` around CAGR, max DD, Calmar/Sortino, and walk-forward OOS pass rate.

## Sandbox result — 2026-05-02 22:14 IST

Ran low-priority isolated install on secondary using `pip --target` outside the repo because system `python3-venv` is not installed.

Import/API smoke passed:
- Riskfolio-Lib 7.2.1
- quantstats 0.0.81
- empyrical-reloaded 0.5.12
- ffn 1.1.5
- alphalens-reloaded 0.4.6
- arch 8.0.0
- pyod 3.2.1
- stumpy 1.14.1-ish import works
- vectorbt 1.0.0 was also installed as a dependency

Small cached-Kite buy-and-hold allocation smoke test, 50 symbols, 499 days, train 349 / test 150. This is **not** a strategy/live-parity result; it only tests allocation behavior.

| Method | Test CAGR | Max DD | Sharpe | Total return |
|---|---:|---:|---:|---:|
| Equal weight | -0.33% | -13.37% | 0.06 | -0.20% |
| Inverse vol | -0.45% | -13.16% | 0.05 | -0.27% |
| Riskfolio CVaR Sharpe | -10.33% | -15.10% | -0.55 | -6.29% |
| Riskfolio min CVaR | -5.03% | -15.07% | -0.24 | -3.02% |
| Riskfolio HRP MV | -1.68% | -12.71% | -0.04 | -1.01% |

Takeaway: Riskfolio imports and solves correctly, but this quick standalone allocation test does **not** show an obvious return improvement. HRP marginally reduced drawdown versus equal weight, but gave worse return. Do not assume Riskfolio helps until tested inside the actual strategy equity curves/walk-forward engine.

Evidence saved locally: `reports/quant_library_sandbox_results_2026-05-02.txt`.
