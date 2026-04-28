# Library Research for Auto_Trader — 2026-04-28

Goal: identify libraries that can materially help Auto_Trader reach robust live-parity performance on Kite 5Y data, not just add generic backtesting tooling.

## Current venv already has
- `skfolio` — already integrated for HRP / walk-forward validation.
- `PyPortfolioOpt` — already available as fallback optimizer.
- `riskfolio-lib` — available for CVaR / risk parity / Black-Litterman style portfolio construction.
- `ta`, `TA-Lib` — already available for indicators.
- `lightgbm`, `xgboost` — already installed but not clearly wired into the equity lab.

## Highest-value additions

### 1. Optuna
Status: installable on Python 3.10 venv (`optuna==4.8.0`).
Why useful:
- Replace brute-force grids with Bayesian/TPE optimization.
- Can optimize directly against walk-forward score, trade density, CAGR, drawdown, and promotion gates.
- Better suited for the current parameter explosion than fixed combinations.
Best use in Auto_Trader:
- New `scripts/optuna_strategy_hunt.py` using Kite 5Y data.
- Objective should penalize zero-trade / low-exposure variants.
- Store every trial in SQLite so no hunt results are discarded.
Expected impact: high. This directly addresses slow, sparse sweeps.

### 2. ruptures
Status: installable (`ruptures==1.1.10`).
Why useful:
- Change-point detection for market regime shifts.
- RS7/RS8 failed because 2021-2024 bull dynamics did not hold into 2024-2026; this can detect regime breaks more objectively than EMA-only gates.
Best use in Auto_Trader:
- Add regime labels from NIFTYETF / NIFTYBEES / India VIX proxies: trend, volatility, drawdown, breadth if available.
- Use labels to select relaxed pullback / mean-reversion / trend-following modes.
Expected impact: high if used to fix signal structure, not just as an extra filter.

### 3. hmmlearn
Status: installable (`hmmlearn==0.3.3`).
Why useful:
- Hidden Markov Models for market regime classification.
- Better than hard-coded thresholds when regimes are latent/noisy.
Best use in Auto_Trader:
- Train on NIFTYETF returns, realized vol, trend slope, ATR%, drawdown.
- Convert inferred states into BUY gate presets: bull/trend, sideways/mean-reversion, bear/risk-off.
Expected impact: medium-high. Needs careful walk-forward training to avoid leakage.

### 4. QuantStats
Status: installable (`quantstats==0.0.81`).
Why useful:
- Better performance diagnostics: CAGR, Sharpe, Sortino, Calmar, drawdowns, monthly returns, rolling metrics.
Best use in Auto_Trader:
- Enhance lab reports and paper-shadow reports; not alpha by itself.
- Generate HTML/JSON diagnostics for best variants.
Expected impact: medium. Improves decision quality and avoids promoting bad equity curves.

### 5. DuckDB + Polars
Status: installable (`duckdb==1.5.2`, `polars==1.40.1`).
Why useful:
- Faster scans/aggregations over 300+ feather histories and report archives.
- Useful for lab diagnostics, feature store, and post-run analysis.
Best use in Auto_Trader:
- Build a local feature/return store from Kite history.
- Speed up universe diagnostics and parameter-result mining.
Expected impact: medium for iteration speed, low for alpha directly.

## Worth using before installing anything else

### riskfolio-lib
Already installed.
Use it for:
- CVaR / CDaR portfolio optimization.
- Risk parity / nested clustered optimization.
- Black-Litterman views from sector rotation.
Best next integration:
- Add `po_cvar_*` variants next to existing `po_hrp_*`.
- Score on OOS drawdown-adjusted CAGR, not raw return.

### lightgbm / xgboost
Already installed.
Use them for:
- Signal ranking overlay, not blind prediction.
- Predict probability that a candidate BUY reaches +X% before stop over next N bars.
- Features: current RULE_SET_7 indicators, ATR%, trend slope, volume z-score, sector, regime label.
Best next integration:
- Generate supervised labels from historical BUY candidates and near-misses.
- Train walk-forward only; no random split.

## Lower priority / skip for now

### vectorbt
Useful and fast, but not installed in the venv. It is installed globally only.
Concern: Auto_Trader already has custom live-parity backtests. vectorbt could help for rapid signal research, but integrating it risks having a second backtest semantics layer.
Recommendation: optional research-only sandbox, not promotion path.

### backtesting.py / bt / zipline
Useful frameworks, but less valuable because Auto_Trader needs Kite/live-parity semantics, portfolio caps, and existing rule functions. Avoid replacing current backtest engine.

### pybroker
Looks relevant for ML + walk-forward, but not available from the currently configured pip index for this venv (`No matching distribution found`). Skip unless installed from another source later.

### mlfinlab
Historically excellent for Lopez de Prado methods, but packaging/licensing/availability can be awkward. skfolio already covers CombinatorialPurgedCV.

## Recommended next actions after current lab finishes
1. Do not install into the active venv while the lab is running.
2. After report lands, install: `optuna ruptures hmmlearn quantstats duckdb polars`.
3. Build `optuna_strategy_hunt.py` with durable SQLite storage and objective = CAGR + trade density - drawdown - OOS penalty.
4. Add regime labels using `ruptures` and/or `hmmlearn`; test RS8-style adaptive gates with walk-forward.
5. Add `riskfolio` CVaR portfolio variants only for top signal configs.
6. Use QuantStats for report diagnostics on the top 10 variants.
