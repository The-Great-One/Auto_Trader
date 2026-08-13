# Auto_Trader Project Map

## Purpose and roots

Auto_Trader is the production paper RSI momentum trader. Prices and historical data are yfinance-fed from the Mac; Kite is retired.

- Planned Mac checkout: `~/Desktop/Projects/trading/Auto_Trader`
- Production checkout: `/home/ubuntu/Auto_Trader`
- Repository: `https://github.com/The-Great-One/Auto_Trader`
- Research repository: `/home/ubuntu/Trader_Labs`

## Current layout

- `scripts/rsi_momentum_paper_shadow.py` — weekday champion signal.
- `scripts/rsi_momentum_paper_ledger.py` — five-minute MTM and paper rebalance.
- `scripts/rsi_momentum_report.py` — shared rotation/report logic.
- `scripts/rsi_224466_rotation_lab.py` — shared indicator and simulation helpers.
- `scripts/nightly_cleanup.py` + `scripts/prune_report_clutter.py` — report retention.
- `Auto_Trader/RULE_SET_2.py` — RS2 sell diagnostics.
- `Auto_Trader/RULE_SET_7.py` — RS7 buy diagnostics.
- `Auto_Trader/utils.py` — indicator frame used by the qlib tracker.
- `tests/` — shadow and ledger safety coverage.

## Live server crons

Times below are the configured server cron times (UTC):

- `15 10 * * 1-5` — paper shadow refresh.
- `*/5 9-15 * * 1-5` — paper ledger under `/tmp/rsi_ledger.lock`.
- `0 22 * * *` — nightly cleanup followed by report pruning.
- `15 12 * * 1-5` — `Trader_Labs/scripts/qlib_rs_daily_tracker.py`.

The qlib tracker runs from `Trader_Labs` but imports `Auto_Trader/RULE_SET_2.py`, `Auto_Trader/RULE_SET_7.py`, and `Auto_Trader/utils.py`. Those files are production dependencies and must not be pruned without migrating the tracker.

## Hermes automation

Mac-side Hermes jobs:

- Fetch yfinance quotes, push `reports/live_prices.json`, invoke the ledger under the shared lock, and deliver Telegram status.
- Rebuild yfinance history on the Mac and sync feather files to production.
- Start/poll the nightly auto-iteration lab in `Trader_Labs`.

## Deploy flow

1. Edit and verify in the canonical local checkout.
2. Commit and push `main` to GitHub.
3. On production: `git pull --ff-only origin main`.

Do not restart services or run the shadow/ledger merely to deploy documentation or cleanup changes. Runtime state stays under ignored `reports/`, `log/`, and `intermediary_files/` paths.
