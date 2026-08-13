# Auto_Trader Project Map

## Purpose and roots

Auto_Trader is the production paper RSI momentum trader. Prices and historical data are yfinance-fed from the Mac; Kite is retired and the Kite engine is masked on the server.

- Local checkout: `~/Desktop/Projects/trading/Auto_Trader`
- Production checkout: `/home/ubuntu/Auto_Trader`
- Repository: `https://github.com/The-Great-One/Auto_Trader`
- Research repository: `/home/ubuntu/Trader_Labs` (nightly auto-iteration lab)

## Current layout

- `scripts/rsi_momentum_paper_shadow.py` — weekday champion signal (hardcoded PARAMS, fail-closed data-quality guard).
- `scripts/rsi_momentum_paper_ledger.py` — five-minute MTM and paper rebalance.
- `scripts/rsi_momentum_report.py` — shared rotation/report logic.
- `scripts/rsi_224466_rotation_lab.py` — shared indicator and simulation helpers.
- `scripts/nightly_cleanup.py` + `scripts/prune_report_clutter.py` — report retention.
- `tests/` — shadow and ledger safety coverage.

## Live server crons

Times are UTC (server local time):

- `15 10 * * 1-5` — paper shadow refresh.
- `*/5 9-15 * * 1-5` — paper ledger under `/tmp/rsi_ledger.lock`.
- `0 22 * * *` — nightly cleanup followed by report pruning.

## Hermes automation (Mac)

- `86fc49d256b5` */30 9-15 Mon-Fri — fetch yfinance quotes (Tickertape REST fallback), push `reports/live_prices.json` (sudo tee; file is root-owned), invoke the ledger under the shared lock, deliver Telegram status.
- `de03edc86b03` Sat 10:00 — rebuild yfinance history feathers on the Mac and rsync to production.
- `7af4ce25de03` / `e31d6f751566` — start/poll the nightly auto-iteration lab in `Trader_Labs`.

## Deploy flow

1. Edit and verify in the canonical local checkout.
2. Commit and push `main` to GitHub.
3. On production: `git pull --ff-only origin main`.

Do not restart services or run the shadow/ledger merely to deploy documentation changes. Runtime state stays under ignored `reports/`, `log/`, and `intermediary_files/` paths.
