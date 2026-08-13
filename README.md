# Auto_Trader

Auto_Trader is the live **paper-only RSI momentum rotation pipeline** for NSE equities. It consumes yfinance-fed historical and intraday prices, generates champion-strategy signals, maintains a simulated ledger, and reports status through Hermes.

No real orders are placed. Zerodha Kite is retired and is not a supported price, login, or execution path.

## Live data and execution flow

1. A Mac-side Hermes cron fetches NSE quotes with yfinance and writes `reports/live_prices.json` on the server.
2. `scripts/rsi_momentum_paper_shadow.py` refreshes the paper signal at 10:15 on weekdays.
3. `scripts/rsi_momentum_paper_ledger.py` marks positions to market every five minutes from 09:00–15:59, rebalancing when it sees a newer signal.
4. A Hermes cron reads the latest ledger output and delivers Telegram status.
5. Historical feathers are rebuilt on the Mac with yfinance and synced to the server; Yahoo is not queried from Oracle Cloud.

Nightly strategy research lives in the separate `Trader_Labs` repository. Its auto-iteration lab runs independently of this production repository. A daily 12:15 UTC qlib tracker in `Trader_Labs` observes the retained RS2/RS7 rule diagnostics.

## Repository contents

- `scripts/rsi_momentum_paper_shadow.py` — champion signal generator.
- `scripts/rsi_momentum_paper_ledger.py` — simulated portfolio, MTM, and rebalance ledger.
- `scripts/rsi_momentum_report.py` and `scripts/rsi_224466_rotation_lab.py` — retained signal/backtest support.
- `scripts/nightly_cleanup.py` and `scripts/prune_report_clutter.py` — generated-report cleanup.
- `Auto_Trader/RULE_SET_2.py`, `RULE_SET_7.py`, and `utils.py` — retained for the live `Trader_Labs` qlib tracker import chain.
- `tests/` — safety tests for the shadow and ledger.

Generated reports, logs, market data, databases, and secrets are ignored by Git.

## Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

The production checkout is `/home/ubuntu/Auto_Trader`. Run tests without invoking the live paper jobs:

```bash
pytest -q
```

Do not manually run the ledger or shadow against production state without an explicit operational reason and the shared ledger lock.

## Deployment

Edit and verify locally, commit to `main`, push to GitHub, then update production with a fast-forward-only pull:

```bash
cd /home/ubuntu/Auto_Trader
git pull --ff-only origin main
```

See `PROJECT_MAP.md` for schedules and operational ownership.
