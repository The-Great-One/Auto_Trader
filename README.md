# Auto_Trader

Auto_Trader is a Python trading automation project for Zerodha Kite. It includes live market data handling, rule-based buy/sell decisions, guarded order execution, paper-shadow reporting, portfolio reporting, and operational dashboards.

> Trading automation can cause financial loss. Review, test, and run in paper/dry-run mode before enabling live execution.

## What is in this repo

- `wednesday.py` — main process launcher.
- `Auto_Trader/` — runtime package for market data, rules, order execution, indicators, logging, Telegram delivery, portfolio/news helpers, and mutual-fund execution helpers.
- `scripts/` — operational scripts for reports, paper-shadow runs, daily supervision, dashboard helpers, deployment verification, and compatibility wrappers for lab entrypoints.
- `dashboard/` — Dash/Streamlit dashboard apps and mutual-fund dashboard utilities.
- `PROJECT_MAP.md` — detailed project navigation and runtime notes.

Research/lab implementations are maintained outside the live runtime repo. The lab entrypoint files that remain here are compatibility wrappers and delegate to a separate Trader_Labs checkout when configured.

## Requirements

- Python 3.10+
- Zerodha Kite account/API credentials for live or paper-integrated workflows
- Optional: Node/npm for dashboard scripts defined in `package.json`

Install Python dependencies:

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Configuration

Create `Auto_Trader/my_secrets.py` locally. This file is ignored by git and should never be committed.

Example structure:

```python
API_KEY = "your_api_key"
API_SECRET = "your_api_secret"
TOTP_KEY = "your_totp_key"
USER_NAME = "your_username"
PASS = "your_password"
TG_TOKEN = "your_telegram_bot_token"
CHANNEL = "your_telegram_channel_or_chat"
```

Optional environment variables are used by specific scripts. See `.env.example` and `PROJECT_MAP.md` for currently supported operational settings.

## Running locally

Start the main runtime:

```bash
python wednesday.py
```

Run a paper-shadow snapshot:

```bash
python scripts/paper_shadow.py
```

Generate a daily portfolio report:

```bash
python scripts/daily_portfolio_report.py
```

Run the active dashboard:

```bash
./scripts/start_dash_ops_dashboard.sh
```

## Strategy rules

Runtime rule files live in `Auto_Trader/`, including:

- `Auto_Trader/RULE_SET_7.py` — current buy-side rule module.
- `Auto_Trader/RULE_SET_2.py` — current sell-side rule module.
- `Auto_Trader/RULE_SET_OPTIONS_1.py` — options research/paper support rule module.

Rule modules expose a `buy_or_sell(...)` style decision function used by the runtime and validation tooling.

## Operational scripts

Common scripts currently present in this repo include:

- `scripts/daily_ops_supervisor.py` — daily operational supervisor.
- `scripts/daily_scorecard.py` — daily trading scorecard.
- `scripts/daily_portfolio_report.py` — holdings/allocation report.
- `scripts/daily_improvement_audit.py` — read-only audit of recent reports/logs.
- `scripts/paper_shadow.py` — offline paper-trader decision snapshot.
- `scripts/mf_order_manager.py` — mutual-fund search, holdings, plans, orders, and SIP helpers with dry-run/live guards.
- `scripts/send_discord_health_alert.py` — optional Discord webhook health alert.
- `scripts/verify_deploy.sh` — deployment verification helper.

## Trader_Labs compatibility

The following files are compatibility wrappers for lab/research workflows:

- `scripts/weekly_strategy_lab.py`
- `scripts/weekly_universe_cagr_check.py`
- `scripts/options_strategy_lab.py`
- `scripts/options_research_supervisor.py`
- `scripts/hourly_lab_status_check.py`

They delegate to a separate Trader_Labs checkout. Configure it with:

```bash
export AT_TRADER_LABS_ROOT=/path/to/Trader_Labs
```

If the environment variable is not set, the wrapper looks for a sibling `../Trader_Labs` directory.

## Running as a systemd service

Example service unit:

```ini
[Unit]
Description=Auto_Trader service
After=network.target

[Service]
User=your_user
WorkingDirectory=/path/to/Auto_Trader
ExecStart=/path/to/Auto_Trader/venv/bin/python /path/to/Auto_Trader/wednesday.py
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

Example timer for daily restart/session refresh:

```ini
[Unit]
Description=Restart Auto_Trader daily

[Timer]
OnCalendar=*-*-* 08:30:00
Persistent=true

[Install]
WantedBy=timers.target
```

## Mutual-fund helper examples

Search mutual funds:

```bash
python scripts/mf_order_manager.py search "fund name"
```

Show built-in rebalance profiles:

```bash
python scripts/mf_order_manager.py profiles
```

Generate a dry-run rebalance plan:

```bash
python scripts/mf_order_manager.py rebalance-plan --refresh-report --profile balanced
```

Live mutual-fund execution requires explicit live flags supported by `scripts/mf_order_manager.py`.

## Reports and generated files

Generated reports, logs, caches, secrets, and local market-data artifacts are ignored by git. Important generated paths include:

- `reports/`
- `log/`
- `logs/`
- `intermediary_files/`
- `Auto_Trader/my_secrets.py`

## License

No license file is currently included in this repository.
