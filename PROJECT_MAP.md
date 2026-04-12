# Auto_Trader Project Map

Living navigation doc for the Auto_Trader system. Update this when structure, runtime flow, or ops behavior changes.

## Project roots

- Local repo: `/Users/sahilgoel/Desktop/Stocks`
- Live server repo: `/home/ubuntu/Auto_Trader`
- Live server host: `ubuntu@168.138.114.147`
- Service entrypoint: `wednesday.py`

## High level runtime flow

1. `wednesday.py`
   - starts market monitor
   - launches ticker, compute, updater, and Telegram worker processes
2. `Auto_Trader/Build_Master.py`
   - builds watchlist / instrument universe
3. `Auto_Trader/kite_ticker.py`
   - receives live market data from Zerodha
4. `Auto_Trader/rt_compute.py`
   - enriches ticks, builds bars, runs decision logic
   - in paper-shadow mode, writes paper reports and sends `[PAPER]` alerts
5. `Auto_Trader/KITE_TRIGGER_ORDER.py`
   - places real buy/sell orders when not in paper mode
6. `Auto_Trader/TelegramLink.py`
   - sends Telegram messages

## Key code areas

### Root
- `wednesday.py` - main multi-process runner used by systemd service
- `README.md` - setup and ops notes
- `requirements.txt` - Python deps
- `PROJECT_MAP.md` - this file

### `Auto_Trader/`
- `__init__.py` - exports runtime entrypoints and sets up logging
- `Build_Master.py` - creates daily instrument/watchlist universe
- `kite_ticker.py` - websocket/ticker handling
- `rt_compute.py` - live decision engine, paper-shadow publish path
- `KITE_TRIGGER_ORDER.py` - order placement + duplicate protection
- `RULE_SET_7.py` - current BUY rule
- `RULE_SET_2.py` - current SELL rule
- `utils.py` - indicators, market-open helpers, shared data utilities
- `mf_execution.py` - guarded mutual-fund order, SIP, rebalance-plan, and profile-selection helper
- `updater.py` - background refresh/update worker
- `TelegramLink.py` - Telegram delivery with retry/backoff
- `my_secrets.py` - secrets and channel config, highly sensitive

### `scripts/`
- `daily_ops_supervisor.py` - daily health run, strategy test, paper-shadow check/self-heal
- `daily_scorecard.py` - daily trading summary from orders/trades/logs
- `daily_portfolio_report.py` - holdings + allocation intelligence
- `send_discord_health_alert.py` - Discord webhook health card
- `paper_shadow.py` - offline paper-trader decision snapshot
- `mf_order_manager.py` - safe CLI for MF instrument lookup, holdings, orders, SIPs, built-in rebalance profiles, rebalance-plan generation, and dry-run/live guarded execution
- `weekly_strategy_lab.py` - parameter sweep / backtest harness for BUY=RULE_SET_7 and SELL=RULE_SET_2 on equities/ETFs
- `options_strategy_lab.py` - research-only parameter sweep / backtest harness for NIFTY options using `RULE_SET_OPTIONS_1`, with no live auto-promotion
- `fetch_nifty_options_data.py` - research data fetcher for NIFTY option contracts plus underlying index context used by the options lab and paper shadow
- `weekly_strategy_supervisor.py` - strategy rotation / supervision logic
- `walkforward_validate.py` - validation helper
- `performance_digest.py` - report summarizer

### `reports/`
Generated outputs, especially:
- `strategy_lab_*.json/csv` - backtest sweep results
- `daily_ops_supervisor_YYYY-MM-DD.{json,md}` - daily ops summary
- `daily_scorecard_YYYY-MM-DD.{json,md}` - daily trading scorecard
- `paper_shadow_latest.json` - cron/self-heal paper decision
- `paper_shadow_live_latest.json` - live service paper decision snapshot
- `portfolio_intel_YYYY-MM-DD.{json,md}` - portfolio intelligence

### `log/`
- `output.log` - info logs
- `error.log` - error logs

### `intermediary_files/`
Working state and cached artifacts, including holdings and historical market data.

### `tests/`
Backtests, permutations, historical analysis, ad hoc research helpers.

## Current production rule model

- BUY: `RULE_SET_7`
- SELL: `RULE_SET_2`

## Current options support status

Live base code is not yet options-ready end to end, but research and paper-shadow support now exist for NIFTY options.

Main blockers:
- `Auto_Trader/utils.py` currently filters instrument master to `instrument_type == "EQ"`
- `Auto_Trader/Build_Master.py` builds the watchlist from fundamentals-approved equities/ETFs only
- `Auto_Trader/KITE_TRIGGER_ORDER.py` hardcodes cash-delivery style order defaults (`PRODUCT_CNC`) and only maps exchanges to NSE/BSE in `trigger(...)`
- `Auto_Trader/KITE_TRIGGER_ORDER.py` sell execution path primarily anchors on holdings snapshots, while options typically live in positions

Implication:
- `scripts/options_strategy_lab.py` and the options section in `scripts/paper_shadow.py` are research-only until live universe building, decision plumbing, and order routing are extended for NFO/options

## Mutual fund support

- Portfolio analysis includes MF holdings via `kite.mf_holdings()`
- MF execution is intentionally separate from `wednesday.py` live trading runtime
- Live MF placement and SIP management currently happen only through `scripts/mf_order_manager.py`
- Rebalance plans are generated from `portfolio_intel` reports, then optionally executed
- Built-in rebalance profiles (`aggressive`, `balanced`, `tax-aware`) can auto-pick or weight MF buy/redeem symbols
- Guardrails:
  - dry-run by default
  - live placement requires `AT_MF_ENABLE_LIVE=1`
  - optional symbol allowlist support
  - per-order, per-run, and per-SIP amount caps

## Current alerting behavior

### Live service alerts
- Sent through `Auto_Trader/TelegramLink.py`
- `[PAPER]` alerts originate from `Auto_Trader/rt_compute.py`
- Paper alerts currently fall back to main Telegram channel if `AT_TEST_TRADER_CHANNEL` is empty

### Paper alert spam guard
- `rt_compute.py` now suppresses repeated unchanged paper BUY/SELL alerts
- Re-alerts only on state change or after cooldown (`AT_PAPER_ALERT_MIN_SECONDS`, default 1800s)

## Schedules and automation

### Live service
- systemd service: `auto_trade.service`
- daily restart timer: `auto_trade.timer` at `08:30`
- shell launcher: `~/auto_trade.sh`
- env overrides: `~/.autotrader_env`

### Current cron jobs on server
- `16:10` daily: `scripts/daily_ops_supervisor.py`
- `16:20` weekdays: `scripts/daily_scorecard.py`

## Strategy lab scope

In `scripts/daily_ops_supervisor.py`:
- weekdays: 50 requested variants
- weekends: 200 requested variants
- baseline is included in results, so tested count is usually requested + 1
- can auto-promote lab winners with guardrails instead of one-run blind promotion
- auto-promotion writes a managed block into `~/.autotrader_env`, records state in `reports/strategy_autopromote_state.json`, and restarts `auto_trade.service` only when repeat, score, return-gain, and cooldown checks pass
- key env knobs:
  - `AT_LAB_AUTOPROMOTE_ENABLED`
  - `AT_LAB_AUTOPROMOTE_MIN_RETURN_GAIN`
  - `AT_LAB_AUTOPROMOTE_MIN_SCORE_GAIN`
  - `AT_LAB_AUTOPROMOTE_LOOKBACK`
  - `AT_LAB_AUTOPROMOTE_MIN_REPEAT`
  - `AT_LAB_AUTOPROMOTE_COOLDOWN_HOURS`

In `scripts/weekly_strategy_lab.py`:
- reads latest `daily_scorecard_*.json` when available
- can also read a tradebook CSV via `AT_LAB_TRADEBOOK_PATH`
- if the day had zero trades, it expands buy-side search space automatically
- if tradebook analysis shows weak 5 to 10 day holds, it biases sell-side search toward tighter time stops
- records scorecard context and tradebook context in the strategy lab JSON output
- disables file logging during lab runs to avoid noisy permission issues

In `scripts/options_strategy_lab.py`:
- prefers symbols from `intermediary_files/options/nifty_options_universe.json` when present, or accepts explicit `AT_OPTIONS_LAB_SYMBOLS`
- defaults to `NIFTY` options with optional `AT_OPTIONS_LAB_SIDE`
- enriches each option contract with underlying NIFTY context from `NIFTY50_INDEX.feather`
- iterates parameter variants around `Auto_Trader/RULE_SET_OPTIONS_1.py`
- uses shorter warmup/min-bar defaults suitable for short-lived weekly option contracts
- never auto-promotes into live trading; output is for research only

In `scripts/fetch_nifty_options_data.py`:
- fetches NFO instrument metadata for `NIFTY` option contracts
- selects near-ATM contracts across configurable nearby strikes and expiries
- downloads contract OHLCV with `oi=True`
- stores underlying `^NSEI` context in `intermediary_files/Hist_Data/NIFTY50_INDEX.feather`
- writes a manifest to `intermediary_files/options/nifty_options_universe.json`
- provides the options data required for `RULE_SET_OPTIONS_1` buy/sell computation in the lab and paper shadow
- key env knobs:
  - `AT_OPTIONS_FETCH_INTERVAL`
  - `AT_NIFTY_OPTIONS_EXPIRY_COUNT`
  - `AT_NIFTY_OPTIONS_STRIKES_EACH_SIDE`
  - `AT_NIFTY_OPTIONS_SIDE`
  - `AT_NIFTY_OPTIONS_DAILY_LOOKBACK_YEARS`
  - `AT_NIFTY_OPTIONS_INTRADAY_LOOKBACK_DAYS`

In `Auto_Trader/RULE_SET_OPTIONS_1.py`:
- base NIFTY options rule that combines underlying trend alignment, option premium momentum, volume confirmation, and OI confirmation
- supports both CE and PE long entries
- exit logic uses profit target, stop loss, time stop, and momentum deterioration

In `scripts/paper_shadow.py`:
- continues writing the existing equity/ETF snapshot to `paper_shadow_latest.json`
- now also writes NIFTY options paper candidates to `paper_shadow_options_latest.json`

Relevant env knobs:
- `AT_DAILY_LAB_MAX_VARIANTS`
- `AT_WEEKEND_LAB_MAX_VARIANTS`
- `AT_LAB_MAX_VARIANTS`
- `AT_LAB_SCORECARD_PATH`
- `AT_LAB_TRADEBOOK_PATH`
- `AT_DISABLE_FILE_LOGGING`

## Known repo drift to watch

Server currently has files that may not exist in the local repo snapshot, especially:
- `scripts/daily_ops_supervisor.py`
- `scripts/daily_scorecard.py`

Before major edits, check whether local and server copies have drifted.

## Fast navigation checklist

When debugging:
- service health -> `systemctl status auto_trade.service`
- recent runtime logs -> `journalctl -u auto_trade.service -n 200 --no-pager`
- Telegram path -> `Auto_Trader/TelegramLink.py`
- paper-shadow logic -> `Auto_Trader/rt_compute.py` and `scripts/paper_shadow.py`
- strategy sweeps -> `scripts/weekly_strategy_lab.py`
- daily automation -> `scripts/daily_ops_supervisor.py`, `scripts/daily_scorecard.py`
- reports -> `reports/`

## Update rule

If a new script, service, report, or alert path is added, update this file in the same work session.
