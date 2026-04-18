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
- `dashboard/ops_dashboard.py` - Streamlit ops dashboard for labs, live/paper snapshots, sentiment cache, and Oracle server visibility

### `Auto_Trader/`
- `__init__.py` - exports runtime entrypoints and sets up logging
- `Build_Master.py` - creates daily instrument/watchlist universe
- `kite_ticker.py` - websocket/ticker handling
- `rt_compute.py` - live decision engine, paper-shadow publish path
- `KITE_TRIGGER_ORDER.py` - order placement + duplicate protection
- `RULE_SET_7.py` - current BUY rule
- `RULE_SET_2.py` - current SELL rule
- `utils.py` - indicators, market-open helpers, shared data utilities
- `twitter_sentiment.py` - X/Twitter fetch, tweet-type classification, cached sentiment snapshots, and trading-decision overlay
- `rnn_lab.py` - lab-only PyTorch GRU/RNN overlay that scores next-bar direction from indicator sequences for research backtests
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
- `fetch_twitter_sentiment.py` - refresh cached X/Twitter sentiment snapshots and tweet-type analysis for tracked symbols
- `options_research_supervisor.py` - weekday options fetch + paper-shadow + options-lab supervisor for NIFTY research automation
- `daily_improvement_audit.py` - read-only daily audit of reports/logs that identifies concrete improvement areas without auto-editing trading code
- `mf_order_manager.py` - safe CLI for MF instrument lookup, holdings, orders, SIPs, built-in rebalance profiles, rebalance-plan generation, and dry-run/live guarded execution
- `weekly_strategy_lab.py` - parameter sweep / backtest harness for BUY=RULE_SET_7 and SELL=RULE_SET_2 on equities/ETFs, now defaulting non-RNN variants to the same live-parity execution engine used by weekly validation, defaulting symbol selection to the full approved fundamentals universe unless explicitly overridden, pre-caching local history into `intermediary_files/Hist_Data`, and evaluating non-RNN variants in parallel worker processes
- `run_full_rnn_equity_lab.py` - wrapper to run the RNN-enabled equity lab across the full approved equity universe
- `options_strategy_lab.py` - research-only parameter sweep / backtest harness for NIFTY options using `RULE_SET_OPTIONS_1`, with no live auto-promotion
- `fetch_nifty_options_data.py` - research data fetcher for NIFTY option contracts plus underlying index context used by the options lab and paper shadow
- `weekly_strategy_supervisor.py` - strategy rotation / supervision logic
- `walkforward_validate.py` - validation helper
- `performance_digest.py` - report summarizer
- `weekly_universe_cagr_check.py` - weekly 5 year validation pack for the live RULE_SET_7/RULE_SET_2 strategy, now using live watchlist parity (`Instruments.feather` when available), local cached history first (`intermediary_files/Hist_Data`), and next-open execution after close-of-bar signals to reduce look-ahead
- `strategy_bucket_diagnostic.py` - bucketed validation helper that splits current strategy performance across Nifty 50, large cap, mid cap, and small cap slices using the same live-parity validation engine as the weekly CAGR pack

### `reports/`
Generated outputs, especially:
- `strategy_lab_*.json/csv` - backtest sweep results
- `daily_ops_supervisor_YYYY-MM-DD.{json,md}` - daily ops summary
- `weekly_universe_cagr_<ISO_WEEK>.{json,md}` - weekly 5 year validation snapshot for the current live strategy across the current approved universe, including CAGR, risk metrics, walk-forward, and Monte Carlo
- `daily_scorecard_YYYY-MM-DD.{json,md}` - daily trading scorecard
- `paper_shadow_latest.json` - cron/self-heal paper decision
- `paper_shadow_options_latest.json` - latest NIFTY options paper-shadow ranking
- `paper_shadow_live_latest.json` - live service paper decision snapshot
- `options_research_supervisor_YYYY-MM-DD.{json,md}` - daily options fetch + paper-shadow + options-lab supervisor summary
- `daily_improvement_audit_YYYY-MM-DD.{json,md}` - read-only daily improvement audit across scorecards, supervisors, paper outputs, and lab runs
- `portfolio_intel_YYYY-MM-DD.{json,md}` - portfolio intelligence

### `log/`
- `output.log` - info logs
- `error.log` - error logs

### `intermediary_files/`
Working state and cached artifacts, including holdings and historical market data.

## Universe classification notes

- `StrongFundamentalsStockList.goodStocks()` now annotates each approved symbol with `MarketCapCr`, `CapBucket`, and `IsNifty50`.
- Default equity universe is now filtered to `LARGE_CAP` + `MID_CAP` (plus approved ETFs) unless `AT_UNIVERSE_CAP_BUCKETS` overrides it.

### `tests/`
Backtests, permutations, historical analysis, ad hoc research helpers.

## Current production rule model

- BUY: `RULE_SET_7`
- SELL: `RULE_SET_2`
- Optional overlay: cached X/Twitter sentiment can veto risky BUYs or force SELL on held names when `AT_TWITTER_SENTIMENT_ENABLED=1`

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
- `15:50` weekdays: `scripts/options_research_supervisor.py`
- `16:10` daily: `scripts/daily_ops_supervisor.py`
  - also runs `weekly_universe_cagr_check.py` once per ISO week on the configured weekday (default Saturday) when markets are closed
- `16:20` weekdays: `scripts/daily_scorecard.py`
- `16:40` weekdays: `scripts/daily_improvement_audit.py`
- Twitter sentiment fetch is available via `scripts/fetch_twitter_sentiment.py`, but no cron is wired yet in this map

## Strategy lab scope

- `scripts/weekly_strategy_lab.py` can optionally evaluate a lab-only RNN overlay when `AT_LAB_RNN_ENABLED=1`
- non-RNN lab variants now default to live-parity execution (`AT_LAB_MATCH_LIVE=1`) so tuning is scored on the same mechanics as deployment
- current RNN behavior is research-only: it filters BUY entries and can accelerate SELL exits inside the lab simulator, but does not affect live trading


In `scripts/daily_ops_supervisor.py`:
- weekdays: 50 requested variants
- weekends: 200 requested variants
- runs `weekly_universe_cagr_check.py` once per ISO week by default on Saturday (`AT_WEEKLY_CAGR_WEEKDAY=5`) and stores summary fields in the ops report
- weekly validation output now includes validation-curve metrics, walk-forward windows, and Monte Carlo loss/drawdown distributions
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
- supports env overrides for history depth via `AT_LAB_HISTORY_PERIOD` and `AT_LAB_MIN_BARS`
- defaults to the full approved fundamentals universe for lab symbol selection; set `AT_LAB_SYMBOLS` to force a smaller explicit basket or `AT_LAB_USE_APPROVED_UNIVERSE=0` to fall back to the older curated list
- pre-caches missing history locally by default before loading indicators; tune with `AT_LAB_PRECACHE`, `AT_LAB_PRECACHE_WORKERS`
- non-RNN variants now route through the live-parity baseline simulator by default; set `AT_LAB_MATCH_LIVE=0` to fall back to the older per-symbol simulator
- non-RNN variants are evaluated in parallel by default on multi-core machines; tune with `AT_LAB_PARALLEL_VARIANTS`, `AT_LAB_MAX_WORKERS`, and optionally `AT_LAB_MP_START`
- can also read a tradebook CSV via `AT_LAB_TRADEBOOK_PATH`
- if the day had zero trades, it expands buy-side search space automatically
- if tradebook analysis shows weak 5 to 10 day holds, it biases sell-side search toward tighter time stops
- records scorecard context and tradebook context in the strategy lab JSON output
- disables file logging during lab runs to avoid noisy permission issues
- weekly validation and bucket diagnostics prefer `intermediary_files/Hist_Data/*.feather` for parity with live and only fall back to Yahoo when `AT_BACKTEST_ALLOW_YF_FALLBACK=1`

In `scripts/options_strategy_lab.py`:
- prefers symbols from `intermediary_files/options/nifty_options_universe.json` when present, or accepts explicit `AT_OPTIONS_LAB_SYMBOLS`
- defaults to `NIFTY` options with optional `AT_OPTIONS_LAB_SIDE`
- enriches each option contract with underlying NIFTY context from `NIFTY50_INDEX.feather`
- builds a context-aware search space around `Auto_Trader/RULE_SET_OPTIONS_1.py`, similar in style to the equity lab
- uses scorecard/tradebook context to loosen entry sensitivity on no-trade days and tighten exits when mid-hold behavior looks weak
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

In `scripts/options_research_supervisor.py`:
- runs `fetch_nifty_options_data.py`, `paper_shadow.py`, and `options_strategy_lab.py` on market-open NSE days
- writes `options_research_supervisor_YYYY-MM-DD.{json,md}` for cron visibility
- captures latest options lab recommendation in the supervisor output
- flags stale paper/lab payloads when the payload timestamp is not from the trade date
- skips fetch/paper/lab cleanly on NSE holidays

In `scripts/daily_improvement_audit.py`:
- reads latest scorecard, ops supervisor, options supervisor, paper outputs, and lab reports
- flags stale outputs, recurring failures, and no-trade days
- writes `daily_improvement_audit_YYYY-MM-DD.{json,md}` with improvement areas
- never auto-edits code or changes configs

Relevant env knobs:
- `AT_DAILY_LAB_MAX_VARIANTS`
- `AT_WEEKEND_LAB_MAX_VARIANTS`
- `AT_LAB_MAX_VARIANTS`
- `AT_LAB_HISTORY_PERIOD`
- `AT_LAB_MIN_BARS`
- `AT_WEEKLY_CAGR_ENABLED`
- `AT_WEEKLY_CAGR_WEEKDAY`
- `AT_WEEKLY_CAGR_HISTORY_PERIOD`
- `AT_WEEKLY_CAGR_MIN_BARS`
- `AT_WEEKLY_CAGR_WF_TRAIN_MONTHS`
- `AT_WEEKLY_CAGR_WF_TEST_MONTHS`
- `AT_WEEKLY_CAGR_WF_STEP_MONTHS`
- `AT_WEEKLY_CAGR_MC_SIMS`
- `AT_WEEKLY_CAGR_MC_BLOCK_MONTHS`
- `AT_WEEKLY_CAGR_MC_SEED`
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
