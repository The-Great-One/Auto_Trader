#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
export HOME="${HOME:-/Users/sahilgoel}"
export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:${PATH:-}"
PY="./venv/bin/python"
LOG="reports/telegram_learning_portfolio_job.log"
mkdir -p reports
{
  echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] start telegram learning portfolio job"
  "$PY" scripts/generate_telegram_trade_audit.py
  "$PY" scripts/generate_channel_learning.py
  "$PY" scripts/live_telegram_options_paper_ledger.py
  echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] done telegram learning portfolio job"
} >> "$LOG" 2>&1
