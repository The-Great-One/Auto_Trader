#!/bin/bash
set -uo pipefail

cd "$(dirname "$0")/.." || exit 1
LOG_FILE="$(dirname "$0")/../reports/telegram_dashboard.log"
mkdir -p "$(dirname "$0")/../reports"
PATTERN='dashboard/ops_dash_app.py'
CMD=(REDACTED_LOCAL_REPO/venv/bin/python REDACTED_LOCAL_REPO/dashboard/ops_dash_app.py)
export DASH_HOST="0.0.0.0"
export DASH_PORT="8504"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] launcher started" >> "$LOG_FILE"
while true; do
  if ! pgrep -f "$PATTERN" >/dev/null 2>&1; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] starting dashboard worker" >> "$LOG_FILE"
    nohup "${CMD[@]}" >> "$LOG_FILE" 2>&1 &
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] dashboard worker pid $!" >> "$LOG_FILE"
    sleep 3
  fi
  sleep 10
done
