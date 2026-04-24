#!/bin/bash
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR" || exit 1
LOG_FILE="$ROOT_DIR/reports/telegram_dashboard.log"
mkdir -p "$ROOT_DIR/reports"
PATTERN='dashboard/ops_dash_app.py'
CMD=("$ROOT_DIR/venv/bin/python" "$ROOT_DIR/dashboard/ops_dash_app.py")
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
