#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
VENV_PY="./venv/bin/python"
EXPORT_DIR="$HOME/.openclaw/telegram-user/exports"
TOOL_DIR="$HOME/.openclaw/workspace/tools"
REPORTS="./reports"
mkdir -p "$REPORTS"

log() { echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] $*"; }

log "Starting telegram learning pipeline"

# Step 1: Fresh export. Use a private copy of the Telethon session DB so the
# live watcher can keep the canonical session open without causing
# sqlite3.OperationalError: database is locked in the fetcher.
log "Step 1: Fetching fresh Telegram data"
FETCH_SESSION_DIR="$(mktemp -d "${TMPDIR:-/tmp}/telegram-reader-session.XXXXXX")"
FETCH_SESSION="$FETCH_SESSION_DIR/reader"
cleanup_fetch_session() { rm -rf "$FETCH_SESSION_DIR"; }
trap cleanup_fetch_session EXIT
if [ -f "$HOME/.openclaw/telegram-user/reader.session" ]; then
  cp "$HOME/.openclaw/telegram-user/reader.session" "$FETCH_SESSION.session"
else
  log "FETCH: canonical Telegram session missing; fetch may require login"
fi
if ! "$VENV_PY" "$TOOL_DIR/telegram_reader.py" --session "$FETCH_SESSION" fetch \
  --chat @FinanceWithSunil --chat @Shortterm01 --chat @DarkHorseOfStockMarket --chat @Milind4Profits \
  --days 14 \
  --output "$EXPORT_DIR/telegram_export_latest.json" 2>&1 | while IFS= read -r line; do log "FETCH: $line"; done; then
  log "FETCH: skipped/failed; continuing with latest export and live watcher updates"
fi

# Step 2: Trade audit merges fresh export + watch_channel_updates.jsonl
log "Step 2: Running trade audit"
AT_TELEGRAM_EXPORT="$EXPORT_DIR/telegram_export_latest.json" \
"$VENV_PY" scripts/generate_telegram_trade_audit.py 2>&1 | while IFS= read -r line; do log "AUDIT: $line"; done || true

# Step 3: Channel learning
log "Step 3: Running channel learning"
"$VENV_PY" scripts/generate_channel_learning.py 2>&1 | while IFS= read -r line; do log "LEARN: $line"; done || true

# Step 4: Paper ledger
log "Step 4: Running paper ledger"
"$VENV_PY" scripts/live_telegram_options_paper_ledger.py 2>&1 | while IFS= read -r line; do log "LEDGER: $line"; done || true

# Step 5: Sync key reports to server if env vars are available
if [ -n "${AT_SERVER_HOST:-}" ] && [ -n "${AT_SERVER_KEY:-}" ]; then
  log "Step 5: Syncing reports to server"
  for f in channel_learning_scores.json telegram_trade_audit_latest.json live_telegram_options_paper_latest.json live_telegram_options_paper_latest.md live_telegram_options_paper_equity_history.jsonl; do
    if [ -f "$REPORTS/$f" ]; then
      scp -i "$AT_SERVER_KEY" -o StrictHostKeyChecking=no "$REPORTS/$f" "ubuntu@$AT_SERVER_HOST:/home/ubuntu/Auto_Trader/reports/" 2>&1 | while IFS= read -r line; do log "SYNC: $line"; done || true
    fi
  done
else
  log "Step 5: Skipping server sync (AT_SERVER_HOST/AT_SERVER_KEY not set)"
fi

log "Pipeline complete"
