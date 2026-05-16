#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.." || exit 1

ENV_FILE="${AT_LOCAL_ENV:-$HOME/.openclaw/workspace/secrets/autotrader_servers.env}"
if [ -f "$ENV_FILE" ]; then
  # shellcheck disable=SC1090
  . "$ENV_FILE"
fi

: "${AT_SERVER_HOST:?AT_SERVER_HOST not set; expected $ENV_FILE}"
: "${AT_SERVER_KEY:?AT_SERVER_KEY not set; expected $ENV_FILE}"

mkdir -p reports
SERVER_TARGET="$AT_SERVER_HOST"
case "$SERVER_TARGET" in
  *@*) ;;
  *) SERVER_TARGET="ubuntu@$SERVER_TARGET" ;;
esac

rsync -avz \
  -e "ssh -i $AT_SERVER_KEY -o StrictHostKeyChecking=no" \
  "$SERVER_TARGET:/home/ubuntu/Auto_Trader/reports/" \
  "reports/"

PY="./venv/bin/python"
if [ ! -x "$PY" ]; then
  PY="python3"
fi
"$PY" scripts/enforce_report_retention.py --days "${AT_REPORT_RETENTION_DAYS:-3}" --apply
