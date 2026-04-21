#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
export DASH_HOST="${DASH_HOST:-0.0.0.0}"
export DASH_PORT="${DASH_PORT:-8504}"
exec "$(dirname "$0")/../venv/bin/python" dashboard/ops_dash_app.py
