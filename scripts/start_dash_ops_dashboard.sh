#!/usr/bin/env bash
set -euo pipefail
cd REDACTED_LOCAL_REPO
export DASH_HOST="${DASH_HOST:-0.0.0.0}"
export DASH_PORT="${DASH_PORT:-8504}"
exec REDACTED_LOCAL_REPO/venv/bin/python dashboard/ops_dash_app.py
