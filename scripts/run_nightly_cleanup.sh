#!/bin/bash
cd "$(dirname "$0")/.." || exit 1
"$(dirname "$0")/../venv/bin/python" scripts/nightly_cleanup.py >> reports/nightly_cleanup_local.log 2>&1