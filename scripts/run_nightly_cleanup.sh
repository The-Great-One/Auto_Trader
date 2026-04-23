#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")/.." || exit 1
"$(dirname "$0")/../venv/bin/python" scripts/nightly_cleanup.py >> reports/nightly_cleanup_local.log 2>&1
"$(dirname "$0")/../venv/bin/python" scripts/prune_report_clutter.py --apply >> reports/nightly_cleanup_local.log 2>&1
