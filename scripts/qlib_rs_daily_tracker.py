#!/usr/bin/env python3
"""Compatibility wrapper: Qlib RS daily tracker lives in Trader_Labs/scripts/qlib_rs_daily_tracker.py."""
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.run_trader_labs import main

if __name__ == "__main__":
    raise SystemExit(main("qlib_rs_daily_tracker.py"))
