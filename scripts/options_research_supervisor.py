#!/usr/bin/env python3
"""Compatibility wrapper: lab code lives in Trader_Labs/scripts/options_research_supervisor.py."""
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.run_trader_labs import main

if __name__ == "__main__":
    raise SystemExit(main("options_research_supervisor.py"))
