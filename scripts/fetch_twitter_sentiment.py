#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "Auto_Trader" / "twitter_sentiment.py"
SPEC = importlib.util.spec_from_file_location("twitter_sentiment", MODULE_PATH)
ts = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(ts)

discover_symbols = ts.discover_symbols
fetch_and_analyze_symbol = ts.fetch_and_analyze_symbol
write_summary = ts.write_summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch and analyze Twitter sentiment for trading symbols.")
    parser.add_argument("--symbols", nargs="*", help="Explicit tradingsymbols to fetch")
    parser.add_argument("--limit", type=int, default=20, help="Max discovered symbols when --symbols is omitted")
    parser.add_argument("--hours-back", type=int, default=6, help="How far back to search recent tweets")
    parser.add_argument("--max-results", type=int, default=25, help="Recent tweets to request per symbol")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    symbols = [str(s).strip().upper() for s in (args.symbols or []) if str(s).strip()]
    if not symbols:
        symbols = discover_symbols(limit=max(1, int(args.limit)))

    analyses = [
        fetch_and_analyze_symbol(
            symbol,
            max_results=max(10, int(args.max_results)),
            hours_back=max(1, int(args.hours_back)),
        )
        for symbol in symbols
    ]
    summary = write_summary(analyses)
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
