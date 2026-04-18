#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from Auto_Trader.news_sentiment import fetch_and_analyze_symbol, write_summary
from Auto_Trader.twitter_sentiment import discover_symbols


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch and analyze RSS/news sentiment for trading symbols.")
    parser.add_argument("--symbols", nargs="*", help="Explicit tradingsymbols to fetch")
    parser.add_argument("--limit", type=int, default=20, help="Max discovered symbols when --symbols is omitted")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    symbols = [str(s).strip().upper() for s in (args.symbols or []) if str(s).strip()]
    if not symbols:
        symbols = discover_symbols(limit=max(1, int(args.limit)))

    analyses = [fetch_and_analyze_symbol(symbol) for symbol in symbols]
    summary = write_summary(analyses)
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
