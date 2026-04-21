#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from Auto_Trader.news_sentiment import (
    compute_sector_rotation,
    discover_symbols,
    fetch_and_analyze_many,
    fetch_and_analyze_topics,
    write_summary,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch and analyze RSS/news sentiment for trading symbols.")
    parser.add_argument("--symbols", nargs="*", help="Explicit tradingsymbols to fetch")
    parser.add_argument("--limit", type=int, default=30, help="Max discovered symbols when --symbols is omitted")
    parser.add_argument("--topics", nargs="*", default=None,
                        help="Market topics to fetch (default: all including sectors)")
    parser.add_argument("--skip-topics", action="store_true", help="Skip topic fetching entirely")
    parser.add_argument("--sector-rotation", action="store_true", help="Also compute sector rotation picks")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    symbols = [str(s).strip().upper() for s in (args.symbols or []) if str(s).strip()]
    if not symbols:
        symbols = discover_symbols(limit=max(1, int(args.limit)))
    print(f"Fetching sentiment for {len(symbols)} symbols: {symbols[:10]}{'...' if len(symbols) > 10 else ''}")

    analyses = fetch_and_analyze_many(symbols)
    summary = write_summary(analyses)

    topic_summary = {"topics": []}
    if not args.skip_topics:
        topics = args.topics if args.topics is not None else None  # None = all TOPIC_CONFIGS keys
        topic_summary = fetch_and_analyze_topics(topics)

    result = {"symbols": summary, "topics": topic_summary}

    if args.sector_rotation:
        rotation = compute_sector_rotation()
        result["sector_rotation"] = rotation
        if rotation.get("hot_picks"):
            print(f"Sector rotation picks: {[p['symbol'] for p in rotation['hot_picks']]}")

    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())