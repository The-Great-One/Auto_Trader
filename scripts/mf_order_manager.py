#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from kiteconnect import KiteConnect

from Auto_Trader.mf_execution import (
    MFExecutionConfig,
    build_buy_orders_from_target_amounts,
    execute_orders,
    load_allowlist,
    normalize_order,
    search_mf_instruments,
)
from Auto_Trader.my_secrets import API_KEY
from Auto_Trader.portfolio_intelligence import build_report
from Auto_Trader.utils import read_session_data

ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / "reports"


def init_kite() -> KiteConnect:
    kite = KiteConnect(api_key=API_KEY)
    kite.set_access_token(read_session_data())
    return kite


def latest_report_path(prefix: str) -> Path | None:
    matches = sorted(REPORTS.glob(f"{prefix}_*.json"))
    return matches[-1] if matches else None


def load_json(path: str | Path) -> dict:
    return json.loads(Path(path).read_text())


def cmd_search(kite, args):
    matches = search_mf_instruments(kite, args.query, limit=args.limit)
    print(json.dumps(matches, indent=2, default=str))


def cmd_holdings(kite, _args):
    print(json.dumps(kite.mf_holdings() or [], indent=2, default=str))


def cmd_orders(kite, _args):
    print(json.dumps(kite.mf_orders() or [], indent=2, default=str))


def cmd_allowlist(_kite, _args):
    config = MFExecutionConfig()
    print(json.dumps(sorted(load_allowlist(config)), indent=2))


def cmd_plan(kite, args):
    payload = load_json(args.plan)
    raw_orders = payload.get("orders", payload)
    if not isinstance(raw_orders, list):
        raise SystemExit("Plan file must be a JSON list or an object with an 'orders' list")
    orders = [normalize_order(item, default_tag=args.tag) for item in raw_orders]
    result = execute_orders(kite, orders, dry_run=not args.execute)
    print(json.dumps(result, indent=2, default=str))


def build_rebalance_orders_from_report(report: dict, mf_symbols: list[str], min_ticket: float) -> list:
    mf_delta = float((report.get("rebalance_advice_inr") or {}).get("MF", 0.0) or 0.0)
    if mf_delta <= 0:
        return []
    if not mf_symbols:
        raise SystemExit("Provide at least one --mf-symbol for MF buys from report")
    per_symbol = mf_delta / len(mf_symbols)
    target_amounts = {
        symbol.strip().upper(): per_symbol
        for symbol in mf_symbols
        if symbol.strip() and per_symbol >= min_ticket
    }
    return build_buy_orders_from_target_amounts(target_amounts, tag="mf_rebalance")


def cmd_from_report(kite, args):
    report_path = Path(args.report) if args.report else latest_report_path("portfolio_intel")
    if report_path is None or not report_path.exists():
        if args.refresh_report:
            report = build_report(kite)
            REPORTS.mkdir(exist_ok=True)
            date = datetime.now().strftime("%Y-%m-%d")
            report_path = REPORTS / f"portfolio_intel_{date}.json"
            report_path.write_text(json.dumps(report, indent=2))
        else:
            raise SystemExit("No portfolio_intel report found. Run daily_portfolio_report.py or use --refresh-report")
    report = load_json(report_path)
    orders = build_rebalance_orders_from_report(report, args.mf_symbol, args.min_ticket)
    result = execute_orders(kite, orders, dry_run=not args.execute)
    result["report_path"] = str(report_path)
    result["report_mf_delta"] = float((report.get("rebalance_advice_inr") or {}).get("MF", 0.0) or 0.0)
    print(json.dumps(result, indent=2, default=str))


def main():
    parser = argparse.ArgumentParser(description="Guarded mutual fund order manager")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("search", help="Search Kite MF instruments")
    p.add_argument("query")
    p.add_argument("--limit", type=int, default=20)
    p.set_defaults(func=cmd_search)

    p = sub.add_parser("holdings", help="Show MF holdings")
    p.set_defaults(func=cmd_holdings)

    p = sub.add_parser("orders", help="Show MF orders")
    p.set_defaults(func=cmd_orders)

    p = sub.add_parser("allowlist", help="Show resolved MF allowlist")
    p.set_defaults(func=cmd_allowlist)

    p = sub.add_parser("plan", help="Execute a JSON MF order plan")
    p.add_argument("plan")
    p.add_argument("--tag", default="mf_manual")
    p.add_argument("--execute", action="store_true", help="Actually place orders. Default is dry-run")
    p.set_defaults(func=cmd_plan)

    p = sub.add_parser("from-report", help="Turn portfolio_intel MF rebalance advice into guarded BUY orders")
    p.add_argument("--report", help="Path to portfolio_intel JSON. Defaults to latest")
    p.add_argument("--refresh-report", action="store_true", help="Build a fresh portfolio report if none exists")
    p.add_argument("--mf-symbol", action="append", default=[], help="MF tradingsymbol to receive BUY allocation. Repeatable")
    p.add_argument("--min-ticket", type=float, default=500.0)
    p.add_argument("--execute", action="store_true", help="Actually place orders. Default is dry-run")
    p.set_defaults(func=cmd_from_report)

    args = parser.parse_args()
    kite = init_kite()
    args.func(kite, args)


if __name__ == "__main__":
    main()
