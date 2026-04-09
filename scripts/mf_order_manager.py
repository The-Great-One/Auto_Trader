#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from kiteconnect import KiteConnect

from Auto_Trader.mf_execution import (
    MFExecutionConfig,
    MFSIPModifyRequest,
    MFSIPRequest,
    available_rebalance_profiles,
    build_rebalance_plan,
    execute_orders,
    execute_sip_cancel,
    execute_sip_modify,
    execute_sips,
    load_allowlist,
    normalize_order,
    normalize_sip,
    search_mf_instruments,
)
from Auto_Trader.my_secrets import API_KEY
from Auto_Trader.portfolio_intelligence import build_report
from Auto_Trader.utils import read_session_data

ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / "reports"
REPORTS.mkdir(exist_ok=True)


def init_kite() -> KiteConnect:
    kite = KiteConnect(api_key=API_KEY)
    kite.set_access_token(read_session_data())
    return kite


def latest_report_path(prefix: str) -> Path | None:
    matches = sorted(REPORTS.glob(f"{prefix}_*.json"))
    return matches[-1] if matches else None


def load_json(path: str | Path):
    return json.loads(Path(path).read_text())


def dump_result(payload: dict, output: str | None = None):
    text = json.dumps(payload, indent=2, default=str)
    if output:
        Path(output).write_text(text)
    print(text)


def resolve_portfolio_report(kite, args) -> tuple[dict, Path | None]:
    if getattr(args, "refresh_report", False):
        report = build_report(kite)
        date = datetime.now().strftime("%Y-%m-%d")
        report_path = REPORTS / f"portfolio_intel_{date}.json"
        report_path.write_text(json.dumps(report, indent=2))
        return report, report_path

    report_path = Path(args.report) if getattr(args, "report", None) else latest_report_path("portfolio_intel")
    if report_path is None or not report_path.exists():
        raise SystemExit("No portfolio_intel report found. Run daily_portfolio_report.py or use --refresh-report")
    return load_json(report_path), report_path


def cmd_search(kite, args):
    dump_result(search_mf_instruments(kite, args.query, limit=args.limit), args.output)


def cmd_holdings(kite, args):
    dump_result(kite.mf_holdings() or [], args.output)


def cmd_orders(kite, args):
    dump_result(kite.mf_orders() or [], args.output)


def cmd_sips(kite, args):
    dump_result(kite.mf_sips() or [], args.output)


def cmd_allowlist(_kite, args):
    config = MFExecutionConfig()
    dump_result(sorted(load_allowlist(config)), args.output)


def cmd_profiles(_kite, args):
    dump_result(available_rebalance_profiles(), args.output)


def cmd_plan(kite, args):
    payload = load_json(args.plan)
    raw_orders = payload.get("orders", payload)
    if not isinstance(raw_orders, list):
        raise SystemExit("Plan file must be a JSON list or an object with an 'orders' list")
    orders = [normalize_order(item, default_tag=args.tag) for item in raw_orders]
    result = execute_orders(kite, orders, dry_run=not args.execute)
    dump_result(result, args.output)


def _parse_weights(values: list[float] | None) -> list[float] | None:
    if not values:
        return None
    return [float(v) for v in values]


def _default_plan_path(prefix: str) -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return str(REPORTS / f"{prefix}_{ts}.json")


def cmd_rebalance_plan(kite, args):
    report, report_path = resolve_portfolio_report(kite, args)
    plan = build_rebalance_plan(
        report,
        kite,
        buy_symbols=args.buy_symbol,
        buy_weights=_parse_weights(args.buy_weight),
        redeem_symbols=args.redeem_symbol,
        redeem_weights=_parse_weights(args.redeem_weight),
        min_ticket=args.min_ticket,
        tag=args.tag,
        profile_name=args.profile,
    )
    plan["report_path"] = str(report_path) if report_path else None
    output_path = args.output or _default_plan_path("mf_rebalance_plan")
    Path(output_path).write_text(json.dumps(plan, indent=2, default=str))

    result = {
        "plan_path": output_path,
        "plan": plan,
    }
    if args.execute:
        orders = [normalize_order(item, default_tag=args.tag) for item in plan.get("orders", [])]
        result["execution"] = execute_orders(kite, orders, dry_run=False)
    dump_result(result)


def cmd_from_report(kite, args):
    # Backward-compatible alias to rebalance-plan.
    cmd_rebalance_plan(kite, args)


def cmd_sip_plan(kite, args):
    payload = load_json(args.plan)
    raw_sips = payload.get("sips", payload)
    if not isinstance(raw_sips, list):
        raise SystemExit("SIP plan file must be a JSON list or an object with a 'sips' list")
    sips = [normalize_sip(item, default_tag=args.tag) for item in raw_sips]
    result = execute_sips(kite, sips, dry_run=not args.execute)
    dump_result(result, args.output)


def cmd_sip_create(kite, args):
    sip = MFSIPRequest(
        tradingsymbol=args.tradingsymbol.strip().upper(),
        amount=float(args.amount),
        instalments=int(args.instalments),
        frequency=args.frequency,
        initial_amount=args.initial_amount,
        instalment_day=args.instalment_day,
        tag=args.tag,
    )
    result = execute_sips(kite, [sip], dry_run=not args.execute)
    dump_result(result, args.output)


def cmd_sip_modify(kite, args):
    request = MFSIPModifyRequest(
        sip_id=args.sip_id,
        amount=args.amount,
        status=args.status,
        instalments=args.instalments,
        frequency=args.frequency,
        instalment_day=args.instalment_day,
    )
    result = execute_sip_modify(kite, request, dry_run=not args.execute)
    dump_result(result, args.output)


def cmd_sip_cancel(kite, args):
    result = execute_sip_cancel(kite, args.sip_id, dry_run=not args.execute)
    dump_result(result, args.output)


def main():
    parser = argparse.ArgumentParser(description="Guarded mutual fund order and SIP manager")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("search", help="Search Kite MF instruments")
    p.add_argument("query")
    p.add_argument("--limit", type=int, default=20)
    p.add_argument("--output")
    p.set_defaults(func=cmd_search)

    p = sub.add_parser("holdings", help="Show MF holdings")
    p.add_argument("--output")
    p.set_defaults(func=cmd_holdings)

    p = sub.add_parser("orders", help="Show MF orders")
    p.add_argument("--output")
    p.set_defaults(func=cmd_orders)

    p = sub.add_parser("sips", help="Show MF SIPs")
    p.add_argument("--output")
    p.set_defaults(func=cmd_sips)

    p = sub.add_parser("allowlist", help="Show resolved MF allowlist")
    p.add_argument("--output")
    p.set_defaults(func=cmd_allowlist)

    p = sub.add_parser("profiles", help="Show built-in MF rebalance profiles")
    p.add_argument("--output")
    p.set_defaults(func=cmd_profiles)

    p = sub.add_parser("plan", help="Execute a JSON MF order plan")
    p.add_argument("plan")
    p.add_argument("--tag", default="mf_manual")
    p.add_argument("--execute", action="store_true", help="Actually place orders. Default is dry-run")
    p.add_argument("--output")
    p.set_defaults(func=cmd_plan)

    p = sub.add_parser("rebalance-plan", help="Create a MF rebalance plan from latest portfolio report")
    p.add_argument("--report", help="Path to portfolio_intel JSON. Defaults to latest")
    p.add_argument("--refresh-report", action="store_true", help="Build a fresh portfolio report if none exists")
    p.add_argument("--buy-symbol", "--mf-symbol", dest="buy_symbol", action="append", default=[], help="MF tradingsymbol to receive MF BUY allocation. Repeatable")
    p.add_argument("--buy-weight", action="append", type=float, default=[], help="Optional weight for each --buy-symbol")
    p.add_argument("--redeem-symbol", action="append", default=[], help="MF tradingsymbol to redeem when MF allocation should shrink. Repeatable")
    p.add_argument("--redeem-weight", action="append", type=float, default=[], help="Optional weight for each --redeem-symbol")
    p.add_argument("--min-ticket", type=float, default=500.0)
    p.add_argument("--profile", choices=sorted(available_rebalance_profiles().keys()))
    p.add_argument("--tag", default="mf_rebalance")
    p.add_argument("--execute", action="store_true", help="Actually place orders after generating plan")
    p.add_argument("--output", help="Path to write plan JSON")
    p.set_defaults(func=cmd_rebalance_plan)

    p = sub.add_parser("from-report", help="Backward-compatible alias for rebalance-plan")
    p.add_argument("--report", help="Path to portfolio_intel JSON. Defaults to latest")
    p.add_argument("--refresh-report", action="store_true")
    p.add_argument("--buy-symbol", "--mf-symbol", dest="buy_symbol", action="append", default=[])
    p.add_argument("--buy-weight", action="append", type=float, default=[])
    p.add_argument("--redeem-symbol", action="append", default=[])
    p.add_argument("--redeem-weight", action="append", type=float, default=[])
    p.add_argument("--min-ticket", type=float, default=500.0)
    p.add_argument("--profile", choices=sorted(available_rebalance_profiles().keys()))
    p.add_argument("--tag", default="mf_rebalance")
    p.add_argument("--execute", action="store_true")
    p.add_argument("--output")
    p.set_defaults(func=cmd_from_report)

    p = sub.add_parser("sip-plan", help="Execute a JSON SIP plan")
    p.add_argument("plan")
    p.add_argument("--tag", default="mf_sip")
    p.add_argument("--execute", action="store_true", help="Actually place SIPs. Default is dry-run")
    p.add_argument("--output")
    p.set_defaults(func=cmd_sip_plan)

    p = sub.add_parser("sip-create", help="Create a single MF SIP")
    p.add_argument("tradingsymbol")
    p.add_argument("--amount", type=float, required=True)
    p.add_argument("--instalments", type=int, required=True)
    p.add_argument("--frequency", default="monthly")
    p.add_argument("--initial-amount", type=float)
    p.add_argument("--instalment-day", type=int)
    p.add_argument("--tag", default="mf_sip")
    p.add_argument("--execute", action="store_true")
    p.add_argument("--output")
    p.set_defaults(func=cmd_sip_create)

    p = sub.add_parser("sip-modify", help="Modify an existing MF SIP")
    p.add_argument("sip_id")
    p.add_argument("--amount", type=float)
    p.add_argument("--status")
    p.add_argument("--instalments", type=int)
    p.add_argument("--frequency")
    p.add_argument("--instalment-day", type=int)
    p.add_argument("--execute", action="store_true")
    p.add_argument("--output")
    p.set_defaults(func=cmd_sip_modify)

    p = sub.add_parser("sip-cancel", help="Cancel an existing MF SIP")
    p.add_argument("sip_id")
    p.add_argument("--execute", action="store_true")
    p.add_argument("--output")
    p.set_defaults(func=cmd_sip_cancel)

    args = parser.parse_args()
    kite = init_kite()
    args.func(kite, args)


if __name__ == "__main__":
    main()
