#!/usr/bin/env python3
import argparse
import datetime as dt
import json
import os
import re
import sys
from collections import defaultdict
from pathlib import Path
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")


def setup_imports(project_root: Path):
    sys.path.insert(0, str(project_root))
    from Auto_Trader.my_secrets import API_KEY  # type: ignore
    from Auto_Trader.utils import read_session_data  # type: ignore
    from kiteconnect import KiteConnect  # type: ignore
    return API_KEY, read_session_data, KiteConnect


def trade_day(day: str | None) -> dt.date:
    if day:
        return dt.date.fromisoformat(day)
    return dt.datetime.now(IST).date()


def parse_ts_ist(ts_raw: str):
    if not ts_raw:
        return None
    try:
        # Kite timestamps are generally naive local exchange time.
        t = dt.datetime.fromisoformat(str(ts_raw).replace("Z", "+00:00"))
        if t.tzinfo is None:
            t = t.replace(tzinfo=IST)
        else:
            t = t.astimezone(IST)
        return t
    except Exception:
        return None


def qty_price_from_trade(tr):
    qty = int(tr.get("quantity") or 0)
    price = float(tr.get("average_price") or tr.get("fill_price") or tr.get("price") or 0.0)
    side = (tr.get("transaction_type") or "").upper()
    sym = tr.get("tradingsymbol") or "UNKNOWN"
    return sym, side, qty, price


def realized_fifo(trades):
    buys = defaultdict(list)  # sym -> list[(qty, price)]
    realized = defaultdict(float)
    unmatched_sell_qty = defaultdict(int)

    trades_sorted = sorted(
        trades,
        key=lambda x: parse_ts_ist(x.get("fill_timestamp") or x.get("order_timestamp") or "") or dt.datetime.min.replace(tzinfo=IST),
    )

    for tr in trades_sorted:
        sym, side, qty, px = qty_price_from_trade(tr)
        if qty <= 0 or px <= 0:
            continue

        if side == "BUY":
            buys[sym].append([qty, px])
        elif side == "SELL":
            rem = qty
            while rem > 0 and buys[sym]:
                bqty, bpx = buys[sym][0]
                take = min(rem, bqty)
                realized[sym] += (px - bpx) * take
                rem -= take
                bqty -= take
                if bqty == 0:
                    buys[sym].pop(0)
                else:
                    buys[sym][0][0] = bqty
            if rem > 0:
                unmatched_sell_qty[sym] += rem

    total = sum(realized.values())
    return total, dict(realized), dict(unmatched_sell_qty)


def get_day_orders_trades(kite, d: dt.date):
    orders = kite.orders() or []
    trades = kite.trades() or []

    day_orders = []
    for o in orders:
        ts = parse_ts_ist(o.get("order_timestamp") or o.get("exchange_update_timestamp") or "")
        if ts and ts.date() == d:
            day_orders.append(o)

    day_trades = []
    for t in trades:
        ts = parse_ts_ist(t.get("fill_timestamp") or t.get("order_timestamp") or "")
        if ts and ts.date() == d:
            day_trades.append(t)

    return day_orders, day_trades


def summarize_logs(log_dir: Path, d: dt.date):
    date_str = d.isoformat()
    files = [log_dir / "output.log", log_dir / "error.log"]

    patterns = {
        "market_blocked": re.compile(r"MARKET orders are blocked", re.I),
        "tick_size": re.compile(r"tick size", re.I),
        "ws_close": re.compile(r"WebSocket closed", re.I),
        "order_failed": re.compile(r"Order placement failed", re.I),
        "buy_placed": re.compile(r"BUY placed", re.I),
        "sell_placed": re.compile(r"SELL placed", re.I),
    }
    counts = {k: 0 for k in patterns}

    for fp in files:
        if not fp.exists():
            continue
        try:
            with fp.open("r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    if date_str not in line:
                        continue
                    for k, rx in patterns.items():
                        if rx.search(line):
                            counts[k] += 1
        except Exception:
            continue

    return counts


def build_report(d: dt.date, day_orders, day_trades, realized_total, realized_by_symbol, unmatched_sell_qty, log_counts):
    status_counts = defaultdict(int)
    for o in day_orders:
        status_counts[(o.get("status") or "UNKNOWN").upper()] += 1

    buys = sum(1 for t in day_trades if (t.get("transaction_type") or "").upper() == "BUY")
    sells = sum(1 for t in day_trades if (t.get("transaction_type") or "").upper() == "SELL")

    lines = []
    lines.append(f"# Auto_Trader Daily Scorecard — {d.isoformat()}")
    lines.append("")
    lines.append("## Snapshot")
    lines.append(f"- Orders today: **{len(day_orders)}**")
    lines.append(f"- Trades today: **{len(day_trades)}** (BUY: {buys}, SELL: {sells})")
    lines.append(f"- Estimated realized PnL (FIFO from today trades): **₹{realized_total:,.2f}**")
    if unmatched_sell_qty:
        lines.append(f"- Note: unmatched SELL qty (possible carry positions): `{unmatched_sell_qty}`")
    lines.append("")

    lines.append("## Order status counts")
    if status_counts:
        for k in sorted(status_counts):
            lines.append(f"- {k}: {status_counts[k]}")
    else:
        lines.append("- No orders found")
    lines.append("")

    lines.append("## Symbol-wise realized PnL")
    if realized_by_symbol:
        for sym, pnl in sorted(realized_by_symbol.items(), key=lambda x: x[1], reverse=True):
            lines.append(f"- {sym}: ₹{pnl:,.2f}")
    else:
        lines.append("- No realized PnL from matched intraday trades")
    lines.append("")

    lines.append("## Reliability signals (from logs)")
    for k, v in log_counts.items():
        lines.append(f"- {k}: {v}")
    lines.append("")

    health = "GOOD"
    if realized_total < 0 or log_counts.get("order_failed", 0) > 3:
        health = "NEEDS_ATTENTION"
    if log_counts.get("market_blocked", 0) > 0 or log_counts.get("tick_size", 0) > 0:
        health = "NEEDS_ATTENTION"

    lines.append("## Verdict")
    lines.append(f"- **{health}**")

    return "\n".join(lines), {
        "date": d.isoformat(),
        "orders": len(day_orders),
        "trades": len(day_trades),
        "buy_trades": buys,
        "sell_trades": sells,
        "estimated_realized_pnl": round(realized_total, 2),
        "realized_by_symbol": realized_by_symbol,
        "unmatched_sell_qty": unmatched_sell_qty,
        "order_status_counts": dict(status_counts),
        "log_counts": log_counts,
        "verdict": health,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", help="YYYY-MM-DD in IST")
    ap.add_argument("--project-root", default="/home/ubuntu/Auto_Trader")
    ap.add_argument("--output-dir", default="/home/ubuntu/Auto_Trader/reports")
    args = ap.parse_args()

    d = trade_day(args.date)
    project_root = Path(args.project_root)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    API_KEY, read_session_data, KiteConnect = setup_imports(project_root)

    kite = KiteConnect(api_key=API_KEY)
    kite.set_access_token(read_session_data())

    day_orders, day_trades = get_day_orders_trades(kite, d)
    realized_total, realized_by_symbol, unmatched_sell_qty = realized_fifo(day_trades)
    log_counts = summarize_logs(project_root / "log", d)

    report_md, report_json = build_report(
        d,
        day_orders,
        day_trades,
        realized_total,
        realized_by_symbol,
        unmatched_sell_qty,
        log_counts,
    )

    md_path = output_dir / f"daily_scorecard_{d.isoformat()}.md"
    js_path = output_dir / f"daily_scorecard_{d.isoformat()}.json"
    md_path.write_text(report_md, encoding="utf-8")
    js_path.write_text(json.dumps(report_json, indent=2), encoding="utf-8")

    print(str(md_path))
    print(str(js_path))


if __name__ == "__main__":
    main()
