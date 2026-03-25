#!/usr/bin/env python3
import argparse
import glob
import json
from datetime import datetime, timedelta
from pathlib import Path


def load_reports(base: Path):
    out = []
    for p in sorted(glob.glob(str(base / "daily_scorecard_*.json"))):
        try:
            d = json.loads(Path(p).read_text())
            out.append(d)
        except Exception:
            pass
    return out


def summarize(reports, days: int):
    cutoff = (datetime.now() - timedelta(days=days)).date().isoformat()
    rows = [r for r in reports if str(r.get("date", "")) >= cutoff]
    if not rows:
        return {"days": days, "count": 0}

    orders = sum(int(r.get("orders", 0) or 0) for r in rows)
    trades = sum(int(r.get("trades", 0) or 0) for r in rows)
    pnl = sum(float(r.get("estimated_realized_pnl", 0) or 0) for r in rows)
    good = sum(1 for r in rows if str(r.get("verdict", "")).upper() == "GOOD")

    return {
        "days": days,
        "count": len(rows),
        "orders": orders,
        "trades": trades,
        "estimated_realized_pnl": round(pnl, 2),
        "good_days": good,
        "good_rate_pct": round(good / len(rows) * 100, 1),
        "from": rows[0].get("date"),
        "to": rows[-1].get("date"),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=7)
    args = ap.parse_args()

    base = Path(__file__).resolve().parents[1] / "reports"
    reports = load_reports(base)
    s = summarize(reports, args.days)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_json = base / f"performance_digest_{args.days}d_{ts}.json"
    out_json.write_text(json.dumps(s, indent=2))

    print(json.dumps(s))


if __name__ == "__main__":
    main()
