#!/usr/bin/env python3
import os
import json
import glob
import sys


def latest(pattern: str):
    files = sorted(glob.glob(pattern))
    return files[-1] if files else None


def load_json(path: str):
    if not path:
        return {}
    with open(path, "r") as f:
        return json.load(f)


def post_webhook(url: str, content: str):
    import requests

    r = requests.post(url, json={"content": content}, timeout=15)
    r.raise_for_status()


def main():
    base = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    webhook = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
    if not webhook:
        raise SystemExit("DISCORD_WEBHOOK_URL not set")

    score = load_json(latest(os.path.join(base, "reports", "daily_scorecard_*.json")))
    intel = load_json(latest(os.path.join(base, "reports", "portfolio_intel_*.json")))

    verdict = score.get("verdict", "N/A")
    pnl = intel.get("total_pnl", "N/A")
    pnl_pct = intel.get("total_pnl_pct", "N/A")
    risk = intel.get("risk_score", "N/A")

    msg = (
        "📊 Daily AutoTrader Health Alert\n"
        f"• Scorecard: {verdict}\n"
        f"• Total P/L: ₹{pnl} ({pnl_pct}%)\n"
        f"• News risk score: {risk}\n"
        "• Portfolio classes: Equity / ETF / MF tracked"
    )
    post_webhook(webhook, msg)
    print("sent")


if __name__ == "__main__":
    main()
