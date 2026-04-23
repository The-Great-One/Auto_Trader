#!/usr/bin/env python3
"""Portfolio tracker: equity + mutual fund holdings with buy/sell/hold recommendations.

Fetches live holdings from Kite, enriches with performance metrics,
and generates per-entry recommendations based on:
  - Recent performance (1m, 3m, 6m, 1y returns vs category)
  - Holding P&L and time held
  - Portfolio concentration / diversification
  - Category / sector outlook from news sentiment
  - Fund-specific signals (ELSS lock-in, small-cap risk, etc.)

Outputs:
  - reports/portfolio_tracker_latest.json  (machine-readable)
  - reports/portfolio_tracker_latest.md    (human-readable summary)
"""
from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

REPORTS_DIR = ROOT / "reports"
OUTPUT_JSON = REPORTS_DIR / "portfolio_tracker_latest.json"
OUTPUT_MD = REPORTS_DIR / "portfolio_tracker_latest.md"


# ── Category & risk metadata ──────────────────────────────────────────

FUND_META: dict[str, dict[str, Any]] = {
    # ICICI Prudential Infrastructure Fund
    "INF109K018M4": {"category": "Infrastructure", "cap": "midcap", "risk": "high", "sip_friendly": True},
    # Bandhan Small Cap Fund
    "INF194KB1AL4": {"category": "Small Cap", "cap": "smallcap", "risk": "very_high", "sip_friendly": True},
    # ICICI Prudential NASDAQ 100 Index Fund
    "INF109KC1U50": {"category": "Index/NASDAQ", "cap": "largecap_intl", "risk": "moderate", "sip_friendly": True},
    # Motilal Oswal Midcap Fund
    "INF247L01445": {"category": "Mid Cap", "cap": "midcap", "risk": "high", "sip_friendly": True},
    # Nippon India Growth Mid Cap Fund
    "INF204K01E54": {"category": "Mid Cap", "cap": "midcap", "risk": "high", "sip_friendly": True},
    # Nippon India Taiwan Equity Fund
    "INF204KC1303": {"category": "International/Taiwan", "cap": "midcap_intl", "risk": "high", "sip_friendly": False},
    # JioBlackRock Flexi Cap Fund
    "INF22M001093": {"category": "Flexi Cap", "cap": "flexicap", "risk": "moderate", "sip_friendly": True},
    # Nippon India Small Cap Fund
    "INF204K01K15": {"category": "Small Cap", "cap": "smallcap", "risk": "very_high", "sip_friendly": True},
    # HDFC Focused Fund
    "INF179K01VK7": {"category": "Focused", "cap": "largecap", "risk": "moderate", "sip_friendly": True},
    # ICICI Prudential Equity & Debt Fund
    "INF109K01Y07": {"category": "Hybrid/Equity+Debt", "cap": "largecap", "risk": "low_moderate", "sip_friendly": True},
    # Parag Parikh ELSS Tax Saver Fund
    "INF879O01100": {"category": "ELSS", "cap": "flexicap", "risk": "moderate", "sip_friendly": True, "lock_in_years": 3},
    # Quant ELSS Tax Saver Fund
    "INF966L01986": {"category": "ELSS", "cap": "flexicap", "risk": "high", "sip_friendly": True, "lock_in_years": 3},
    # SBI Banking & Financial Services Fund
    "INF200KA1507": {"category": "Sectoral/Banking", "cap": "largecap", "risk": "high", "sip_friendly": True},
    # HDFC Flexi Cap Fund
    "INF179K01UT0": {"category": "Flexi Cap", "cap": "flexicap", "risk": "moderate", "sip_friendly": True},
    # Quant Small Cap Fund
    "INF966L01689": {"category": "Small Cap", "cap": "smallcap", "risk": "very_high", "sip_friendly": True},
}

# Category-level outlook from news/market context
# Updated dynamically below from news_sentiment if available
CATEGORY_OUTLOOK: dict[str, str] = {
    "Infrastructure": "positive",   # govt capex push
    "Small Cap": "cautious",        # frothy valuations, FII selling
    "Mid Cap": "neutral",           # mixed signals
    "Index/NASDAQ": "positive",     # US tech resilience
    "International/Taiwan": "cautious",  # geopolitical risk
    "Flexi Cap": "positive",        # flexibility in volatile markets
    "Focused": "neutral",          # depends on stock picks
    "Hybrid/Equity+Debt": "positive",  # stable allocation
    "ELSS": "positive",            # tax benefit + long-term compounding
    "Sectoral/Banking": "positive", # rate cuts, credit growth
}


def compute_recommendation(entry: dict[str, Any], portfolio_context: dict[str, Any]) -> dict[str, str]:
    """Generate buy/sell/hold + rationale for a single holding."""
    symbol = entry.get("tradingsymbol", "?")
    fund_name = entry.get("fund") or entry.get("tradingsymbol", "")
    avg_price = float(entry.get("average_price", 0))
    last_price = float(entry.get("last_price", 0))
    qty = float(entry.get("quantity", 0))
    pnl = float(entry.get("pnl", 0))
    xirr = float(entry.get("xirr", 0))
    current_value = last_price * qty
    cost_value = avg_price * qty
    gain_pct = ((last_price - avg_price) / avg_price * 100) if avg_price > 0 else 0.0
    is_mf = "fund" in entry
    meta = FUND_META.get(symbol, {}) if is_mf else {}
    category = meta.get("category", "Equity/ETF") if is_mf else "Equity/ETF"
    risk = meta.get("risk", "moderate")
    cap = meta.get("cap", "largecap")
    outlook = CATEGORY_OUTLOOK.get(category, "neutral")

    # Portfolio-level context
    total_value = portfolio_context.get("total_value", 1)
    weight_pct = (current_value / total_value * 100) if total_value > 0 else 0
    mf_weight = portfolio_context.get("mf_weight_pct", 0)
    eq_weight = portfolio_context.get("eq_weight_pct", 0)

    reasons: list[str] = []
    action = "hold"  # default

    if is_mf:
        # ── MF-specific logic ──
        is_elss = "lock_in_years" in meta

        # 1. Current P&L direction
        if gain_pct > 15 and risk in ("very_high", "high"):
            reasons.append(f"up {gain_pct:.1f}% — high-risk fund gains worth booking partially")
        elif gain_pct > 25 and risk == "moderate":
            reasons.append(f"up {gain_pct:.1f}% — solid gains, consider trimming to rebalance")
        elif gain_pct < -10:
            reasons.append(f"down {gain_pct:.1f}% — underperforming, review fund specifics")
        elif gain_pct < -5 and risk == "very_high":
            reasons.append(f"down {gain_pct:.1f}% — small cap drawdown, assess if thesis intact")
        else:
            reasons.append(f"{'up' if gain_pct >= 0 else 'down'} {abs(gain_pct):.1f}% from avg")

        # 2. Category outlook
        if outlook == "positive":
            reasons.append(f"{category} outlook is positive")
        elif outlook == "cautious":
            reasons.append(f"{category} outlook is cautious")
            if risk in ("very_high", "high"):
                reasons.append("high risk + cautious outlook = reduce exposure")
        elif outlook == "negative":
            reasons.append(f"{category} outlook is negative")

        # 3. Portfolio concentration
        if weight_pct > 15:
            reasons.append(f"heavy weight at {weight_pct:.1f}% — consider trimming")
        elif weight_pct < 2 and current_value > 0:
            reasons.append(f"tiny position at {weight_pct:.1f}% — consider topping up")

        # 4. Risk-specific
        if risk == "very_high" and weight_pct > 8:
            reasons.append("very high risk + large allocation → reduce")
        if cap == "smallcap" and mf_weight > 30:
            reasons.append("small cap over-representation in portfolio")

        # 5. ELSS lock-in
        if is_elss:
            reasons.append("ELSS with 3yr lock-in — hold for tax benefit")

        # 6. SIP-friendly + new fund
        if meta.get("sip_friendly") and gain_pct < 3 and qty > 0:
            reasons.append("good SIP candidate — keep adding systematically")

        # 7. Taiwan/international specific
        if category in ("International/Taiwan",):
            reasons.append("geopolitical concentration risk — small allocation ok")

        # Decision
        sell_signals = sum(1 for r in reasons if "reduce" in r or "trim" in r or "booking" in r)
        buy_signals = sum(1 for r in reasons if "topping up" in r or "keep adding" in r or "SIP candidate" in r)
        # Outlook bonus
        if outlook == "positive" and risk != "very_high":
            buy_signals += 1
        elif outlook == "cautious" and risk in ("very_high", "high"):
            sell_signals += 1
        elif outlook == "negative":
            sell_signals += 1

        if sell_signals >= 2 and buy_signals == 0:
            action = "sell"
        elif sell_signals >= 2 and buy_signals >= 1:
            action = "hold"  # mixed signals, hold and watch
        elif sell_signals >= 1 and buy_signals == 0 and not is_elss:
            action = "hold"  # one sell signal not enough without strong buy counter
        elif buy_signals >= 2:
            action = "buy"
        elif buy_signals >= 1 and outlook not in ("negative",):
            action = "buy"
        else:
            action = "hold"

    else:
        # ── Equity/ETF logic ──
        if gain_pct > 20:
            reasons.append(f"up {gain_pct:.1f}% — consider partial booking")
        elif gain_pct > 5:
            reasons.append(f"up {gain_pct:.1f}% — healthy gain, hold")
        elif gain_pct < -10:
            reasons.append(f"down {gain_pct:.1f}% — significant loss, review thesis")
        else:
            reasons.append(f"{'up' if gain_pct >= 0 else 'down'} {abs(gain_pct):.1f}%")

        # ETF-specific
        if "ETF" in category:
            reasons.append("ETF — low-cost market proxy, hold for long term")

        # Concentration
        if weight_pct > 40:
            reasons.append(f"extreme concentration at {weight_pct:.1f}% — diversify urgently")
        elif weight_pct > 25:
            reasons.append(f"heavy weight at {weight_pct:.1f}% — consider diversifying")

        sell_signals = sum(1 for r in reasons if "booking" in r or "diversif" in r)
        buy_signals = sum(1 for r in reasons if "hold for" in r or "healthy gain" in r)

        if sell_signals >= 2:
            action = "sell"
        elif sell_signals >= 1 and gain_pct > 20:
            action = "sell"
        elif buy_signals >= 1:
            action = "hold"
        else:
            action = "hold"

    rationale = "; ".join(reasons)
    return {"action": action, "rationale": rationale}


def build_report(holdings_data: dict[str, Any]) -> dict[str, Any]:
    """Build the full portfolio tracker report."""
    equity_holdings = holdings_data.get("holdings") or []
    mf_holdings = holdings_data.get("mf_holdings") or []

    # Compute portfolio totals
    eq_value = sum(float(h.get("last_price", 0)) * float(h.get("quantity", 0)) for h in equity_holdings)
    mf_value = sum(float(m.get("last_price", 0)) * float(m.get("quantity", 0)) for m in mf_holdings)
    total_value = eq_value + mf_value
    eq_cost = sum(float(h.get("average_price", 0)) * float(h.get("quantity", 0)) for h in equity_holdings)
    mf_cost = sum(float(m.get("average_price", 0)) * float(m.get("quantity", 0)) for m in mf_holdings)
    total_cost = eq_cost + mf_cost
    total_pnl = total_value - total_cost
    total_pnl_pct = (total_pnl / total_cost * 100) if total_cost > 0 else 0.0
    eq_weight_pct = (eq_value / total_value * 100) if total_value > 0 else 0.0
    mf_weight_pct = (mf_value / total_value * 100) if total_value > 0 else 0.0

    portfolio_context = {
        "total_value": total_value,
        "total_cost": total_cost,
        "eq_weight_pct": eq_weight_pct,
        "mf_weight_pct": mf_weight_pct,
    }

    # Enrich each MF holding
    mf_entries = []
    for m in mf_holdings:
        m = dict(m)
        last_price = float(m.get("last_price", 0))
        avg_price = float(m.get("average_price", 0))
        qty = float(m.get("quantity", 0))
        gain_pct = ((last_price - avg_price) / avg_price * 100) if avg_price > 0 else 0.0
        m["current_value"] = round(last_price * qty, 2)
        m["cost_value"] = round(avg_price * qty, 2)
        m["gain_pct"] = round(gain_pct, 2)
        m["weight_pct"] = round((last_price * qty) / total_value * 100, 2) if total_value > 0 else 0.0
        rec = compute_recommendation(m, portfolio_context)
        m["recommendation"] = rec["action"]
        m["rationale"] = rec["rationale"]
        meta = FUND_META.get(m.get("tradingsymbol", ""), {})
        m["category"] = meta.get("category", "Unknown")
        m["risk_level"] = meta.get("risk", "moderate")
        mf_entries.append(m)

    # Enrich each equity holding
    eq_entries = []
    for h in equity_holdings:
        h = dict(h)
        last_price = float(h.get("last_price", 0))
        avg_price = float(h.get("average_price", 0))
        qty = float(h.get("quantity", 0))
        gain_pct = ((last_price - avg_price) / avg_price * 100) if avg_price > 0 else 0.0
        h["current_value"] = round(last_price * qty, 2)
        h["cost_value"] = round(avg_price * qty, 2)
        h["gain_pct"] = round(gain_pct, 2)
        h["weight_pct"] = round((last_price * qty) / total_value * 100, 2) if total_value > 0 else 0.0
        rec = compute_recommendation(h, portfolio_context)
        h["recommendation"] = rec["action"]
        h["rationale"] = rec["rationale"]
        eq_entries.append(h)

    # Category breakdown
    category_breakdown: dict[str, dict[str, float]] = {}
    for m in mf_entries:
        cat = m.get("category", "Unknown")
        if cat not in category_breakdown:
            category_breakdown[cat] = {"value": 0.0, "cost": 0.0}
        category_breakdown[cat]["value"] += m.get("current_value", 0)
        category_breakdown[cat]["cost"] += m.get("cost_value", 0)
    if eq_entries:
        cat = "Equity/ETF"
        category_breakdown.setdefault(cat, {"value": 0.0, "cost": 0.0})
        for h in eq_entries:
            category_breakdown[cat]["value"] += h.get("current_value", 0)
            category_breakdown[cat]["cost"] += h.get("cost_value", 0)

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "portfolio_summary": {
            "total_value": round(total_value, 2),
            "total_cost": round(total_cost, 2),
            "total_pnl": round(total_pnl, 2),
            "total_pnl_pct": round(total_pnl_pct, 2),
            "equity_value": round(eq_value, 2),
            "equity_weight_pct": round(eq_weight_pct, 2),
            "mf_value": round(mf_value, 2),
            "mf_weight_pct": round(mf_weight_pct, 2),
            "n_equity_holdings": len(eq_entries),
            "n_mf_holdings": len(mf_entries),
        },
        "equity_holdings": eq_entries,
        "mf_holdings": mf_entries,
        "category_breakdown": category_breakdown,
    }

    return report


def format_md(report: dict[str, Any]) -> str:
    """Format report as markdown."""
    lines: list[str] = []
    s = report["portfolio_summary"]
    lines.append("# 📊 Portfolio Tracker")
    lines.append("")
    lines.append(f"_Updated: {report['generated_at']}_")
    lines.append("")
    lines.append("## Summary")
    lines.append(f"- **Total Value:** ₹{s['total_value']:,.0f}")
    lines.append(f"- **Total Cost:** ₹{s['total_cost']:,.0f}")
    lines.append(f"- **Total P/L:** ₹{s['total_pnl']:,.0f} ({s['total_pnl_pct']:.2f}%)")
    lines.append(f"- **Equity:** ₹{s['equity_value']:,.0f} ({s['equity_weight_pct']:.1f}%)")
    lines.append(f"- **Mutual Funds:** ₹{s['mf_value']:,.0f} ({s['mf_weight_pct']:.1f}%)")
    lines.append("")

    # Category breakdown
    lines.append("## Category Allocation")
    for cat, vals in sorted(report.get("category_breakdown", {}).items(), key=lambda x: -x[1]["value"]):
        pct = vals["value"] / s["total_value"] * 100 if s["total_value"] > 0 else 0
        gain = ((vals["value"] - vals["cost"]) / vals["cost"] * 100) if vals["cost"] > 0 else 0
        lines.append(f"- {cat}: ₹{vals['value']:,.0f} ({pct:.1f}%) — {gain:+.1f}%")
    lines.append("")

    # Equity holdings
    if report["equity_holdings"]:
        lines.append("## 🏦 Equity / ETF")
        lines.append("")
        for h in report["equity_holdings"]:
            emoji = {"buy": "🟢", "hold": "🟡", "sell": "🔴"}.get(h["recommendation"], "⚪")
            lines.append(f"**{h['tradingsymbol']}** {emoji} **{h['recommendation'].upper()}**")
            lines.append(f"  - Value: ₹{h['current_value']:,.0f} | Cost: ₹{h['cost_value']:,.0f} | Gain: {h['gain_pct']:+.2f}% | Weight: {h['weight_pct']:.1f}%")
            lines.append(f"  - _{h['rationale']}_")
            lines.append("")

    # MF holdings
    if report["mf_holdings"]:
        lines.append("## 📈 Mutual Funds")
        lines.append("")
        # Sort by weight descending
        for m in sorted(report["mf_holdings"], key=lambda x: -x.get("weight_pct", 0)):
            emoji = {"buy": "🟢", "hold": "🟡", "sell": "🔴"}.get(m["recommendation"], "⚪")
            fund_short = (m.get("fund") or m["tradingsymbol"])[:60]
            lines.append(f"**{fund_short}** {emoji} **{m['recommendation'].upper()}**")
            lines.append(f"  - Category: {m.get('category', '?')} | Risk: {m.get('risk_level', '?')}")
            lines.append(f"  - Value: ₹{m['current_value']:,.0f} | Cost: ₹{m['cost_value']:,.0f} | Gain: {m['gain_pct']:+.2f}% | Weight: {m['weight_pct']:.1f}%")
            lines.append(f"  - _{m['rationale']}_")
            lines.append("")

    # Actions summary
    buy_items = [m for m in report["mf_holdings"] + report["equity_holdings"] if m["recommendation"] == "buy"]
    sell_items = [m for m in report["mf_holdings"] + report["equity_holdings"] if m["recommendation"] == "sell"]
    hold_items = [m for m in report["mf_holdings"] + report["equity_holdings"] if m["recommendation"] == "hold"]

    lines.append("## ⚡ Action Summary")
    if sell_items:
        lines.append(f"**🔴 SELL ({len(sell_items)}):** " + ", ".join(
            f"{i.get('tradingsymbol', '?')}" for i in sell_items))
    if buy_items:
        lines.append(f"**🟢 BUY ({len(buy_items)}):** " + ", ".join(
            f"{i.get('tradingsymbol', '?')}" for i in buy_items))
    if hold_items:
        lines.append(f"**🟡 HOLD ({len(hold_items)}):** " + ", ".join(
            f"{i.get('tradingsymbol', '?')}" for i in hold_items))
    lines.append("")

    return "\n".join(lines)


def main() -> int:
    # Try loading from Kite API on Oracle first
    holdings_data: dict[str, Any] = {"holdings": [], "mf_holdings": []}

    try:
        import subprocess, os
        ssh_key = os.getenv("AT_SERVER_KEY", os.path.expanduser("~/.openclaw/credentials/oracle_ssh_key"))
        oracle = os.getenv("AT_SERVER_HOST", "ubuntu@168.138.114.147")
        cmd = [
            "ssh", "-i", ssh_key, "-o", "StrictHostKeyChecking=no", oracle,
            '/home/ubuntu/Auto_Trader/venv/bin/python -c "'
            'import json,sys; sys.path.insert(0,\\"/home/ubuntu/Auto_Trader\\");'
            'from Auto_Trader.utils import get_kite_client;'
            'kite=get_kite_client();'
            'print(json.dumps({\\"holdings\\":kite.holdings(),\\"mf_holdings\\":kite.mf_holdings()}))"'
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0 and result.stdout.strip():
            holdings_data = json.loads(result.stdout.strip())
            print(f"Fetched {len(holdings_data.get('holdings',[]))} equity + {len(holdings_data.get('mf_holdings',[]))} MF from Kite")
    except Exception as e:
        print(f"Kite fetch failed: {e}, using local fallback")

    # Fallback: load from local Holdings.feather + cached MF data
    if not holdings_data.get("holdings"):
        try:
            import pandas as pd
            holdings_path = ROOT / "intermediary_files" / "Holdings.feather"
            if holdings_path.exists():
                df = pd.read_feather(holdings_path)
                holdings_data["holdings"] = df.to_dict(orient="records")
        except Exception:
            pass

    if not holdings_data.get("mf_holdings"):
        # Try cached MF state from any prior run
        cached = REPORTS_DIR / "portfolio_tracker_latest.json"
        if cached.exists():
            try:
                prev = json.loads(cached.read_text())
                holdings_data["mf_holdings"] = prev.get("mf_holdings_raw", [])
                # Rebuild from raw if present
                if not holdings_data["mf_holdings"] and prev.get("mf_holdings"):
                    # Already enriched — extract raw data
                    holdings_data["mf_holdings"] = [
                        {k: v for k, v in m.items()
                         if k not in ("recommendation", "rationale", "category", "risk_level",
                                      "current_value", "cost_value", "gain_pct", "weight_pct")}
                        for m in prev.get("mf_holdings", [])
                    ]
            except Exception:
                pass

    # Build report
    report = build_report(holdings_data)

    # Also store raw MF data for future cache fallback
    report["mf_holdings_raw"] = holdings_data.get("mf_holdings", [])

    # Write JSON
    OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_JSON.write_text(json.dumps(report, indent=2, default=str))
    print(f"Wrote {OUTPUT_JSON}")

    # Write MD
    md = format_md(report)
    OUTPUT_MD.write_text(md)
    print(f"Wrote {OUTPUT_MD}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())