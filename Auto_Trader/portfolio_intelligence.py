import os
import json
import logging
from dataclasses import dataclass
from typing import Dict, List, Tuple

import pandas as pd
import requests

logger = logging.getLogger("Auto_Trade_Logger")


@dataclass
class PortfolioTargets:
    equity: float = 0.55
    etf: float = 0.25
    mf: float = 0.20

    def normalized(self) -> Dict[str, float]:
        vals = {
            "EQUITY": max(0.0, float(self.equity)),
            "ETF": max(0.0, float(self.etf)),
            "MF": max(0.0, float(self.mf)),
        }
        s = sum(vals.values())
        if s <= 0:
            return {"EQUITY": 0.55, "ETF": 0.25, "MF": 0.20}
        return {k: v / s for k, v in vals.items()}


def _safe_float(x, default=0.0):
    try:
        return float(x)
    except Exception:
        return default


def _classify_symbol(symbol: str) -> str:
    s = (symbol or "").upper()
    if "ETF" in s or "BEES" in s:
        return "ETF"
    return "EQUITY"


def fetch_portfolio_snapshot(kite) -> pd.DataFrame:
    rows: List[dict] = []

    # Equity + ETF holdings
    for h in kite.holdings() or []:
        qty = _safe_float(h.get("quantity"), 0) + _safe_float(h.get("t1_quantity"), 0)
        if qty <= 0:
            continue
        avg = _safe_float(h.get("average_price"), 0)
        ltp = _safe_float(h.get("last_price"), _safe_float(h.get("close_price"), 0))
        asset = _classify_symbol(h.get("tradingsymbol"))
        rows.append(
            {
                "asset_class": asset,
                "symbol": h.get("tradingsymbol"),
                "qty": qty,
                "avg": avg,
                "ltp": ltp,
                "cost": qty * avg,
                "value": qty * ltp,
            }
        )

    # Mutual funds
    try:
        for mf in kite.mf_holdings() or []:
            qty = _safe_float(mf.get("quantity"), 0)
            nav = _safe_float(mf.get("last_price"), _safe_float(mf.get("nav"), 0))
            inv = _safe_float(mf.get("average_price"), _safe_float(mf.get("last_price"), 0))
            if qty <= 0 and _safe_float(mf.get("amount"), 0) <= 0:
                continue
            value = qty * nav if qty > 0 else _safe_float(mf.get("amount"), 0)
            cost = qty * inv if qty > 0 else value
            rows.append(
                {
                    "asset_class": "MF",
                    "symbol": mf.get("tradingsymbol") or mf.get("scheme_name") or "MF_SCHEME",
                    "qty": qty,
                    "avg": inv,
                    "ltp": nav,
                    "cost": cost,
                    "value": value,
                }
            )
    except Exception as e:
        logger.warning("mf_holdings fetch failed: %s", e)

    return pd.DataFrame(rows)


def news_risk_score() -> Tuple[int, List[str]]:
    sources = [
        # Global
        "https://feeds.reuters.com/reuters/worldNews",
        "https://feeds.reuters.com/reuters/businessNews",
        # India-focused
        "https://www.moneycontrol.com/rss/business.xml",
        "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms",
        "https://www.livemint.com/rss/markets",
        "https://www.businesstoday.in/rss/markets",
    ]
    risk_words = [
        "war",
        "missile",
        "sanction",
        "attack",
        "conflict",
        "oil spike",
        "crude spikes",
        "emergency",
        "invasion",
        "terror",
        "border tension",
        "loc",
        "geopolitical",
        "india-pakistan",
        "middle east conflict",
    ]
    calm_words = ["ceasefire", "truce", "deal", "eases", "cools", "decline in oil"]

    headlines: List[str] = []
    for url in sources:
        try:
            r = requests.get(url, timeout=12)
            text = r.text.lower()
            # lightweight extraction
            headlines.extend([x.strip() for x in text.split("<title>")[1:25]])
        except Exception:
            continue

    score = 0
    matched = []
    for h in headlines:
        t = h.split("</title>")[0]
        for w in risk_words:
            if w in t:
                score += 1
                matched.append(t)
        for w in calm_words:
            if w in t:
                score -= 1

    score = max(-5, min(10, score))
    return score, matched[:5]


def dynamic_targets(base: PortfolioTargets, risk_score: int) -> Dict[str, float]:
    t = base.normalized()
    # risk-off: reduce equity, raise ETF+MF
    if risk_score >= 5:
        t["EQUITY"] -= 0.15
        t["ETF"] += 0.10
        t["MF"] += 0.05
    elif risk_score >= 2:
        t["EQUITY"] -= 0.08
        t["ETF"] += 0.05
        t["MF"] += 0.03
    elif risk_score <= -2:
        t["EQUITY"] += 0.06
        t["ETF"] -= 0.03
        t["MF"] -= 0.03

    # re-normalize and clamp
    for k in t:
        t[k] = max(0.05, t[k])
    s = sum(t.values())
    return {k: v / s for k, v in t.items()}


def allocation(df: pd.DataFrame) -> Dict[str, float]:
    if df.empty:
        return {"EQUITY": 0.0, "ETF": 0.0, "MF": 0.0}
    grp = df.groupby("asset_class")["value"].sum().to_dict()
    total = sum(grp.values()) or 1.0
    return {
        "EQUITY": grp.get("EQUITY", 0.0) / total,
        "ETF": grp.get("ETF", 0.0) / total,
        "MF": grp.get("MF", 0.0) / total,
    }


def rebalance_advice(current: Dict[str, float], target: Dict[str, float], total_value: float) -> Dict[str, float]:
    out = {}
    for k in ["EQUITY", "ETF", "MF"]:
        out[k] = (target.get(k, 0) - current.get(k, 0)) * total_value
    return out


def build_report(kite) -> dict:
    df = fetch_portfolio_snapshot(kite)
    total_value = float(df["value"].sum()) if not df.empty else 0.0
    total_cost = float(df["cost"].sum()) if not df.empty else 0.0
    pnl = total_value - total_cost

    risk_score, matched = news_risk_score()
    base = PortfolioTargets(
        equity=float(os.getenv("AT_TARGET_EQUITY", "0.55")),
        etf=float(os.getenv("AT_TARGET_ETF", "0.25")),
        mf=float(os.getenv("AT_TARGET_MF", "0.20")),
    )
    tgt = dynamic_targets(base, risk_score)
    cur = allocation(df)
    advice = rebalance_advice(cur, tgt, total_value)

    return {
        "total_value": round(total_value, 2),
        "total_cost": round(total_cost, 2),
        "total_pnl": round(pnl, 2),
        "total_pnl_pct": round((pnl / total_cost * 100) if total_cost else 0, 2),
        "risk_score": risk_score,
        "risk_headlines": matched,
        "current_allocation": cur,
        "target_allocation": tgt,
        "rebalance_advice_inr": {k: round(v, 2) for k, v in advice.items()},
    }


def format_markdown(report: dict) -> str:
    lines = [
        "# Portfolio Intelligence Report",
        f"- Total Value: ₹{report['total_value']}",
        f"- Total Cost: ₹{report['total_cost']}",
        f"- Total P/L: ₹{report['total_pnl']} ({report['total_pnl_pct']}%)",
        f"- News Risk Score: {report['risk_score']}",
        "",
        "## Allocation (Current -> Target)",
    ]
    for k in ["EQUITY", "ETF", "MF"]:
        c = report["current_allocation"].get(k, 0) * 100
        t = report["target_allocation"].get(k, 0) * 100
        d = report["rebalance_advice_inr"].get(k, 0)
        lines.append(f"- {k}: {c:.1f}% -> {t:.1f}% (₹{d:+.0f})")

    if report.get("risk_headlines"):
        lines += ["", "## Top risk headlines", *[f"- {h}" for h in report["risk_headlines"]]]

    return "\n".join(lines)
