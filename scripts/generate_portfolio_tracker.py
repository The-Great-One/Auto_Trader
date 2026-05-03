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
from datetime import datetime, timezone, timedelta
import math
import re
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

REPORTS_DIR = ROOT / "reports"
OUTPUT_JSON = REPORTS_DIR / "portfolio_tracker_latest.json"
OUTPUT_MD = REPORTS_DIR / "portfolio_tracker_latest.md"
DASHBOARD_DIR = ROOT / "dashboard"
if str(DASHBOARD_DIR) not in sys.path:
    sys.path.insert(0, str(DASHBOARD_DIR))


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



def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default



def _parse_dt(value: Any) -> datetime | None:
    if value in (None, "", "None"):
        return None
    try:
        if isinstance(value, datetime):
            return value.replace(tzinfo=None)
        # pandas handles date/datetime strings and date objects cleanly.
        import pandas as pd
        ts = pd.to_datetime(value, errors="coerce")
        if pd.isna(ts):
            return None
        return ts.to_pydatetime().replace(tzinfo=None)
    except Exception:
        return None


def _xnpv(rate: float, cashflows: list[tuple[datetime, float]]) -> float:
    if not cashflows:
        return 0.0
    t0 = min(d for d, _ in cashflows)
    total = 0.0
    for d, amount in cashflows:
        years = max(0.0, (d - t0).days / 365.0)
        total += amount / ((1.0 + rate) ** years)
    return total


def _xirr(cashflows: list[tuple[datetime, float]]) -> float | None:
    flows = [(d, float(v)) for d, v in cashflows if d is not None and abs(float(v)) > 1e-9]
    if len(flows) < 2 or not any(v < 0 for _, v in flows) or not any(v > 0 for _, v in flows):
        return None
    # Bisection is slower than Newton but much more stable for short MF cashflows.
    low, high = -0.95, 5.0
    f_low, f_high = _xnpv(low, flows), _xnpv(high, flows)
    expand = 0
    while f_low * f_high > 0 and expand < 8:
        high *= 2
        f_high = _xnpv(high, flows)
        expand += 1
    if f_low * f_high > 0:
        return None
    for _ in range(100):
        mid = (low + high) / 2.0
        f_mid = _xnpv(mid, flows)
        if abs(f_mid) < 1e-5:
            return mid
        if f_low * f_mid <= 0:
            high = mid
            f_high = f_mid
        else:
            low = mid
            f_low = f_mid
    return (low + high) / 2.0


def _order_cashflow_amount(order: dict[str, Any]) -> float:
    amount = _safe_float(order.get("amount"))
    if amount > 0:
        return amount
    qty = _safe_float(order.get("quantity"))
    price = _safe_float(order.get("average_price")) or _safe_float(order.get("price"))
    return qty * price


def _build_mf_cashflows_by_symbol(mf_orders: list[dict[str, Any]], mf_entries: list[dict[str, Any]]) -> tuple[dict[str, list[tuple[datetime, float]]], dict[str, Any]]:
    today = datetime.now().replace(tzinfo=None)
    flows: dict[str, list[tuple[datetime, float]]] = {}
    completed_orders = 0
    skipped_orders = 0
    for order in mf_orders or []:
        status = str(order.get("status") or "").upper()
        if status not in {"COMPLETE", "COMPLETED"}:
            skipped_orders += 1
            continue
        symbol = str(order.get("tradingsymbol") or "").strip().upper()
        if not symbol:
            continue
        dt = _parse_dt(order.get("exchange_timestamp") or order.get("order_timestamp"))
        amount = _order_cashflow_amount(order)
        if not dt or amount <= 0:
            skipped_orders += 1
            continue
        tx = str(order.get("transaction_type") or "BUY").upper()
        sign = -1.0 if tx == "BUY" else 1.0
        flows.setdefault(symbol, []).append((dt, sign * amount))
        completed_orders += 1

    total_terminal = 0.0
    for m in mf_entries:
        symbol = str(m.get("tradingsymbol") or "").strip().upper()
        value = _safe_float(m.get("current_value"))
        if symbol and value > 0:
            flows.setdefault(symbol, []).append((today, value))
            total_terminal += value
    meta = {
        "completed_orders_used": completed_orders,
        "orders_skipped": skipped_orders,
        "terminal_value": round(total_terminal, 2),
        "as_of": today.isoformat(timespec="seconds"),
    }
    return flows, meta


def enrich_mf_xirr(mf_entries: list[dict[str, Any]], mf_orders: list[dict[str, Any]]) -> dict[str, Any]:
    flows_by_symbol, meta = _build_mf_cashflows_by_symbol(mf_orders, mf_entries)
    today = datetime.now().replace(tzinfo=None)
    all_flows: list[tuple[datetime, float]] = []
    by_symbol: dict[str, dict[str, Any]] = {}
    for m in mf_entries:
        symbol = str(m.get("tradingsymbol") or "").strip().upper()
        flows = list(flows_by_symbol.get(symbol, []))
        terminal_value = _safe_float(m.get("current_value"))
        cost_value = _safe_float(m.get("cost_value"))
        dated_invested = -sum(v for _, v in flows if v < 0)
        missing_cost = max(0.0, cost_value - dated_invested)
        xirr_source = "kite_completed_orders"
        if missing_cost > max(500.0, cost_value * 0.05):
            # Kite's MF order endpoint is often recent-only. Add a clearly marked
            # synthetic opening lot so the dashboard can still show a usable
            # estimated XIRR while surfacing that exact dated lots are missing.
            first_dt = min((d for d, v in flows if v < 0), default=today)
            synthetic_dt = min(first_dt - timedelta(days=365), today - timedelta(days=365))
            flows.append((synthetic_dt, -missing_cost))
            xirr_source = "estimated_with_synthetic_opening_lot"
        invested = -sum(v for _, v in flows if v < 0)
        redeemed = sum(v for _, v in flows if v > 0) - terminal_value
        x = _xirr(flows)
        if x is not None:
            m["xirr_pct"] = round(x * 100.0, 2)
            m["xirr_available"] = True
        else:
            # Holding gain is not XIRR; keep it visibly separate.
            m["xirr_pct"] = None
            m["xirr_available"] = False
        m["xirr_source"] = xirr_source if m.get("xirr_available") else "unavailable"
        m["xirr_missing_cost_estimated"] = round(missing_cost, 2)
        m["cashflow_invested"] = round(invested, 2)
        m["cashflow_redeemed"] = round(max(0.0, redeemed), 2)
        m["cashflow_count"] = len(flows)
        by_symbol[symbol] = {
            "fund": m.get("fund") or symbol,
            "xirr_pct": m.get("xirr_pct"),
            "current_value": m.get("current_value"),
            "cashflow_count": len(flows),
        }
        all_flows.extend(flows)
    portfolio_xirr = _xirr(all_flows)
    return {
        "portfolio_xirr_pct": round(portfolio_xirr * 100.0, 2) if portfolio_xirr is not None else None,
        "by_symbol": by_symbol,
        **meta,
        "method": "Completed Kite MF orders as dated cash outflows/inflows plus current holding value as terminal inflow. Processing orders are excluded.",
    }


REPLACEMENT_CANDIDATES_BY_CATEGORY: dict[str, list[str]] = {
    "Flexi Cap": ["Parag Parikh Flexi Cap Fund Direct Growth", "HDFC Flexi Cap Fund Direct Growth", "JM Flexicap Fund Direct Growth", "Kotak Flexicap Fund Direct Growth"],
    "Focused": ["SBI Focused Equity Fund Direct Growth", "HDFC Focused 30 Fund Direct Growth", "ICICI Prudential Focused Equity Fund Direct Growth"],
    "Mid Cap": ["Motilal Oswal Midcap Fund Direct Growth", "HDFC Mid-Cap Opportunities Fund Direct Growth", "Edelweiss Mid Cap Fund Direct Growth", "Kotak Emerging Equity Scheme Direct Growth"],
    "Small Cap": ["Nippon India Small Cap Fund Direct Growth", "Tata Small Cap Fund Direct Growth", "HSBC Small Cap Fund Direct Growth", "Bandhan Small Cap Fund Direct Growth", "Quant Small Cap Fund Direct Growth"],
    "ELSS": ["Parag Parikh ELSS Tax Saver Fund Direct Growth", "Quant ELSS Tax Saver Fund Direct Growth", "Mirae Asset ELSS Tax Saver Fund Direct Growth", "DSP ELSS Tax Saver Fund Direct Growth"],
    "Hybrid/Equity+Debt": ["ICICI Prudential Equity & Debt Fund Direct Growth", "HDFC Balanced Advantage Fund Direct Growth", "Edelweiss Aggressive Hybrid Fund Direct Growth"],
    "Sectoral/Banking": ["Parag Parikh Flexi Cap Fund Direct Growth", "HDFC Flexi Cap Fund Direct Growth", "ICICI Prudential Nifty Bank ETF", "Nippon India Banking & Financial Services Fund Direct Growth"],
    "Infrastructure": ["ICICI Prudential Infrastructure Fund Direct Growth", "HDFC Infrastructure Fund Direct Growth", "Parag Parikh Flexi Cap Fund Direct Growth"],
    "Index/NASDAQ": ["ICICI Prudential Nasdaq 100 Index Fund Direct Growth", "Motilal Oswal Nasdaq 100 Fund of Fund Direct Growth", "Mirae Asset NYSE FANG+ ETF Fund of Fund Direct Growth"],
    "International/Taiwan": ["ICICI Prudential Nasdaq 100 Index Fund Direct Growth", "Motilal Oswal Nasdaq 100 Fund of Fund Direct Growth", "Parag Parikh Flexi Cap Fund Direct Growth"],
    "Unknown": ["Parag Parikh Flexi Cap Fund Direct Growth", "HDFC Flexi Cap Fund Direct Growth"],
}


def _tokens(text: str) -> set[str]:
    stop = {"fund", "direct", "plan", "growth", "option", "regular", "the", "and", "of"}
    return {t for t in re.findall(r"[a-z0-9]+", str(text).lower()) if t not in stop and len(t) > 1}


def _match_scheme_code(query: str) -> tuple[int | None, str | None]:
    try:
        from mf_dash_utils import fetch_scheme_list
        schemes = fetch_scheme_list()
    except Exception:
        return None, None
    qtok = _tokens(query)
    if not qtok:
        return None, None
    best_score = -1.0
    best = None
    for row in schemes.itertuples(index=False):
        name = str(row.scheme_name)
        ntok = _tokens(name)
        if not ntok:
            continue
        score = len(qtok & ntok) / max(1, len(qtok | ntok))
        # Prefer direct growth matches.
        lname = name.lower()
        if "direct" in lname:
            score += 0.08
        if "growth" in lname:
            score += 0.05
        if score > best_score:
            best_score = score
            best = row
    if best is None or best_score < 0.28:
        return None, None
    return int(best.scheme_code), str(best.scheme_name)


def _nav_metric_for_query(query: str) -> dict[str, Any] | None:
    code, matched_name = _match_scheme_code(query)
    if not code:
        return None
    try:
        from mf_dash_utils import fetch_nav_history
        import pandas as pd
        nav = fetch_nav_history(code)
        if nav is None or nav.empty or len(nav) < 120:
            return None
        nav = nav.sort_values("date").reset_index(drop=True)
        latest_date = pd.Timestamp(nav["date"].iloc[-1])
        latest_nav = float(nav["nav"].iloc[-1])
        def cagr(years: int) -> float | None:
            cutoff = latest_date - pd.DateOffset(years=years)
            hist = nav[nav["date"] <= cutoff]
            if hist.empty:
                return None
            start_nav = float(hist["nav"].iloc[-1])
            if start_nav <= 0:
                return None
            actual_years = max(0.25, (latest_date - pd.Timestamp(hist["date"].iloc[-1])).days / 365.0)
            return ((latest_nav / start_nav) ** (1.0 / actual_years) - 1.0) * 100.0
        one = cagr(1)
        three = cagr(3)
        five = cagr(5)
        ret = nav.set_index("date")["nav"].pct_change().dropna()
        vol = float(ret.tail(756).std() * (252 ** 0.5) * 100.0) if len(ret) > 30 else None
        score = (three if three is not None else 0.0) * 0.55 + (five if five is not None else (three or 0.0)) * 0.25 + (one if one is not None else 0.0) * 0.20 - (vol or 0.0) * 0.08
        return {
            "scheme_code": code,
            "matched_name": matched_name,
            "return_1y_pct": round(one, 2) if one is not None else None,
            "return_3y_cagr_pct": round(three, 2) if three is not None else None,
            "return_5y_cagr_pct": round(five, 2) if five is not None else None,
            "vol_3y_pct": round(vol, 2) if vol is not None else None,
            "score": round(score, 2),
        }
    except Exception:
        return None


def build_mf_replacement_plan(mf_entries: list[dict[str, Any]]) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    evaluated = 0
    for m in sorted(mf_entries, key=lambda x: _safe_float(x.get("current_value")), reverse=True):
        category = str(m.get("category") or "Unknown")
        xirr = m.get("xirr_pct")
        gain = _safe_float(m.get("gain_pct"))
        weak = (xirr is not None and _safe_float(xirr) < 10.0) or gain < 0 or str(m.get("recommendation") or "").lower() == "sell"
        if not weak:
            continue
        current_metric = _nav_metric_for_query(str(m.get("fund") or m.get("tradingsymbol") or ""))
        candidates = []
        for q in REPLACEMENT_CANDIDATES_BY_CATEGORY.get(category, REPLACEMENT_CANDIDATES_BY_CATEGORY["Unknown"]):
            metric = _nav_metric_for_query(q)
            if not metric:
                continue
            if current_metric and metric.get("scheme_code") == current_metric.get("scheme_code"):
                continue
            candidates.append(metric)
            evaluated += 1
        candidates = sorted(candidates, key=lambda x: _safe_float(x.get("score")), reverse=True)[:3]
        for cand in candidates:
            current_3y = current_metric.get("return_3y_cagr_pct") if current_metric else None
            uplift = None
            if current_3y is not None and cand.get("return_3y_cagr_pct") is not None:
                uplift = round(_safe_float(cand.get("return_3y_cagr_pct")) - _safe_float(current_3y), 2)
                if uplift < 0.5:
                    continue
            rows.append({
                "priority": "high" if (xirr is not None and _safe_float(xirr) < 6) or gain < -3 else "medium",
                "current_fund": (m.get("fund") or m.get("tradingsymbol") or "?")[:72],
                "category": category,
                "current_value": round(_safe_float(m.get("current_value")), 2),
                "current_xirr_pct": xirr,
                "current_gain_pct": round(gain, 2),
                "current_3y_cagr_pct": current_3y,
                "replacement_fund": cand.get("matched_name"),
                "replacement_1y_pct": cand.get("return_1y_pct"),
                "replacement_3y_cagr_pct": cand.get("return_3y_cagr_pct"),
                "replacement_5y_cagr_pct": cand.get("return_5y_cagr_pct"),
                "replacement_vol_3y_pct": cand.get("vol_3y_pct"),
                "estimated_3y_cagr_uplift_pct": uplift,
                "action_logic": "Switch only after exit-load, LTCG/STCG, and ELSS lock-in checks; use this as a shortlist, not an auto-order.",
            })
    return {
        "method": "Compares weak current holdings with curated same-sleeve/direct-plan candidates using MFAPI NAV 1Y/3Y/5Y CAGR and volatility.",
        "evaluated_candidates": evaluated,
        "actions": rows[:18],
    }

def build_mf_xirr_boosters(mf_entries: list[dict[str, Any]], portfolio_summary: dict[str, Any]) -> dict[str, Any]:
    """Build actionable, current-holding based MF XIRR improvement ideas.

    This is intentionally a decision-support layer, not an execution engine. It
    does not assume exact investor-level XIRR because Kite holdings expose NAV,
    average cost, and quantity but not the full dated cash-flow ledger needed for
    true XIRR. The dashboard therefore labels this as an XIRR booster plan and
    uses current gain, sleeve weight, risk, category concentration, and tax/ELSS
    constraints to route future SIP/top-up/redeem decisions.
    """
    mf_value = max(1e-9, _safe_float(portfolio_summary.get("mf_value")))
    total_value = max(1e-9, _safe_float(portfolio_summary.get("total_value")))
    mf_weight_pct = _safe_float(portfolio_summary.get("mf_weight_pct"))

    enriched: list[dict[str, Any]] = []
    category_values: dict[str, float] = {}
    risk_values: dict[str, float] = {}
    for m in mf_entries:
        row = dict(m)
        value = _safe_float(row.get("current_value"))
        cat = str(row.get("category") or "Unknown")
        risk = str(row.get("risk_level") or "moderate")
        row["mf_sleeve_pct"] = round(value / mf_value * 100.0, 2) if mf_value > 0 else 0.0
        enriched.append(row)
        category_values[cat] = category_values.get(cat, 0.0) + value
        risk_values[risk] = risk_values.get(risk, 0.0) + value

    category_pct = {k: round(v / mf_value * 100.0, 2) for k, v in category_values.items()}
    risk_pct = {k: round(v / mf_value * 100.0, 2) for k, v in risk_values.items()}
    elss_pct = sum(v for k, v in category_pct.items() if "ELSS" in k.upper())
    sector_pct = sum(v for k, v in category_pct.items() if any(tok in k.upper() for tok in ["SECTOR", "BANK", "INFRA"]) )
    small_pct = sum(v for k, v in category_pct.items() if "SMALL" in k.upper())
    intl_pct = sum(v for k, v in category_pct.items() if any(tok in k.upper() for tok in ["NASDAQ", "INTERNATIONAL", "TAIWAN"]) )
    flexi_core_pct = sum(v for k, v in category_pct.items() if any(tok in k.upper() for tok in ["FLEXI", "FOCUSED"]) )

    rows: list[dict[str, Any]] = []

    def add(priority: str, action: str, fund: str, current_value: float, mf_pct: float, gain_pct: float, why: str, route: str) -> None:
        rows.append({
            "priority": priority,
            "action": action,
            "fund": fund[:72],
            "current_value": round(current_value, 2),
            "mf_sleeve_pct": round(mf_pct, 2),
            "gain_pct": round(gain_pct, 2),
            "xirr_logic": why,
            "suggested_route": route,
        })

    # 1) Put new money where current holdings are underweight and either in drawdown
    # or still small enough to move the portfolio XIRR meaningfully.
    topup_candidates = []
    for m in enriched:
        cat = str(m.get("category") or "")
        risk = str(m.get("risk_level") or "")
        gain = _safe_float(m.get("gain_pct"))
        mf_pct = _safe_float(m.get("mf_sleeve_pct"))
        value = _safe_float(m.get("current_value"))
        rec = str(m.get("recommendation") or "").lower()
        if rec == "buy" and mf_pct < 12 and risk in {"moderate", "high", "low_moderate"}:
            score = (12 - mf_pct) + max(0.0, -gain) * 1.5 + (2.0 if any(x in cat for x in ["Flexi", "Focused", "Mid", "NASDAQ"]) else 0.0)
            topup_candidates.append((score, m))
    for _, m in sorted(topup_candidates, key=lambda x: -x[0])[:5]:
        cat = str(m.get("category") or "")
        gain = _safe_float(m.get("gain_pct"))
        route = "Route fresh SIP/top-up here in tranches; prefer dips instead of lump-sum chase."
        if gain < 0:
            why = f"Underweight {cat} holding is below average cost; adding now can lower cost basis and improve future XIRR if thesis holds."
        else:
            why = f"Underweight {cat} holding with buy signal; fresh money has more XIRR impact here than adding to already-heavy winners."
        add("high", "top_up", m.get("fund") or m.get("tradingsymbol") or "?", _safe_float(m.get("current_value")), _safe_float(m.get("mf_sleeve_pct")), gain, why, route)

    # 2) Stop new money into buckets that are already too large; this boosts future
    # XIRR by avoiding concentration drag and stale locked capital.
    if elss_pct > 30:
        for m in sorted([x for x in enriched if "ELSS" in str(x.get("category", "")).upper()], key=lambda x: -_safe_float(x.get("current_value")))[:3]:
            add(
                "high",
                "pause_fresh_sip",
                m.get("fund") or m.get("tradingsymbol") or "?",
                _safe_float(m.get("current_value")),
                _safe_float(m.get("mf_sleeve_pct")),
                _safe_float(m.get("gain_pct")),
                f"ELSS is {elss_pct:.1f}% of the MF sleeve; extra money here reduces flexibility and can trap future rebalancing behind lock-ins.",
                "Keep existing locked units; route new tax-saving only up to 80C need, otherwise redirect SIP to flexible core/international.",
            )

    if sector_pct > 15:
        for m in sorted([x for x in enriched if any(tok in str(x.get("category", "")).upper() for tok in ["SECTOR", "BANK", "INFRA"])], key=lambda x: -_safe_float(x.get("current_value")))[:3]:
            action = "trim_on_strength" if _safe_float(m.get("gain_pct")) > 3 else "cap_fresh_sip"
            add(
                "medium",
                action,
                m.get("fund") or m.get("tradingsymbol") or "?",
                _safe_float(m.get("current_value")),
                _safe_float(m.get("mf_sleeve_pct")),
                _safe_float(m.get("gain_pct")),
                f"Sector/thematic funds are {sector_pct:.1f}% of MF sleeve; cyclic concentration can hurt XIRR in sideways regimes.",
                "Do not add fresh SIP until sleeve falls below ~12–15%; harvest gains only after exit-load/tax checks.",
            )

    # 3) Improve diversification using current holdings rather than new fund sprawl.
    if intl_pct < 5:
        nasdaq = [x for x in enriched if "NASDAQ" in str(x.get("category", "")).upper()]
        for m in nasdaq[:1]:
            add(
                "medium",
                "increase_global_diversifier",
                m.get("fund") or m.get("tradingsymbol") or "?",
                _safe_float(m.get("current_value")),
                _safe_float(m.get("mf_sleeve_pct")),
                _safe_float(m.get("gain_pct")),
                f"International exposure is only {intl_pct:.1f}% of MF sleeve; a small global sleeve can smooth India-only drawdowns and help risk-adjusted XIRR.",
                "Build gradually toward ~5–8% MF sleeve; avoid Taiwan concentration unless explicitly desired.",
            )

    # 4) Tiny satellite cleanup: small positions rarely move XIRR but add tracking
    # noise. Keep only if they have a deliberate role.
    for m in sorted([x for x in enriched if _safe_float(x.get("mf_sleeve_pct")) < 1.0], key=lambda x: _safe_float(x.get("mf_sleeve_pct")))[:4]:
        add(
            "low",
            "consolidate_or_scale",
            m.get("fund") or m.get("tradingsymbol") or "?",
            _safe_float(m.get("current_value")),
            _safe_float(m.get("mf_sleeve_pct")),
            _safe_float(m.get("gain_pct")),
            "Tiny satellite position has too little capital to materially lift portfolio XIRR unless scaled with conviction.",
            "Either scale to at least ~3–5% MF sleeve over time or fold future money into the core winners.",
        )

    # De-duplicate by fund/action while preserving priority order.
    priority_rank = {"high": 0, "medium": 1, "low": 2}
    seen: set[tuple[str, str]] = set()
    deduped = []
    for row in sorted(rows, key=lambda r: (priority_rank.get(str(r["priority"]), 9), -abs(_safe_float(r.get("gain_pct"))))):
        key = (str(row["action"]), str(row["fund"]))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)

    return {
        "method": "XIRR booster heuristics from current holdings; true XIRR needs dated cash-flow lots.",
        "mf_value": round(mf_value, 2),
        "mf_weight_pct": round(mf_weight_pct, 2),
        "mf_vs_total_pct": round(mf_value / total_value * 100.0, 2),
        "category_pct": category_pct,
        "risk_pct": risk_pct,
        "diagnostics": {
            "elss_pct_of_mf": round(elss_pct, 2),
            "sector_thematic_pct_of_mf": round(sector_pct, 2),
            "smallcap_pct_of_mf": round(small_pct, 2),
            "international_pct_of_mf": round(intl_pct, 2),
            "flexi_focused_pct_of_mf": round(flexi_core_pct, 2),
        },
        "actions": deduped[:12],
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
        m["mf_sleeve_pct"] = round((last_price * qty) / mf_value * 100, 2) if mf_value > 0 else 0.0
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

    mf_xirr_summary = enrich_mf_xirr(mf_entries, holdings_data.get("mf_orders") or [])
    mf_xirr_boosters = build_mf_xirr_boosters(mf_entries, {
        "total_value": round(total_value, 2),
        "mf_value": round(mf_value, 2),
        "mf_weight_pct": round(mf_weight_pct, 2),
    })
    mf_replacement_plan = build_mf_replacement_plan(mf_entries)

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
            "mf_xirr_pct": mf_xirr_summary.get("portfolio_xirr_pct"),
            "n_equity_holdings": len(eq_entries),
            "n_mf_holdings": len(mf_entries),
        },
        "equity_holdings": eq_entries,
        "mf_holdings": mf_entries,
        "category_breakdown": category_breakdown,
        "mf_xirr_summary": mf_xirr_summary,
        "mf_xirr_boosters": mf_xirr_boosters,
        "mf_replacement_plan": mf_replacement_plan,
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
    if s.get("mf_xirr_pct") is not None:
        lines.append(f"- **MF XIRR:** {s['mf_xirr_pct']:+.2f}%")
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

    # MF XIRR booster plan
    xirr_plan = report.get("mf_xirr_boosters") or {}
    if xirr_plan.get("actions"):
        lines.append("## 🚀 MF XIRR Booster Plan")
        lines.append("")
        diag = xirr_plan.get("diagnostics") or {}
        lines.append(f"- ELSS: {diag.get('elss_pct_of_mf', 0):.1f}% of MF sleeve | Sector/thematic: {diag.get('sector_thematic_pct_of_mf', 0):.1f}% | International: {diag.get('international_pct_of_mf', 0):.1f}%")
        for a in xirr_plan.get("actions", [])[:8]:
            lines.append(f"- **{a.get('priority', '').upper()} / {a.get('action', '')}:** {a.get('fund')} — {a.get('xirr_logic')} _{a.get('suggested_route')}_")
        lines.append("")

    # MF replacement plan
    repl_plan = report.get("mf_replacement_plan") or {}
    if repl_plan.get("actions"):
        lines.append("## 🔁 MF Replacement Shortlist")
        lines.append("")
        for r in repl_plan.get("actions", [])[:10]:
            uplift = r.get("estimated_3y_cagr_uplift_pct")
            uplift_txt = f" | est 3Y CAGR uplift {uplift:+.2f}pp" if uplift is not None else ""
            lines.append(f"- **{r.get('priority', '').upper()}** replace/watch **{r.get('current_fund')}** → **{r.get('replacement_fund')}**{uplift_txt}. Current XIRR: {r.get('current_xirr_pct', '-')}; replacement 3Y CAGR: {r.get('replacement_3y_cagr_pct', '-') }%")
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
            xirr_text = f" | XIRR: {m['xirr_pct']:+.2f}%" if m.get("xirr_pct") is not None else ""
            lines.append(f"  - Value: ₹{m['current_value']:,.0f} | Cost: ₹{m['cost_value']:,.0f} | Gain: {m['gain_pct']:+.2f}%{xirr_text} | Weight: {m['weight_pct']:.1f}%")
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
        oracle = os.getenv("AT_SERVER_HOST")
        if not oracle:
            return {"error": "AT_SERVER_HOST env var not set"}
        oracle_target = oracle if "@" in oracle else f"ubuntu@{oracle}"
        cmd = [
            "ssh", "-i", ssh_key, "-o", "StrictHostKeyChecking=no", oracle_target,
            '/home/ubuntu/Auto_Trader/venv/bin/python -c "'
            'import json,sys; sys.path.insert(0,\\"/home/ubuntu/Auto_Trader\\");'
            'from Auto_Trader.utils import get_kite_client;'
            'kite=get_kite_client();'
            'print(json.dumps({\\"holdings\\":kite.holdings(),\\"mf_holdings\\":kite.mf_holdings(),\\"mf_orders\\":kite.mf_orders()}, default=str))"'
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0 and result.stdout.strip():
            holdings_data = json.loads(result.stdout.strip())
            print(f"Fetched {len(holdings_data.get('holdings',[]))} equity + {len(holdings_data.get('mf_holdings',[]))} MF + {len(holdings_data.get('mf_orders',[]))} MF orders from Kite")
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
                holdings_data["mf_orders"] = prev.get("mf_orders_raw", [])
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
    report["mf_orders_raw"] = holdings_data.get("mf_orders", [])

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