#!/usr/bin/env python3
"""Daily audit of Auto_Trader reports/logs to identify concrete improvement areas.

This script is intentionally read-only. It does not edit code, change env,
or restart services. It writes JSON and Markdown reports that highlight
freshness issues, recurring failures, and likely next improvements.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pandas_market_calendars as mcal

ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / "reports"
REPORTS.mkdir(exist_ok=True)


def ist_now() -> datetime:
    return datetime.now()


def is_market_open_today() -> tuple[bool, str]:
    nse = mcal.get_calendar("NSE")
    today = ist_now().date()
    valid = nse.valid_days(start_date=str(today), end_date=str(today))
    return len(valid) > 0, str(today)



def _load_json(path: Path):
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except Exception:
        return None



def _tail(path: Path, lines: int = 80) -> str:
    if not path.exists():
        return ""
    text = path.read_text(errors="ignore").splitlines()
    return "\n".join(text[-lines:])



def _latest(pattern: str) -> Path | None:
    matches = sorted(REPORTS.glob(pattern))
    return matches[-1] if matches else None



def _is_fresh_ts(value: str | None, trade_date: str) -> bool:
    return str(value or "").startswith(trade_date)



def main():
    now = ist_now()
    market_open, trade_date = is_market_open_today()

    scorecard_path = REPORTS / f"daily_scorecard_{trade_date}.json"
    ops_path = REPORTS / f"daily_ops_supervisor_{trade_date}.json"
    options_path = REPORTS / f"options_research_supervisor_{trade_date}.json"
    paper_path = REPORTS / "paper_shadow_latest.json"
    paper_options_path = REPORTS / "paper_shadow_options_latest.json"
    equity_lab_path = _latest("strategy_lab_*.json")
    options_lab_path = _latest("options_strategy_lab_*.json")

    scorecard = _load_json(scorecard_path) or {}
    ops = _load_json(ops_path) or {}
    options = _load_json(options_path) or {}
    paper = _load_json(paper_path) or {}
    paper_options = _load_json(paper_options_path) or {}
    equity_lab = _load_json(equity_lab_path) or {}
    options_lab = _load_json(options_lab_path) or {}

    issues = []
    improvements = []

    def add_issue(severity: str, area: str, detail: str):
        issues.append({"severity": severity, "area": area, "detail": detail})

    def add_improvement(priority: str, title: str, detail: str):
        improvements.append({"priority": priority, "title": title, "detail": detail})

    if market_open and scorecard and int(scorecard.get("orders", 0) or 0) == 0 and int(scorecard.get("trades", 0) or 0) == 0:
        add_issue("medium", "scorecard", "No orders or trades recorded for the trade date.")
        add_improvement("high", "Improve entry sensitivity review", "No-trade day detected. Revisit BUY gate strictness and inspect near-miss candidates in equity and options paper outputs.")

    strategy = ops.get("strategy_test") or {}
    if strategy:
        if strategy.get("stale_report"):
            add_issue("high", "equity_lab", f"Daily ops is looking at a stale strategy lab report: {strategy.get('file')}")
            add_improvement("high", "Prevent stale equity lab reuse", "If weekly_strategy_lab fails or no fresh report is produced, surface that as a hard failure and skip candidate reuse.")
        if not strategy.get("ok"):
            add_issue("high", "equity_lab", f"Equity strategy lab failed today: {strategy.get('stderr') or strategy.get('reason')}")
            add_improvement("high", "Repair equity lab data loading", "Investigate missing/empty history for the requested lab basket and rebuild bad caches before the daily run.")

    paper_trader = ops.get("paper_trader") or {}
    if paper_trader and not paper_trader.get("paper_executed"):
        add_issue("high", "paper_shadow", f"Daily ops paper check did not complete cleanly: {paper_trader.get('reason')}")
        add_improvement("medium", "Add explicit paper freshness checks", "Validate both paper_shadow_latest.json and paper_shadow_options_latest.json timestamps in daily ops, not just existence.")

    options_paper = options.get("paper_shadow") or {}
    if options_paper:
        if not options_paper.get("ok"):
            add_issue("high", "options_paper", f"Options paper shadow failed today: {options_paper.get('stderr') or options_paper.get('reason')}")
        elif options_paper.get("stale_report"):
            add_issue("high", "options_paper", "Options paper shadow payload is stale for the trade date.")

    options_lab_run = options.get("options_lab") or {}
    if options_lab_run:
        if not options_lab_run.get("ok"):
            add_issue("high", "options_lab", f"Scheduled options lab failed today: {options_lab_run.get('stderr') or options_lab_run.get('reason')}")
        elif options_lab_run.get("stale_report"):
            add_issue("high", "options_lab", "Scheduled options lab payload is stale for the trade date.")

    paper_generated = paper.get("generated_at")
    if paper and not _is_fresh_ts(paper_generated, trade_date):
        add_issue("medium", "equity_paper_file", f"paper_shadow_latest.json is stale: {paper_generated}")

    paper_options_generated = paper_options.get("generated_at")
    if paper_options and not _is_fresh_ts(paper_options_generated, trade_date):
        add_issue("medium", "options_paper_file", f"paper_shadow_options_latest.json is stale: {paper_options_generated}")

    options_top = paper_options.get("top_candidate") or {}
    if options_top and options_top.get("decision") != "BUY":
        add_improvement("medium", "Explain options near-misses better", f"Current top options candidate is HOLD with score {options_top.get('score')}. Add gate-by-gate miss diagnostics so near-buy setups are easier to tune.")

    opt_rec = options_lab.get("recommendation") or {}
    if opt_rec:
        tested = int(opt_rec.get("tested_variants", 0) or 0)
        if tested < 100:
            add_improvement("medium", "Run deeper options sweeps", f"Latest options lab tested {tested} variants. Consider a deeper weekend sweep for better parameter separation.")
        if float(opt_rec.get("improvement_return_pct", 0) or 0) <= 0:
            add_improvement("medium", "Improve options ranking/search separation", "Current options lab did not beat baseline. Expand diversity across expiries/strikes and improve ranking metrics beyond simple return/score.")

    cron_snapshots = {
        "daily_ops_supervisor_cron": _tail(REPORTS / "daily_ops_supervisor_cron.log"),
        "options_research_supervisor_cron": _tail(REPORTS / "options_research_supervisor_cron.log"),
        "scorecard_cron": _tail(REPORTS / "scorecard_cron.log"),
    }

    daily_iteration = {
        "equity": (ops.get("iteration_plan") or {}),
        "options": (options.get("iteration_plan") or {}),
    }

    summary = {
        "generated_at": now.isoformat(),
        "trade_date": trade_date,
        "market_open": market_open,
        "paths": {
            "scorecard": str(scorecard_path),
            "daily_ops": str(ops_path),
            "options_supervisor": str(options_path),
            "paper_shadow": str(paper_path),
            "paper_shadow_options": str(paper_options_path),
            "equity_lab": str(equity_lab_path) if equity_lab_path else None,
            "options_lab": str(options_lab_path) if options_lab_path else None,
        },
        "issues": issues,
        "improvement_areas": improvements,
        "snapshots": {
            "scorecard": {
                "orders": scorecard.get("orders"),
                "trades": scorecard.get("trades"),
                "verdict": scorecard.get("verdict"),
            },
            "daily_ops": {
                "strategy_ok": strategy.get("ok"),
                "strategy_stale": strategy.get("stale_report"),
                "paper_ok": paper_trader.get("paper_executed"),
                "paper_reason": paper_trader.get("reason"),
            },
            "options_supervisor": {
                "fetch_ok": (options.get("fetch") or {}).get("ok"),
                "paper_ok": options_paper.get("ok"),
                "paper_stale": options_paper.get("stale_report"),
                "lab_ok": options_lab_run.get("ok"),
                "lab_stale": options_lab_run.get("stale_report"),
            },
        },
        "daily_iteration": daily_iteration,
        "log_tails": cron_snapshots,
    }

    out_json = REPORTS / f"daily_improvement_audit_{trade_date}.json"
    out_md = REPORTS / f"daily_improvement_audit_{trade_date}.md"
    out_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    lines = [
        f"# Daily Improvement Audit, {trade_date}",
        "",
        f"- Market open: **{market_open}**",
        f"- Issues found: **{len(issues)}**",
        f"- Improvement areas: **{len(improvements)}**",
        "",
        "## Issues",
    ]
    if issues:
        for row in issues:
            lines.append(f"- [{row['severity']}] **{row['area']}**: {row['detail']}")
    else:
        lines.append("- None")

    lines += ["", "## Improvement areas"]
    if improvements:
        for row in improvements:
            lines.append(f"- [{row['priority']}] **{row['title']}**: {row['detail']}")
    else:
        lines.append("- None")

    lines += ["", "## Daily iteration plans"]
    for asset, plan in daily_iteration.items():
        lines.append("")
        lines.append(f"### {asset.capitalize()}")
        items = plan.get("items") or []
        if items:
            for item in items:
                lines.append(f"- [{item.get('priority')}] **{item.get('focus')}**: {item.get('detail')}")
        else:
            lines.append("- None")

    out_md.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2))
    print(f"Saved: {out_json}")
    print(f"Saved: {out_md}")


if __name__ == "__main__":
    main()
