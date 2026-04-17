#!/usr/bin/env python3
"""Weekday supervisor for NIFTY options research automation.

Runs the research-only options data fetch, then refreshes the paper-shadow
snapshot. Writes a daily summary report so cron runs are easy to inspect.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import pandas_market_calendars as mcal

ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / "reports"
SCRIPTS = ROOT / "scripts"
PYTHON_BIN = os.getenv("AT_PYTHON_BIN", sys.executable or "/home/ubuntu/Auto_Trader/venv/bin/python")
REPORTS.mkdir(exist_ok=True)


def ist_now() -> datetime:
    return datetime.now()


def is_market_open_today() -> tuple[bool, str]:
    nse = mcal.get_calendar("NSE")
    today = ist_now().date()
    valid = nse.valid_days(start_date=str(today), end_date=str(today))
    open_today = len(valid) > 0
    return open_today, str(today)


def _extract_json_payload(text: str):
    text = (text or "").strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except Exception:
        pass
    start = text.find("{")
    if start < 0:
        return None
    depth = 0
    for idx in range(start, len(text)):
        ch = text[idx]
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                try:
                    return json.loads(text[start : idx + 1])
                except Exception:
                    return None
    return None



def _run_json_script(script_name: str) -> dict:
    cmd = [PYTHON_BIN, str(SCRIPTS / script_name)]
    proc = subprocess.run(cmd, cwd=str(ROOT), capture_output=True, text=True)
    payload = _extract_json_payload(proc.stdout)
    return {
        "ok": proc.returncode == 0,
        "returncode": proc.returncode,
        "payload": payload,
        "stdout": proc.stdout[-2000:],
        "stderr": proc.stderr[-2000:],
    }


def _date_matches_trade_date(value: str | None, trade_date: str) -> bool:
    return str(value or "").startswith(trade_date)



def _mark_stale(run_result: dict, trade_date: str, payload_path: list[str] | None = None):
    payload = run_result.get("payload") or {}
    generated_at = payload
    for key in payload_path or []:
        generated_at = (generated_at or {}).get(key, {})
    if isinstance(generated_at, dict):
        generated_at = generated_at.get("generated_at")
    run_result["generated_at"] = generated_at
    run_result["stale_report"] = False if generated_at is None else (not _date_matches_trade_date(generated_at, trade_date))
    run_result["stale_reason"] = None if not run_result["stale_report"] else "payload_not_from_trade_date"
    return run_result



def _render_markdown(summary: dict) -> str:
    fetch = summary.get("fetch", {})
    paper = summary.get("paper_shadow", {})
    lab = summary.get("options_lab", {})
    fetch_payload = fetch.get("payload") or {}
    paper_payload = paper.get("payload") or {}
    lab_payload = lab.get("payload") or {}
    options_shadow = paper_payload.get("options_shadow") or {}
    top = options_shadow.get("top_candidate") or {}
    rec = lab_payload.get("recommendation") or {}
    best = rec.get("best") or {}

    lines = [
        f"# Options Research Supervisor, {summary['trade_date']}",
        "",
        f"- Market open: **{summary['market_open']}** (NSE calendar)",
        f"- Fetch ran: **{fetch.get('ok', False)}**",
        f"- Paper shadow ran: **{paper.get('ok', False)}**",
        f"- Paper shadow stale: **{paper.get('stale_report', False)}**",
        f"- Options lab ran: **{lab.get('ok', False)}**",
        f"- Options lab stale: **{lab.get('stale_report', False)}**",
    ]

    if summary["market_open"]:
        lines += [
            f"- Contracts selected: **{fetch_payload.get('contracts_selected')}**",
            f"- Contracts fetched: **{fetch_payload.get('contracts_fetched')}**",
            f"- Contracts failed: **{len(fetch_payload.get('contracts_failed') or [])}**",
            f"- Options evaluated: **{options_shadow.get('evaluated')}** / **{options_shadow.get('universe_size')}**",
            f"- BUY candidates now: **{len(options_shadow.get('buy_candidates') or [])}**",
            f"- Top candidate: **{(top.get('symbol') or 'none')}**",
            f"- Top decision: **{(top.get('decision') or 'n/a')}**",
            f"- Top score: **{top.get('score')}**",
            f"- Lab variants tested: **{rec.get('tested_variants')}**",
            f"- Lab best: **{best.get('name') or 'n/a'}**",
            f"- Lab best return %: **{best.get('total_return_pct')}**",
            f"- Lab improvement % vs baseline: **{rec.get('improvement_return_pct')}**",
        ]
    else:
        lines += ["- Reason: **market_closed**"]

    if fetch.get("stderr"):
        lines += ["", "## Fetch stderr", "```", str(fetch["stderr"]), "```"]
    if paper.get("stderr"):
        lines += ["", "## Paper shadow stderr", "```", str(paper["stderr"]), "```"]
    if lab.get("stderr"):
        lines += ["", "## Options lab stderr", "```", str(lab["stderr"]), "```"]

    return "\n".join(lines) + "\n"


def main():
    now = ist_now()
    market_open, trade_date = is_market_open_today()

    summary = {
        "generated_at": now.isoformat(),
        "trade_date": trade_date,
        "market_open": market_open,
        "calendar": "NSE",
        "fetch": {
            "ok": False,
            "returncode": None,
            "payload": None,
            "stdout": "",
            "stderr": "",
            "reason": "market_closed" if not market_open else None,
        },
        "paper_shadow": {
            "ok": False,
            "returncode": None,
            "payload": None,
            "stdout": "",
            "stderr": "",
            "reason": "market_closed" if not market_open else None,
        },
        "options_lab": {
            "ok": False,
            "returncode": None,
            "payload": None,
            "stdout": "",
            "stderr": "",
            "reason": "market_closed" if not market_open else None,
        },
    }

    if market_open:
        fetch = _run_json_script("fetch_nifty_options_data.py")
        fetch["reason"] = None if fetch.get("ok") else f"failed_rc_{fetch.get('returncode')}"
        summary["fetch"] = fetch

        if fetch.get("ok"):
            paper = _run_json_script("paper_shadow.py")
            paper["reason"] = None if paper.get("ok") else f"failed_rc_{paper.get('returncode')}"
            summary["paper_shadow"] = _mark_stale(paper, trade_date, ["options_shadow"])

            lab = _run_json_script("options_strategy_lab.py")
            lab["reason"] = None if lab.get("ok") else f"failed_rc_{lab.get('returncode')}"
            summary["options_lab"] = _mark_stale(lab, trade_date)
        else:
            summary["paper_shadow"]["reason"] = "skipped_due_to_fetch_failure"
            summary["options_lab"]["reason"] = "skipped_due_to_fetch_failure"

    out_json = REPORTS / f"options_research_supervisor_{trade_date}.json"
    out_md = REPORTS / f"options_research_supervisor_{trade_date}.md"
    out_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    out_md.write_text(_render_markdown(summary), encoding="utf-8")

    print(json.dumps(summary, indent=2))
    print(f"Saved: {out_json}")
    print(f"Saved: {out_md}")


if __name__ == "__main__":
    main()
