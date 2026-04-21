#!/usr/bin/env python3
from __future__ import annotations

import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
REPORTS_DIR = ROOT / "reports"
STATUS_PATH = ROOT / "intermediary_files" / "lab_status" / "weekly_strategy_lab_status.json"
OUT_PATH = REPORTS_DIR / "hourly_lab_status_latest.json"
HISTORY_PATH = REPORTS_DIR / "hourly_lab_status_history.jsonl"
LAB_PROCESS_PATTERNS = [
    "scripts/weekly_strategy_lab.py",
    "scripts/volatility_sizing_lab.py",
    "scripts/sizing_exit_sweep.py",
    "scripts/regime_filter_lab.py",
    "scripts/focused_cluster_lab.py",
]
STATUS_REPORT_FILES = [
    "sizing_exit_sweep_latest.json",
    "volatility_sizing_lab_latest.json",
    "regime_filter_lab_latest.json",
    "focused_cluster_lab_latest.json",
]


def now_iso() -> str:
    return datetime.now().astimezone().isoformat()


def load_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except Exception:
        return None


def parse_dt(raw: Any) -> datetime | None:
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(str(raw))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.now().astimezone().tzinfo)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def latest_strategy_report() -> tuple[str | None, dict[str, Any] | None]:
    candidates = sorted(REPORTS_DIR.glob("strategy_lab*.json"))
    if not candidates:
        return None, None
    path = candidates[-1]
    return path.name, load_json(path)


def collect_completed_reports() -> list[dict[str, Any]]:
    reports: list[dict[str, Any]] = []

    report_name, report_payload = latest_strategy_report()
    recommendation = (report_payload or {}).get("recommendation") or {}
    best = recommendation.get("best") or {}
    if report_name and recommendation:
        reports.append(
            {
                "name": report_name,
                "source": "strategy_lab",
                "generated_at": recommendation.get("generated_at"),
                "generated_at_dt": parse_dt(recommendation.get("generated_at")),
                "tested_variants": recommendation.get("tested_variants"),
                "batch": recommendation.get("batch"),
                "best_name": best.get("name"),
                "best_return_pct": best.get("total_return_pct"),
                "best_score": best.get("selection_score"),
                "best_drawdown_pct": best.get("max_drawdown_pct"),
            }
        )

    for file_name in STATUS_REPORT_FILES:
        payload = load_json(REPORTS_DIR / file_name) or {}
        if not payload:
            continue
        reports.append(
            {
                "name": file_name,
                "source": file_name.removesuffix("_latest.json"),
                "generated_at": payload.get("generated_at"),
                "generated_at_dt": parse_dt(payload.get("generated_at")),
                "tested_variants": payload.get("variants_done") or payload.get("variants_total"),
                "batch": None,
                "best_name": payload.get("best_variant"),
                "best_return_pct": payload.get("best_return_pct"),
                "best_score": payload.get("best_score"),
                "best_drawdown_pct": payload.get("best_drawdown_pct"),
            }
        )

    return reports


def running_lab_processes() -> list[str]:
    found: list[str] = []
    for pattern in LAB_PROCESS_PATTERNS:
        try:
            proc = subprocess.run(
                ["pgrep", "-fl", pattern],
                capture_output=True,
                text=True,
                timeout=5,
            )
        except Exception:
            continue
        if proc.returncode not in {0, 1}:
            continue
        found.extend([line.strip() for line in (proc.stdout or "").splitlines() if line.strip()])
    return sorted(set(found))


def stale_minutes(status: dict[str, Any] | None) -> float | None:
    if not status:
        return None
    raw = status.get("updated_at")
    if not raw:
        return None
    try:
        local_tz = datetime.now().astimezone().tzinfo
        updated = datetime.fromisoformat(str(raw))
        if updated.tzinfo is None:
            updated = updated.replace(tzinfo=local_tz)
        delta = datetime.now(timezone.utc) - updated.astimezone(timezone.utc)
        return round(delta.total_seconds() / 60.0, 1)
    except Exception:
        return None


def build_summary() -> dict[str, Any]:
    status = load_json(STATUS_PATH) or {}
    completed_reports = collect_completed_reports()
    latest_completed = None
    if completed_reports:
        latest_completed = max(completed_reports, key=lambda row: row.get("generated_at_dt") or datetime.min.replace(tzinfo=timezone.utc))
    best_completed = None
    if completed_reports:
        best_completed = max(completed_reports, key=lambda row: float(row.get("best_return_pct") if row.get("best_return_pct") is not None else float("-inf")))

    processes = running_lab_processes()
    age_min = stale_minutes(status)
    status_running = str(status.get("status") or "").lower() == "running"
    stale_running = bool(status_running and not processes and age_min is not None and age_min >= 30)

    def _clean_report(report: dict[str, Any] | None) -> dict[str, Any] | None:
        if not report:
            return None
        return {k: v for k, v in report.items() if k != "generated_at_dt"}

    summary = {
        "generated_at": now_iso(),
        "lab_status_path": str(STATUS_PATH),
        "status": status,
        "status_age_minutes": age_min,
        "running_processes": processes,
        "process_running": bool(processes),
        "stale_running_status": stale_running,
        "latest_report": _clean_report(latest_completed),
        "best_report": _clean_report(best_completed),
        "completed_reports": [_clean_report(row) for row in sorted(completed_reports, key=lambda row: row.get("generated_at_dt") or datetime.min.replace(tzinfo=timezone.utc), reverse=True)],
    }

    if stale_running:
        summary["summary"] = "Lab status is stale, marked running but no known lab process is active."
    elif processes:
        summary["summary"] = f"Lab process active, {len(processes)} matching process(es)."
    elif latest_completed:
        summary["summary"] = f"No active lab process. Latest completed lab is {latest_completed.get('name')} and best known completed lab is {best_completed.get('best_name')} at {best_completed.get('best_return_pct')}%."
    else:
        summary["summary"] = "No active lab process and no completed lab report found."
    return summary


def main() -> int:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    summary = build_summary()
    OUT_PATH.write_text(json.dumps(summary, indent=2))
    with HISTORY_PATH.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(summary) + "\n")
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
