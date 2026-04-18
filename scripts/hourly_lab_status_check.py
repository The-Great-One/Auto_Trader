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


def now_iso() -> str:
    return datetime.now().astimezone().isoformat()


def load_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except Exception:
        return None


def latest_strategy_report() -> tuple[str | None, dict[str, Any] | None]:
    candidates = sorted(REPORTS_DIR.glob("strategy_lab*.json"))
    if not candidates:
        return None, None
    path = candidates[-1]
    return path.name, load_json(path)


def running_lab_processes() -> list[str]:
    try:
        proc = subprocess.run(
            ["pgrep", "-fl", "scripts/weekly_strategy_lab.py"],
            capture_output=True,
            text=True,
            timeout=5,
        )
    except Exception:
        return []
    if proc.returncode not in {0, 1}:
        return []
    return [line.strip() for line in (proc.stdout or "").splitlines() if line.strip()]


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
    report_name, report_payload = latest_strategy_report()
    recommendation = (report_payload or {}).get("recommendation") or {}
    best = recommendation.get("best") or {}
    processes = running_lab_processes()
    age_min = stale_minutes(status)
    status_running = str(status.get("status") or "").lower() == "running"
    stale_running = bool(status_running and not processes and age_min is not None and age_min >= 30)

    summary = {
        "generated_at": now_iso(),
        "lab_status_path": str(STATUS_PATH),
        "status": status,
        "status_age_minutes": age_min,
        "running_processes": processes,
        "process_running": bool(processes),
        "stale_running_status": stale_running,
        "latest_report": {
            "name": report_name,
            "generated_at": recommendation.get("generated_at"),
            "tested_variants": recommendation.get("tested_variants"),
            "batch": recommendation.get("batch"),
            "best_name": best.get("name"),
            "best_return_pct": best.get("total_return_pct"),
            "best_score": best.get("selection_score"),
            "best_drawdown_pct": best.get("max_drawdown_pct"),
        },
    }

    if stale_running:
        summary["summary"] = "Lab status is stale, marked running but no weekly_strategy_lab.py process is active."
    elif processes:
        summary["summary"] = f"Lab process active, {len(processes)} matching process(es)."
    elif report_name:
        summary["summary"] = f"No active lab process, latest completed report is {report_name}."
    else:
        summary["summary"] = "No active lab process and no strategy lab report found."
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
