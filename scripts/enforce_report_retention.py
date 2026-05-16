#!/usr/bin/env python3
"""Enforce a small top-level reports/ working set.

Moves top-level report files older than N days out of reports/ so latest-report
readers do not accidentally select stale artifacts. This intentionally does not
walk subdirectories.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REPORTS_DIR = ROOT / "reports"
DEFAULT_ARCHIVE_DIR = Path(os.getenv("AT_REPORT_ARCHIVE_DIR", str(Path.home() / "autotrader_report_retention_archive")))
SUMMARY_NAME = "report_retention_latest.json"

KEEP_EXACT = {
    SUMMARY_NAME,
    ".gitkeep",
}

STAMP_RE = re.compile(r"(?P<ymd>20\d{2}-\d{2}-\d{2}|20\d{6})(?:[_-]?(?P<hms>\d{6}))?")


def report_timestamp(path: Path) -> float:
    """Return embedded report timestamp when present, otherwise filesystem mtime."""
    matches = list(STAMP_RE.finditer(path.name))
    parsed: list[datetime] = []
    for match in matches:
        ymd = match.group("ymd")
        hms = match.group("hms")
        for fmt in (("%Y-%m-%d" if "-" in ymd else "%Y%m%d") + ("_%H%M%S" if hms else ""),):
            raw = ymd + (("_" + hms) if hms else "")
            try:
                parsed.append(datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc))
            except ValueError:
                pass
    if parsed:
        return max(parsed).timestamp()
    return path.stat().st_mtime


def enforce_retention(days: float, archive_dir: Path, apply: bool) -> dict:
    now = datetime.now(timezone.utc)
    cutoff_ts = now.timestamp() - days * 86400
    REPORTS_DIR.mkdir(exist_ok=True)
    archive_dir.mkdir(parents=True, exist_ok=True)

    candidates = []
    kept = []
    for path in sorted(REPORTS_DIR.iterdir(), key=lambda p: p.name):
        if not path.is_file():
            continue
        stat = path.stat()
        source_ts = report_timestamp(path)
        age_days = (now.timestamp() - source_ts) / 86400
        entry = {
            "name": path.name,
            "mtime": datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat(),
            "report_time": datetime.fromtimestamp(source_ts, timezone.utc).isoformat(),
            "age_days": round(age_days, 3),
            "size_bytes": stat.st_size,
        }
        if path.name in KEEP_EXACT or source_ts >= cutoff_ts:
            kept.append(entry)
        else:
            candidates.append(entry)

    moved = []
    if apply:
        batch_dir = archive_dir / now.strftime("%Y%m%dT%H%M%SZ")
        batch_dir.mkdir(parents=True, exist_ok=True)
        for entry in candidates:
            src = REPORTS_DIR / entry["name"]
            if not src.exists():
                continue
            dst = batch_dir / src.name
            if dst.exists():
                suffix = 1
                while True:
                    alt = batch_dir / f"{src.stem}.{suffix}{src.suffix}"
                    if not alt.exists():
                        dst = alt
                        break
                    suffix += 1
            shutil.move(str(src), str(dst))
            moved.append({"from": str(src.relative_to(ROOT)), "to": str(dst)})

    result = {
        "generated_at": now.isoformat(),
        "retention_days": days,
        "reports_dir": str(REPORTS_DIR),
        "archive_dir": str(archive_dir),
        "applied": apply,
        "planned_move_count": len(candidates),
        "moved_count": len(moved),
        "kept_count": len(kept),
        "moved": moved[:2000],
        "kept_sample": kept[:100],
    }
    (REPORTS_DIR / SUMMARY_NAME).write_text(json.dumps(result, indent=2), encoding="utf-8")
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Move top-level reports older than N days out of reports/.")
    parser.add_argument("--days", type=float, default=3.0, help="Retention window in days for top-level reports/ files")
    parser.add_argument("--archive-dir", type=Path, default=DEFAULT_ARCHIVE_DIR, help="Archive directory outside reports/")
    parser.add_argument("--apply", action="store_true", help="Actually move files; omit for dry run")
    args = parser.parse_args()

    result = enforce_retention(args.days, args.archive_dir, args.apply)
    print(json.dumps({
        "applied": result["applied"],
        "retention_days": result["retention_days"],
        "planned_move_count": result["planned_move_count"],
        "moved_count": result["moved_count"],
        "kept_count": result["kept_count"],
        "summary": str(REPORTS_DIR / SUMMARY_NAME),
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
