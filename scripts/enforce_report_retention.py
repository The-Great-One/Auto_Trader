#!/usr/bin/env python3
"""Enforce a small reports/ working set.

Moves report files older than N days out of reports/ so latest-report readers and
manual audits do not accidentally select stale artifacts. By default this scans
all files under reports/, including legacy reports/archive/ trees, and preserves
relative paths in an archive outside reports/.
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
        fmt = ("%Y-%m-%d" if "-" in ymd else "%Y%m%d") + ("_%H%M%S" if hms else "")
        raw = ymd + (("_" + hms) if hms else "")
        try:
            parsed.append(datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc))
        except ValueError:
            pass
    if parsed:
        return max(parsed).timestamp()
    return path.stat().st_mtime


def _iter_report_files(recursive: bool) -> list[Path]:
    if not REPORTS_DIR.exists():
        return []
    iterator = REPORTS_DIR.rglob("*") if recursive else REPORTS_DIR.iterdir()
    return sorted((p for p in iterator if p.is_file()), key=lambda p: str(p.relative_to(REPORTS_DIR)))


def _remove_empty_dirs() -> int:
    removed = 0
    for path in sorted((p for p in REPORTS_DIR.rglob("*") if p.is_dir()), key=lambda p: len(p.parts), reverse=True):
        try:
            path.rmdir()
            removed += 1
        except OSError:
            pass
    return removed


def enforce_retention(days: float, archive_dir: Path, apply: bool, recursive: bool = True) -> dict:
    now = datetime.now(timezone.utc)
    cutoff_ts = now.timestamp() - days * 86400
    REPORTS_DIR.mkdir(exist_ok=True)
    archive_dir.mkdir(parents=True, exist_ok=True)

    candidates = []
    kept = []
    for path in _iter_report_files(recursive=recursive):
        stat = path.stat()
        source_ts = report_timestamp(path)
        age_days = (now.timestamp() - source_ts) / 86400
        rel = path.relative_to(REPORTS_DIR)
        entry = {
            "name": path.name,
            "relative_path": str(rel),
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
    removed_empty_dirs = 0
    if apply:
        batch_dir = archive_dir / now.strftime("%Y%m%dT%H%M%SZ")
        batch_dir.mkdir(parents=True, exist_ok=True)
        for entry in candidates:
            src = REPORTS_DIR / entry["relative_path"]
            if not src.exists():
                continue
            dst = batch_dir / entry["relative_path"]
            dst.parent.mkdir(parents=True, exist_ok=True)
            if dst.exists():
                suffix = 1
                while True:
                    alt = dst.with_name(f"{dst.stem}.{suffix}{dst.suffix}")
                    if not alt.exists():
                        dst = alt
                        break
                    suffix += 1
            shutil.move(str(src), str(dst))
            moved.append({"from": str(src.relative_to(ROOT)), "to": str(dst)})
        removed_empty_dirs = _remove_empty_dirs()

    result = {
        "generated_at": now.isoformat(),
        "retention_days": days,
        "reports_dir": str(REPORTS_DIR),
        "archive_dir": str(archive_dir),
        "recursive": recursive,
        "applied": apply,
        "planned_move_count": len(candidates),
        "moved_count": len(moved),
        "kept_count": len(kept),
        "removed_empty_dirs": removed_empty_dirs,
        "moved": moved[:2000],
        "kept_sample": kept[:100],
    }
    (REPORTS_DIR / SUMMARY_NAME).write_text(json.dumps(result, indent=2), encoding="utf-8")
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Move report files older than N days out of reports/.")
    parser.add_argument("--days", type=float, default=3.0, help="Retention window in days for reports/ files")
    parser.add_argument("--archive-dir", type=Path, default=DEFAULT_ARCHIVE_DIR, help="Archive directory outside reports/")
    parser.add_argument("--apply", action="store_true", help="Actually move files; omit for dry run")
    parser.add_argument("--top-level-only", action="store_true", help="Only scan direct children of reports/")
    args = parser.parse_args()

    result = enforce_retention(args.days, args.archive_dir, args.apply, recursive=not args.top_level_only)
    print(json.dumps({
        "applied": result["applied"],
        "retention_days": result["retention_days"],
        "recursive": result["recursive"],
        "planned_move_count": result["planned_move_count"],
        "moved_count": result["moved_count"],
        "kept_count": result["kept_count"],
        "removed_empty_dirs": result["removed_empty_dirs"],
        "summary": str(REPORTS_DIR / SUMMARY_NAME),
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
