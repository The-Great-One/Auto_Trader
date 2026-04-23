#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import shutil
from collections import defaultdict
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REPORTS_DIR = ROOT / "reports"
ARCHIVE_DIR = REPORTS_DIR / "archive"
SUMMARY_PATH = REPORTS_DIR / "report_clutter_cleanup_latest.json"

TIMESTAMPED_RE = re.compile(r"^(?P<prefix>.+?)_(?P<stamp>\d{8}_\d{6})$")
DATED_RE = re.compile(r"^(?P<prefix>.+?)_(?P<stamp>\d{4}-\d{2}-\d{2})$")

SKIP_STEMS = {
    "paper_shadow_latest",
    "paper_shadow_live_latest",
    "paper_shadow_options_latest",
    "oracle_paper_shadow_latest",
    "portfolio_tracker_latest",
    "regime_filter_lab_latest",
    "sizing_exit_sweep_latest",
    "thirty_cagr_hunt_latest",
    "volatility_sizing_lab_latest",
}
SKIP_SUFFIXES = (
    ".jsonl",
    ".latest.json",
    ".latest.md",
)
KEEP_PREFIXES = (
    "channel_learning_scores",
    "hourly_lab_status_history",
    "hourly_lab_status_latest",
    "paper_shadow_portfolio_state",
    "regime_filter_lab_history",
    "sizing_exit_sweep_history",
    "strategy_metrics_history",
    "telegram_trade_audit_latest",
    "thirty_cagr_hunt_checkpoint",
    "thirty_cagr_hunt_history",
    "volatility_sizing_lab_history",
    "weekly_strategy_supervisor",
)
LOG_KEEP_EXACT = {
    "daily_systems_check.log",
    "daily_systems_check_err.log",
    "nightly_cleanup_local.log",
    "perf_digest_daily.log",
    "perf_digest_weekly.log",
    "portfolio_intel_cron.log",
    "scorecard_cron.log",
    "telegram_dashboard.log",
    "telegram_paper_ledger.log",
    "telegram_paper_ledger_err.log",
    "weekly_strategy_lab.log",
}


def _keep_file(path: Path) -> bool:
    name = path.name
    stem = path.stem
    if path.is_dir():
        return True
    if name in LOG_KEEP_EXACT:
        return True
    if stem in SKIP_STEMS:
        return True
    if any(name.startswith(prefix) for prefix in KEEP_PREFIXES):
        return True
    if any(name.endswith(suffix) for suffix in SKIP_SUFFIXES):
        return True
    if ".latest." in name or name.endswith("_latest.json") or name.endswith("_latest.md"):
        return True
    if name.endswith(".log") and "_cron_" not in name and re.search(r"_\d{8}(?:_\d{6})?\.log$", name) is None:
        return True
    return False


def _family_key(path: Path) -> tuple[str, str] | None:
    stem = path.stem
    ts = TIMESTAMPED_RE.match(stem)
    if ts:
        return ("timestamped", ts.group("prefix"))
    dt_match = DATED_RE.match(stem)
    if dt_match:
        return ("dated", dt_match.group("prefix"))
    legacy_log = re.match(r"^(?P<prefix>.+?)_\d{8}_\d{6}\.legacy$", stem)
    if legacy_log:
        return ("timestamped", legacy_log.group("prefix"))
    return None


def _family_stamp(path: Path) -> datetime | None:
    stem = path.stem
    ts = TIMESTAMPED_RE.match(stem)
    if ts:
        return datetime.strptime(ts.group("stamp"), "%Y%m%d_%H%M%S")
    dt_match = DATED_RE.match(stem)
    if dt_match:
        return datetime.strptime(dt_match.group("stamp"), "%Y-%m-%d")
    legacy_log = re.match(r"^(?P<prefix>.+?)_(?P<stamp>\d{8}_\d{6})\.legacy$", stem)
    if legacy_log:
        return datetime.strptime(legacy_log.group("stamp"), "%Y%m%d_%H%M%S")
    return None


def plan_cleanup(retain_timestamped: int = 3, retain_dated: int = 7) -> dict:
    grouped: dict[tuple[str, str], dict[str, object]] = defaultdict(lambda: {"families": defaultdict(list), "order": {}})

    for path in REPORTS_DIR.iterdir():
        if _keep_file(path):
            continue
        key = _family_key(path)
        stamp = _family_stamp(path)
        if key is None or stamp is None:
            continue
        family = path.stem
        grouped[key]["families"][family].append(path)
        grouped[key]["order"][family] = stamp

    planned = []
    kept = []

    for (kind, prefix), payload in grouped.items():
        families: dict[str, list[Path]] = payload["families"]
        order: dict[str, datetime] = payload["order"]
        retain = retain_timestamped if kind == "timestamped" else retain_dated
        ranked = sorted(order.items(), key=lambda item: item[1], reverse=True)
        keep_families = {family for family, _ in ranked[:retain]}
        for family, stamp in ranked:
            files = sorted(families[family], key=lambda p: p.name)
            entry = {
                "prefix": prefix,
                "kind": kind,
                "family": family,
                "stamp": stamp.isoformat(),
                "files": [p.name for p in files],
            }
            if family in keep_families:
                kept.append(entry)
            else:
                planned.append(entry)

    return {
        "generated_at": datetime.now().isoformat(),
        "retain_timestamped": retain_timestamped,
        "retain_dated": retain_dated,
        "planned_archives": planned,
        "kept_families": kept,
    }


def apply_cleanup(plan: dict) -> dict:
    moved = []
    for entry in plan.get("planned_archives", []):
        stamp = datetime.fromisoformat(entry["stamp"])
        target_dir = ARCHIVE_DIR / stamp.strftime("%Y-%m") / entry["prefix"]
        target_dir.mkdir(parents=True, exist_ok=True)
        for name in entry["files"]:
            src = REPORTS_DIR / name
            if not src.exists():
                continue
            dst = target_dir / name
            shutil.move(str(src), str(dst))
            moved.append({"from": str(src.relative_to(ROOT)), "to": str(dst.relative_to(ROOT))})
    return {"moved": moved, "moved_count": len(moved)}


def main() -> int:
    parser = argparse.ArgumentParser(description="Archive old timestamped report clutter from reports/.")
    parser.add_argument("--apply", action="store_true", help="Move old clutter into reports/archive/")
    parser.add_argument("--retain-timestamped", type=int, default=3)
    parser.add_argument("--retain-dated", type=int, default=7)
    args = parser.parse_args()

    REPORTS_DIR.mkdir(exist_ok=True)
    plan = plan_cleanup(args.retain_timestamped, args.retain_dated)
    result = {
        **plan,
        "applied": bool(args.apply),
        "archive_root": str(ARCHIVE_DIR),
    }
    if args.apply:
        result.update(apply_cleanup(plan))
    else:
        result.update({"moved": [], "moved_count": 0})

    SUMMARY_PATH.write_text(json.dumps(result, indent=2))
    print(json.dumps({
        "planned_families": len(plan.get("planned_archives", [])),
        "moved_count": result.get("moved_count", 0),
        "summary_path": str(SUMMARY_PATH),
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
