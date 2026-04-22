#!/usr/bin/env python3
"""Scan auto_trade logs for symbols that repeatedly fail and add them to the exclusion list."""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from Auto_Trader.Build_Master import load_exclusion_list, add_excluded_symbol

SSH_KEY = os.getenv("AT_SERVER_KEY", os.path.expanduser("~/.openclaw/credentials/oracle_ssh_key"))
SERVER_HOST = os.getenv("AT_SERVER_HOST", os.getenv("AT_ORACLE", ""))
SERVER_REPO = os.getenv("AT_SERVER_REPO", "/home/ubuntu/Auto_Trader")
FAILURE_PATTERNS = [
    r"Failed.*fetch.*?(\b[A-Z]{3,}\b)",
    r"error.*download.*?(\b[A-Z]{3,}\b)",
    r"empty.*dataframe.*?(\b[A-Z]{3,}\b)",
    r"No.*data.*?(\b[A-Z]{3,}\b)\.feather",
    r"missing_or_empty.*?(\b[A-Z]{3,}\b)",
    r"skipped.*?(\b[A-Z]{3,}\b)",
]
THRESHOLD = 3  # how many failure mentions before excluding


def scan_server_logs() -> Counter:
    """SSH to the server and count symbol failures from recent logs."""
    if not SERVER_HOST:
        print("AT_SERVER_HOST not set, cannot scan remote logs")
        return Counter()

    cmd = [
        "ssh", "-i", SSH_KEY,
        "-o", "StrictHostKeyChecking=no",
        "-o", "ConnectTimeout=10",
        SERVER_HOST,
        f"journalctl -u auto_trade.service --since '7 days ago' --no-pager 2>/dev/null; "
        f"cat {SERVER_REPO}/reports/scorecard_cron.log 2>/dev/null; "
        f"cat {SERVER_REPO}/reports/daily_ops_supervisor_cron.log 2>/dev/null",
    ]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        text = proc.stdout or ""
    except Exception as exc:
        print(f"SSH scan failed: {exc}")
        return Counter()

    counter: Counter = Counter()
    for pattern in FAILURE_PATTERNS:
        for match in re.finditer(pattern, text, flags=re.IGNORECASE):
            sym = match.group(1).upper()
            if len(sym) >= 3 and sym.isalpha():
                counter[sym] += 1
    return counter


def scan_local_reports() -> Counter:
    """Scan local report files for skipped/failed symbols."""
    counter: Counter = Counter()
    reports_dir = ROOT / "reports"
    for path in reports_dir.glob("*.json"):
        try:
            data = json.loads(path.read_text())
        except Exception:
            continue
        if isinstance(data, dict):
            skipped = data.get("skipped_symbols") or data.get("data_context", {}).get("skipped_symbols") or {}
            if isinstance(skipped, dict):
                for sym, reason in skipped.items():
                    counter[str(sym).upper()] += 1
    return counter


def main() -> int:
    already_excluded = load_exclusion_list()
    print(f"Currently excluded: {sorted(already_excluded) or 'none'}")

    server_failures = scan_server_logs()
    local_failures = scan_local_reports()
    combined: Counter = server_failures + local_failures

    if not combined:
        print("No symbol failures found")
        return 0

    print(f"Failure counts: {dict(combined.most_common(20))}")

    new_exclusions = []
    for sym, count in combined.most_common():
        if count >= THRESHOLD and sym not in already_excluded:
            add_excluded_symbol(sym, reason=f"auto_excluded_{count}_failures")
            new_exclusions.append(sym)
            print(f"  EXCLUDED {sym} ({count} failures)")

    if new_exclusions:
        print(f"\nNewly excluded: {new_exclusions}")
    else:
        print("\nNo new symbols to exclude (all already excluded or below threshold)")

    # Save exclusion list to server too
    if SERVER_HOST and new_exclusions:
        exclusion_path = ROOT / "intermediary_files" / "symbol_exclusions.json"
        remote_path = f"{SERVER_REPO}/intermediary_files/symbol_exclusions.json"
        try:
            subprocess.run(
                ["scp", "-i", SSH_KEY, "-o", "StrictHostKeyChecking=no", str(exclusion_path), f"{SERVER_HOST}:{remote_path}"],
                capture_output=True, text=True, timeout=15,
            )
            print(f"Synced exclusion list to server")
        except Exception as exc:
            print(f"Failed to sync exclusion list to server: {exc}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())