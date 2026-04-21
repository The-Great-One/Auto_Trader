#!/usr/bin/env python3
"""Nightly cleanup: review scripts for dead code, unused imports, and stale files."""
from __future__ import annotations

import ast
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
REPORTS_DIR = ROOT / "reports"
REPORT_PATH = REPORTS_DIR / "nightly_cleanup_latest.json"


def _python_files() -> list[Path]:
    files = []
    for p in ROOT.rglob("*.py"):
        parts = p.relative_to(ROOT).parts
        if any(x in parts for x in ("venv", "__pycache__", ".git", "node_modules", "Deprecated")):
            continue
        files.append(p)
    return sorted(files)


def _check_unused_imports(path: Path) -> list[str]:
    """Simple check for obviously unused imports."""
    issues = []
    try:
        source = path.read_text(errors="replace")
        tree = ast.parse(source)
    except Exception:
        return issues

    imported_names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                name = alias.asname or alias.name.split(".")[0]
                imported_names.add(name)
        elif isinstance(node, ast.ImportFrom):
            for alias in (node.names or []):
                raw = getattr(alias, 'name', alias) if not isinstance(alias, str) else alias
                asname = getattr(alias, 'asname', None)
                name = asname or (raw.split(".")[0] if isinstance(raw, str) else str(raw).split(".")[0])
                imported_names.add(name)

    # Check which imports are actually used
    for name in sorted(imported_names):
        if name.startswith("_"):
            continue
        # Count occurrences beyond the import line itself
        pattern = rf'\b{re.escape(name)}\b'
        matches = re.findall(pattern, source)
        if len(matches) <= 1:  # only the import line
            issues.append(f"Unused import: {name}")

    return issues


def _check_large_files(limit_mb: float = 1.0) -> list[dict]:
    """Find unusually large files in the repo."""
    large = []
    for p in ROOT.rglob("*"):
        if any(x in p.relative_to(ROOT).parts for x in ("venv", ".git", "__pycache__", "node_modules", "Deprecated")):
            continue
        if p.is_file():
            size_mb = p.stat().st_size / (1024 * 1024)
            if size_mb > limit_mb:
                large.append({"file": str(p.relative_to(ROOT)), "size_mb": round(size_mb, 2)})
    return large


def _check_stale_reports(days: int = 14) -> list[dict]:
    """Find report files older than N days."""
    stale = []
    cutoff = datetime.now().timestamp() - days * 86400
    for p in REPORTS_DIR.glob("*"):
        if p.is_file() and p.stat().st_mtime < cutoff:
            age_days = (datetime.now().timestamp() - p.stat().st_mtime) / 86400
            stale.append({"file": p.name, "age_days": round(age_days, 1)})
    return stale


def _check_hardcoded_secrets() -> list[dict]:
    """Check for hardcoded IPs, key paths, or API tokens."""
    patterns = [
        (r'\b(?!127\.0\.0\.1|0\.0\.0\.0)\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b', "hardcoded IP address"),
        (r'(api_key|apikey|secret_key|token)\s*=\s*["\'][a-zA-Z0-9]{10,}', "hardcoded secret/token"),
        (r'ssh-key-\d{4}-\d{2}-\d{2}(?:\.key)?', "hardcoded SSH key reference"),
    ]
    # Skip files that use env var fallbacks (os.getenv, os.path.expanduser)
    skip_if_env = ["os.getenv", "expanduser"]
    issues = []
    for path in _python_files():
        try:
            source = path.read_text(errors="replace")
        except Exception:
            continue
        for pattern, desc in patterns:
            for match in re.finditer(pattern, source):
                # Get the line containing the match
                line_start = source.rfind('\n', 0, match.start()) + 1
                line_end = source.find('\n', match.end())
                line = source[line_start:line_end].strip()
                # Skip if the line uses env var fallbacks
                if any(skip in line for skip in skip_if_env):
                    continue
                issues.append({"file": str(path.relative_to(ROOT)), "issue": desc, "line": line[:120]})
    return issues


def _git_status() -> dict[str, Any]:
    """Quick git working-tree status."""
    try:
        result = subprocess.run(
            ["git", "status", "--short"],
            capture_output=True, text=True, cwd=ROOT, timeout=10,
        )
        dirty = [line.strip() for line in result.stdout.strip().splitlines() if line.strip()]
        return {"dirty_files": len(dirty), "files": dirty[:10]}
    except Exception:
        return {"dirty_files": -1, "files": []}


def run_cleanup() -> dict[str, Any]:
    print("Running nightly cleanup review...")
    start = datetime.now()

    # 1. Unused imports
    import_issues = {}
    for path in _python_files():
        issues = _check_unused_imports(path)
        if issues:
            import_issues[str(path.relative_to(ROOT))] = issues

    # 2. Large files
    large_files = _check_large_files()

    # 3. Stale reports
    stale_reports = _check_stale_reports()

    # 4. Hardcoded secrets check
    secret_issues = _check_hardcoded_secrets()

    # 5. Git status
    git_info = _git_status()

    result = {
        "generated_at": start.isoformat(),
        "duration_seconds": (datetime.now() - start).total_seconds(),
        "unused_imports": import_issues,
        "large_files": large_files,
        "stale_reports": stale_reports,
        "secret_leaks": secret_issues,
        "git_status": git_info,
        "summary": {
            "files_with_unused_imports": len(import_issues),
            "large_files": len(large_files),
            "stale_reports": len(stale_reports),
            "secret_leaks": len(secret_issues),
            "dirty_files": git_info.get("dirty_files", -1),
        },
    }

    REPORTS_DIR.mkdir(exist_ok=True)
    REPORT_PATH.write_text(json.dumps(result, indent=2))
    print(f"Cleanup review done in {result['duration_seconds']:.1f}s")
    print(f"Summary: {json.dumps(result['summary'], indent=2)}")
    return result


if __name__ == "__main__":
    raise SystemExit(0 if run_cleanup() else 1)