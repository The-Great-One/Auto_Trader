#!/usr/bin/env python3
"""Compatibility launcher for lab scripts moved to Trader_Labs."""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

def resolve_labs_root() -> Path:
    candidates = []
    env_root = os.getenv("AT_TRADER_LABS_ROOT") or os.getenv("TRADER_LABS_ROOT")
    if env_root:
        candidates.append(Path(env_root).expanduser())
    candidates.extend([
        ROOT.parent / "Trader_Labs",
        Path.home() / "Trader_Labs",
        Path("/home/ubuntu/Trader_Labs"),
    ])
    for cand in candidates:
        if (cand / "scripts").exists():
            return cand
    raise SystemExit(
        "Trader_Labs repo not found. Set AT_TRADER_LABS_ROOT=/path/to/Trader_Labs "
        "or clone it next to Auto_Trader."
    )

def main(script_name: str | None = None) -> int:
    if script_name is None:
        if len(sys.argv) < 2:
            raise SystemExit("usage: run_trader_labs.py <script-name> [args...]")
        script_name = sys.argv[1]
        args = sys.argv[2:]
    else:
        args = sys.argv[1:]
    labs_root = resolve_labs_root()
    script = labs_root / "scripts" / script_name
    if not script.exists():
        raise SystemExit(f"Trader_Labs script not found: {script}")
    env = os.environ.copy()
    env.setdefault("AUTOTRADER_ROOT", str(ROOT))
    env.setdefault("AT_RESEARCH_MODE", "1")
    return subprocess.call([sys.executable, str(script), *args], cwd=str(labs_root), env=env)

if __name__ == "__main__":
    raise SystemExit(main())
