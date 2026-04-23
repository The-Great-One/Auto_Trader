#!/usr/bin/env python3
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_TOOL = Path.home() / '.openclaw' / 'workspace' / 'tools' / 'telegram_channel_learning.py'
PYTHON = ROOT / 'venv' / 'bin' / 'python'


def main() -> int:
    if not WORKSPACE_TOOL.exists():
        raise FileNotFoundError(f'missing {WORKSPACE_TOOL}')
    python = str(PYTHON if PYTHON.exists() else Path(sys.executable))
    proc = subprocess.run([python, str(WORKSPACE_TOOL)], cwd=str(ROOT), text=True)
    return int(proc.returncode)


if __name__ == '__main__':
    raise SystemExit(main())
