#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from Auto_Trader.StrongFundamentalsStockList import goodStocks  # noqa: E402

STATUS_DIR = ROOT / "intermediary_files" / "lab_status"
STATUS_DIR.mkdir(exist_ok=True)
STATUS_PATH = STATUS_DIR / "weekly_strategy_lab_status.json"


def write_status(**updates) -> None:
    current = {}
    if STATUS_PATH.exists():
        try:
            current = json.loads(STATUS_PATH.read_text())
        except Exception:
            current = {}
    current.update(updates)
    STATUS_PATH.write_text(json.dumps(current, indent=2))


def build_equity_symbol_list(limit: int | None = None) -> list[str]:
    df = goodStocks()
    if df is None or df.empty:
        raise RuntimeError("Strong fundamentals universe is empty")
    df["AssetClass"] = df["AssetClass"].astype(str).str.upper().str.strip()
    df["Symbol"] = df["Symbol"].astype(str).str.upper().str.strip()
    equities = df[df["AssetClass"] == "EQUITY"]["Symbol"].dropna().unique().tolist()
    if limit is not None:
        equities = equities[: max(1, int(limit))]
    return equities


def main() -> int:
    write_status(status="running", phase="building_full_equity_universe", message="preparing full RNN equity lab run", updated_at=datetime.now().isoformat(), current_symbol=None, current_variant=None, progress_pct=0.0, variants_done=0, symbols_loaded=0, symbols_index=0)
    limit_raw = os.getenv("AT_FULL_RNN_LIMIT", "").strip()
    limit = int(limit_raw) if limit_raw else None
    symbols = build_equity_symbol_list(limit=limit)
    write_status(status="running", phase="starting_weekly_lab", message="launching weekly lab on full equity universe", updated_at=datetime.now().isoformat(), universe_size=len(symbols), universe_symbols=symbols[:200], universe_symbols_truncated=len(symbols) > 200)

    env = os.environ.copy()
    env.setdefault("AT_LAB_RNN_ENABLED", "1")
    env.setdefault("AT_LAB_RNN_SEQ_LEN", "20")
    env.setdefault("AT_LAB_RNN_EPOCHS", "8")
    env.setdefault("AT_LAB_RNN_HIDDEN", "16")
    env.setdefault("AT_LAB_RNN_BUY_THRESHOLD", "0.50")
    env.setdefault("AT_LAB_RNN_SELL_THRESHOLD", "0.40")
    env.setdefault("AT_LAB_MAX_VARIANTS", "20")
    env["AT_LAB_SYMBOLS"] = ",".join(symbols)

    print(f"Running weekly_strategy_lab.py on {len(symbols)} equity symbols")
    cmd = [str(ROOT / "venv" / "bin" / "python"), str(ROOT / "scripts" / "weekly_strategy_lab.py")]
    proc = subprocess.run(cmd, cwd=str(ROOT), env=env)
    if proc.returncode != 0:
        write_status(status="failed", phase="failed", error=f"full equity wrapper exit code {proc.returncode}", updated_at=datetime.now().isoformat())
    return int(proc.returncode)


if __name__ == "__main__":
    raise SystemExit(main())
