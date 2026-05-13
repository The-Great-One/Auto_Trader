#!/usr/bin/env python3
"""Continuous local research loop for the 30% CAGR mission.

Local-only: runs strategy-lab batches, validates promising candidates with the
weekly CAGR validator, persists state, and continues until target CAGR is hit or
all configured variants are exhausted.
"""
from __future__ import annotations

import glob
import json
import os
import subprocess
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
STATE_PATH = ROOT / "intermediary_files/lab_status/cagr_mission_state.json"
LAB_ENV = ROOT / "intermediary_files/lab_status/current_strategy_lab.env"
CAGR_ENV = ROOT / "intermediary_files/lab_status/current_cagr_validation.env"
REPORTS = ROOT / "reports"
TARGET_CAGR = float(os.getenv("AT_MISSION_TARGET_CAGR", "30"))
BATCH_LIMIT = int(os.getenv("AT_MISSION_BATCH_LIMIT", "12"))
MAX_BATCHES = int(os.getenv("AT_MISSION_MAX_BATCHES", "999"))
SLEEP_BETWEEN = int(os.getenv("AT_MISSION_SLEEP_SECONDS", "5"))

SYMBOLS = "ABCAPITAL,AEROFLEX,AETHER,APLAPOLLO,BBOX,BHARATWIRE,BOSCHLTD,BSE,CGPOWER,COLPAL,DOMS,FEDERALBNK,GENUSPOWER,GESHIP,GOODLUCK,GRANULES,GRSE,HAL,HDFCAMC,HUDCO,INDIGO,KERNEX,MARICO,MARINE,MAZDOCK,MPHASIS,NORTHARC,NTPCGREEN,OFSS,PAGEIND,PARAS,RADICO,RRKABEL,SBILIFE,SCHNEIDER,SKYGOLD,SPAL,SRF,STAR,SUNFLAG,TANLA,TARIL,TIPSMUSIC,TRENT,UJJIVANSFB,VIJAYA,WOCKPHARMA"


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text())
    except Exception:
        return default


def save_state(state: dict[str, Any]) -> None:
    state["updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%S%z")
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(state, indent=2, sort_keys=True))


def latest(pattern: str) -> Path | None:
    files = [Path(p) for p in glob.glob(str(REPORTS / pattern))]
    return max(files, key=lambda p: p.stat().st_mtime) if files else None


def run(cmd: list[str], env: dict[str, str] | None = None) -> int:
    merged = os.environ.copy()
    if env:
        merged.update(env)
    print("RUN", " ".join(cmd), flush=True)
    return subprocess.run(cmd, cwd=ROOT, env=merged).returncode


def write_env(path: Path, values: dict[str, Any]) -> None:
    lines = []
    for k, v in values.items():
        if v is None:
            continue
        lines.append(f"{k}={v}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n")


def env_key(prefix: str, key: str) -> str:
    return f"{prefix}_{key.upper()}"


def cagr_env_for(candidate: dict[str, Any], sizing: dict[str, Any]) -> dict[str, Any]:
    params = candidate.get("params", {}) or {}
    buy = params.get("buy", {}) or {}
    sell = params.get("sell", {}) or {}
    env: dict[str, Any] = {
        "AT_WEEKLY_CAGR_MIN_BARS": 1000,
        "AT_LAB_REGIME_FILTER_ENABLED": 0,
        "AT_LAB_MODE": 1,
        "AT_LAB_SYMBOLS": SYMBOLS,
        **sizing,
    }
    for k, v in buy.items():
        env[env_key("AT_BUY", k)] = v
    for k, v in sell.items():
        if k == "equity_time_stop_bars":
            env["AT_EQUITY_TIME_STOP_BARS"] = v
        elif k == "fund_time_stop_bars":
            env["AT_FUND_TIME_STOP_BARS"] = v
        elif k == "fund_time_stop_min_profit_pct":
            env["AT_FUND_TIME_STOP_MIN_PROFIT_PCT"] = v
        else:
            env[env_key("AT_SELL", k)] = v
    return env


def read_payload(path: Path) -> dict[str, Any]:
    raw = load_json(path, {})
    return raw.get("payload", raw)


def validate_candidate(candidate: dict[str, Any], state: dict[str, Any]) -> dict[str, Any]:
    sizing_profiles = [
        {"name": "fixed_20k_regime_off", "env": {"FUND_ALLOCATION": 20000, "AT_BACKTEST_VOL_SIZING_ENABLED": 0}},
        {"name": "vol_4pct_30max_regime_off", "env": {"AT_BACKTEST_VOL_SIZING_ENABLED": 1, "AT_BACKTEST_RISK_PER_TRADE_PCT": 0.04, "AT_BACKTEST_MAX_POSITION_NOTIONAL_PCT": 0.3}},
    ]
    best: dict[str, Any] = {"cagr_pct": -999, "candidate": candidate.get("name")}
    for profile in sizing_profiles:
        write_env(CAGR_ENV, cagr_env_for(candidate, profile["env"]))
        code = run(["rtk", "bash", "scripts/run_local_cagr_validation.sh"])
        report = latest("weekly_universe_cagr_*.json")
        if code != 0 or not report:
            result = {"profile": profile["name"], "error": f"validator_exit_{code}"}
        else:
            payload = read_payload(report)
            result = {
                "profile": profile["name"],
                "report": report.name,
                "cagr_pct": payload.get("cagr_pct"),
                "curve_cagr_pct": ((payload.get("validation") or {}).get("portfolio_metrics") or {}).get("curve_cagr_pct"),
                "total_return_pct": ((payload.get("backtest") or {}).get("total_return_pct")),
                "max_drawdown_pct": ((payload.get("backtest") or {}).get("max_drawdown_pct")),
                "walkforward_positive_pct": (((payload.get("validation") or {}).get("walkforward") or {}).get("summary") or {}).get("positive_window_pct"),
            }
            cagr = float(result.get("cagr_pct") or -999)
            if cagr > float(best.get("cagr_pct") or -999):
                best = {**result, "candidate": candidate.get("name"), "params": candidate.get("params")}
        state.setdefault("validations", []).append(result | {"candidate": candidate.get("name")})
        save_state(state)
        if float(best.get("cagr_pct") or -999) >= TARGET_CAGR:
            break
    return best


def infer_next_offset(state: dict[str, Any]) -> int:
    if "next_offset" in state:
        return int(state["next_offset"])
    max_next = 0
    for p in REPORTS.glob("strategy_lab_batch_*_*.json"):
        payload = read_payload(p)
        batch = ((payload.get("recommendation") or {}).get("batch") or {})
        if "offset" in batch and "tested_variants" in batch:
            max_next = max(max_next, int(batch["offset"]) + int(batch.get("tested_variants") or BATCH_LIMIT))
    return max_next


def main() -> int:
    state = load_json(STATE_PATH, {"status": "running", "target_cagr_pct": TARGET_CAGR, "validations": []})
    offset = infer_next_offset(state)
    batches = 0
    while batches < MAX_BATCHES:
        state["status"] = "running"
        state["current_step"] = f"strategy_lab_offset_{offset}"
        state["next_offset"] = offset
        save_state(state)

        write_env(LAB_ENV, {
            "AT_LAB_PRECACHE": 0,
            "AT_LAB_HISTORY_PERIOD": "5y",
            "AT_LAB_VARIANT_OFFSET": offset,
            "AT_LAB_VARIANT_LIMIT": BATCH_LIMIT,
            "AT_LAB_MAX_VARIANTS": 1000,
            "AT_LAB_SYMBOLS": SYMBOLS,
        })
        before = latest("strategy_lab_batch_*.json")
        code = run(["rtk", "bash", "scripts/run_local_strategy_lab.sh"])
        after = latest("strategy_lab_batch_*.json")
        if code != 0 or not after or after == before:
            state["status"] = "blocked"
            state["blocked_reason"] = f"strategy_lab_exit_{code}_no_new_report"
            save_state(state)
            return code or 2

        payload = read_payload(after)
        rec = payload.get("recommendation") or {}
        batch = rec.get("batch") or {}
        ranked = rec.get("ranked") or []
        full_count = int(batch.get("full_variant_count") or 0)
        tested = int(batch.get("tested_variants") or len(ranked) or BATCH_LIMIT)
        best_lab = (rec.get("best") or (ranked[0] if ranked else {}))
        state.setdefault("lab_batches", []).append({
            "report": after.name,
            "offset": offset,
            "tested": tested,
            "full_variant_count": full_count,
            "best_name": best_lab.get("name"),
            "best_total_return_pct": best_lab.get("total_return_pct"),
            "best_max_drawdown_pct": best_lab.get("max_drawdown_pct"),
        })
        if best_lab and float(best_lab.get("total_return_pct") or -999) > float((state.get("best_lab") or {}).get("total_return_pct") or -999):
            state["best_lab"] = best_lab
        save_state(state)

        candidates = [c for c in ranked[:2] if c.get("name") != "baseline_current"] or ([best_lab] if best_lab else [])
        for cand in candidates:
            best_val = validate_candidate(cand, state)
            if best_val and float(best_val.get("cagr_pct") or -999) > float((state.get("best_validation") or {}).get("cagr_pct") or -999):
                state["best_validation"] = best_val
                save_state(state)
            if float(best_val.get("cagr_pct") or -999) >= TARGET_CAGR:
                state["status"] = "achieved"
                state["current_step"] = "target_hit"
                save_state(state)
                print(json.dumps({"achieved": True, "best_validation": best_val}, indent=2), flush=True)
                return 0

        offset += max(tested, BATCH_LIMIT)
        state["next_offset"] = offset
        save_state(state)
        if full_count and offset >= full_count:
            state["status"] = "blocked"
            state["blocked_reason"] = f"exhausted_configured_variants_at_{full_count}_without_{TARGET_CAGR}_cagr"
            save_state(state)
            return 3
        batches += 1
        time.sleep(SLEEP_BETWEEN)
    state["status"] = "paused"
    state["blocked_reason"] = f"max_batches_{MAX_BATCHES}_reached"
    save_state(state)
    return 4


if __name__ == "__main__":
    raise SystemExit(main())
