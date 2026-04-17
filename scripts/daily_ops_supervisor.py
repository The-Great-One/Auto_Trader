#!/usr/bin/env python3
"""Daily Auto_Trader ops supervisor.

1) Uses NSE market calendar to determine open/closed day.
2) Runs 10 strategy variants and compares against baseline.
3) Verifies paper trader execution on market-open days; attempts self-heal if missing.
4) Maintains rolling metrics history.
"""

from __future__ import annotations

import importlib
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas_market_calendars as mcal

ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / "reports"
SCRIPTS = ROOT / "scripts"
REPORTS.mkdir(exist_ok=True)
ENV_FILE = Path("/home/ubuntu/.autotrader_env")
PROMOTION_STATE_FILE = REPORTS / "strategy_autopromote_state.json"
PROMOTION_HISTORY_FILE = REPORTS / "strategy_autopromote_history.jsonl"
AUTOPROMOTE_BEGIN = "# BEGIN OPENCLAW AUTOPROMOTE"
AUTOPROMOTE_END = "# END OPENCLAW AUTOPROMOTE"

PARAM_ENV_MAP = {
    "buy": {
        "adx_min": "AT_BUY_ADX_MIN",
        "adx_strong_min": "AT_BUY_ADX_STRONG_MIN",
        "mmi_risk_off": "AT_BUY_MMI_RISK_OFF",
        "min_atr_pct": "AT_BUY_MIN_ATR_PCT",
        "max_atr_pct": "AT_BUY_MAX_ATR_PCT",
        "max_extension_atr": "AT_BUY_MAX_EXTENSION_ATR",
        "max_obv_zscore": "AT_BUY_MAX_OBV_ZSCORE",
        "obv_min_zscore": "AT_BUY_OBV_MIN_ZSCORE",
        "volume_confirm_mult": "AT_BUY_VOLUME_CONFIRM_MULT",
        "cmf_strong_min": "AT_BUY_CMF_STRONG_MIN",
        "cmf_base_min": "AT_BUY_CMF_BASE_MIN",
        "cmf_weak_min": "AT_BUY_CMF_WEAK_MIN",
        "rsi_floor": "AT_BUY_RSI_FLOOR",
        "stoch_pull_max": "AT_BUY_STOCH_PULL_MAX",
        "stoch_momo_max": "AT_BUY_STOCH_MOMO_MAX",
    },
    "sell": {
        "ema_break_atr_mult": "AT_SELL_EMA_BREAK_ATR_MULT",
        "relative_volume_exit": "AT_SELL_RELATIVE_VOLUME_EXIT",
        "breakeven_trigger_pct": "AT_SELL_BREAKEVEN_TRIGGER_PCT",
        "momentum_exit_rsi": "AT_SELL_MOMENTUM_EXIT_RSI",
        "equity_time_stop_bars": "AT_EQUITY_TIME_STOP_BARS",
        "equity_time_stop_min_profit_pct": "AT_EQUITY_TIME_STOP_MIN_PROFIT_PCT",
        "fund_time_stop_bars": "AT_FUND_TIME_STOP_BARS",
        "fund_time_stop_min_profit_pct": "AT_FUND_TIME_STOP_MIN_PROFIT_PCT",
        "equity_review_start_bars": "AT_EQUITY_REVIEW_START_BARS",
        "equity_review_end_bars": "AT_EQUITY_REVIEW_END_BARS",
        "equity_review_max_profit_pct": "AT_EQUITY_REVIEW_MAX_PROFIT_PCT",
        "equity_review_rsi": "AT_EQUITY_REVIEW_RSI",
        "equity_review_macd_hist": "AT_EQUITY_REVIEW_MACD_HIST",
    },
}


def ist_now() -> datetime:
    # server timezone may vary; this keeps output consistent with IST workflow.
    return datetime.now()


def is_market_open_today() -> tuple[bool, str]:
    nse = mcal.get_calendar("NSE")
    today = ist_now().date()
    valid = nse.valid_days(start_date=str(today), end_date=str(today))
    open_today = len(valid) > 0
    return open_today, str(today)


DEFAULT_DAILY_LAB_MAX_VARIANTS = max(
    1,
    int(os.getenv("AT_DAILY_LAB_MAX_VARIANTS", os.getenv("AT_LAB_MAX_VARIANTS", "50"))),
)
DEFAULT_WEEKEND_LAB_MAX_VARIANTS = max(
    DEFAULT_DAILY_LAB_MAX_VARIANTS,
    int(os.getenv("AT_WEEKEND_LAB_MAX_VARIANTS", "200")),
)


def resolve_strategy_lab_max_variants(now: datetime | None = None) -> int:
    now = now or ist_now()
    return (
        DEFAULT_WEEKEND_LAB_MAX_VARIANTS
        if now.weekday() >= 5
        else DEFAULT_DAILY_LAB_MAX_VARIANTS
    )


def _safe_float(value, default=None):
    try:
        return float(value)
    except Exception:
        return default


def _normalize_param_value(value: Any):
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return int(value)
    try:
        f = float(value)
    except Exception:
        return value
    if abs(f - round(f)) < 1e-9:
        return int(round(f))
    return round(f, 6)


def _extract_candidate(rec: dict) -> dict:
    best = rec.get("best", {}) or {}
    params = best.get("params", {}) or {}
    buy = {
        k: _normalize_param_value(v)
        for k, v in (params.get("buy", {}) or {}).items()
        if k in PARAM_ENV_MAP["buy"]
    }
    sell = {
        k: _normalize_param_value(v)
        for k, v in (params.get("sell", {}) or {}).items()
        if k in PARAM_ENV_MAP["sell"]
    }
    env_updates = {
        **{PARAM_ENV_MAP["buy"][k]: str(v) for k, v in buy.items()},
        **{PARAM_ENV_MAP["sell"][k]: str(v) for k, v in sell.items()},
    }
    key = json.dumps({"buy": buy, "sell": sell}, sort_keys=True)
    return {
        "key": key,
        "name": best.get("name"),
        "buy": buy,
        "sell": sell,
        "env_updates": env_updates,
        "return_pct": best.get("total_return_pct"),
        "selection_score": best.get("selection_score"),
        "max_drawdown_pct": best.get("max_drawdown_pct"),
        "trades": best.get("trades"),
        "empty": not (buy or sell),
    }


def _date_matches_trade_date(value: str | None, trade_date: str) -> bool:
    text = str(value or "")
    return text.startswith(trade_date)



def run_strategy_lab(trade_date: str, max_variants: int | None = None) -> dict:
    requested_variants = int(max_variants or resolve_strategy_lab_max_variants())
    env = os.environ.copy()
    env["AT_LAB_MAX_VARIANTS"] = str(requested_variants)
    cmd = ["/home/ubuntu/Auto_Trader/venv/bin/python", str(SCRIPTS / "weekly_strategy_lab.py")]
    proc = subprocess.run(cmd, cwd=str(ROOT), capture_output=True, text=True, env=env)

    latest = sorted(REPORTS.glob("strategy_lab_*.json"))
    out = {
        "ok": proc.returncode == 0,
        "returncode": proc.returncode,
        "stderr": proc.stderr[-1200:],
        "stdout": proc.stdout[-1200:],
        "file": str(latest[-1]) if latest else None,
        "requested_variants": requested_variants,
        "tested_variants": 0,
        "baseline_return_pct": None,
        "best_return_pct": None,
        "improvement_return_pct": None,
        "improvement_score": None,
        "should_promote": False,
        "best_name": None,
        "candidate": None,
        "report_generated_at": None,
        "stale_report": False,
        "stale_reason": None,
    }

    if latest:
        payload = json.loads(latest[-1].read_text())
        rec = payload.get("recommendation", {})
        out["report_generated_at"] = rec.get("generated_at")
        out["stale_report"] = not _date_matches_trade_date(rec.get("generated_at"), trade_date)
        out["stale_reason"] = None if not out["stale_report"] else "latest_strategy_lab_report_not_from_trade_date"
        if not out["stale_report"]:
            out["tested_variants"] = int(rec.get("tested_variants", 0) or 0)
            out["baseline_return_pct"] = rec.get("baseline", {}).get("total_return_pct")
            out["best_return_pct"] = rec.get("best", {}).get("total_return_pct")
            out["improvement_return_pct"] = rec.get("improvement_return_pct")
            out["improvement_score"] = rec.get("improvement_score")
            out["should_promote"] = bool(rec.get("should_promote", False)) and proc.returncode == 0
            out["best_name"] = rec.get("best", {}).get("name")
            out["candidate"] = _extract_candidate(rec)
        else:
            out["should_promote"] = False
            if out["ok"]:
                out["ok"] = False
                out["stale_reason"] = "strategy_lab_completed_but_no_fresh_report_found"

    return out


def _iso_week_key(now: datetime) -> str:
    iso = now.isocalendar()
    return f"{iso.year}_W{iso.week:02d}"



def _load_weekly_universe_cagr_report(report_week: str) -> dict | None:
    path = REPORTS / f"weekly_universe_cagr_{report_week}.json"
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text())
        payload["file"] = str(path)
        return payload
    except Exception:
        return None



def run_weekly_universe_cagr_check(now: datetime, market_open: bool) -> dict:
    enabled = os.getenv("AT_WEEKLY_CAGR_ENABLED", "1").strip().lower() not in {"0", "false", "no"}
    weekday_target = max(0, min(6, int(os.getenv("AT_WEEKLY_CAGR_WEEKDAY", "5"))))
    report_week = _iso_week_key(now)
    result = {
        "enabled": enabled,
        "report_week": report_week,
        "scheduled_weekday": weekday_target,
        "ok": False,
        "ran": False,
        "reason": None,
        "file": None,
        "cagr_pct": None,
        "total_return_pct": None,
        "coverage_pct": None,
        "tested_symbols": 0,
        "requested_symbols": 0,
    }

    existing = _load_weekly_universe_cagr_report(report_week)
    if existing is not None:
        universe = existing.get("universe", {}) or {}
        backtest = existing.get("backtest", {}) or {}
        result.update(
            {
                "ok": True,
                "ran": False,
                "reason": "already_ran_this_week",
                "file": existing.get("file"),
                "cagr_pct": existing.get("cagr_pct"),
                "total_return_pct": backtest.get("total_return_pct"),
                "coverage_pct": universe.get("coverage_pct"),
                "tested_symbols": int(universe.get("tested_symbols", 0) or 0),
                "requested_symbols": int(universe.get("requested_symbols", 0) or 0),
                "generated_at": existing.get("generated_at"),
            }
        )
        return result

    if not enabled:
        result["reason"] = "disabled"
        return result
    if market_open:
        result["reason"] = "market_open"
        return result
    if now.weekday() != weekday_target:
        result["reason"] = "not_scheduled_day"
        return result

    env = os.environ.copy()
    env["AT_WEEKLY_CAGR_REPORT_WEEK"] = report_week
    cmd = ["/home/ubuntu/Auto_Trader/venv/bin/python", str(SCRIPTS / "weekly_universe_cagr_check.py")]
    proc = subprocess.run(cmd, cwd=str(ROOT), capture_output=True, text=True, env=env)
    payload = _load_weekly_universe_cagr_report(report_week)
    result["ran"] = True
    result["ok"] = proc.returncode == 0 and payload is not None
    result["reason"] = "completed" if result["ok"] else f"failed_rc_{proc.returncode}"
    result["stdout"] = proc.stdout[-1200:]
    result["stderr"] = proc.stderr[-1200:]
    if payload is not None:
        universe = payload.get("universe", {}) or {}
        backtest = payload.get("backtest", {}) or {}
        result.update(
            {
                "file": payload.get("file"),
                "generated_at": payload.get("generated_at"),
                "cagr_pct": payload.get("cagr_pct"),
                "total_return_pct": backtest.get("total_return_pct"),
                "coverage_pct": universe.get("coverage_pct"),
                "tested_symbols": int(universe.get("tested_symbols", 0) or 0),
                "requested_symbols": int(universe.get("requested_symbols", 0) or 0),
            }
        )
    return result



def check_and_fix_paper_execution(market_open: bool, trade_date: str) -> dict:
    """If market open, ensure paper_shadow file exists for today. If missing, run it."""
    file_latest = REPORTS / "paper_shadow_latest.json"
    result = {
        "market_open": market_open,
        "paper_executed": False,
        "self_healed": False,
        "decision": None,
        "reason": None,
        "file": str(file_latest),
    }

    if not market_open:
        result["reason"] = "market_closed"
        return result

    def _is_today_payload(p: Path) -> tuple[bool, dict | None]:
        if not p.exists():
            return False, None
        try:
            data = json.loads(p.read_text())
            ts = str(data.get("generated_at", ""))
            return ts.startswith(trade_date), data
        except Exception:
            return False, None

    ok_today, payload = _is_today_payload(file_latest)
    if ok_today:
        result["paper_executed"] = True
        result["decision"] = payload.get("decision") if payload else None
        result["reason"] = "already_executed"
        return result

    # try to self-heal by running paper shadow now
    cmd = ["/home/ubuntu/Auto_Trader/venv/bin/python", str(SCRIPTS / "paper_shadow.py")]
    proc = subprocess.run(cmd, cwd=str(ROOT), capture_output=True, text=True)
    ok_today, payload = _is_today_payload(file_latest)

    result["paper_executed"] = ok_today and proc.returncode == 0
    result["self_healed"] = result["paper_executed"]
    result["decision"] = (payload or {}).get("decision")
    result["reason"] = "self_heal_run" if result["paper_executed"] else f"failed_rc_{proc.returncode}"
    if not result["paper_executed"]:
        result["error"] = (proc.stderr or proc.stdout)[-1200:]
    return result


def append_metrics(history_row: dict):
    hist_jsonl = REPORTS / "strategy_metrics_history.jsonl"
    with hist_jsonl.open("a", encoding="utf-8") as f:
        f.write(json.dumps(history_row, ensure_ascii=False) + "\n")


def _load_promotion_state() -> dict:
    try:
        return json.loads(PROMOTION_STATE_FILE.read_text()) if PROMOTION_STATE_FILE.exists() else {}
    except Exception:
        return {}


def _save_promotion_state(state: dict):
    PROMOTION_STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")


def _append_promotion_history(row: dict):
    with PROMOTION_HISTORY_FILE.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")


def _load_recent_candidates(limit: int) -> list[dict]:
    out = []
    for path in sorted(REPORTS.glob("strategy_lab_*.json"), reverse=True)[: limit * 3]:
        try:
            payload = json.loads(path.read_text())
            rec = payload.get("recommendation", {})
            cand = _extract_candidate(rec)
            out.append(
                {
                    "path": str(path),
                    "generated_at": rec.get("generated_at"),
                    "should_promote": bool(rec.get("should_promote", False)),
                    "improvement_return_pct": rec.get("improvement_return_pct"),
                    "improvement_score": rec.get("improvement_score"),
                    "candidate": cand,
                }
            )
        except Exception:
            continue
        if len(out) >= limit:
            break
    return out


def _current_effective_candidate(candidate: dict) -> dict:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    buy_mod = importlib.import_module("Auto_Trader.RULE_SET_7")
    sell_mod = importlib.import_module("Auto_Trader.RULE_SET_2")
    return {
        "buy": {
            k: _normalize_param_value((buy_mod.CONFIG or {}).get(k))
            for k in (candidate.get("buy") or {})
        },
        "sell": {
            k: _normalize_param_value((sell_mod.CONFIG or {}).get(k))
            for k in (candidate.get("sell") or {})
        },
    }


def _render_autopromote_block(env_updates: dict[str, str]) -> str:
    lines = [AUTOPROMOTE_BEGIN]
    for key in sorted(env_updates):
        lines.append(f'export {key}="{env_updates[key]}"')
    lines.append(AUTOPROMOTE_END)
    return "\n".join(lines)


def _write_managed_env_block(env_updates: dict[str, str]):
    current = ENV_FILE.read_text(encoding="utf-8") if ENV_FILE.exists() else ""
    block = _render_autopromote_block(env_updates)
    pattern = re.compile(
        rf"{re.escape(AUTOPROMOTE_BEGIN)}.*?{re.escape(AUTOPROMOTE_END)}\n?",
        re.S,
    )
    if pattern.search(current):
        updated = pattern.sub(block + "\n", current).rstrip() + "\n"
    else:
        updated = (current.rstrip() + "\n\n" + block + "\n") if current.strip() else (block + "\n")
    ENV_FILE.write_text(updated, encoding="utf-8")


def maybe_auto_promote(strategy: dict, market_open: bool) -> dict:
    enabled = os.getenv("AT_LAB_AUTOPROMOTE_ENABLED", "1").strip().lower() not in {"0", "false", "no"}
    result = {
        "enabled": enabled,
        "applied": False,
        "restarted_service": False,
        "reason": None,
        "candidate": strategy.get("candidate"),
        "env_updates": None,
        "repeat_hits": 0,
        "lookback": 0,
    }
    if not enabled:
        result["reason"] = "disabled"
        return result
    if market_open:
        result["reason"] = "market_open"
        return result
    if not strategy.get("ok"):
        result["reason"] = "lab_failed"
        return result
    if not strategy.get("should_promote"):
        result["reason"] = "lab_declined_promotion"
        return result

    candidate = strategy.get("candidate") or {}
    if candidate.get("empty"):
        result["reason"] = "candidate_has_no_env_mappable_params"
        return result

    effective_candidate = _current_effective_candidate(candidate)
    if effective_candidate == {"buy": candidate.get("buy") or {}, "sell": candidate.get("sell") or {}}:
        result["reason"] = "already_active_effective_config"
        result["env_updates"] = candidate.get("env_updates")
        return result

    min_return_gain = _safe_float(os.getenv("AT_LAB_AUTOPROMOTE_MIN_RETURN_GAIN", "1.0"), 1.0)
    min_score_gain = _safe_float(os.getenv("AT_LAB_AUTOPROMOTE_MIN_SCORE_GAIN", "1.0"), 1.0)
    lookback = max(1, int(os.getenv("AT_LAB_AUTOPROMOTE_LOOKBACK", "3")))
    min_repeat = max(1, int(os.getenv("AT_LAB_AUTOPROMOTE_MIN_REPEAT", "2")))
    cooldown_hours = max(0, int(os.getenv("AT_LAB_AUTOPROMOTE_COOLDOWN_HOURS", "24")))

    if (_safe_float(strategy.get("improvement_return_pct"), -999) or -999) < min_return_gain:
        result["reason"] = "insufficient_return_gain"
        return result
    score_gain = _safe_float(strategy.get("improvement_score"), 0.0) or 0.0
    if score_gain < min_score_gain:
        result["reason"] = "insufficient_score_gain"
        return result

    recent = _load_recent_candidates(lookback)
    repeat_hits = sum(
        1
        for row in recent
        if row.get("should_promote")
        and (row.get("candidate") or {}).get("key") == candidate.get("key")
    )
    result["repeat_hits"] = repeat_hits
    result["lookback"] = lookback
    if repeat_hits < min_repeat:
        result["reason"] = "repeat_guard_not_met"
        return result

    state = _load_promotion_state()
    current_key = ((state.get("candidate") or {}).get("key")) if state else None
    if current_key == candidate.get("key"):
        result["reason"] = "already_active"
        result["env_updates"] = candidate.get("env_updates")
        return result

    promoted_at_raw = state.get("promoted_at") if state else None
    if promoted_at_raw:
        try:
            promoted_at = datetime.fromisoformat(promoted_at_raw)
            if datetime.now() - promoted_at < timedelta(hours=cooldown_hours):
                result["reason"] = "cooldown_active"
                return result
        except Exception:
            pass

    env_updates = candidate.get("env_updates") or {}
    _write_managed_env_block(env_updates)
    proc = subprocess.run(["sudo", "systemctl", "restart", "auto_trade.service"], capture_output=True, text=True)
    result["applied"] = proc.returncode == 0
    result["restarted_service"] = proc.returncode == 0
    result["env_updates"] = env_updates
    result["reason"] = "applied" if proc.returncode == 0 else f"restart_failed_rc_{proc.returncode}"
    if proc.returncode != 0:
        result["error"] = (proc.stderr or proc.stdout)[-1200:]
        return result

    state = {
        "promoted_at": datetime.now().isoformat(),
        "source_report": strategy.get("file"),
        "candidate": candidate,
        "improvement_return_pct": strategy.get("improvement_return_pct"),
        "improvement_score": strategy.get("improvement_score"),
        "repeat_hits": repeat_hits,
        "lookback": lookback,
    }
    _save_promotion_state(state)
    _append_promotion_history(state)
    return result


def main():
    now = ist_now()
    market_open, trade_date = is_market_open_today()

    strategy = run_strategy_lab(trade_date)
    weekly_universe_cagr = run_weekly_universe_cagr_check(now, market_open)
    paper = check_and_fix_paper_execution(market_open, trade_date)
    autopromote = maybe_auto_promote(strategy, market_open)

    summary = {
        "generated_at": now.isoformat(),
        "trade_date": trade_date,
        "market_open": market_open,
        "calendar": "NSE",
        "strategy_test": strategy,
        "paper_trader": paper,
        "weekly_universe_cagr": weekly_universe_cagr,
        "autopromote": autopromote,
    }

    out_json = REPORTS / f"daily_ops_supervisor_{trade_date}.json"
    out_md = REPORTS / f"daily_ops_supervisor_{trade_date}.md"

    out_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    lines = [
        f"# Daily Ops Supervisor — {trade_date}",
        "",
        f"- Market open: **{market_open}** (NSE calendar)",
        f"- Strategies tested: **{strategy.get('tested_variants', 0)}**",
        f"- Strategy report stale: **{strategy.get('stale_report')}**",
        f"- Baseline return %: **{strategy.get('baseline_return_pct')}**",
        f"- Best return %: **{strategy.get('best_return_pct')}**",
        f"- Improvement return %: **{strategy.get('improvement_return_pct')}**",
        f"- Promote candidate: **{strategy.get('should_promote')}**",
        f"- Auto-promote applied: **{autopromote.get('applied')}**",
        f"- Auto-promote reason: **{autopromote.get('reason')}**",
        "",
        "## Weekly universe CAGR check",
        f"- Status: **{weekly_universe_cagr.get('reason')}**",
        f"- Requested symbols: **{weekly_universe_cagr.get('requested_symbols')}**",
        f"- Tested symbols: **{weekly_universe_cagr.get('tested_symbols')}**",
        f"- Coverage %: **{weekly_universe_cagr.get('coverage_pct')}**",
        f"- CAGR %: **{weekly_universe_cagr.get('cagr_pct')}**",
        f"- Total return %: **{weekly_universe_cagr.get('total_return_pct')}**",
        "",
        "## Paper trader check",
        f"- Executed today: **{paper.get('paper_executed')}**",
        f"- Self-healed: **{paper.get('self_healed')}**",
        f"- Decision: **{paper.get('decision')}**",
        f"- Reason: **{paper.get('reason')}**",
    ]
    if paper.get("error"):
        lines += ["", "### Error", "```", str(paper["error"]), "```"]

    out_md.write_text("\n".join(lines) + "\n", encoding="utf-8")

    append_metrics(
        {
            "trade_date": trade_date,
            "market_open": market_open,
            "tested_variants": strategy.get("tested_variants", 0),
            "baseline_return_pct": strategy.get("baseline_return_pct"),
            "best_return_pct": strategy.get("best_return_pct"),
            "improvement_return_pct": strategy.get("improvement_return_pct"),
            "autopromote_applied": autopromote.get("applied"),
            "autopromote_reason": autopromote.get("reason"),
            "weekly_universe_cagr_reason": weekly_universe_cagr.get("reason"),
            "weekly_universe_cagr_pct": weekly_universe_cagr.get("cagr_pct"),
            "weekly_universe_cagr_tested_symbols": weekly_universe_cagr.get("tested_symbols"),
            "paper_executed": paper.get("paper_executed"),
            "paper_self_healed": paper.get("self_healed"),
            "paper_decision": paper.get("decision"),
        }
    )

    print(json.dumps(summary, indent=2))
    print(f"Saved: {out_json}")
    print(f"Saved: {out_md}")


if __name__ == "__main__":
    main()
