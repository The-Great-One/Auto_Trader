#!/usr/bin/env python3
"""Aggressive sizing + exit sweep around the vol_sizing_006 winner.

Tests:
1. Wider sizing parameter neighborhood (risk%, ATR mult, max position%)
2. Sell-side exit tuning (breakeven trigger, time stops, momentum RSI)
3. Combined best sizing + tighter exits
4. Regime filter variations with sizing

Goal: push from 16.66% toward 20%+ while keeping drawdown under control.
"""
from __future__ import annotations

import json
import os
import sys
from contextlib import contextmanager
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import weekly_strategy_lab as lab

OUT_DIR = ROOT / "reports"
STATUS_PATH = OUT_DIR / "sizing_exit_sweep_latest.json"
HISTORY_PATH = OUT_DIR / "sizing_exit_sweep_history.jsonl"
CHECKPOINT_PATH = OUT_DIR / "sizing_exit_sweep_checkpoint.json"

# Winning config from vol_sizing_006
WINNING_BUY = {
    "adx_min": 10,
    "volume_confirm_mult": 0.85,
    "ich_cloud_bull": 0,
    "regime_filter_enabled": 1,
    "regime_ema_fast": 50,
    "regime_ema_slow": 200,
}
WINNING_SELL = {"breakeven_trigger_pct": 4.0}
WINNING_SIZING = {
    "AT_BACKTEST_VOL_SIZING_ENABLED": "1",
    "AT_BACKTEST_RISK_PER_TRADE_PCT": "0.01",
    "AT_BACKTEST_ATR_STOP_MULT": "2.5",
    "AT_BACKTEST_MAX_POSITION_NOTIONAL_PCT": "0.25",
}

VARIANTS = []

# ---- Group 1: Sizing neighborhood ----
for risk in [0.008, 0.01, 0.012, 0.015, 0.02]:
    for atr_mult in [2.0, 2.5, 3.0, 3.5]:
        for max_pct in [0.20, 0.25, 0.30]:
            VARIANTS.append({
                "name": f"size_r{risk}_a{atr_mult}_m{max_pct}",
                "buy": dict(WINNING_BUY),
                "sell": dict(WINNING_SELL),
                "env": {
                    "AT_BACKTEST_VOL_SIZING_ENABLED": "1",
                    "AT_BACKTEST_RISK_PER_TRADE_PCT": str(risk),
                    "AT_BACKTEST_ATR_STOP_MULT": str(atr_mult),
                    "AT_BACKTEST_MAX_POSITION_NOTIONAL_PCT": str(max_pct),
                },
            })

# ---- Group 2: Exit tuning on top of winning sizing ----
for bep in [2.5, 3.0, 4.0, 5.0, 6.0]:
    for time_stop in [6, 8, 10, 12]:
        VARIANTS.append({
            "name": f"exit_bep{bep}_ts{time_stop}",
            "buy": dict(WINNING_BUY),
            "sell": {"breakeven_trigger_pct": bep},
            "env": dict(WINNING_SIZING),
            "extra_env": {
                "AT_EQUITY_TIME_STOP_BARS": str(time_stop),
                "AT_EQUITY_TIME_STOP_MIN_PROFIT_PCT": "1.0",
            },
        })

# ---- Group 3: Momentum exit variations ----
for mom_rsi in [38, 42, 48, 52]:
    VARIANTS.append({
        "name": f"mom_rsi{mom_rsi}",
        "buy": dict(WINNING_BUY),
        "sell": {**WINNING_SELL, "momentum_exit_rsi": float(mom_rsi)},
        "env": dict(WINNING_SIZING),
    })

# ---- Group 4: ADX entry loosening with best sizing ----
for adx in [8, 10, 12, 14]:
    for vol_mult in [0.75, 0.85, 0.95]:
        VARIANTS.append({
            "name": f"entry_adx{adx}_vol{vol_mult}",
            "buy": {**WINNING_BUY, "adx_min": adx, "volume_confirm_mult": vol_mult},
            "sell": dict(WINNING_SELL),
            "env": dict(WINNING_SIZING),
        })

# ---- Group 5: Regime variation with best sizing ----
for ema_f in [30, 40, 50]:
    for ema_s in [150, 200, 250]:
        VARIANTS.append({
            "name": f"regime_ef{ema_f}_es{ema_s}",
            "buy": {**WINNING_BUY, "regime_ema_fast": ema_f, "regime_ema_slow": ema_s},
            "sell": dict(WINNING_SELL),
            "env": dict(WINNING_SIZING),
        })

# ---- Group 6: Combined winners — best sizing + best exit + loosest entry ----
VARIANTS.append({
    "name": "combo_aggressive_001",
    "buy": {**WINNING_BUY, "adx_min": 8, "volume_confirm_mult": 0.75},
    "sell": {"breakeven_trigger_pct": 5.0, "momentum_exit_rsi": 48.0},
    "env": {**WINNING_SIZING, "AT_BACKTEST_RISK_PER_TRADE_PCT": "0.015", "AT_BACKTEST_ATR_STOP_MULT": "3.0", "AT_BACKTEST_MAX_POSITION_NOTIONAL_PCT": "0.30"},
})
VARIANTS.append({
    "name": "combo_aggressive_002",
    "buy": {**WINNING_BUY, "adx_min": 8, "volume_confirm_mult": 0.75},
    "sell": {"breakeven_trigger_pct": 6.0, "momentum_exit_rsi": 42.0},
    "env": {**WINNING_SIZING, "AT_BACKTEST_RISK_PER_TRADE_PCT": "0.012", "AT_BACKTEST_ATR_STOP_MULT": "3.5", "AT_BACKTEST_MAX_POSITION_NOTIONAL_PCT": "0.30"},
})

# Fixed baseline for comparison
VARIANTS.insert(0, {
    "name": "fixed_size_baseline",
    "buy": dict(WINNING_BUY),
    "sell": dict(WINNING_SELL),
    "env": {"AT_BACKTEST_VOL_SIZING_ENABLED": "0"},
})

print(f"Total variants: {len(VARIANTS)}")


def now_iso() -> str:
    return datetime.now().astimezone().isoformat()


def write_status(payload: dict) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    STATUS_PATH.write_text(json.dumps(payload, indent=2))
    with HISTORY_PATH.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload) + "\n")


def load_checkpoint() -> dict:
    if not CHECKPOINT_PATH.exists():
        return {}
    try:
        payload = json.loads(CHECKPOINT_PATH.read_text())
        if int(payload.get("variants_total", 0) or 0) != len(VARIANTS):
            return {}
        return payload
    except Exception:
        return {}


def save_checkpoint(*, results: list[dict], completed_names: list[str], best_result: dict | None, data_context: dict, score_context: dict, trade_context: dict) -> None:
    CHECKPOINT_PATH.write_text(
        json.dumps(
            {
                "generated_at": now_iso(),
                "variants_total": len(VARIANTS),
                "completed_names": completed_names,
                "results": results,
                "best_result": best_result,
                "data_context": data_context,
                "scorecard_context": score_context,
                "tradebook_context": trade_context,
            },
            indent=2,
        )
    )


@contextmanager
def env_overrides(patch: dict):
    old = {k: os.environ.get(k) for k in patch.keys()}
    try:
        for k, v in patch.items():
            os.environ[k] = str(v)
        yield
    finally:
        for k, prev in old.items():
            if prev is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = prev


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    os.environ.setdefault("AT_LAB_MATCH_LIVE", "1")
    os.environ.setdefault("AT_LAB_RNN_ENABLED", "0")

    checkpoint = load_checkpoint()
    score_context = checkpoint.get("scorecard_context") or lab.load_scorecard_context()
    trade_context = checkpoint.get("tradebook_context") or lab.load_tradebook_context()
    fundamental_context = lab.load_fundamental_context()
    data_map, data_context = lab.load_data(trade_context, fundamental_context)

    total = len(VARIANTS)
    completed_names = list(checkpoint.get("completed_names") or [])
    stored_results = list(checkpoint.get("results") or [])
    results = [lab.BacktestResult(**row) for row in stored_results]
    done_set = set(completed_names)
    best_so_far = None
    for row in stored_results:
        candidate = lab.BacktestResult(**row)
        if best_so_far is None or (candidate.selection_score, candidate.total_return_pct) > (best_so_far.selection_score, best_so_far.total_return_pct):
            best_so_far = candidate

    write_status({
        "generated_at": now_iso(),
        "status": "running",
        "phase": "initializing",
        "message": "resuming sizing+exit sweep" if done_set else "starting sizing+exit sweep",
        "variants_total": total,
        "variants_done": len(done_set),
        "best_variant": best_so_far.name if best_so_far else None,
        "best_return_pct": best_so_far.total_return_pct if best_so_far else None,
        "best_score": best_so_far.selection_score if best_so_far else None,
    })

    for idx, variant in enumerate(VARIANTS, start=1):
        name = variant["name"]
        if name in done_set:
            continue
        buy_params = variant["buy"]
        sell_params = variant["sell"]
        env_patch = variant.get("env", {})
        extra_env = variant.get("extra_env", {})
        full_env = {**env_patch, **extra_env}

        with env_overrides(full_env):
            result = lab.run_variant(name, data_map, buy_params, sell_params, rnn_params={"enabled": False}, rnn_models={})

        result.params.setdefault("simulation", {})
        result.params["simulation"]["sizing_exit_sweep_env"] = dict(full_env)
        results.append(result)
        completed_names.append(name)
        done_set.add(name)

        if best_so_far is None or (result.selection_score, result.total_return_pct) > (best_so_far.selection_score, best_so_far.total_return_pct):
            best_so_far = result

        save_checkpoint(
            results=[asdict(r) for r in results],
            completed_names=completed_names,
            best_result=asdict(best_so_far) if best_so_far else None,
            data_context=data_context,
            score_context=score_context,
            trade_context=trade_context,
        )
        write_status({
            "generated_at": now_iso(),
            "status": "running",
            "phase": "evaluating_variants",
            "message": "sizing+exit sweep in progress",
            "variants_total": total,
            "variants_done": len(done_set),
            "current_variant": name,
            "best_variant": best_so_far.name,
            "best_return_pct": best_so_far.total_return_pct,
            "best_score": best_so_far.selection_score,
            "best_drawdown_pct": best_so_far.max_drawdown_pct,
            "progress_pct": round((len(done_set) / total) * 100.0, 1),
        })

    ranked = sorted(results, key=lambda r: (r.selection_score, r.total_return_pct, -abs(r.max_drawdown_pct), r.win_rate_pct), reverse=True)
    baseline = next(r for r in ranked if r.name == "fixed_size_baseline")
    best = ranked[0]
    payload = {
        "recommendation": {
            "generated_at": now_iso(),
            "lab_type": "sizing_exit_sweep",
            "production_rule_model": "BUY=RULE_SET_7, SELL=RULE_SET_2",
            "data_context": data_context,
            "scorecard_context": score_context,
            "tradebook_context": trade_context,
            "baseline": asdict(baseline),
            "best": asdict(best),
            "tested_variants": len(ranked),
            "improvement_return_pct": round(best.total_return_pct - baseline.total_return_pct, 2),
            "improvement_score": round(best.selection_score - baseline.selection_score, 3),
        },
        "ranked": [asdict(r) for r in ranked],
    }

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = OUT_DIR / f"sizing_exit_sweep_{ts}.json"
    csv_path = OUT_DIR / f"sizing_exit_sweep_{ts}.csv"
    json_path.write_text(json.dumps(payload, indent=2))

    # Compact CSV for quick analysis
    rows = []
    for r in ranked:
        env = r.params.get("simulation", {}).get("sizing_exit_sweep_env", {})
        rows.append({
            "name": r.name,
            "return_pct": r.total_return_pct,
            "drawdown_pct": r.max_drawdown_pct,
            "score": r.selection_score,
            "trades": r.trades,
            "win_rate_pct": r.win_rate_pct,
            "vol_sizing": env.get("AT_BACKTEST_VOL_SIZING_ENABLED", "0"),
            "risk_pct": env.get("AT_BACKTEST_RISK_PER_TRADE_PCT", ""),
            "atr_mult": env.get("AT_BACKTEST_ATR_STOP_MULT", ""),
            "max_pos_pct": env.get("AT_BACKTEST_MAX_POSITION_NOTIONAL_PCT", ""),
            "time_stop": env.get("AT_EQUITY_TIME_STOP_BARS", ""),
            "adx_min": r.params.get("buy", {}).get("adx_min", ""),
            "vol_mult": r.params.get("buy", {}).get("volume_confirm_mult", ""),
            "bep_pct": r.params.get("sell", {}).get("breakeven_trigger_pct", ""),
            "mom_rsi": r.params.get("sell", {}).get("momentum_exit_rsi", ""),
        })
    pd.DataFrame(rows).to_csv(csv_path, index=False)

    write_status({
        "generated_at": now_iso(),
        "status": "completed",
        "phase": "done",
        "message": "sizing+exit sweep complete",
        "best_variant": best.name,
        "best_return_pct": best.total_return_pct,
        "best_score": best.selection_score,
        "best_drawdown_pct": best.max_drawdown_pct,
        "variants_total": total,
        "variants_done": total,
        "output_json": str(json_path),
        "output_csv": str(csv_path),
    })
    if CHECKPOINT_PATH.exists():
        CHECKPOINT_PATH.unlink()

    print(f"\n=== TOP 10 ===")
    for r in ranked[:10]:
        env = r.params.get("simulation", {}).get("sizing_exit_sweep_env", {})
        print(f"  {r.name:35s} ret={r.total_return_pct:7.2f}%  dd={r.max_drawdown_pct:7.2f}%  score={r.selection_score:7.3f}  trades={r.trades:4d}  win={r.win_rate_pct:5.1f}%")
    print(f"\nBaseline: {baseline.total_return_pct:.2f}%")
    print(f"Best: {best.name} → {best.total_return_pct:.2f}% (dd={best.max_drawdown_pct:.2f}%)")
    print(f"Saved: {json_path}")
    print(f"Saved: {csv_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())