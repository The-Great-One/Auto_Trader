#!/usr/bin/env python3
"""Optuna-driven strategy hunt for Auto_Trader equity rules.

Purpose:
- Move beyond brute-force grids.
- Store every trial durably.
- Optimize toward robust, trade-dense, drawdown-aware results on Kite 5Y data.

This script intentionally reuses weekly_strategy_lab's live-parity-ish simple simulator
and data loader so results remain comparable to the latest lab run.
"""

from __future__ import annotations

import json
import math
import os
import sys
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any

# Keep historical backtests clean and comparable.
os.environ.setdefault("AT_DISABLE_FILE_LOGGING", "1")
os.environ.setdefault("AT_LAB_HISTORY_PERIOD", "5y")
os.environ.setdefault("AT_LAB_MODE", "1")
os.environ.setdefault("AT_LAB_MATCH_LIVE", "0")
os.environ.setdefault("AT_LAB_REGIME_FILTER_ENABLED", "0")
os.environ.setdefault("AT_LAB_MIN_BARS", "260")
# Do not fetch during research hunts; use the already-synced Kite feather cache.
os.environ.setdefault("AT_LAB_PRECACHE", "0")
os.environ.setdefault("AT_BACKTEST_MAX_POSITION_NOTIONAL_PCT", "0.15")
os.environ.setdefault("AT_BACKTEST_RISK_PER_TRADE_PCT", "0.02")
os.environ.setdefault("AT_MAX_SINGLE_SYMBOL_WEIGHT", "0.50")
os.environ.setdefault("FUND_ALLOCATION", "15000")
os.environ.setdefault("AT_BACKTEST_STARTING_CAPITAL", "100000")

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import optuna  # noqa: E402

import scripts.weekly_strategy_lab as lab  # noqa: E402

REPORT_DIR = REPO / "reports"
STATUS_PATH = REPO / "intermediary_files" / "lab_status" / "optuna_strategy_hunt_status.json"
DB_PATH = REPO / "intermediary_files" / "optuna" / "strategy_hunt.sqlite3"
TRIALS_JSONL = REPO / "reports" / "optuna_strategy_hunt_trials.jsonl"
HIST_DIR = REPO / "intermediary_files" / "Hist_Data"

# Same 103-symbol universe from the completed 2026-04-28 lab, so Iteration 1 is comparable.
SEED_REPORT = REPORT_DIR / "strategy_lab_20260428_235524.json"


def _write_status(**updates: Any) -> None:
    STATUS_PATH.parent.mkdir(parents=True, exist_ok=True)
    current: dict[str, Any] = {}
    if STATUS_PATH.exists():
        try:
            current = json.loads(STATUS_PATH.read_text())
        except Exception:
            current = {}
    current.update(updates)
    current["updated_at"] = datetime.now().isoformat()
    tmp = STATUS_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(current, indent=2, default=str))
    os.replace(tmp, STATUS_PATH)


def _append_trial(payload: dict[str, Any]) -> None:
    TRIALS_JSONL.parent.mkdir(parents=True, exist_ok=True)
    with TRIALS_JSONL.open("a") as f:
        f.write(json.dumps(payload, default=str, sort_keys=True) + "\n")


def _seed_symbols_from_report() -> list[str]:
    if not SEED_REPORT.exists():
        return []
    try:
        d = json.loads(SEED_REPORT.read_text())
        symbols = d["recommendation"]["baseline"].get("symbols_tested", [])
        return [str(s).strip().upper() for s in symbols if str(s).strip()]
    except Exception:
        return []


def _sample_params(trial: optuna.Trial) -> tuple[dict[str, Any], dict[str, Any]]:
    """Sample around the best region, but allow structural variation.

    Previous lab result: trade density improved, but returns were weak and DD high.
    This search focuses on:
    - keeping density high enough,
    - cutting drawdown / whipsaw exits,
    - testing slightly tighter trend-quality gates around the ultra-loose winners.
    """
    archetype = trial.suggest_categorical(
        "archetype",
        ["ultra_loose", "loose_quality", "mean_reversion", "trend_quality", "hold_winners"],
    )

    if archetype == "ultra_loose":
        buy = {
            "adx_min": trial.suggest_categorical("adx_min", [4, 5, 6, 7, 8]),
            "volume_confirm_mult": trial.suggest_float("volume_confirm_mult", 0.55, 0.82, step=0.05),
            "ich_cloud_bull": 0,
            "vwap_buy_above": trial.suggest_categorical("vwap_buy_above", [0, 0, 1]),
            "rsi_floor": trial.suggest_int("rsi_floor", 28, 38, step=2),
            "stoch_pull_max": trial.suggest_int("stoch_pull_max", 88, 100, step=2),
            "max_extension_atr": trial.suggest_float("max_extension_atr", 3.0, 5.0, step=0.25),
            "max_obv_zscore": trial.suggest_float("max_obv_zscore", 4.0, 7.0, step=0.5),
            "cci_buy_min": trial.suggest_int("cci_buy_min", -225, -100, step=25),
            "cmf_base_min": trial.suggest_categorical("cmf_base_min", [-0.05, -0.02, 0.0, 0.02]),
            "mmi_risk_off": trial.suggest_categorical("mmi_risk_off", [70, 75, 80, 85]),
        }
    elif archetype == "mean_reversion":
        buy = {
            "adx_min": trial.suggest_categorical("adx_min", [4, 6, 8, 10]),
            "volume_confirm_mult": trial.suggest_float("volume_confirm_mult", 0.55, 0.9, step=0.05),
            "ich_cloud_bull": 0,
            "vwap_buy_above": 0,
            "rsi_floor": trial.suggest_int("rsi_floor", 26, 36, step=2),
            "stoch_pull_max": trial.suggest_int("stoch_pull_max", 90, 100, step=2),
            "cci_buy_min": trial.suggest_int("cci_buy_min", -250, -125, step=25),
            "max_extension_atr": trial.suggest_float("max_extension_atr", 2.5, 4.5, step=0.25),
            "obv_min_zscore": trial.suggest_categorical("obv_min_zscore", [-0.5, -0.25, 0.0, 0.25]),
            "cmf_base_min": trial.suggest_categorical("cmf_base_min", [-0.08, -0.05, -0.02, 0.0]),
        }
    elif archetype == "trend_quality":
        buy = {
            "adx_min": trial.suggest_categorical("adx_min", [8, 10, 12, 14]),
            "adx_strong_min": trial.suggest_categorical("adx_strong_min", [16, 18, 20, 22]),
            "volume_confirm_mult": trial.suggest_float("volume_confirm_mult", 0.75, 1.15, step=0.05),
            "ich_cloud_bull": trial.suggest_categorical("ich_cloud_bull", [0, 1]),
            "vwap_buy_above": trial.suggest_categorical("vwap_buy_above", [0, 1]),
            "rsi_floor": trial.suggest_int("rsi_floor", 36, 46, step=2),
            "stoch_pull_max": trial.suggest_int("stoch_pull_max", 78, 94, step=2),
            "cci_buy_min": trial.suggest_int("cci_buy_min", -150, -50, step=25),
            "max_extension_atr": trial.suggest_float("max_extension_atr", 2.0, 3.5, step=0.25),
        }
    elif archetype == "hold_winners":
        buy = {
            "adx_min": trial.suggest_categorical("adx_min", [5, 6, 8, 10]),
            "volume_confirm_mult": trial.suggest_float("volume_confirm_mult", 0.6, 0.9, step=0.05),
            "ich_cloud_bull": 0,
            "vwap_buy_above": trial.suggest_categorical("vwap_buy_above", [0, 0, 1]),
            "rsi_floor": trial.suggest_int("rsi_floor", 32, 40, step=2),
            "stoch_pull_max": trial.suggest_int("stoch_pull_max", 86, 98, step=2),
            "cci_buy_min": trial.suggest_int("cci_buy_min", -200, -100, step=25),
            "max_extension_atr": trial.suggest_float("max_extension_atr", 3.0, 4.5, step=0.25),
            "max_obv_zscore": trial.suggest_float("max_obv_zscore", 4.0, 6.0, step=0.5),
        }
    else:  # loose_quality
        buy = {
            "adx_min": trial.suggest_categorical("adx_min", [6, 8, 10, 12]),
            "volume_confirm_mult": trial.suggest_float("volume_confirm_mult", 0.65, 0.95, step=0.05),
            "ich_cloud_bull": 0,
            "vwap_buy_above": trial.suggest_categorical("vwap_buy_above", [0, 1]),
            "rsi_floor": trial.suggest_int("rsi_floor", 32, 42, step=2),
            "stoch_pull_max": trial.suggest_int("stoch_pull_max", 84, 98, step=2),
            "cci_buy_min": trial.suggest_int("cci_buy_min", -200, -75, step=25),
            "max_extension_atr": trial.suggest_float("max_extension_atr", 2.5, 4.0, step=0.25),
            "obv_min_zscore": trial.suggest_categorical("obv_min_zscore", [-0.25, 0.0, 0.25, 0.5]),
            "cmf_base_min": trial.suggest_categorical("cmf_base_min", [-0.02, 0.0, 0.02, 0.05]),
        }

    sell_style = trial.suggest_categorical(
        "sell_style",
        ["baseline", "quick_cut", "wide_breakeven", "long_hold", "momentum_capture"],
    )
    if sell_style == "quick_cut":
        sell = {
            "equity_time_stop_bars": trial.suggest_int("equity_time_stop_bars", 4, 10, step=1),
            "equity_review_rsi": trial.suggest_float("equity_review_rsi", 42, 52, step=2),
            "momentum_exit_rsi": trial.suggest_float("momentum_exit_rsi", 35, 44, step=1),
            "breakeven_trigger_pct": trial.suggest_float("breakeven_trigger_pct", 1.5, 3.5, step=0.5),
        }
    elif sell_style == "wide_breakeven":
        sell = {
            "breakeven_trigger_pct": trial.suggest_float("breakeven_trigger_pct", 4.0, 7.0, step=0.5),
            "equity_review_rsi": trial.suggest_float("equity_review_rsi", 38, 46, step=2),
            "fund_time_stop_min_profit_pct": trial.suggest_float("fund_time_stop_min_profit_pct", 0.3, 1.5, step=0.3),
        }
    elif sell_style == "long_hold":
        sell = {
            "equity_time_stop_bars": trial.suggest_int("equity_time_stop_bars", 12, 30, step=3),
            "fund_time_stop_bars": trial.suggest_int("fund_time_stop_bars", 18, 36, step=3),
            "breakeven_trigger_pct": trial.suggest_float("breakeven_trigger_pct", 3.5, 6.0, step=0.5),
            "momentum_exit_rsi": trial.suggest_float("momentum_exit_rsi", 32, 40, step=1),
        }
    elif sell_style == "momentum_capture":
        sell = {
            "relative_volume_exit": trial.suggest_float("relative_volume_exit", 1.2, 2.0, step=0.1),
            "momentum_exit_rsi": trial.suggest_float("momentum_exit_rsi", 32, 42, step=1),
            "equity_review_rsi": trial.suggest_float("equity_review_rsi", 38, 46, step=2),
            "equity_time_stop_bars": trial.suggest_int("equity_time_stop_bars", 10, 24, step=2),
        }
    else:
        sell = {}

    return buy, sell


def _objective_factory(data_map: dict[str, Any], baseline_return: float, min_trades: int, trial_limit: int):
    def objective(trial: optuna.Trial) -> float:
        buy, sell = _sample_params(trial)
        name = f"optuna_{trial.number:05d}_{trial.params.get('archetype', 'x')}_{trial.params.get('sell_style', 'x')}"
        _write_status(
            status="running",
            phase="trial",
            trial_number=trial.number,
            trial_limit=trial_limit,
            current_variant=name,
            progress_pct=round((trial.number / max(1, trial_limit)) * 100.0, 1),
        )
        result = lab.run_variant(name, data_map, buy, sell, rnn_params={"enabled": False}, rnn_models={})

        # Objective: reward returns and density; penalize drawdown and dead/no-trade behavior.
        density_bonus = min(float(result.trades) / max(1, min_trades), 1.5)
        dd_penalty = max(0.0, abs(float(result.max_drawdown_pct)) - 12.0) * 0.18
        no_trade_penalty = 15.0 if result.trades < max(50, min_trades // 4) else 0.0
        low_edge_penalty = 3.0 if result.total_return_pct <= baseline_return else 0.0
        score = (
            float(result.total_return_pct) * 2.0
            + float(result.selection_score) * 0.35
            + density_bonus
            + (float(result.win_rate_pct) - 50.0) * 0.08
            - dd_penalty
            - no_trade_penalty
            - low_edge_penalty
        )
        payload = {
            "ts": datetime.now().isoformat(),
            "trial": trial.number,
            "objective": round(float(score), 6),
            "result": asdict(result),
        }
        _append_trial(payload)
        trial.set_user_attr("result", asdict(result))
        trial.set_user_attr("objective_components", {
            "density_bonus": density_bonus,
            "dd_penalty": dd_penalty,
            "no_trade_penalty": no_trade_penalty,
            "low_edge_penalty": low_edge_penalty,
        })
        return float(score)

    return objective


def _cached_symbols_from_feather() -> list[str]:
    """Return symbol names that have cached feather files in Hist_Data."""
    if not HIST_DIR.exists():
        return []
    return sorted(
        f.stem.upper()
        for f in HIST_DIR.glob("*.feather")
        if f.stat().st_size > 1024  # skip stubs
    )


def main() -> None:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    seed_symbols = _seed_symbols_from_report()
    cached_symbols = _cached_symbols_from_feather()

    if os.getenv("AT_LAB_SYMBOLS", "").strip():
        pass  # explicit override — use it as-is
    elif seed_symbols:
        # Only use seed symbols that exist in the feather cache on this machine.
        available = [s for s in seed_symbols if s in cached_symbols]
        if available:
            os.environ["AT_LAB_SYMBOLS"] = ",".join(available)
            os.environ["AT_LAB_USE_APPROVED_UNIVERSE"] = "0"
            print(f"Using {len(available)}/{len(seed_symbols)} seed symbols available in feather cache")
        else:
            print(f"WARNING: 0/{len(seed_symbols)} seed symbols in cache; falling back to cached symbols")
            os.environ["AT_LAB_SYMBOLS"] = ",".join(cached_symbols[:120])
            os.environ["AT_LAB_USE_APPROVED_UNIVERSE"] = "0"
    elif cached_symbols:
        os.environ["AT_LAB_SYMBOLS"] = ",".join(cached_symbols[:120])
        os.environ["AT_LAB_USE_APPROVED_UNIVERSE"] = "0"
        print(f"Using {min(120, len(cached_symbols))} symbols from feather cache")

    n_trials = int(os.getenv("AT_OPTUNA_TRIALS", "160"))
    study_name = os.getenv("AT_OPTUNA_STUDY", "equity_rs7_iter1_103sym")

    _write_status(status="running", phase="loading_data", trial_limit=n_trials, study_name=study_name)
    scorecard_context = lab.load_scorecard_context()
    tradebook_context = lab.load_tradebook_context()
    # Avoid live broker/fundamental calls here (they can require TOTP and break detached hunts).
    # With AT_LAB_SYMBOLS seeded from the latest report, load_data does not need fundamentals.
    fundamental_context = {
        "fundamentals_found": False,
        "approved_equities": [],
        "approved_etfs": [],
        "code_findings": ["disabled_for_optuna_cached_history_hunt"],
    }
    data_map, data_context = lab.load_data(tradebook_context, fundamental_context)
    baseline = lab.run_variant("optuna_baseline_current", data_map, {}, {}, rnn_params={"enabled": False}, rnn_models={})

    min_trades = max(200, int(baseline.trades * 1.2))
    _write_status(
        status="running",
        phase="optimizing",
        trial_limit=n_trials,
        study_name=study_name,
        universe_size=len(data_map),
        symbols_loaded=len(data_map),
        baseline=asdict(baseline),
        min_trades=min_trades,
        data_context=data_context,
    )

    sampler = optuna.samplers.TPESampler(seed=42, multivariate=True, group=True)
    pruner = optuna.pruners.NopPruner()
    study = optuna.create_study(
        study_name=study_name,
        storage=f"sqlite:///{DB_PATH}",
        load_if_exists=True,
        direction="maximize",
        sampler=sampler,
        pruner=pruner,
    )
    completed = len([t for t in study.trials if t.state == optuna.trial.TrialState.COMPLETE])
    remaining = max(0, n_trials - completed)
    if remaining:
        study.optimize(
            _objective_factory(data_map, baseline.total_return_pct, min_trades, n_trials),
            n_trials=remaining,
            gc_after_trial=True,
            show_progress_bar=False,
        )

    complete_trials = [t for t in study.trials if t.state == optuna.trial.TrialState.COMPLETE]
    ranked = sorted(complete_trials, key=lambda t: float(t.value if t.value is not None else -1e9), reverse=True)
    top_payload = []
    for t in ranked[:20]:
        top_payload.append({
            "trial": t.number,
            "value": t.value,
            "params": t.params,
            "result": t.user_attrs.get("result"),
            "objective_components": t.user_attrs.get("objective_components"),
        })

    best = top_payload[0] if top_payload else None
    wf = None
    if best and best.get("result"):
        _write_status(status="running", phase="walk_forward_best", best_trial=best["trial"])
        try:
            wf = lab.run_walk_forward_validation(
                data_map,
                best["result"].get("params", {}).get("buy", {}),
                best["result"].get("params", {}).get("sell", {}),
                n_splits=5,
            )
        except Exception as exc:
            wf = {"error": str(exc)}

    payload = {
        "generated_at": datetime.now().isoformat(),
        "study_name": study_name,
        "storage": str(DB_PATH),
        "trials_requested": n_trials,
        "trials_complete": len(complete_trials),
        "universe_size": len(data_map),
        "seed_report": str(SEED_REPORT),
        "baseline": asdict(baseline),
        "best": best,
        "top_trials": top_payload,
        "walk_forward_best": wf,
        "next_gate": {
            "promote_candidate": bool(
                best
                and best.get("result")
                and best["result"].get("total_return_pct", 0) >= 8.0
                and best["result"].get("trades", 0) >= min_trades
                and abs(best["result"].get("max_drawdown_pct", 999)) <= abs(baseline.max_drawdown_pct) + 1.0
                and wf
                and wf.get("mean_oos_return_pct", 0) > 0
                and wf.get("positive_folds", 0) >= 3
            ),
            "min_total_return_pct": 8.0,
            "min_trades": min_trades,
            "min_positive_wf_folds": 3,
        },
    }
    out = REPORT_DIR / f"optuna_strategy_hunt_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    out.write_text(json.dumps(payload, indent=2, default=str))
    _write_status(
        status="done",
        phase="completed",
        progress_pct=100.0,
        latest_report=str(out),
        best_trial=best.get("trial") if best else None,
        best_value=best.get("value") if best else None,
        best_return_pct=(best.get("result") or {}).get("total_return_pct") if best else None,
        best_trades=(best.get("result") or {}).get("trades") if best else None,
    )
    print(json.dumps(payload, indent=2, default=str)[:6000])
    print(f"Saved: {out}")


if __name__ == "__main__":
    main()
