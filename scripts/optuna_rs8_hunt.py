#!/usr/bin/env python3
"""Optuna-driven strategy hunt for RULE_SET_8 (Adaptive Regime-Switching).

Purpose:
- Break through the zero-trades-in-sideways problem by explicitly searching
  regime-switching archetypes where mean-reversion entries fire in sideways
  markets and trend-following entries fire in bull markets.
- Reuses weekly_strategy_lab's simple simulator with RS8's evaluate_signal
  patched in as the buy decision function.

Key difference from the original optuna_strategy_hunt.py:
- Adds regime_switch archetypes that tune RS8 regime detection thresholds
  AND per-regime entry parameters (bull + sideways) simultaneously.
- The backtest uses RULE_SET_8.evaluate_signal instead of RS7 buy_or_sell.
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

os.environ.setdefault("AT_DISABLE_FILE_LOGGING", "1")
os.environ.setdefault("AT_LAB_HISTORY_PERIOD", "5y")
os.environ.setdefault("AT_LAB_MODE", "1")
os.environ.setdefault("AT_LAB_MATCH_LIVE", "0")
os.environ.setdefault("AT_LAB_REGIME_FILTER_ENABLED", "0")
os.environ.setdefault("AT_LAB_MIN_BARS", "260")
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
import Auto_Trader.RULE_SET_7 as rs7_mod  # noqa: E402
import Auto_Trader.RULE_SET_8 as rs8_mod  # noqa: E402
import scripts.weekly_strategy_lab as lab  # noqa: E402

REPORT_DIR = REPO / "reports"
STATUS_PATH = REPO / "intermediary_files" / "lab_status" / "optuna_rs8_hunt_status.json"
DB_PATH = REPO / "intermediary_files" / "optuna" / "rs8_strategy_hunt.sqlite3"
TRIALS_JSONL = REPO / "reports" / "optuna_rs8_hunt_trials.jsonl"
HIST_DIR = REPO / "intermediary_files" / "Hist_Data"

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


def _cached_symbols_from_feather() -> list[str]:
    if not HIST_DIR.exists():
        return []
    return sorted(
        f.stem.upper()
        for f in HIST_DIR.glob("*.feather")
        if f.stat().st_size > 1024
    )


def _sample_params(trial: optuna.Trial) -> tuple[dict[str, Any], dict[str, Any]]:
    """Sample regime-switching parameter space for RS8.

    The search focuses on:
    1. Regime detection thresholds (when do we classify bull/sideways/bear?)
    2. Bull regime entry: trend-following (breakout/pullback)
    3. Sideways regime entry: mean-reversion (oversold bounce)
    4. Exit/sell style: quick_cut, wide_breakeven, long_hold
    """
    archetype = trial.suggest_categorical(
        "archetype",
        ["regime_switch_balanced", "regime_switch_aggressive", "regime_switch_conservative"],
    )
    a = archetype

    # ── Regime detection ──
    regime_adx_threshold = trial.suggest_categorical(f"{a}_regime_adx", [15, 18, 20, 22, 25])
    regime_adx_sideways_max = trial.suggest_categorical(f"{a}_regime_adx_sideways_max", [20, 25, 30, 35])

    # ── Bull regime gates ──
    bull_adx_min = trial.suggest_categorical(f"{a}_bull_adx_min", [4, 6, 8, 10])
    bull_volume_mult = trial.suggest_float(f"{a}_bull_vol_mult", 0.3, 0.9, step=0.1)
    bull_rsi_floor = trial.suggest_int(f"{a}_bull_rsi_floor", 28, 40, step=2)
    bull_cmf_min = trial.suggest_categorical(f"{a}_bull_cmf_min", [-0.10, -0.05, 0.0, 0.02])
    bull_max_extension_atr = trial.suggest_float(f"{a}_bull_max_ext_atr", 3.0, 5.0, step=0.5)

    # ── Sideways regime gates (mean-reversion) ──
    if "aggressive" in archetype:
        side_rsi_oversold_range = [25, 30, 35, 40]
        side_stoch_oversold_range = [15, 20, 25, 30]
        side_cmf_min_range = [-0.15, -0.10, -0.05, 0.0]
    elif "conservative" in archetype:
        side_rsi_oversold_range = [30, 35, 40, 45]
        side_stoch_oversold_range = [20, 25, 30, 35]
        side_cmf_min_range = [-0.05, 0.0, 0.02, 0.05]
    else:  # balanced
        side_rsi_oversold_range = [28, 32, 35, 38]
        side_stoch_oversold_range = [20, 25, 30]
        side_cmf_min_range = [-0.10, -0.05, 0.0, 0.02]

    side_rsi_oversold = trial.suggest_categorical(f"{a}_side_rsi_os", side_rsi_oversold_range)
    side_rsi_oversold_exit = trial.suggest_categorical(f"{a}_side_rsi_os_exit", [45, 50, 55, 60])
    side_stoch_k_oversold = trial.suggest_categorical(f"{a}_side_stoch_os", side_stoch_oversold_range)
    side_cmf_min = trial.suggest_categorical(f"{a}_side_cmf_min", side_cmf_min_range)
    side_volume_min_mult = trial.suggest_float(f"{a}_side_vol_mult", 0.3, 0.9, step=0.1)
    side_bb_lower_touch = trial.suggest_categorical(f"{a}_side_bb_touch", [0, 1])
    side_adx_max = trial.suggest_categorical(f"{a}_side_adx_max", [25, 30, 35, 40])

    buy = {
        # Regime detection
        "regime_mode": "auto",
        "regime_adx_threshold": regime_adx_threshold,
        "regime_adx_sideways_max": regime_adx_sideways_max,
        # Bull
        "bull_adx_min": bull_adx_min,
        "bull_volume_confirm_mult": bull_volume_mult,
        "bull_rsi_floor": bull_rsi_floor,
        "bull_cmf_min": bull_cmf_min,
        "bull_max_extension_atr": bull_max_extension_atr,
        "bull_obv_min_zscore": -1.0,
        "bull_min_atr_pct": 0.0,
        "bull_max_atr_pct": 0.12,
        "bull_macd_required": 0,
        # Sideways
        "side_rsi_oversold": side_rsi_oversold,
        "side_rsi_oversold_exit": side_rsi_oversold_exit,
        "side_stoch_k_oversold": side_stoch_k_oversold,
        "side_cmf_min": side_cmf_min,
        "side_volume_min_mult": side_volume_min_mult,
        "side_bb_lower_touch": side_bb_lower_touch,
        "side_adx_max": side_adx_max,
        # Bear: allow sideways-style mean-reversion entries? (0=no, 1=yes)
        "bear_allow_longs": trial.suggest_categorical(f"{a}_bear_allow_longs", [0, 1]),
        # Risk
        "max_position_loss_pct": 5.0,
        "sector_cap_pct": 25.0,
        "trailing_stop_atr_mult": trial.suggest_float(f"{a}_trail_atr", 2.5, 4.5, step=0.5),
        "breakeven_trigger_pct": trial.suggest_float(f"{a}_bep_trigger", 1.5, 4.0, step=0.5),
        "time_stop_bars": trial.suggest_categorical(f"{a}_time_stop", [10, 15, 20, 25]),
        # Legacy compat
        "regime_filter_enabled": 0,
        "regime_ema_fast": 50,
        "regime_ema_slow": 200,
    }

    # ── Sell style ──
    sell_style = trial.suggest_categorical(
        "sell_style",
        ["quick_cut", "wide_breakeven", "long_hold"],
    )
    s = sell_style

    if sell_style == "quick_cut":
        sell = {
            "equity_time_stop_bars": trial.suggest_int(f"{s}_equity_time_stop_bars", 4, 10, step=1),
            "equity_review_rsi": trial.suggest_float(f"{s}_equity_review_rsi", 42, 52, step=2),
            "momentum_exit_rsi": trial.suggest_float(f"{s}_momentum_exit_rsi", 35, 44, step=1),
            "breakeven_trigger_pct": trial.suggest_float(f"{s}_breakeven_trigger_pct", 1.5, 3.5, step=0.5),
        }
    elif sell_style == "wide_breakeven":
        sell = {
            "breakeven_trigger_pct": trial.suggest_float(f"{s}_breakeven_trigger_pct", 4.0, 7.0, step=0.5),
            "equity_review_rsi": trial.suggest_float(f"{s}_equity_review_rsi", 38, 46, step=2),
            "fund_time_stop_min_profit_pct": trial.suggest_float(f"{s}_fund_time_stop_min_profit_pct", 0.3, 1.5, step=0.3),
        }
    else:  # long_hold
        sell = {
            "equity_time_stop_bars": trial.suggest_int(f"{s}_equity_time_stop_bars", 12, 30, step=3),
            "fund_time_stop_bars": trial.suggest_int(f"{s}_fund_time_stop_bars", 18, 36, step=3),
            "breakeven_trigger_pct": trial.suggest_float(f"{s}_breakeven_trigger_pct", 3.5, 6.0, step=0.5),
            "momentum_exit_rsi": trial.suggest_float(f"{s}_momentum_exit_rsi", 32, 40, step=1),
        }

    return buy, sell


def _apply_rs8_config(buy_params: dict[str, Any]) -> None:
    """Patch RULE_SET_8.CONFIG with the sampled parameters before backtesting."""
    for key, value in buy_params.items():
        if key in rs8_mod.CONFIG:
            rs8_mod.CONFIG[key] = value


def _make_rs8_wrapper() -> Any:
    """Create a buy_or_sell-compatible wrapper around RS8.evaluate_signal.

    RS8.evaluate_signal returns (decision_str, diagnostics_dict) tuple,
    but RS7.buy_or_sell returns just a string. The lab checks
    str(sig).upper() == 'BUY' — so we must return a plain string.
    """
    def _rs8_buy_or_sell(df, row, holdings):
        decision, _diag = rs8_mod.evaluate_signal(df, row, holdings)
        return str(decision).upper()  # Return plain string like RS7
    return _rs8_buy_or_sell


def _run_variant_rs8(name: str, data_map: dict, buy: dict, sell: dict, **kwargs) -> Any:
    """Run a backtest variant using RS8's evaluate_signal as the buy decision.

    The lab hardcodes RULE_SET_7.buy_or_sell for buy signals (line ~1018).
    We monkey-patch it with a wrapper that calls RS8.evaluate_signal
    and extracts just the decision string (since RS8 returns a tuple
    but the lab expects a plain string like RS7.buy_or_sell returns).
    """
    _apply_rs8_config(buy_params=buy)

    # Save the original and replace with RS8 wrapper
    original_buy_or_sell = rs7_mod.buy_or_sell
    rs7_mod.buy_or_sell = _make_rs8_wrapper()

    try:
        result = lab.run_variant(name, data_map, buy, sell, rnn_params={"enabled": False}, rnn_models={})
    finally:
        # Restore original
        rs7_mod.buy_or_sell = original_buy_or_sell

    return result


def _objective_factory(data_map: dict[str, Any], baseline_return: float, min_trades: int, trial_limit: int):
    def objective(trial: optuna.Trial) -> float:
        buy, sell = _sample_params(trial)
        archetype = trial.params.get("archetype", "unknown")
        sell_style = trial.params.get("sell_style", "unknown")
        name = f"rs8_{trial.number:05d}_{archetype}_{sell_style}"
        _write_status(
            status="running",
            phase="trial",
            trial_number=trial.number,
            trial_limit=trial_limit,
            current_variant=name,
            progress_pct=round((trial.number / max(1, trial_limit)) * 100.0, 1),
        )
        result = _run_variant_rs8(name, data_map, buy, sell)

        # Objective: heavily reward density, penalize zero-trade folds, reward returns.
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


def main() -> None:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    cached_symbols = _cached_symbols_from_feather()
    seed_symbols = []
    if SEED_REPORT.exists():
        try:
            d = json.loads(SEED_REPORT.read_text())
            seed_symbols = d["recommendation"]["baseline"].get("symbols_tested", [])
            seed_symbols = [str(s).strip().upper() for s in seed_symbols if str(s).strip()]
        except Exception:
            pass

    if os.getenv("AT_LAB_SYMBOLS", "").strip():
        pass
    elif seed_symbols:
        available = [s for s in seed_symbols if s in cached_symbols]
        if available:
            os.environ["AT_LAB_SYMBOLS"] = ",".join(available)
            os.environ["AT_LAB_USE_APPROVED_UNIVERSE"] = "0"
            print(f"Using {len(available)}/{len(seed_symbols)} seed symbols from feather cache")
        else:
            os.environ["AT_LAB_SYMBOLS"] = ",".join(cached_symbols[:120])
            os.environ["AT_LAB_USE_APPROVED_UNIVERSE"] = "0"
    elif cached_symbols:
        os.environ["AT_LAB_SYMBOLS"] = ",".join(cached_symbols[:120])
        os.environ["AT_LAB_USE_APPROVED_UNIVERSE"] = "0"
        print(f"Using {min(120, len(cached_symbols))} symbols from feather cache")

    n_trials = int(os.getenv("AT_OPTUNA_TRIALS", "200"))
    study_name = os.getenv("AT_OPTUNA_STUDY", "equity_rs8_regime_switch_v1")

    _write_status(status="running", phase="loading_data", trial_limit=n_trials, study_name=study_name)
    scorecard_context = lab.load_scorecard_context()
    tradebook_context = lab.load_tradebook_context()
    fundamental_context = {
        "fundamentals_found": False,
        "approved_equities": [],
        "approved_etfs": [],
        "code_findings": ["disabled_for_optuna_rs8_hunt"],
    }
    data_map, data_context = lab.load_data(tradebook_context, fundamental_context)
    baseline = _run_variant_rs8("optuna_rs8_baseline", data_map, {}, {})

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
    print(f"Study: {study_name}, completed: {completed}, remaining: {remaining}/{n_trials}")

    if remaining:
        study.optimize(
            _objective_factory(data_map, baseline.total_return_pct, min_trades, n_trials),
            n_trials=remaining,
            gc_after_trial=True,
            show_progress_bar=False,
        )

    # ── Results ──
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
            # Re-apply RS8 config for WF validation
            best_buy = best["result"].get("params", {}).get("buy", {})
            _apply_rs8_config(best_buy)
            original_buy_or_sell_wf = rs7_mod.buy_or_sell
            rs7_mod.buy_or_sell = _make_rs8_wrapper()
            try:
                wf = lab.run_walk_forward_validation(
                    data_map,
                    best_buy,
                    best["result"].get("params", {}).get("sell", {}),
                    n_splits=4,
                )
            finally:
                rs7_mod.buy_or_sell = original_buy_or_sell_wf
        except Exception as exc:
            wf = {"error": str(exc)}
        # RS7 already restored in the inner finally block

    payload = {
        "generated_at": datetime.now().isoformat(),
        "study_name": study_name,
        "hunt_type": "regime_switch_rs8",
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
    out = REPORT_DIR / f"optuna_rs8_hunt_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
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
    print(f"Saved: {out}")


if __name__ == "__main__":
    main()