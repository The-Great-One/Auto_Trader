#!/usr/bin/env python3
"""Targeted 5Y hunt for 30% CAGR.

Anchors on the proven live-parity winner (`exit_bep3.0_ts6`) and searches a
small but aggressive neighborhood around exposure, concentration, and entry
sensitivity. This avoids wasting time on tiny daily tweaks that cannot bridge
from ~21% CAGR to 30%.
"""
from __future__ import annotations

import json
import os
import sys
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import weekly_strategy_lab as lab
from scripts.five_year_validate_report_winner import _compute_curve_metrics, _load_report, _pick_variant
from scripts.five_year_validation import PERIOD, load_5y_data
from scripts.weekly_universe_cagr_check import run_baseline_detailed

OUT_DIR = ROOT / "reports"
OUT_DIR.mkdir(parents=True, exist_ok=True)
STATUS_PATH = OUT_DIR / "thirty_cagr_hunt_latest.json"
HISTORY_PATH = OUT_DIR / "thirty_cagr_hunt_history.jsonl"
CHECKPOINT_PATH = OUT_DIR / "thirty_cagr_hunt_checkpoint.json"
DEFAULT_REPORT = OUT_DIR / "sizing_exit_sweep_latest.json"
DEFAULT_VARIANT = "exit_bep3.0_ts6"

EXPOSURE_PROFILES = {
    "scale4_base": {
        "risk_per_trade_pct": 0.04,
        "atr_stop_mult": 2.5,
        "max_position_notional_pct": 0.80,
        "target_equity": 1.00,
        "max_single_symbol_weight": 0.50,
    },
    "scale5": {
        "risk_per_trade_pct": 0.05,
        "atr_stop_mult": 2.5,
        "max_position_notional_pct": 1.00,
        "target_equity": 1.00,
        "max_single_symbol_weight": 0.65,
    },
    "scale6": {
        "risk_per_trade_pct": 0.06,
        "atr_stop_mult": 2.5,
        "max_position_notional_pct": 1.00,
        "target_equity": 1.00,
        "max_single_symbol_weight": 0.80,
    },
    "tight5": {
        "risk_per_trade_pct": 0.05,
        "atr_stop_mult": 2.0,
        "max_position_notional_pct": 1.00,
        "target_equity": 1.00,
        "max_single_symbol_weight": 0.65,
    },
    "tight6": {
        "risk_per_trade_pct": 0.06,
        "atr_stop_mult": 2.0,
        "max_position_notional_pct": 1.00,
        "target_equity": 1.00,
        "max_single_symbol_weight": 0.80,
    },
    "wide5": {
        "risk_per_trade_pct": 0.05,
        "atr_stop_mult": 3.0,
        "max_position_notional_pct": 1.00,
        "target_equity": 1.00,
        "max_single_symbol_weight": 0.65,
    },
    "focused5": {
        "risk_per_trade_pct": 0.05,
        "atr_stop_mult": 2.5,
        "max_position_notional_pct": 1.00,
        "target_equity": 1.00,
        "max_single_symbol_weight": 1.00,
    },
    # ── Pass 2: aggressive profiles to push toward 30% CAGR ──
    "aggressive7": {
        "risk_per_trade_pct": 0.07,
        "atr_stop_mult": 2.0,
        "max_position_notional_pct": 1.20,
        "target_equity": 1.00,
        "max_single_symbol_weight": 1.00,
    },
    "aggressive8": {
        "risk_per_trade_pct": 0.08,
        "atr_stop_mult": 2.0,
        "max_position_notional_pct": 1.30,
        "target_equity": 1.00,
        "max_single_symbol_weight": 1.00,
    },
    "compound6": {
        "risk_per_trade_pct": 0.06,
        "atr_stop_mult": 2.0,
        "max_position_notional_pct": 1.00,
        "target_equity": 1.00,
        "max_single_symbol_weight": 0.80,
    },
    "concentrated5": {
        "risk_per_trade_pct": 0.05,
        "atr_stop_mult": 1.8,
        "max_position_notional_pct": 1.50,
        "target_equity": 1.00,
        "max_single_symbol_weight": 1.00,
    },
}

ENTRY_PATCHES = {
    "base": {},
    "loose_a8_v075": {"adx_min": 8, "volume_confirm_mult": 0.75},
    "loose_a8_v075_e28": {"adx_min": 8, "volume_confirm_mult": 0.75, "max_extension_atr": 2.8},
    "loose_a8_v085_e32": {"adx_min": 8, "volume_confirm_mult": 0.85, "max_extension_atr": 3.2},
    "regime30_150_loose": {
        "adx_min": 8,
        "volume_confirm_mult": 0.75,
        "regime_ema_fast": 30,
        "regime_ema_slow": 150,
    },
    "regime20_100_loose": {
        "adx_min": 8,
        "volume_confirm_mult": 0.75,
        "regime_ema_fast": 20,
        "regime_ema_slow": 100,
    },
    "regime20_100_loose_e28": {
        "adx_min": 8,
        "volume_confirm_mult": 0.75,
        "max_extension_atr": 2.8,
        "regime_ema_fast": 20,
        "regime_ema_slow": 100,
    },
    # ── Pass 2: entry patches for 30% push ──
    "ultra_loose_a6_v06": {
        "adx_min": 6,
        "volume_confirm_mult": 0.60,
        "ich_cloud_bull": 0,
    },
    "regime30_150_ultra_loose": {
        "adx_min": 6,
        "volume_confirm_mult": 0.60,
        "ich_cloud_bull": 0,
        "regime_filter_enabled": 1,
        "regime_ema_fast": 30,
        "regime_ema_slow": 150,
    },
    "regime30_150_loose_long": {
        "adx_min": 8,
        "volume_confirm_mult": 0.75,
        "ich_cloud_bull": 0,
        "regime_filter_enabled": 1,
        "regime_ema_fast": 30,
        "regime_ema_slow": 150,
    },
    "momentum_rsi": {
        "adx_min": 8,
        "volume_confirm_mult": 0.75,
        "ich_cloud_bull": 0,
        "rsi_floor": 40,
        "stoch_momo_max": 85,
        "regime_filter_enabled": 1,
        "regime_ema_fast": 30,
        "regime_ema_slow": 150,
    },
}

VARIANT_BLUEPRINTS = [
    ("scale4_base", "base"),
    ("scale5", "base"),
    ("tight5", "base"),
    ("scale6", "base"),
    ("tight6", "base"),
    ("wide5", "base"),
    ("focused5", "base"),
    ("scale5", "loose_a8_v075"),
    ("tight5", "loose_a8_v075"),
    ("scale5", "loose_a8_v075_e28"),
    ("tight5", "loose_a8_v075_e28"),
    ("focused5", "loose_a8_v075_e28"),
    ("scale5", "loose_a8_v085_e32"),
    ("tight5", "loose_a8_v085_e32"),
    ("scale5", "regime30_150_loose"),
    ("tight5", "regime30_150_loose"),
    ("scale6", "regime30_150_loose"),
    ("scale5", "regime20_100_loose"),
    ("tight6", "regime20_100_loose"),
    ("scale6", "regime20_100_loose_e28"),
    # ── Pass 2 variants: aggressive sizing + compound + ultra-loose entry + longer holds ──
    ("aggressive7", "regime30_150_loose"),
    ("aggressive7", "ultra_loose_a6_v06"),
    ("aggressive7", "regime30_150_ultra_loose"),
    ("aggressive7", "momentum_rsi"),
    ("aggressive8", "regime30_150_loose"),
    ("aggressive8", "regime30_150_ultra_loose"),
    ("compound6", "regime30_150_loose"),
    ("compound6", "regime30_150_ultra_loose"),
    ("concentrated5", "regime30_150_loose_long"),
    ("concentrated5", "ultra_loose_a6_v06"),
    ("tight5", "regime30_150_ultra_loose"),
    ("tight5", "ultra_loose_a6_v06"),
    ("tight6", "regime30_150_ultra_loose"),
    ("tight6", "momentum_rsi"),
]


def now_iso() -> str:
    return datetime.now().astimezone().isoformat()


def write_status(payload: dict) -> None:
    STATUS_PATH.write_text(json.dumps(payload, indent=2))
    with HISTORY_PATH.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload) + "\n")


@contextmanager
def env_overrides(patch: dict[str, str]):
    old = {k: os.environ.get(k) for k in patch}
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


def profile_env(profile: dict) -> dict[str, str]:
    target_equity = float(profile["target_equity"])
    return {
        "AT_BACKTEST_VOL_SIZING_ENABLED": "1",
        "AT_BACKTEST_RISK_PER_TRADE_PCT": str(profile["risk_per_trade_pct"]),
        "AT_BACKTEST_ATR_STOP_MULT": str(profile["atr_stop_mult"]),
        "AT_BACKTEST_MAX_POSITION_NOTIONAL_PCT": str(profile["max_position_notional_pct"]),
        "AT_TARGET_EQUITY": str(target_equity),
        "AT_TARGET_ETF": str(max(0.0, 1.0 - target_equity)),
        "AT_MAX_SINGLE_SYMBOL_WEIGHT": str(profile["max_single_symbol_weight"]),
        "AT_LAB_MATCH_LIVE": "1",
        "AT_LAB_RNN_ENABLED": "0",
    }


SELL_PATCHES = {
    "long_hold": {
        "breakeven_trigger_pct": 4.0,
        "equity_time_stop_bars": 10,
        "equity_time_stop_min_profit_pct": 1.5,
        "momentum_exit_rsi": 38.0,
    },
    "very_long_hold": {
        "breakeven_trigger_pct": 5.0,
        "equity_time_stop_bars": 14,
        "equity_time_stop_min_profit_pct": 2.0,
        "momentum_exit_rsi": 35.0,
    },
}


def build_variants(base_buy: dict, base_sell: dict, base_env: dict[str, str]) -> list[dict]:
    variants = []
    for profile_name, entry_name in VARIANT_BLUEPRINTS:
        exposure = EXPOSURE_PROFILES[profile_name]
        entry_patch = ENTRY_PATCHES[entry_name]
        merged_buy = dict(base_buy)
        merged_buy.update(entry_patch)
        merged_sell = dict(base_sell)
        # Apply longer hold sell patches for variants that need it
        if entry_name.endswith("_long") or entry_name == "momentum_rsi":
            merged_sell.update(SELL_PATCHES["long_hold"])
        if entry_name == "ultra_loose_a6_v06":
            merged_sell.update(SELL_PATCHES["very_long_hold"])
        env_patch = dict(base_env)
        env_patch.update(profile_env(exposure))
        variants.append(
            {
                "name": f"{profile_name}__{entry_name}",
                "buy": merged_buy,
                "sell": merged_sell,
                "env": env_patch,
                "profile": {"name": profile_name, **exposure},
                "entry_patch": {"name": entry_name, **entry_patch},
            }
        )
    return variants


def run_variant(data_map: dict[str, pd.DataFrame], buy: dict, sell: dict):
    old_r2 = dict(lab.RULE_SET_2.CONFIG)
    old_r7 = dict(lab.RULE_SET_7.CONFIG)
    try:
        lab.RULE_SET_2.CONFIG.clear()
        lab.RULE_SET_2.CONFIG.update(old_r2)
        lab.RULE_SET_2.CONFIG.update(sell)
        lab.RULE_SET_7.CONFIG.clear()
        lab.RULE_SET_7.CONFIG.update(old_r7)
        lab.RULE_SET_7.CONFIG.update(buy)
        return run_baseline_detailed(data_map)
    finally:
        lab.RULE_SET_2.CONFIG.clear()
        lab.RULE_SET_2.CONFIG.update(old_r2)
        lab.RULE_SET_7.CONFIG.clear()
        lab.RULE_SET_7.CONFIG.update(old_r7)


def result_key(row: dict) -> tuple:
    cagr = float(row.get("cagr_pct") or -999.0)
    ret = float(row.get("return_pct") or -999.0)
    sharpe = float(row.get("sharpe") or -999.0)
    dd = abs(float(row.get("drawdown_pct") or 0.0))
    return (cagr, ret, sharpe, -dd)


def load_checkpoint(expected_total: int) -> dict:
    if not CHECKPOINT_PATH.exists():
        return {}
    try:
        payload = json.loads(CHECKPOINT_PATH.read_text())
    except Exception:
        return {}
    if int(payload.get("variants_total", 0) or 0) != expected_total:
        return {}
    return payload


def save_checkpoint(*, payload: dict) -> None:
    CHECKPOINT_PATH.write_text(json.dumps(payload, indent=2))


def main() -> int:
    report_path = Path(os.getenv("AT_30_CAGR_REPORT", str(DEFAULT_REPORT)))
    variant_name = os.getenv("AT_30_CAGR_VARIANT", DEFAULT_VARIANT).strip() or DEFAULT_VARIANT

    report_obj = _load_report(report_path)
    variant = _pick_variant(report_obj, variant_name)
    base_buy = dict((variant.get("params") or {}).get("buy") or {})
    base_sell = dict((variant.get("params") or {}).get("sell") or {})
    base_env = dict((((variant.get("params") or {}).get("simulation") or {}).get("sizing_exit_sweep_env")) or {})
    symbols = list(((report_obj.get("recommendation") or {}).get("data_context") or {}).get("loaded_symbols") or variant.get("symbols_tested") or [])
    if not symbols:
        raise ValueError("No symbols found for 30% CAGR hunt")

    variants = build_variants(base_buy, base_sell, base_env)
    total = len(variants)
    checkpoint = load_checkpoint(total)

    if checkpoint.get("symbols") == symbols and checkpoint.get("report_path") == str(report_path) and checkpoint.get("variant_name") == variant_name:
        done_names = set(checkpoint.get("completed_names") or [])
        rows = list(checkpoint.get("rows") or [])
        data_context = checkpoint.get("data_context") or {}
        data_map, fresh_data_context = load_5y_data(symbols)
        if fresh_data_context:
            data_context = fresh_data_context
    else:
        done_names = set()
        rows = []
        data_map, data_context = load_5y_data(symbols)

    if not data_map:
        raise ValueError("No 5Y data loaded for 30% CAGR hunt")

    best_row = max(rows, key=result_key, default=None)
    write_status(
        {
            "generated_at": now_iso(),
            "status": "running",
            "phase": "initializing",
            "message": "resuming 30% CAGR hunt" if done_names else "starting 30% CAGR hunt",
            "report_path": str(report_path),
            "variant_name": variant_name,
            "variants_total": total,
            "variants_done": len(done_names),
            "best_variant": (best_row or {}).get("name"),
            "best_cagr_pct": (best_row or {}).get("cagr_pct"),
            "best_return_pct": (best_row or {}).get("return_pct"),
            "best_drawdown_pct": (best_row or {}).get("drawdown_pct"),
        }
    )

    for idx, cfg in enumerate(variants, start=1):
        name = cfg["name"]
        if name in done_names:
            continue
        with env_overrides(cfg["env"]):
            result, details, sim_meta = run_variant(data_map, cfg["buy"], cfg["sell"])
        curve = _compute_curve_metrics(sim_meta.get("portfolio_equity"))
        row = {
            "name": name,
            "profile": cfg["profile"],
            "entry_patch": cfg["entry_patch"],
            "buy": cfg["buy"],
            "sell": cfg["sell"],
            "env": cfg["env"],
            "return_pct": result.total_return_pct,
            "drawdown_pct": result.max_drawdown_pct,
            "trades": result.trades,
            "win_rate_pct": result.win_rate_pct,
            "selection_score": result.selection_score,
            "active_symbols": sum(1 for v in details.values() if int(v.get("trades", 0) or 0) > 0),
            **curve,
        }
        rows.append(row)
        done_names.add(name)
        if best_row is None or result_key(row) > result_key(best_row):
            best_row = row

        checkpoint_payload = {
            "generated_at": now_iso(),
            "report_path": str(report_path),
            "variant_name": variant_name,
            "symbols": symbols,
            "variants_total": total,
            "completed_names": sorted(done_names),
            "rows": rows,
            "data_context": data_context,
        }
        save_checkpoint(payload=checkpoint_payload)
        write_status(
            {
                "generated_at": now_iso(),
                "status": "running",
                "phase": "evaluating_variants",
                "message": "30% CAGR hunt in progress",
                "report_path": str(report_path),
                "variant_name": variant_name,
                "variants_total": total,
                "variants_done": len(done_names),
                "current_variant": name,
                "best_variant": best_row.get("name") if best_row else None,
                "best_cagr_pct": best_row.get("cagr_pct") if best_row else None,
                "best_return_pct": best_row.get("return_pct") if best_row else None,
                "best_drawdown_pct": best_row.get("drawdown_pct") if best_row else None,
                "progress_pct": round((len(done_names) / max(total, 1)) * 100.0, 1),
            }
        )

    ranked = sorted(rows, key=result_key, reverse=True)
    payload = {
        "generated_at": now_iso(),
        "status": "completed",
        "objective": "Find a credible >=30% CAGR path on 5Y live-parity validation",
        "anchor": {
            "report_path": str(report_path),
            "variant_name": variant_name,
            "base_buy": base_buy,
            "base_sell": base_sell,
            "base_env": base_env,
        },
        "test_period": PERIOD,
        "data_context": data_context,
        "tested_variants": len(ranked),
        "best": ranked[0] if ranked else None,
        "ranked": ranked,
    }

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = OUT_DIR / f"thirty_cagr_hunt_{ts}.json"
    csv_path = OUT_DIR / f"thirty_cagr_hunt_{ts}.csv"
    json_path.write_text(json.dumps(payload, indent=2))
    pd.DataFrame(ranked).to_csv(csv_path, index=False)
    CHECKPOINT_PATH.write_text(json.dumps({
        "generated_at": now_iso(),
        "report_path": str(report_path),
        "variant_name": variant_name,
        "symbols": symbols,
        "variants_total": total,
        "completed_names": sorted(done_names),
        "rows": ranked,
        "data_context": data_context,
        "output_json": str(json_path),
        "output_csv": str(csv_path),
    }, indent=2))
    write_status(
        {
            "generated_at": now_iso(),
            "status": "completed",
            "phase": "done",
            "message": "30% CAGR hunt complete",
            "report_path": str(report_path),
            "variant_name": variant_name,
            "variants_total": total,
            "variants_done": total,
            "best_variant": (ranked[0] or {}).get("name") if ranked else None,
            "best_cagr_pct": (ranked[0] or {}).get("cagr_pct") if ranked else None,
            "best_return_pct": (ranked[0] or {}).get("return_pct") if ranked else None,
            "best_drawdown_pct": (ranked[0] or {}).get("drawdown_pct") if ranked else None,
            "output_json": str(json_path),
            "output_csv": str(csv_path),
        }
    )
    print(json.dumps(payload.get("best") or {}, indent=2))
    print(f"Saved: {json_path}")
    print(f"Saved: {csv_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
