#!/usr/bin/env python3
"""
30% CAGR Hunt on Kite 5Y Data (256 symbols).
Standards: Kite data only, 5Y training, all results stored.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

os.environ.setdefault("AT_DISABLE_FILE_LOGGING", "1")

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import weekly_strategy_lab as lab
from scripts.weekly_universe_cagr_check import run_baseline_detailed

HIST_DIR = ROOT / "intermediary_files" / "Hist_Data"
OUT_DIR = ROOT / "reports"
OUT_DIR.mkdir(exist_ok=True)

MIN_ROWS = 1000
MIN_SPAN_DAYS = 1200  # ~3.3 years


# ── Data Loading ──────────────────────────────────────────────

def load_kite_symbols(min_rows=MIN_ROWS, min_span=MIN_SPAN_DAYS) -> dict[str, pd.DataFrame]:
    """Load all valid Kite feather files, compute indicators."""
    from Auto_Trader.utils import Indicators

    data_map = {}
    skipped = 0
    for fp in sorted(HIST_DIR.glob("*.feather")):
        try:
            df = pd.read_feather(fp)
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
            df = df.dropna(subset=["Date"]).sort_values("Date").reset_index(drop=True)
            if len(df) < min_rows:
                skipped += 1
                continue
            span = (df.iloc[-1]["Date"] - df.iloc[0]["Date"]).days
            if span < min_span:
                skipped += 1
                continue
            enriched = Indicators(df)
            if enriched is not None and len(enriched) >= min_rows:
                data_map[fp.stem] = enriched
        except Exception:
            skipped += 1

    print(f"Loaded {len(data_map)} symbols (skipped {skipped})")
    return data_map


# ── Variant Blueprints ────────────────────────────────────────

# The core problem: Kite data produces very few BUY signals with default gates.
# Strategy: systematically loosen every gate combination.

FULL_VARIANT_BLUEPRINTS = []

# ── Pass 1: Individual gate relaxation ──

# ADX gates
for adx in [5, 8, 10, 12, 15]:
    FULL_VARIANT_BLUEPRINTS.append({
        "name": f"adx_{adx}",
        "buy": {"adx_min": adx},
        "sell": {},
    })

# Volume gates  
for vol in [0.3, 0.5, 0.75, 0.85, 1.0]:
    FULL_VARIANT_BLUEPRINTS.append({
        "name": f"vol_{vol}",
        "buy": {"volume_confirm_mult": vol},
        "sell": {},
    })

# RSI floor
for rsi in [20, 25, 30, 35, 40, 45]:
    FULL_VARIANT_BLUEPRINTS.append({
        "name": f"rsi_{rsi}",
        "buy": {"rsi_floor": rsi},
        "sell": {},
    })

# CMF gates
for cmf in [-0.1, -0.05, 0.0, 0.03, 0.05]:
    FULL_VARIANT_BLUEPRINTS.append({
        "name": f"cmf_{cmf}",
        "buy": {"cmf_base_min": cmf, "cmf_strong_min": cmf, "cmf_weak_min": cmf},
        "sell": {},
    })

# OBV zscore
for obv in [-2.0, -1.0, -0.5, 0.0, 0.5]:
    FULL_VARIANT_BLUEPRINTS.append({
        "name": f"obv_{obv}",
        "buy": {"obv_min_zscore": obv},
        "sell": {},
    })

# Regime filters
for fast, slow in [(20, 100), (30, 150), (50, 200), (0, 0)]:
    FULL_VARIANT_BLUEPRINTS.append({
        "name": f"regime_{fast}_{slow}",
        "buy": {"regime_ema_fast": fast, "regime_ema_slow": slow, "regime_filter_enabled": 1 if fast > 0 else 0},
        "sell": {},
    })

# ATR bands
FULL_VARIANT_BLUEPRINTS.append({
    "name": "atr_wide",
    "buy": {"min_atr_pct": 0.0, "max_atr_pct": 0.15, "max_extension_atr": 5.0},
    "sell": {},
})

# Disable optional gates
FULL_VARIANT_BLUEPRINTS.append({
    "name": "no_optional_gates",
    "buy": {"ich_cloud_bull": 0, "vwap_buy_above": 0, "sar_buy_enabled": 0, "di_cross_enabled": 0, "di_plus_min": 0, "cci_buy_min": -100, "willr_oversold_max": -20, "mmi_risk_off": 100},
    "sell": {},
})

# ── Pass 2: Winning combinations from pass 1 ──
# Loosen everything that helps simultaneously

FULL_VARIANT_BLUEPRINTS.append({
    "name": "ultra_loose_base",
    "buy": {
        "adx_min": 5,
        "volume_confirm_mult": 0.3,
        "rsi_floor": 25,
        "cmf_base_min": -0.1, "cmf_strong_min": -0.1, "cmf_weak_min": -0.1,
        "obv_min_zscore": -2.0,
        "min_atr_pct": 0.0, "max_atr_pct": 0.15, "max_extension_atr": 5.0,
        "ich_cloud_bull": 0, "vwap_buy_above": 0, "sar_buy_enabled": 0,
        "di_cross_enabled": 0, "di_plus_min": 0, "cci_buy_min": -100,
        "willr_oversold_max": -20, "mmi_risk_off": 100,
        "regime_filter_enabled": 0,
    },
    "sell": {},
})

# Ultra loose + regime variations
for fast, slow in [(20, 100), (30, 150), (50, 200)]:
    FULL_VARIANT_BLUEPRINTS.append({
        "name": f"ultra_loose_regime_{fast}_{slow}",
        "buy": {
            "adx_min": 5, "volume_confirm_mult": 0.3, "rsi_floor": 25,
            "cmf_base_min": -0.1, "cmf_strong_min": -0.1, "cmf_weak_min": -0.1,
            "obv_min_zscore": -2.0,
            "min_atr_pct": 0.0, "max_atr_pct": 0.15, "max_extension_atr": 5.0,
            "ich_cloud_bull": 0, "vwap_buy_above": 0, "sar_buy_enabled": 0,
            "di_cross_enabled": 0, "di_plus_min": 0, "cci_buy_min": -100,
            "willr_oversold_max": -20, "mmi_risk_off": 100,
            "regime_filter_enabled": 1, "regime_ema_fast": fast, "regime_ema_slow": slow,
        },
        "sell": {},
    })

# Moderate loose combos (less aggressive)
for adx, vol, rsi in [(8, 0.5, 30), (8, 0.75, 35), (10, 0.5, 35), (12, 0.75, 40)]:
    FULL_VARIANT_BLUEPRINTS.append({
        "name": f"moderate_a{adx}_v{vol}_r{rsi}",
        "buy": {
            "adx_min": adx, "volume_confirm_mult": vol, "rsi_floor": rsi,
            "cmf_base_min": -0.05, "cmf_strong_min": -0.05, "cmf_weak_min": -0.05,
            "obv_min_zscore": -1.0,
            "min_atr_pct": 0.0, "max_atr_pct": 0.12, "max_extension_atr": 3.0,
            "ich_cloud_bull": 0, "vwap_buy_above": 0, "regime_filter_enabled": 0,
        },
        "sell": {},
    })

# ── Pass 3: Exit strategy variations ──

EXIT_BLUEPRINTS = [
    {"name": "bep2_ts4", "sell": {"breakeven_trigger_pct": 2.0, "equity_time_stop_bars": 4}},
    {"name": "bep3_ts6", "sell": {"breakeven_trigger_pct": 3.0, "equity_time_stop_bars": 6}},
    {"name": "bep4_ts8", "sell": {"breakeven_trigger_pct": 4.0, "equity_time_stop_bars": 8}},
    {"name": "bep5_ts10", "sell": {"breakeven_trigger_pct": 5.0, "equity_time_stop_bars": 10}},
    {"name": "bep0_ts12", "sell": {"breakeven_trigger_pct": 0.0, "equity_time_stop_bars": 12}},
    {"name": "trail_atr3", "sell": {"trailing_stop_atr_mult": 3.0}},
    {"name": "trail_atr4", "sell": {"trailing_stop_atr_mult": 4.0}},
]

# Pair the best entry combos with exit combos
TOP_ENTRY_NAMES = [
    "ultra_loose_base",
    "ultra_loose_regime_30_150",
    "moderate_a8_v0.5_r30",
    "moderate_a8_v0.75_r35",
    "no_optional_gates",
    "adx_5",
    "vol_0.3",
    "rsi_25",
]

PASS3_VARIANTS = []
for entry_name in TOP_ENTRY_NAMES:
    entry_bp = next(bp for bp in FULL_VARIANT_BLUEPRINTS if bp["name"] == entry_name)
    for exit_bp in EXIT_BLUEPRINTS:
        combined_buy = {**entry_bp["buy"]}
        combined_sell = {**exit_bp["sell"]}
        PASS3_VARIANTS.append({
            "name": f"{entry_name}__{exit_bp['name']}",
            "buy": combined_buy,
            "sell": combined_sell,
        })

FULL_VARIANT_BLUEPRINTS.extend(PASS3_VARIANTS)

print(f"Total variants: {len(FULL_VARIANT_BLUEPRINTS)}")


# ── Backtest Runner ───────────────────────────────────────────

@dataclass
class VariantResult:
    name: str
    total_return_pct: float = 0.0
    cagr_pct: float = 0.0
    max_drawdown_pct: float = 0.0
    trades: int = 0
    win_rate_pct: float = 0.0
    sharpe: float = 0.0
    active_symbols: int = 0
    selection_score: float = 0.0
    error: str = ""


def _compute_curve_metrics(equity_curve: list[float]) -> dict:
    """Compute CAGR, Sharpe, max DD from equity curve."""
    if len(equity_curve) < 20:
        return {"cagr_pct": 0.0, "sharpe": 0.0, "max_dd_pct": 0.0}
    s = pd.Series(equity_curve, dtype=float)
    final = s.iloc[-1]
    total_days = len(s)  # approximate: 1 bar ≈ 1 trading day
    calendar_years = total_days / 252.0
    cagr = ((final / s.iloc[0]) ** (1.0 / max(calendar_years, 0.01)) - 1.0) * 100.0
    peak = s.cummax()
    dd = ((s - peak) / peak * 100.0).min()
    rets = s.pct_change().dropna()
    sharpe = float(rets.mean() / rets.std() * np.sqrt(252)) if len(rets) > 5 and rets.std() > 0 else 0.0
    return {"cagr_pct": round(cagr, 2), "sharpe": round(sharpe, 2), "max_dd_pct": round(float(dd), 2)}


def run_variant(data_map: dict[str, pd.DataFrame], buy: dict, sell: dict) -> VariantResult:
    old_r2 = dict(lab.RULE_SET_2.CONFIG)
    old_r7 = dict(lab.RULE_SET_7.CONFIG)
    try:
        lab.RULE_SET_2.CONFIG.clear()
        lab.RULE_SET_2.CONFIG.update(old_r2)
        lab.RULE_SET_2.CONFIG.update(sell)
        lab.RULE_SET_7.CONFIG.clear()
        lab.RULE_SET_7.CONFIG.update(old_r7)
        lab.RULE_SET_7.CONFIG.update(buy)
        result, details, sim_meta = run_baseline_detailed(data_map)
        eq = sim_meta.get("portfolio_equity")
        curve = _compute_curve_metrics(eq) if eq is not None and len(eq) > 20 else {"cagr_pct": 0.0, "sharpe": 0.0}
        return VariantResult(
            name="",
            total_return_pct=round(result.total_return_pct, 2),
            cagr_pct=curve["cagr_pct"],
            max_drawdown_pct=round(result.max_drawdown_pct, 2),
            trades=result.trades,
            win_rate_pct=round(result.win_rate_pct, 1),
            sharpe=curve["sharpe"],
            active_symbols=sum(1 for v in details.values() if int(v.get("trades", 0) or 0) > 0),
            selection_score=round(result.selection_score, 3),
        )
    except Exception as e:
        return VariantResult(name="", error=str(e)[:200])
    finally:
        lab.RULE_SET_2.CONFIG.clear()
        lab.RULE_SET_2.CONFIG.update(old_r2)
        lab.RULE_SET_7.CONFIG.clear()
        lab.RULE_SET_7.CONFIG.update(old_r7)


# ── Main Hunt Loop ────────────────────────────────────────────

STATUS_FILE = OUT_DIR / "kite_hunt_status.json"
HISTORY_FILE = OUT_DIR / "kite_cagr_hunt_history.jsonl"


def save_status(status: dict):
    STATUS_FILE.write_text(json.dumps(status, indent=2))


def append_history(entry: dict):
    with open(HISTORY_FILE, "a") as f:
        f.write(json.dumps(entry) + "\n")


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--offset", type=int, default=0, help="Start index for variant slice")
    parser.add_argument("--limit", type=int, default=0, help="Number of variants to run (0=all)")
    parser.add_argument("--shard", type=str, default="", help="Shard label (e.g. primary/secondary/local)")
    args = parser.parse_args()

    all_variants = FULL_VARIANT_BLUEPRINTS
    total_all = len(all_variants)

    if args.limit > 0:
        variants = all_variants[args.offset:args.offset + args.limit]
    else:
        variants = all_variants

    shard_label = args.shard or f"offset{args.offset}"

    print("=" * 60)
    print(f"30% CAGR HUNT — Kite 5Y Data — Shard: {shard_label}")
    print(f"Variants: {len(variants)} (offset={args.offset}, limit={args.limit or total_all})")
    print("=" * 60)

    # Load all symbols
    data_map = load_kite_symbols()
    if not data_map:
        print("ERROR: No symbols loaded")
        return 1

    # Set sizing config
    os.environ["AT_BACKTEST_VOL_SIZING_ENABLED"] = "1"
    os.environ["AT_BACKTEST_RISK_PER_TRADE_PCT"] = "0.05"
    os.environ["AT_BACKTEST_ATR_STOP_MULT"] = "2.0"
    os.environ["AT_BACKTEST_MAX_POSITION_NOTIONAL_PCT"] = "1.0"
    os.environ["AT_MAX_SINGLE_SYMBOL_WEIGHT"] = "0.65"
    os.environ["AT_TARGET_EQUITY"] = "1.0"
    os.environ["AT_TARGET_ETF"] = "0.0"

    total = len(variants)
    ranked: list[dict] = []
    best_cagr = 0.0
    best_name = ""

    save_status({
        "generated_at": datetime.now().isoformat(),
        "status": "running",
        "phase": "loading",
        "shard": shard_label,
        "message": f"Starting hunt shard {shard_label} with {len(data_map)} symbols, {total} variants",
        "symbols_loaded": len(data_map),
        "variants_total": total,
        "variants_done": 0,
        "best_variant": "",
        "best_cagr_pct": 0.0,
    })

    for idx, bp in enumerate(variants, 1):
        name = bp["name"]
        t0 = time.time()
        result = run_variant(data_map, bp["buy"], bp["sell"])
        elapsed = time.time() - t0

        result.name = name
        row = asdict(result)
        row["shard"] = shard_label
        row["global_idx"] = args.offset + idx
        ranked.append(row)
        append_history({**row, "variant_idx": args.offset + idx, "shard": shard_label, "elapsed_s": round(elapsed, 1)})

        if result.cagr_pct > best_cagr:
            best_cagr = result.cagr_pct
            best_name = name

        # Progress log
        ts = datetime.now().strftime("%H:%M:%S")
        print(f"{ts} {idx}/{total} {name} cagr={result.cagr_pct}% ret={result.total_return_pct}% dd={result.max_drawdown_pct}% trades={result.trades} act={result.active_symbols} [{elapsed:.0f}s]", flush=True)

        save_status({
            "generated_at": datetime.now().isoformat(),
            "status": "running",
            "phase": "sweeping",
            "shard": shard_label,
            "message": f"variant {idx}/{total} (shard {shard_label})",
            "symbols_loaded": len(data_map),
            "variants_total": total,
            "variants_done": idx,
            "best_variant": best_name,
            "best_cagr_pct": round(best_cagr, 2),
            "best_return_pct": round(max(r.get("total_return_pct", 0) for r in ranked), 2),
            "best_drawdown_pct": round(min(r.get("max_drawdown_pct", 0) for r in ranked), 2),
        })

    # Final ranked results
    ranked.sort(key=lambda r: r.get("cagr_pct", 0) or 0, reverse=True)

    final_report = {
        "generated_at": datetime.now().isoformat(),
        "hunt_label": f"30% CAGR Hunt — Kite 5Y — Shard {shard_label}",
        "data_source": "kite_feather_5y",
        "shard": shard_label,
        "symbols_loaded": len(data_map),
        "variants_total": total,
        "best_variant": ranked[0]["name"] if ranked else "",
        "best_cagr_pct": ranked[0].get("cagr_pct", 0) if ranked else 0,
        "ranked": ranked[:50],
    }

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = OUT_DIR / f"kite_cagr_hunt_{shard_label}_{ts}.json"
    report_path.write_text(json.dumps(final_report, indent=2))
    print(f"\nReport saved: {report_path}")

    # Print top 10
    print(f"\n{'='*60}")
    print(f"TOP 10 VARIANTS — Shard {shard_label}")
    print(f"{'='*60}")
    for i, r in enumerate(ranked[:10], 1):
        print(f"  {i}. {r['name']}: CAGR={r.get('cagr_pct',0)}% Ret={r.get('total_return_pct',0)}% DD={r.get('max_drawdown_pct',0)}% Trades={r.get('trades',0)} Active={r.get('active_symbols',0)} Sharpe={r.get('sharpe',0)}")

    save_status({
        "generated_at": datetime.now().isoformat(),
        "status": "completed",
        "phase": "done",
        "shard": shard_label,
        "message": f"Hunt shard {shard_label} complete",
        "symbols_loaded": len(data_map),
        "variants_total": total,
        "variants_done": total,
        "best_variant": ranked[0]["name"] if ranked else "",
        "best_cagr_pct": ranked[0].get("cagr_pct", 0) if ranked else 0,
        "report_path": str(report_path),
    })

    return 0


if __name__ == "__main__":
    raise SystemExit(main())