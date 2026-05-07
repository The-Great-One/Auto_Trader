#!/usr/bin/env python3
"""
Targeted 30% CAGR hunt — builds on ultra_loose_regime_30_150__bep2_ts4 (21% CAGR best).
Explores Telegram-informed refinements:
- Tighter exits (Telegram shows 5d positive but 10d fading)
- Regime variants with more aggressive signal filtering
- Ichimoku cloud + ADX combinations (from focus_combo_169 insights)
- Trailing stop improvements
- Risk sizing variants

Uses the kite_cagr_hunt framework for consistency.
"""

import json
import os
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

os.environ.setdefault("AT_DISABLE_FILE_LOGGING", "1")
os.environ["AT_RESEARCH_MODE"] = "1"
os.environ["AT_LAB_PRECACHE"] = "0"
os.environ["AT_LAB_CACHE_ONLY"] = "1"

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import weekly_strategy_lab as lab
from scripts.weekly_universe_cagr_check import run_baseline_detailed
from Auto_Trader.utils import Indicators

HIST_DIR = ROOT / "intermediary_files" / "Hist_Data"
OUT_DIR = ROOT / "reports"
OUT_DIR.mkdir(exist_ok=True)

MIN_ROWS = 1000
MIN_SPAN_DAYS = 1200


def load_kite_symbols(min_rows=MIN_ROWS, min_span=MIN_SPAN_DAYS):
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


@dataclass
class VariantResult:
    name: str = ""
    total_return_pct: float = 0.0
    cagr_pct: float = 0.0
    max_drawdown_pct: float = 0.0
    trades: int = 0
    win_rate_pct: float = 0.0
    sharpe: float = 0.0
    active_symbols: int = 0
    selection_score: float = 0.0
    error: str = ""


def _compute_curve_metrics(equity_curve):
    if len(equity_curve) < 20:
        return {"cagr_pct": 0.0, "sharpe": 0.0, "max_dd_pct": 0.0}
    s = pd.Series(equity_curve, dtype=float)
    final = s.iloc[-1]
    total_days = len(s)
    calendar_years = total_days / 252.0
    cagr = ((final / s.iloc[0]) ** (1.0 / max(calendar_years, 0.01)) - 1.0) * 100.0
    peak = s.cummax()
    dd = ((s - peak) / peak * 100.0).min()
    rets = s.pct_change().dropna()
    sharpe = float(rets.mean() / rets.std() * np.sqrt(252)) if len(rets) > 5 and rets.std() > 0 else 0.0
    return {"cagr_pct": round(cagr, 2), "sharpe": round(sharpe, 2), "max_dd_pct": round(float(dd), 2)}


def run_variant(data_map, buy, sell):
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
        return VariantResult(error=str(e)[:200])
    finally:
        lab.RULE_SET_2.CONFIG.clear()
        lab.RULE_SET_2.CONFIG.update(old_r2)
        lab.RULE_SET_7.CONFIG.clear()
        lab.RULE_SET_7.CONFIG.update(old_r7)


# ── Variant blueprints targeting 30% CAGR ──
# Base: ultra_loose_regime_30_150 (21% CAGR, best so far)
# Strategy: tighten DD control, add quick exits (Telegram 5d-positive insight),
#            test Ichimoku+ADX combos, improve risk/reward

ULTRA_LOOSE_REGIME_30_150 = {
    "adx_min": 5, "volume_confirm_mult": 0.3, "rsi_floor": 25,
    "cmf_base_min": -0.1, "cmf_strong_min": -0.1, "cmf_weak_min": -0.1,
    "obv_min_zscore": -2.0,
    "min_atr_pct": 0.0, "max_atr_pct": 0.15, "max_extension_atr": 5.0,
    "ich_cloud_bull": 0, "vwap_buy_above": 0, "sar_buy_enabled": 0,
    "di_cross_enabled": 0, "di_plus_min": 0, "cci_buy_min": -100,
    "willr_oversold_max": -20, "mmi_risk_off": 100,
    "regime_filter_enabled": 1, "regime_ema_fast": 30, "regime_ema_slow": 150,
}

ULTRA_LOOSE_BASE = {
    "adx_min": 5, "volume_confirm_mult": 0.3, "rsi_floor": 25,
    "cmf_base_min": -0.1, "cmf_strong_min": -0.1, "cmf_weak_min": -0.1,
    "obv_min_zscore": -2.0,
    "min_atr_pct": 0.0, "max_atr_pct": 0.15, "max_extension_atr": 5.0,
    "ich_cloud_bull": 0, "vwap_buy_above": 0, "sar_buy_enabled": 0,
    "di_cross_enabled": 0, "di_plus_min": 0, "cci_buy_min": -100,
    "willr_oversold_max": -20, "mmi_risk_off": 100,
    "regime_filter_enabled": 0,
}

VARIANTS = []

# ── Group 1: Winning base + tighter risk/exits (Telegram: quick exit) ──
for risk_pct, atr_mult, max_pos in [(0.02, 2.5, 0.10), (0.03, 2.0, 0.08), (0.01, 3.0, 0.15), (0.04, 1.5, 0.06)]:
    VARIANTS.append({
        "name": f"r30_150_risk{risk_pct}_atr{atr_mult}_pos{max_pos}",
        "buy": {**ULTRA_LOOSE_REGIME_30_150},
        "sell": {"breakeven_trigger_pct": 2.0, "equity_time_stop_bars": 4},
        "risk_per_trade_pct": risk_pct,
        "atr_stop_mult": atr_mult,
        "max_position_pct": max_pos,
    })

# ── Group 2: Regime base + Telegram quick exits (3-8 bar time stops) ──
for ts in [3, 4, 5, 6, 8, 10, 12]:
    VARIANTS.append({
        "name": f"r30_150_bep2_ts{ts}",
        "buy": {**ULTRA_LOOSE_REGIME_30_150},
        "sell": {"breakeven_trigger_pct": 2.0, "equity_time_stop_bars": ts},
    })

# ── Group 3: Regime + breakeven variants ──
for bep in [1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0]:
    VARIANTS.append({
        "name": f"r30_150_bep{bep}_ts4",
        "buy": {**ULTRA_LOOSE_REGIME_30_150},
        "sell": {"breakeven_trigger_pct": bep, "equity_time_stop_bars": 4},
    })

# ── Group 4: Regime + trailing stops ──
for trail in [2.0, 2.5, 3.0, 3.5, 4.0]:
    VARIANTS.append({
        "name": f"r30_150_trail{trail}_bep2_ts4",
        "buy": {**ULTRA_LOOSE_REGIME_30_150},
        "sell": {"breakeven_trigger_pct": 2.0, "equity_time_stop_bars": 4, "trailing_stop_atr_mult": trail},
    })

# ── Group 5: Ichimoku cloud variants (from focus_combo_169) ──
VARIANTS.append({
    "name": "r30_150_ich_bep2_ts4",
    "buy": {**ULTRA_LOOSE_REGIME_30_150, "ich_cloud_bull": 1},
    "sell": {"breakeven_trigger_pct": 2.0, "equity_time_stop_bars": 4},
})
VARIANTS.append({
    "name": "r30_150_ich_ts6",
    "buy": {**ULTRA_LOOSE_REGIME_30_150, "ich_cloud_bull": 1},
    "sell": {"breakeven_trigger_pct": 2.0, "equity_time_stop_bars": 6},
})
VARIANTS.append({
    "name": "r30_150_ich_ts8",
    "buy": {**ULTRA_LOOSE_REGIME_30_150, "ich_cloud_bull": 1},
    "sell": {"breakeven_trigger_pct": 2.0, "equity_time_stop_bars": 8},
})

# ── Group 6: ADX tightening with regime (stronger trend filter) ──
for adx in [10, 12, 15, 18]:
    for ts in [4, 6, 8]:
        buy_patch = {**ULTRA_LOOSE_REGIME_30_150, "adx_min": adx, "adx_strong_min": adx + 8}
        VARIANTS.append({
            "name": f"r30_150_adx{adx}_strong{adx+8}_ts{ts}",
            "buy": buy_patch,
            "sell": {"breakeven_trigger_pct": 2.0, "equity_time_stop_bars": ts},
        })

# ── Group 7: No-regime base + best exit combos ──
for ts in [3, 4, 5, 6]:
    VARIANTS.append({
        "name": f"norge_bep2_ts{ts}",
        "buy": {**ULTRA_LOOSE_BASE},
        "sell": {"breakeven_trigger_pct": 2.0, "equity_time_stop_bars": ts},
    })

# ── Group 8: Regime + moderate ADX + CMF/OBV ──
VARIANTS.append({
    "name": "r30_150_adx8_cmf005_bep2_ts4",
    "buy": {**ULTRA_LOOSE_REGIME_30_150, "adx_min": 8, "cmf_base_min": 0.05, "cmf_strong_min": 0.05, "cmf_weak_min": 0.0, "obv_min_zscore": 0.5},
    "sell": {"breakeven_trigger_pct": 2.0, "equity_time_stop_bars": 4},
})
VARIANTS.append({
    "name": "r30_150_adx8_obv05_bep2_ts5",
    "buy": {**ULTRA_LOOSE_REGIME_30_150, "adx_min": 8, "obv_min_zscore": 0.5},
    "sell": {"breakeven_trigger_pct": 2.0, "equity_time_stop_bars": 5},
})

# ── Group 9: Different regime EMA pairs ──
for fast, slow in [(20, 100), (20, 150), (30, 100), (30, 200), (50, 150), (50, 200)]:
    VARIANTS.append({
        "name": f"regime_{fast}_{slow}_bep2_ts4",
        "buy": {**ULTRA_LOOSE_BASE, "regime_filter_enabled": 1, "regime_ema_fast": fast, "regime_ema_slow": slow},
        "sell": {"breakeven_trigger_pct": 2.0, "equity_time_stop_bars": 4},
    })

# ── Group 10: Combined Ichimoku + ADX + quick exits ──
for adx in [8, 10, 12]:
    for ts in [4, 5, 6]:
        VARIANTS.append({
            "name": f"r30_150_ich_adx{adx}_ts{ts}",
            "buy": {**ULTRA_LOOSE_REGIME_30_150, "ich_cloud_bull": 1, "adx_min": adx},
            "sell": {"breakeven_trigger_pct": 2.0, "equity_time_stop_bars": ts},
        })


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--shard", type=str, default="")
    args = parser.parse_args()

    all_variants = VARIANTS
    total_all = len(all_variants)
    print(f"Total targeted variants: {total_all}")

    if args.limit > 0:
        variants = all_variants[args.offset:args.offset + args.limit]
    else:
        variants = all_variants

    shard_label = args.shard or f"targeted_offset{args.offset}"

    print("=" * 60)
    print(f"30% CAGR TARGETED HUNT — Shard: {shard_label}")
    print(f"Variants: {len(variants)} (offset={args.offset})")
    print("=" * 60)

    data_map = load_kite_symbols()
    if not data_map:
        print("ERROR: No symbols loaded")
        return 1

    # Default sizing (same as kite_cagr_hunt for parity)
    os.environ["AT_BACKTEST_VOL_SIZING_ENABLED"] = "1"
    os.environ["AT_BACKTEST_RISK_PER_TRADE_PCT"] = "0.05"
    os.environ["AT_BACKTEST_ATR_STOP_MULT"] = "2.0"
    os.environ["AT_BACKTEST_MAX_POSITION_NOTIONAL_PCT"] = "1.0"
    os.environ["AT_MAX_SINGLE_SYMBOL_WEIGHT"] = "0.65"
    os.environ["AT_TARGET_EQUITY"] = "1.0"
    os.environ["AT_TARGET_ETF"] = "0.0"

    ranked = []
    best_cagr = 0.0
    best_name = ""

    STATUS_FILE = OUT_DIR / "kite_hunt_targeted_status.json"
    HISTORY_FILE = OUT_DIR / "kite_cagr_hunt_targeted_history.jsonl"

    for idx, bp in enumerate(variants, 1):
        name = bp["name"]
        buy = bp["buy"]
        sell = bp.get("sell", {})

        # Handle risk sizing overrides
        risk_pct = bp.get("risk_per_trade_pct", None)
        if risk_pct is not None:
            os.environ["AT_BACKTEST_RISK_PER_TRADE_PCT"] = str(risk_pct)
        atr_mult = bp.get("atr_stop_mult", None)
        if atr_mult is not None:
            os.environ["AT_BACKTEST_ATR_STOP_MULT"] = str(atr_mult)
        max_pos = bp.get("max_position_pct", None)
        if max_pos is not None:
            os.environ["AT_BACKTEST_MAX_POSITION_NOTIONAL_PCT"] = str(max_pos)

        t0 = time.time()
        result = run_variant(data_map, buy, sell)
        elapsed = time.time() - t0

        # Reset to defaults
        os.environ["AT_BACKTEST_RISK_PER_TRADE_PCT"] = "0.05"
        os.environ["AT_BACKTEST_ATR_STOP_MULT"] = "2.0"
        os.environ["AT_BACKTEST_MAX_POSITION_NOTIONAL_PCT"] = "1.0"

        result.name = name
        row = asdict(result)
        row["shard"] = shard_label
        ranked.append(row)

        if HISTORY_FILE.exists() or idx == 1:
            with open(HISTORY_FILE, "a") as f:
                f.write(json.dumps({**row, "elapsed_s": round(elapsed, 1)}) + "\n")

        if result.cagr_pct > best_cagr:
            best_cagr = result.cagr_pct
            best_name = name

        ts = datetime.now().strftime("%H:%M:%S")
        print(f"{ts} {idx}/{len(variants)} {name} cagr={result.cagr_pct}% ret={result.total_return_pct}% dd={result.max_drawdown_pct}% trades={result.trades} [{elapsed:.0f}s]", flush=True)

        STATUS_FILE.write_text(json.dumps({
            "generated_at": datetime.now().isoformat(),
            "status": "running",
            "shard": shard_label,
            "variants_done": idx,
            "variants_total": len(variants),
            "best_variant": best_name,
            "best_cagr_pct": round(best_cagr, 2),
        }, indent=2))

    ranked.sort(key=lambda r: r.get("cagr_pct", 0) or 0, reverse=True)

    final_report = {
        "generated_at": datetime.now().isoformat(),
        "hunt_label": f"30% CAGR Targeted Hunt — Shard {shard_label}",
        "data_source": "kite_feather_5y",
        "shard": shard_label,
        "symbols_loaded": len(data_map),
        "variants_total": len(variants),
        "best_variant": ranked[0]["name"] if ranked else "",
        "best_cagr_pct": ranked[0].get("cagr_pct", 0) if ranked else 0,
        "ranked": ranked[:50],
    }

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = OUT_DIR / f"kite_cagr_hunt_targeted_{shard_label}_{ts}.json"
    report_path.write_text(json.dumps(final_report, indent=2))
    print(f"\nReport saved: {report_path}")

    print(f"\n{'='*60}")
    print(f"TOP 10 — {shard_label}")
    print(f"{'='*60}")
    for i, r in enumerate(ranked[:10], 1):
        print(f"  {i}. {r['name']}: CAGR={r.get('cagr_pct',0)}% Ret={r.get('total_return_pct',0)}% DD={r.get('max_drawdown_pct',0)}% Trades={r.get('trades',0)} Sharpe={r.get('sharpe',0)}")

    STATUS_FILE.write_text(json.dumps({
        "generated_at": datetime.now().isoformat(),
        "status": "completed",
        "shard": shard_label,
        "variants_total": len(variants),
        "best_variant": ranked[0]["name"] if ranked else "",
        "best_cagr_pct": ranked[0].get("cagr_pct", 0) if ranked else 0,
        "report_path": str(report_path),
    }, indent=2))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())