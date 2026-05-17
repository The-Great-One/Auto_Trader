#!/usr/bin/env python3
"""
Walk-forward validation of top telegram_confluence sweep candidates — v2.
Fixed: verbose error logging, proper EMA20 column validation, per-symbol safety.
Targets the variants showing best headline CAGR from the sweep,
with proper 5-fold expanding-window OOS validation.
"""
from __future__ import annotations
import json, os, sys, traceback
from datetime import datetime
from pathlib import Path
import pandas as pd
import numpy as np

os.environ.setdefault("AT_RESEARCH_MODE", "1")
os.environ.setdefault("AT_LAB_PRECACHE", "0")
os.environ.setdefault("AT_LAB_CACHE_ONLY", "1")
os.environ.setdefault("AT_DISABLE_FILE_LOGGING", "1")
os.environ["AT_LAB_MODE"] = "1"
os.environ["AT_BACKTEST_VOL_SIZING_ENABLED"] = "1"
os.environ["AT_BACKTEST_RISK_PER_TRADE_PCT"] = "0.02"
os.environ["AT_BACKTEST_ATR_STOP_MULT"] = "2.5"
os.environ["AT_BACKTEST_MAX_POSITION_NOTIONAL_PCT"] = "0.10"
os.environ["AT_MAX_SINGLE_SYMBOL_WEIGHT"] = "0.15"
os.environ["AT_PORTFOLIO_BAND"] = "0.10"
os.environ["AT_TARGET_EQUITY"] = "1.0"
os.environ["AT_TARGET_ETF"] = "0.0"
os.environ["AT_LAB_MATCH_LIVE"] = "0"

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from Auto_Trader.utils import Indicators
from scripts.weekly_strategy_lab import run_walk_forward_validation, RULE_SET_2, RULE_SET_7
from scripts.weekly_universe_cagr_check import run_baseline_detailed

OUT_DIR = ROOT / "reports"
MIN_ROWS = 400
MIN_SPAN_DAYS = 500

# Required columns that RULE_SET_7.evaluate_signal accesses directly (not via .get())
REQUIRED_COLS = {"EMA20", "EMA50", "ADX", "MACD", "MACD_Signal", "RSI", "ATR",
                "CMF", "OBV", "SMA_20_Volume", "Supertrend", "Supertrend_Direction"}


def load_kite_symbols(min_rows=MIN_ROWS, min_span=MIN_SPAN_DAYS):
    """Load Kite cached feather data and enrich with Indicators.
    Validates that required indicator columns exist."""
    hist_dir = ROOT / "intermediary_files" / "Hist_Data"
    data_map = {}
    skipped = 0
    missing_cols = 0
    for fp in sorted(hist_dir.glob("*.feather")):
        sym = fp.stem
        try:
            df = pd.read_feather(fp)
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
            # Normalize timezone: strip tz info to avoid tz-naive/tz-aware comparison errors
            if hasattr(df["Date"].dtype, "tz") and df["Date"].dtype.tz is not None:
                df["Date"] = df["Date"].dt.tz_localize(None)
            df = df.dropna(subset=["Date"]).sort_values("Date").reset_index(drop=True)
            if len(df) < min_rows:
                skipped += 1
                continue
            span = (df["Date"].max() - df["Date"].min()).days
            if span < min_span:
                skipped += 1
                continue
            enriched = Indicators(df)
            if enriched is None or len(enriched) < min_rows:
                skipped += 1
                continue
            # Validate required columns
            missing = REQUIRED_COLS - set(enriched.columns)
            if missing:
                print(f"[WF-VALIDATE] WARNING: {sym} missing required columns: {missing}")
                missing_cols += 1
                skipped += 1
                continue
            data_map[sym] = enriched
        except Exception as e:
            skipped += 1
    print(f"[WF-VALIDATE] Loaded {len(data_map)} symbols (skipped {skipped}, missing_cols {missing_cols})")
    return data_map


def _compute_cagr(equity_curve):
    if len(equity_curve) < 20:
        return 0.0, 0.0, 0.0
    s = pd.Series(equity_curve, dtype=float)
    final = s.iloc[-1]
    total_days = len(s)
    years = total_days / 252.0
    cagr = ((final / s.iloc[0]) ** (1.0 / max(years, 0.01)) - 1.0) * 100.0
    peak = s.cummax()
    dd = ((s - peak) / peak * 100.0).min()
    rets = s.pct_change().dropna()
    sharpe = float(rets.mean() / rets.std() * np.sqrt(252)) if len(rets) > 5 and rets.std() > 0 else 0.0
    return round(cagr, 2), round(sharpe, 2), round(float(dd), 2)


def run_variant_direct(data_map, buy, sell, name="variant"):
    """Run a single variant by patching RULE_SET configs."""
    from dataclasses import dataclass
    @dataclass
    class VR:
        total_return_pct: float = 0.0
        cagr_pct: float = 0.0
        max_drawdown_pct: float = 0.0
        trades: int = 0
        win_rate_pct: float = 0.0
        sharpe: float = 0.0
        active_symbols: int = 0
        selection_score: float = 0.0
        universe: str = ""
        error: str = ""
    old_r2 = dict(RULE_SET_2.CONFIG)
    old_r7 = dict(RULE_SET_7.CONFIG)
    try:
        RULE_SET_2.CONFIG.clear()
        RULE_SET_2.CONFIG.update(old_r2)
        RULE_SET_2.CONFIG.update(sell)
        RULE_SET_7.CONFIG.clear()
        RULE_SET_7.CONFIG.update(old_r7)
        RULE_SET_7.CONFIG.update(buy)
        result, details, sim_meta = run_baseline_detailed(data_map)
        eq = sim_meta.get("portfolio_equity")
        cagr, sharpe, dd = _compute_cagr(eq) if eq is not None and len(eq) > 20 else (0.0, 0.0, 0.0)
        return VR(
            total_return_pct=round(result.total_return_pct, 2),
            cagr_pct=cagr,
            max_drawdown_pct=dd or round(result.max_drawdown_pct, 2),
            trades=result.trades if hasattr(result, 'trades') else 0,
            win_rate_pct=round(result.win_rate_pct, 1) if hasattr(result, 'win_rate_pct') else 0.0,
            sharpe=sharpe,
            active_symbols=sum(1 for v in details.values() if int(v.get("trades", 0) or 0) > 0) if isinstance(details, dict) else 0,
            selection_score=round(result.selection_score, 3) if hasattr(result, 'selection_score') else 0.0,
        )
    except Exception as e:
        tb = traceback.format_exc()
        print(f"[WF-VALIDATE] FULL BACKTRACE for {name}:\n{tb}")
        return VR(error=str(e)[:300])
    finally:
        RULE_SET_2.CONFIG.clear()
        RULE_SET_2.CONFIG.update(old_r2)
        RULE_SET_7.CONFIG.clear()
        RULE_SET_7.CONFIG.update(old_r7)


# ── Top candidates from telegram_confluence_full sweep ──
COMBO263_TIGHT = {
    "sr_bounce_enabled": 1, "sr_vpoc_reclaim_enabled": 1,
    "sr_near_support_pct": 0.02, "volume_confirm_mult": 0.75,
    "rsi_floor": 38, "ich_cloud_bull": 0, "vwap_buy_above": 0,
}

REGIME_30_150 = {
    "regime_filter_enabled": 1, "regime_ema_fast": 30, "regime_ema_slow": 150,
    "adx_min": 5, "volume_confirm_mult": 0.3, "rsi_floor": 25,
}

CANDIDATES = [
    # combo263 variants — best headline from full universe
    {"name": "combo263_bep2.0_ts4", "buy": {**COMBO263_TIGHT}, "sell": {"breakeven_trigger_pct": 2.0, "equity_time_stop_bars": 4}},
    {"name": "combo263_bep2.5_ts4", "buy": {**COMBO263_TIGHT}, "sell": {"breakeven_trigger_pct": 2.5, "equity_time_stop_bars": 4}},
    {"name": "combo263_bep1.5_ts4", "buy": {**COMBO263_TIGHT}, "sell": {"breakeven_trigger_pct": 1.5, "equity_time_stop_bars": 4}},
    {"name": "combo263_bep2.0_ts5", "buy": {**COMBO263_TIGHT}, "sell": {"breakeven_trigger_pct": 2.0, "equity_time_stop_bars": 5}},
    {"name": "combo263_bep2.5_ts5", "buy": {**COMBO263_TIGHT}, "sell": {"breakeven_trigger_pct": 2.5, "equity_time_stop_bars": 5}},
    # Regime 30/150 variants
    {"name": "regime30_sr_bounce_ts4", "buy": {**REGIME_30_150, "sr_bounce_enabled": 1, "volume_confirm_mult": 0.85}, "sell": {"breakeven_trigger_pct": 2.0, "equity_time_stop_bars": 4}},
    {"name": "regime30_sr_bounce_ts5", "buy": {**REGIME_30_150, "sr_bounce_enabled": 1, "volume_confirm_mult": 0.85}, "sell": {"breakeven_trigger_pct": 2.0, "equity_time_stop_bars": 5}},
    {"name": "regime30_adx18_ich_ts4", "buy": {**REGIME_30_150, "adx_min": 18, "adx_strong_min": 18, "ich_cloud_bull": 1}, "sell": {"breakeven_trigger_pct": 2.0, "equity_time_stop_bars": 4}},
    # combo282 variants
    {"name": "combo282_bep2.0_ts5", "buy": {"sr_bounce_enabled": 1, "sr_vpoc_reclaim_enabled": 1, "sr_near_support_pct": 0.02, "volume_confirm_mult": 0.85, "rsi_floor": 38, "ich_cloud_bull": 0, "vwap_buy_above": 0, "regime_filter_enabled": 1, "regime_ema_fast": 30, "regime_ema_slow": 150}, "sell": {"breakeven_trigger_pct": 2.0, "equity_time_stop_bars": 5}},
    # Baselines
    {"name": "baseline_adx18", "buy": {"adx_strong_min": 18}, "sell": {}},
    {"name": "baseline_adx18_tim15", "buy": {"adx_strong_min": 18}, "sell": {"equity_time_stop_bars": 15}},
    {"name": "baseline_tim15", "buy": {}, "sell": {"equity_time_stop_bars": 15}},
]


def main():
    print(f"[WF-VALIDATE-V2] Starting walk-forward validation of top candidates")
    print(f"[WF-VALIDATE-V2] {len(CANDIDATES)} candidates to validate")

    # Load data with Indicators enrichment
    all_data = load_kite_symbols()
    print(f"[WF-VALIDATE-V2] Loaded {len(all_data)} symbols from Kite cache")

    results = []
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    for i, cand in enumerate(CANDIDATES):
        name = cand["name"]
        buy = cand["buy"]
        sell = cand["sell"]
        universe_label = cand.get("universe", "full")

        data_map = all_data

        print(f"\n[WF-VALIDATE-V2] {i+1}/{len(CANDIDATES)}: {name} (universe={universe_label}, symbols={len(data_map)})")

        # Run full-universe backtest
        try:
            vr = run_variant_direct(data_map, buy, sell, name=name)
            full_result = {
                "name": name,
                "total_return_pct": vr.total_return_pct,
                "cagr_pct": vr.cagr_pct,
                "max_drawdown_pct": vr.max_drawdown_pct,
                "trades": vr.trades,
                "win_rate_pct": vr.win_rate_pct,
                "sharpe": vr.sharpe,
                "active_symbols": vr.active_symbols,
                "selection_score": vr.selection_score,
                "error": vr.error,
            }
        except Exception as e:
            tb = traceback.format_exc()
            print(f"[WF-VALIDATE-V2] ERROR running {name}: {e}\n{tb}")
            full_result = {"name": name, "total_return_pct": 0, "cagr_pct": 0, "max_drawdown_pct": 0, "trades": 0, "error": str(e)}

        # Run walk-forward validation (5-fold) using weekly_strategy_lab
        try:
            wf_result = run_walk_forward_validation(data_map=data_map, buy_params=buy, sell_params=sell, n_splits=5)
            wf_result["name"] = name
        except Exception as e:
            tb = traceback.format_exc()
            print(f"[WF-VALIDATE-V2] ERROR in WF for {name}: {e}\n{tb}")
            wf_result = {"name": name, "n_folds": 5, "mean_oos_return_pct": 0, "min_oos_return_pct": 0, "positive_folds": 0, "error": str(e)}

        result = {
            "name": name,
            "universe": universe_label,
            "symbols_tested": len(data_map),
            "full": {k: v for k, v in full_result.items() if not isinstance(v, (list, dict)) or k == "error"},
            "walk_forward": wf_result,
        }
        results.append(result)

        # Print summary
        hl_ret = full_result.get("total_return_pct", 0)
        hl_cagr = full_result.get("cagr_pct", 0)
        hl_dd = full_result.get("max_drawdown_pct", 0)
        hl_trades = full_result.get("trades", 0)
        oos_mean = wf_result.get("mean_oos_return_pct", 0)
        oos_min = wf_result.get("min_oos_return_pct", 0)
        oos_pos = wf_result.get("positive_folds", 0)
        print(f"  → {name}: ret={hl_ret:.2f}%, cagr={hl_cagr:.2f}%, DD={hl_dd:.2f}%, trades={hl_trades}")
        print(f"  → WF: OOS_mean={oos_mean:.2f}%, OOS_min={oos_min:.2f}%, pos_folds={oos_pos}/5")

    # Save results
    report = {
        "generated_at": datetime.now().isoformat(),
        "label": "wf_validate_top_candidates_v2",
        "n_candidates": len(CANDIDATES),
        "universe_size": len(all_data),
        "validation_method": "walk_forward_5fold",
        "results": results,
    }

    out_path = OUT_DIR / f"wf_validate_top_candidates_v2_{ts}.json"
    with open(out_path, "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"\n[WF-VALIDATE-V2] Results saved to {out_path}")

    # Print final summary
    print("\n" + "=" * 80)
    print("WALK-FORWARD VALIDATION SUMMARY (v2)")
    print("=" * 80)
    for r in results:
        name = r["name"]
        full = r.get("full", {})
        wf = r.get("walk_forward", {})
        err = full.get("error", "") or wf.get("error", "")
        print(f"{name}:")
        if err:
            print(f"  ERROR: {err[:100]}")
        print(f"  Headline: ret={full.get('total_return_pct',0):.2f}%, cagr={full.get('cagr_pct',0):.2f}%, DD={full.get('max_drawdown_pct',0):.2f}%, trades={full.get('trades',0)}")
        print(f"  WF: OOS_mean={wf.get('mean_oos_return_pct',0):.2f}%, OOS_min={wf.get('min_oos_return_pct',0):.2f}%, pos_folds={wf.get('positive_folds',0)}/5")

    # Identify best by OOS quality
    valid = [r for r in results if r.get("walk_forward", {}).get("mean_oos_return_pct", 0) > 0]
    if valid:
        best_oos = max(valid, key=lambda r: r["walk_forward"].get("mean_oos_return_pct", 0))
        print(f"\nBest OOS candidate: {best_oos['name']} (OOS_mean={best_oos['walk_forward'].get('mean_oos_return_pct',0):.2f}%)")


if __name__ == "__main__":
    main()