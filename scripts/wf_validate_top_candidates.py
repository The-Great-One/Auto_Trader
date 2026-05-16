#!/usr/bin/env python3
"""
Walk-forward validation of top telegram_confluence sweep candidates.
Targets the variants showing best headline CAGR from the sweep,
with proper 5-fold expanding-window OOS validation.
Uses Kite 5Y cached feather data on secondary Oracle.
"""
from __future__ import annotations
import json, os, sys
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
from scripts.weekly_strategy_lab import run_variant as lab_run_variant, run_walk_forward_validation, RULE_SET_2, RULE_SET_7
from scripts.weekly_universe_cagr_check import run_baseline_detailed

OUT_DIR = ROOT / "reports"
MIN_ROWS = 400
MIN_SPAN_DAYS = 500


def load_kite_symbols(min_rows=MIN_ROWS, min_span=MIN_SPAN_DAYS):
    """Load Kite cached feather data and enrich with Indicators."""
    hist_dir = ROOT / "intermediary_files" / "Hist_Data"
    data_map = {}
    skipped = 0
    for fp in sorted(hist_dir.glob("*.feather")):
        sym = fp.stem
        try:
            df = pd.read_feather(fp)
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
            df = df.dropna(subset=["Date"]).sort_values("Date").reset_index(drop=True)
            if len(df) < min_rows:
                skipped += 1
                continue
            span = (df["Date"].max() - df["Date"].min()).days
            if span < min_span:
                skipped += 1
                continue
            enriched = Indicators(df)
            if enriched is not None and len(enriched) >= min_rows:
                data_map[sym] = enriched
        except Exception:
            skipped += 1
    print(f"[WF-VALIDATE] Loaded {len(data_map)} symbols (skipped {skipped})")
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
        import traceback
        traceback.print_exc()
        return VR(error=str(e)[:200])
    finally:
        RULE_SET_2.CONFIG.clear()
        RULE_SET_2.CONFIG.update(old_r2)
        RULE_SET_7.CONFIG.clear()
        RULE_SET_7.CONFIG.update(old_r7)

# ── Top candidates from telegram_confluence_full sweep ──
# combo263 variants (headline 37.42% CAGR on full universe)
COMBO263_TIGHT_TS8 = {
    "sr_bounce_enabled": 1, "sr_vpoc_reclaim_enabled": 1,
    "sr_near_support_pct": 0.02, "volume_confirm_mult": 0.75,
    "rsi_floor": 38, "ich_cloud_bull": 0, "vwap_buy_above": 0,
    "adx_strong_min": 18,
}

REGIME_30_150 = {
    "regime_filter_enabled": 1, "regime_ema_fast": 30, "regime_ema_slow": 150,
}

ADX18_ICH = {"adx_min": 18, "adx_strong_min": 18, "ich_cloud_bull": 1}

CANDIDATES = [
    # combo263 variants — best headline from full universe
    {"name": "combo263_bep2.0_ts4", "buy": {**COMBO263_TIGHT_TS8}, "sell": {"breakeven_trigger_pct": 2.0, "equity_time_stop_bars": 4}},
    {"name": "combo263_bep2.5_ts4", "buy": {**COMBO263_TIGHT_TS8}, "sell": {"breakeven_trigger_pct": 2.5, "equity_time_stop_bars": 4}},
    {"name": "combo263_bep1.5_ts4", "buy": {**COMBO263_TIGHT_TS8}, "sell": {"breakeven_trigger_pct": 1.5, "equity_time_stop_bars": 4}},
    {"name": "combo263_bep2.0_ts5", "buy": {**COMBO263_TIGHT_TS8}, "sell": {"breakeven_trigger_pct": 2.0, "equity_time_stop_bars": 5}},
    {"name": "combo263_bep2.5_ts5", "buy": {**COMBO263_TIGHT_TS8}, "sell": {"breakeven_trigger_pct": 2.5, "equity_time_stop_bars": 5}},
    # Telegram confluence — restricted universe
    {"name": "tg_combo263_ts5", "buy": {**COMBO263_TIGHT_TS8, "_telegram_symbols_only": True}, "sell": {"breakeven_trigger_pct": 2.0, "equity_time_stop_bars": 5}, "universe": "telegram"},
    {"name": "tg_combo263_ts8", "buy": {**COMBO263_TIGHT_TS8, "_telegram_symbols_only": True}, "sell": {"breakeven_trigger_pct": 2.0, "equity_time_stop_bars": 8}, "universe": "telegram"},
    # Regime 30/150 variants
    {"name": "regime30_sr_bounce_ts4", "buy": {**REGIME_30_150, "sr_bounce_enabled": 1, "volume_confirm_mult": 0.85}, "sell": {"breakeven_trigger_pct": 2.0, "equity_time_stop_bars": 4}},
    {"name": "regime30_sr_bounce_ts5", "buy": {**REGIME_30_150, "sr_bounce_enabled": 1, "volume_confirm_mult": 0.85}, "sell": {"breakeven_trigger_pct": 2.0, "equity_time_stop_bars": 5}},
    {"name": "regime30_adx18_ich_ts4", "buy": {**REGIME_30_150, "adx_min": 18, "adx_strong_min": 18, "ich_cloud_bull": 1}, "sell": {"breakeven_trigger_pct": 2.0, "equity_time_stop_bars": 4}},
    # combo282 variants
    {"name": "combo282_bep2.0_ts5", "buy": {"sr_bounce_enabled": 1, "sr_vpoc_reclaim_enabled": 1, "sr_near_support_pct": 0.02, "volume_confirm_mult": 0.85, "rsi_floor": 38, "ich_cloud_bull": 0, "vwap_buy_above": 0, "regime_filter_enabled": 1, "regime_ema_fast": 30, "regime_ema_slow": 150}, "sell": {"breakeven_trigger_pct": 2.0, "equity_time_stop_bars": 5}},
    # Baseline for comparison
    {"name": "baseline_adx18", "buy": {"adx_strong_min": 18}, "sell": {}},
]


def load_telegram_symbols():
    """Load Telegram channel symbols from the learning scores file."""
    scores_path = ROOT / "reports" / "channel_learning_scores.json"
    if not scores_path.exists():
        return set()
    try:
        with open(scores_path) as f:
            d = json.load(f)
        syms = set()
        # Handle various structures
        channels = d
        if isinstance(d, dict) and "channels" in d:
            channels = d["channels"]
        if isinstance(d, dict) and "symbols" in d:
            # Top-level symbols list
            for s in d["symbols"]:
                syms.add(s.upper().replace(".NS", "").replace(".BO", ""))
            if "channels" in d:
                channels = d["channels"]
        if isinstance(channels, dict):
            for ch_name, ch_data in channels.items():
                if isinstance(ch_data, dict):
                    for s in ch_data.get("symbols_seen", ch_data.get("symbols", ch_data.get("top_symbols", []))):
                        syms.add(s.upper().replace(".NS", "").replace(".BO", ""))
        elif isinstance(channels, list):
            for ch_data in channels:
                if isinstance(ch_data, dict):
                    for s in ch_data.get("symbols_seen", ch_data.get("symbols", ch_data.get("top_symbols", []))):
                        syms.add(s.upper().replace(".NS", "").replace(".BO", ""))
        return syms
    except Exception as e:
        print(f"[WF-VALIDATE] Warning: could not load Telegram symbols: {e}")
        return set()


def main():
    print(f"[WF-VALIDATE] Starting walk-forward validation of top candidates")
    print(f"[WF-VALIDATE] {len(CANDIDATES)} candidates to validate")

    # Load data with Indicators enrichment
    all_data = load_kite_symbols()
    print(f"[WF-VALIDATE] Loaded {len(all_data)} symbols from Kite cache")

    telegram_syms = load_telegram_symbols()
    print(f"[WF-VALIDATE] Found {len(telegram_syms)} Telegram channel symbols")

    results = []
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    for i, cand in enumerate(CANDIDATES):
        name = cand["name"]
        buy = cand["buy"]
        sell = cand["sell"]
        universe_label = cand.get("universe", "full")

        # Select universe
        if universe_label == "telegram":
            data_map = {k: v for k, v in all_data.items() if k in telegram_syms or k.upper() in telegram_syms}
            if len(data_map) < 10:
                print(f"[WF-VALIDATE] WARNING: Only {len(data_map)} Telegram symbols found, using all data")
                data_map = all_data
                universe_label = "full"
        else:
            data_map = all_data

        print(f"\n[WF-VALIDATE] {i+1}/{len(CANDIDATES)}: {name} (universe={universe_label}, symbols={len(data_map)})")

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
            print(f"[WF-VALIDATE] ERROR running {name}: {e}")
            import traceback; traceback.print_exc()
            full_result = {"name": name, "total_return_pct": 0, "cagr_pct": 0, "max_drawdown_pct": 0, "trades": 0, "error": str(e)}

        # Run walk-forward validation (5-fold) using weekly_strategy_lab
        try:
            wf_result = run_walk_forward_validation(data_map=data_map, buy_params=buy, sell_params=sell, n_splits=5)
            wf_result["name"] = name
        except Exception as e:
            print(f"[WF-VALIDATE] ERROR in WF for {name}: {e}")
            import traceback; traceback.print_exc()
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
        "label": "wf_validate_top_candidates",
        "n_candidates": len(CANDIDATES),
        "universe_size": len(all_data),
        "validation_method": "walk_forward_5fold",
        "results": results,
    }

    out_path = OUT_DIR / f"wf_validate_top_candidates_{ts}.json"
    with open(out_path, "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"\n[WF-VALIDATE] Results saved to {out_path}")

    # Print final summary
    print("\n" + "=" * 80)
    print("WALK-FORWARD VALIDATION SUMMARY")
    print("=" * 80)
    for r in results:
        name = r["name"]
        full = r.get("full", {})
        wf = r.get("walk_forward", {})
        print(f"{name}:")
        print(f"  Headline: ret={full.get('total_return_pct',0):.2f}%, cagr={full.get('cagr_pct',0):.2f}%, DD={full.get('max_drawdown_pct',0):.2f}%, trades={full.get('trades',0)}")
        print(f"  WF: OOS_mean={wf.get('mean_oos_return_pct',0):.2f}%, OOS_min={wf.get('min_oos_return_pct',0):.2f}%, pos_folds={wf.get('positive_folds',0)}/5")

    # Identify best by OOS quality
    valid = [r for r in results if r.get("walk_forward", {}).get("mean_oos_return_pct", 0) > 0]
    if valid:
        best_oos = max(valid, key=lambda r: r["walk_forward"].get("mean_oos_return_pct", 0))
        print(f"\nBest OOS candidate: {best_oos['name']} (OOS_mean={best_oos['walk_forward'].get('mean_oos_return_pct',0):.2f}%)")


if __name__ == "__main__":
    main()