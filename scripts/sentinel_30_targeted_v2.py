#!/usr/bin/env python3
"""
Sentinel 30% CAGR Targeted v2 — builds on sentinel sweep findings.

Key insights from previous runs:
- combo263_tight_ts8: 66% IS, but only 2/5 positive WF folds, -2.65% worst fold
- curated_combo_164: 38% IS, 3/5 positive WF, -0.76% worst fold — best robustness
- adx18_ich on 245-symbol: 2.46% IS, 0.51% mean OOS, 3/5 folds, worst -0.04%
- Fold 4 (Oct 2025-Apr 2026) is consistently negative — regime problem
- Telegram channels: @shortterm01 (8.41% 5d avg, 75% positive), @darkhorseofstockmarket
  (21.51% max_favorable, 53.6 confidence) — confluence opportunity

Attack vectors:
1. SR bounce + momentum exit with improved regime filter (faster exits in bear phase)
2. Tighter breakeven triggers (Telegram insight: high max_favorable but weak close returns)
3. Trailing ATR stops to lock in gains during favorable folds
4. ADX+Ichimoku combos on curated 39-symbol universe (higher hit rate per symbol)
5. Telegram watchlist boost variant: only trade symbols from Telegram channels
6. Mixed universe: curated 39 + high-conviction Telegram symbols
"""

from __future__ import annotations
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

MIN_ROWS = 400
MIN_SPAN_DAYS = 500

# Telegram-mentioned high-conviction symbols (from channel_learning_scores + audit)
TELEGRAM_SYMBOLS = [
    "GRANULES", "TIPSMUSIC", "HDFCAMC", "TRENT", "COLPAL", "MAZDOCK",
    "LTM", "SRF", "BSE",
]

# Sentinel curated universe (best-performing symbols from previous sweeps)
CURATED_SYMBOLS = [
    "RELIANCE", "TCS", "HDFCBANK", "INFY", "ICICIBANK", "HINDUNILVR",
    "SBIN", "BHARTIARTL", "ITC", "KOTAKBANK", "LT", "AXISBANK",
    "BAJFINANCE", "ASIANPAINT", "MARUTI", "TITAN", "SUNPHARMA",
    "TATAMOTORS", "WIPRO", "HCLTECH", "ULTRACEMCO", "NESTLEIND",
    "POWERGRID", "ONGC", "NTPC", "COALINDIA", "ADANIENT",
    "TECHM", "BAJAJFINSV", "TATASTEEL", "HINDALCO", "DRREDDY",
    "CIPLA", "DIVISLAB", "APOLLOHOSP", "EICHERMOT", "HEROMOTOCO",
    "M&M", "BPCL",
]


def load_kite_symbols(min_rows=MIN_ROWS, min_span=MIN_SPAN_DAYS, symbol_filter=None):
    data_map = {}
    skipped = 0
    for fp in sorted(HIST_DIR.glob("*.feather")):
        sym = fp.stem
        if symbol_filter is not None and sym not in symbol_filter:
            continue
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
                data_map[sym] = enriched
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
    universe: str = ""
    error: str = ""


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


def run_variant(data_map, buy, sell, universe_label="full"):
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
        cagr, sharpe, dd = _compute_cagr(eq) if eq is not None and len(eq) > 20 else (0.0, 0.0, 0.0)
        return VariantResult(
            total_return_pct=round(result.total_return_pct, 2),
            cagr_pct=cagr,
            max_drawdown_pct=dd or round(result.max_drawdown_pct, 2),
            trades=result.trades,
            win_rate_pct=round(result.win_rate_pct, 1),
            sharpe=sharpe,
            active_symbols=sum(1 for v in details.values() if int(v.get("trades", 0) or 0) > 0),
            selection_score=round(result.selection_score, 3),
            universe=universe_label,
        )
    except Exception as e:
        return VariantResult(error=str(e)[:200], universe=universe_label)
    finally:
        lab.RULE_SET_2.CONFIG.clear()
        lab.RULE_SET_2.CONFIG.update(old_r2)
        lab.RULE_SET_7.CONFIG.clear()
        lab.RULE_SET_7.CONFIG.update(old_r7)


# ── Variant definitions targeting 30% CAGR ──

# Best sentinel sweep configs as base
COMBO263_TIGHT_TS8 = {
    "sr_bounce_enabled": 1, "sr_vpoc_reclaim_enabled": 1, "sr_near_support_pct": 0.02,
    "volume_confirm_mult": 0.75, "rsi_floor": 38, "ich_cloud_bull": 0, "vwap_buy_above": 0,
}
COMBO263_TIGHT_TS8_SELL = {"momentum_exit_rsi": 38.0, "equity_review_rsi": 45.0, "equity_time_stop_bars": 8}

COMBO282_SR_BOUNCE = {
    "sr_breakout_enabled": 1, "sr_breakout_buffer_pct": 0.005,
    "sr_bounce_enabled": 1, "volume_confirm_mult": 0.85, "adx_strong_min": 18, "ich_cloud_bull": 0,
}
COMBO282_SR_BOUNCE_SELL = {"momentum_exit_rsi": 38.0, "equity_review_rsi": 45.0, "equity_time_stop_bars": 10}

REGIME_30_150 = {
    "adx_min": 5, "volume_confirm_mult": 0.3, "rsi_floor": 25,
    "regime_filter_enabled": 1, "regime_ema_fast": 30, "regime_ema_slow": 150,
    "ich_cloud_bull": 0, "vwap_buy_above": 0, "max_extension_atr": 5.0,
}

ADX18_ICH = {"adx_strong_min": 18, "ich_cloud_bull": 1, "volume_confirm_mult": 0.75}

# ── Build variant grid ──
VARIANTS = []

# ── Group A: combo263 variants with tighter risk control (best IS, need OOS stability) ──
for ts in [4, 5, 6, 8, 10]:
    for bep in [1.5, 2.0, 2.5]:
        VARIANTS.append({
            "name": f"combo263_bep{bep}_ts{ts}",
            "buy": {**COMBO263_TIGHT_TS8},
            "sell": {"momentum_exit_rsi": 38.0, "equity_review_rsi": 45.0,
                     "breakeven_trigger_pct": bep, "equity_time_stop_bars": ts},
        })

# ── Group B: combo263 + trailing stop ──
for trail in [2.0, 2.5, 3.0]:
    VARIANTS.append({
        "name": f"combo263_trail{trail}_ts8",
        "buy": {**COMBO263_TIGHT_TS8},
        "sell": {"momentum_exit_rsi": 38.0, "equity_review_rsi": 45.0,
                 "equity_time_stop_bars": 8, "trailing_stop_atr_mult": trail},
    })

# ── Group C: combo282 (SR breakout+bounce) with tighter exits ──
for ts in [5, 6, 8, 10]:
    for bep in [1.5, 2.0, 2.5]:
        VARIANTS.append({
            "name": f"combo282_bep{bep}_ts{ts}",
            "buy": {**COMBO282_SR_BOUNCE},
            "sell": {"momentum_exit_rsi": 38.0, "equity_review_rsi": 45.0,
                     "breakeven_trigger_pct": bep, "equity_time_stop_bars": ts},
        })

# ── Group D: Regime 30/150 + SR bounce + momentum (combine best features) ──
for ts in [4, 5, 6, 8]:
    VARIANTS.append({
        "name": f"regime30_sr_bounce_ts{ts}",
        "buy": {**REGIME_30_150, "sr_bounce_enabled": 1, "sr_near_support_pct": 0.02},
        "sell": {"breakeven_trigger_pct": 2.0, "equity_time_stop_bars": ts},
    })

# ── Group E: ADX18+ICH + SR bounce (merge two best ideas) ──
for ts in [4, 5, 6, 8]:
    VARIANTS.append({
        "name": f"adx18_ich_srb_ts{ts}",
        "buy": {**ADX18_ICH, "sr_bounce_enabled": 1, "sr_near_support_pct": 0.02},
        "sell": {"breakeven_trigger_pct": 2.0, "equity_time_stop_bars": ts},
    })

# ── Group F: ADX18+ICH + regime filter (address fold 4 negative) ──
for fast, slow in [(30, 150), (20, 100), (50, 200)]:
    for ts in [5, 8, 10]:
        VARIANTS.append({
            "name": f"adx18_ich_regime{fast}_{slow}_ts{ts}",
            "buy": {**ADX18_ICH, "regime_filter_enabled": 1, "regime_ema_fast": fast, "regime_ema_slow": slow},
            "sell": {"breakeven_trigger_pct": 2.0, "equity_time_stop_bars": ts},
        })

# ── Group G: Regime 30/150 + ADX18+ICH combo ──
for ts in [4, 5, 6, 8]:
    VARIANTS.append({
        "name": f"regime30_adx18_ich_ts{ts}",
        "buy": {**REGIME_30_150, "adx_min": 18, "adx_strong_min": 18, "ich_cloud_bull": 1},
        "sell": {"breakeven_trigger_pct": 2.0, "equity_time_stop_bars": ts},
    })

# ── Group H: Telegram watchlist boost — only trade symbols in Telegram channels ──
for buy_cfg, buy_label in [
    (COMBO263_TIGHT_TS8, "combo263"),
    (ADX18_ICH, "adx18ich"),
    (REGIME_30_150, "regime30"),
]:
    for ts in [5, 8]:
        VARIANTS.append({
            "name": f"tg_{buy_label}_ts{ts}",
            "buy": {**buy_cfg, "_telegram_symbols_only": True},
            "sell": {"breakeven_trigger_pct": 2.0, "equity_time_stop_bars": ts},
            "universe": "telegram",
        })

# ── Group I: Mixed universe — curated 39 + Telegram symbols ──
for buy_cfg, buy_label in [
    (COMBO263_TIGHT_TS8, "combo263"),
    (ADX18_ICH, "adx18ich"),
]:
    for ts in [5, 8]:
        VARIANTS.append({
            "name": f"mixed_{buy_label}_ts{ts}",
            "buy": {**buy_cfg},
            "sell": {"breakeven_trigger_pct": 2.0, "equity_time_stop_bars": ts},
            "universe": "mixed",
        })


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--shard", type=str, default="")
    parser.add_argument("--universe", type=str, default="curated", choices=["curated", "full", "telegram", "mixed"])
    args = parser.parse_args()

    # Select universe
    if args.universe == "curated":
        symbol_filter = set(CURATED_SYMBOLS + TELEGRAM_SYMBOLS)
    elif args.universe == "telegram":
        symbol_filter = set(TELEGRAM_SYMBOLS)
    elif args.universe == "mixed":
        symbol_filter = None  # full universe but track Telegram symbols separately
    else:
        symbol_filter = None

    all_variants = VARIANTS[args.offset:]
    if args.limit > 0:
        all_variants = all_variants[:args.limit]

    shard_label = args.shard or f"v2_{args.universe}_offset{args.offset}"

    print("=" * 60)
    print(f"SENTINEL 30% TARGETED v2 — {shard_label}")
    print(f"Universe: {args.universe} | Variants: {len(all_variants)}")
    print("=" * 60)

    data_map = load_kite_symbols(symbol_filter=symbol_filter)
    if not data_map:
        print("ERROR: No symbols loaded")
        return 1

    # Default sizing
    os.environ["AT_BACKTEST_VOL_SIZING_ENABLED"] = "1"
    os.environ["AT_BACKTEST_RISK_PER_TRADE_PCT"] = "0.05"
    os.environ["AT_BACKTEST_ATR_STOP_MULT"] = "2.0"
    os.environ["AT_BACKTEST_MAX_POSITION_NOTIONAL_PCT"] = "1.0"
    os.environ["AT_MAX_SINGLE_SYMBOL_WEIGHT"] = "0.65"

    ranked = []
    best_cagr = 0.0
    best_name = ""

    STATUS_FILE = OUT_DIR / "sentinel_30_targeted_v2_status.json"
    HISTORY_FILE = OUT_DIR / "sentinel_30_targeted_v2_history.jsonl"

    for idx, bp in enumerate(all_variants, 1):
        name = bp["name"]
        buy = {k: v for k, v in bp["buy"].items() if not k.startswith("_")}
        sell = bp.get("sell", {})
        universe_label = bp.get("universe", args.universe)

        t0 = time.time()
        result = run_variant(data_map, buy, sell, universe_label=universe_label)
        elapsed = time.time() - t0

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
        print(f"{ts} {idx}/{len(all_variants)} {name} cagr={result.cagr_pct}% ret={result.total_return_pct}% "
              f"dd={result.max_drawdown_pct}% trades={result.trades} sharpe={result.sharpe} [{elapsed:.0f}s]", flush=True)

        STATUS_FILE.write_text(json.dumps({
            "generated_at": datetime.now().isoformat(),
            "status": "running",
            "shard": shard_label,
            "universe": args.universe,
            "variants_done": idx,
            "variants_total": len(all_variants),
            "best_variant": best_name,
            "best_cagr_pct": round(best_cagr, 2),
        }, indent=2))

    ranked.sort(key=lambda r: r.get("cagr_pct", 0) or 0, reverse=True)

    final_report = {
        "generated_at": datetime.now().isoformat(),
        "hunt_label": f"Sentinel 30% Targeted v2 — {shard_label}",
        "data_source": "kite_feather_5y",
        "shard": shard_label,
        "universe": args.universe,
        "symbols_loaded": len(data_map),
        "variants_total": len(all_variants),
        "best_variant": ranked[0]["name"] if ranked else "",
        "best_cagr_pct": ranked[0].get("cagr_pct", 0) if ranked else 0,
        "ranked": ranked[:50],
    }

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = OUT_DIR / f"sentinel_30_targeted_v2_{shard_label}_{ts}.json"
    report_path.write_text(json.dumps(final_report, indent=2))
    print(f"\nReport saved: {report_path}")

    print(f"\n{'='*60}")
    print(f"TOP 15 — {shard_label}")
    print(f"{'='*60}")
    for i, r in enumerate(ranked[:15], 1):
        print(f"  {i}. {r['name']}: CAGR={r.get('cagr_pct',0)}% Ret={r.get('total_return_pct',0)}% "
              f"DD={r.get('max_drawdown_pct',0)}% Trades={r.get('trades',0)} Sharpe={r.get('sharpe',0)}")

    STATUS_FILE.write_text(json.dumps({
        "generated_at": datetime.now().isoformat(),
        "status": "completed",
        "shard": shard_label,
        "universe": args.universe,
        "variants_total": len(all_variants),
        "best_variant": ranked[0]["name"] if ranked else "",
        "best_cagr_pct": ranked[0].get("cagr_pct", 0) if ranked else 0,
        "report_path": str(report_path),
    }, indent=2))

    # ── Auto-walk-forward validate top 5 ──
    print(f"\n{'='*60}")
    print("AUTO WF VALIDATION — Top 5 candidates")
    print(f"{'='*60}")
    from scripts.weekly_strategy_lab import run_walk_forward_validation

    top5 = ranked[:5]
    wf_results = []
    for cand in top5:
        name = cand["name"]
        buy = {k: v for k, v in cand.items() if k in [
            "adx_min", "adx_strong_min", "volume_confirm_mult", "rsi_floor",
            "ich_cloud_bull", "vwap_buy_above", "sr_bounce_enabled", "sr_vpoc_reclaim_enabled",
            "sr_near_support_pct", "sr_breakout_enabled", "sr_breakout_buffer_pct",
            "regime_filter_enabled", "regime_ema_fast", "regime_ema_slow",
            "max_extension_atr", "cmf_base_min", "obv_min_zscore", "sar_buy_enabled",
            "di_cross_enabled", "stoch_pull_max", "stoch_momo_max",
        ] and v not in (None, 0, 0.0)}
        sell_keys = ["breakeven_trigger_pct", "equity_time_stop_bars", "momentum_exit_rsi",
                     "equity_review_rsi", "fund_time_stop_bars", "trailing_stop_atr_mult"]
        sell = {k: v for k, v in cand.items() if k in sell_keys and v not in (None, 0)}

        try:
            wf = run_walk_forward_validation(data_map, buy, sell, n_splits=5)
            wf_summary = {
                "name": name,
                "cagr_pct": cand.get("cagr_pct", 0),
                "total_return_pct": cand.get("total_return_pct", 0),
                "max_drawdown_pct": cand.get("max_drawdown_pct", 0),
                "trades": cand.get("trades", 0),
                "n_folds": wf.get("n_folds", 5),
                "mean_oos_return_pct": round(wf.get("mean_oos_return_pct", 0), 2),
                "std_oos_return_pct": round(wf.get("std_oos_return_pct", 0), 2),
                "min_oos_return_pct": round(wf.get("min_oos_return_pct", 0), 2),
                "max_oos_return_pct": round(wf.get("max_oos_return_pct", 0), 2),
                "positive_folds": wf.get("positive_folds", 0),
            }
            wf_results.append(wf_summary)
            print(f"  {name}: OOS mean={wf_summary['mean_oos_return_pct']}% "
                  f"min={wf_summary['min_oos_return_pct']}% max={wf_summary['max_oos_return_pct']}% "
                  f"+ve={wf_summary['positive_folds']}/{wf_summary['n_folds']}")
        except Exception as e:
            print(f"  {name}: WF FAILED: {e}")
            wf_results.append({"name": name, "error": str(e)[:200]})

    final_report["wf_results"] = wf_results
    final_report["generated_at"] = datetime.now().isoformat()
    report_path.write_text(json.dumps(final_report, indent=2))

    # Save WF results separately
    wf_path = OUT_DIR / f"sentinel_30_targeted_v2_wf_{shard_label}_{ts}.json"
    wf_path.write_text(json.dumps({
        "generated_at": datetime.now().isoformat(),
        "shard": shard_label,
        "universe": args.universe,
        "wf_results": wf_results,
    }, indent=2))
    print(f"WF results saved: {wf_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())