#!/usr/bin/env python3
"""
Sentinel 30% CAGR — Aggressive Alpha Iteration

The current best (combo282_bep1.5_ts5) achieves ~7.5% CAGR on 5Y data.
This is an architecture limitation of the base RS7 system.

Attack vectors for 30%:
1. Concentrated high-alpha universe: only symbols with >10% max_20d from Telegram channels
2. Aggressive position sizing: risk 8-12% per trade instead of 5%
3. Momentum-only entries (no mean-reversion): trend-following with pyramiding
4. Regime-adaptive: trade 2x size in bull regime, 0.5x in bear
5. Quick-profit exit ladder: take 50% at +3%, trail remainder
6. Combine best OOS-robust variant (regime30_srb) with concentrated sizing

Previous best OOS-robust: regime30_srb_ts8 = 3.55% mean OOS, 3/5 folds, -1.4% worst
Previous best IS: combo282_bep1.5 = 7.42% CAGR, 42.85% ret, -15.33% DD
"""

from __future__ import annotations
import json, os, sys, time
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

MIN_ROWS = 100
MIN_SPAN_DAYS = 600

# High-alpha Telegram universe (symbols with >5% max_20d from audit)
TELEGRAM_ALPHA = [
    # From @shortterm01: COALINDIA (+24%), GPIL (+15%), MARINE (+8%)
    "COALINDIA", "GPIL", "MARINE",
    # From @darkhorseofstockmarket: VIJAYA (+21%), BOSCHLTD (+18%), CAMS (+127%), MAZDOCK (+9%), HAL (+10%)
    "VIJAYA", "BOSCHLTD", "MAZDOCK", "HAL",
    # From @milind4profits: RRKABEL (+27%), SKYGOLD (+26%), BHARATWIRE (+18%), GRSE (+17%), HUDCO (+12%)
    "RRKABEL", "SKYGOLD", "BHARATWIRE", "GRSE", "HUDCO",
    # From @financewithsunil: AEROFLEX (+44%), SUNFLAG (+33%), BBOX (+32%), SCHNEIDER (+16%)
    # Note: some small-caps may not have 5Y data
    "SCHNEIDER",
    # High-confidence curated additions
    "RELIANCE", "TATAMOTORS", "HINDALCO", "SBIN", "COALINDIA",
]

# Full Nifty 50 + high-alpha midcaps from Telegram
CURATED_EXPANDED = [
    # Nifty 50 core
    "RELIANCE", "TCS", "HDFCBANK", "INFY", "ICICIBANK", "HINDUNILVR",
    "SBIN", "BHARTIARTL", "ITC", "KOTAKBANK", "LT", "AXISBANK",
    "BAJFINANCE", "ASIANPAINT", "MARUTI", "TITAN", "SUNPHARMA",
    "TATAMOTORS", "WIPRO", "HCLTECH", "ULTRACEMCO", "NESTLEIND",
    "POWERGRID", "ONGC", "NTPC", "COALINDIA", "ADANIENT",
    "TECHM", "BAJAJFINSV", "TATASTEEL", "HINDALCO", "DRREDDY",
    "CIPLA", "DIVISLAB", "APOLLOHOSP", "EICHERMOT", "HEROMOTOCO",
    "M&M", "BPCL",
    # High-alpha from Telegram + audit
    "GRANULES", "GRSE", "HUDCO", "SKYGOLD", "RRKABEL", "BHARATWIRE",
    "COALINDIA", "GPIL", "MARINE", "BOSCHLTD", "HAL", "VIJAYA",
    "SCHNEIDER", "AEROFLEX", "SUNFLAG", "BBOX",
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


def run_variant(data_map, buy, sell, universe_label="full", risk_pct="0.05", max_weight="0.65"):
    old_r2 = dict(lab.RULE_SET_2.CONFIG)
    old_r7 = dict(lab.RULE_SET_7.CONFIG)
    # Save and override env sizing
    old_risk = os.environ.get("AT_BACKTEST_RISK_PER_TRADE_PCT", "0.05")
    old_weight = os.environ.get("AT_MAX_SINGLE_SYMBOL_WEIGHT", "0.65")
    try:
        lab.RULE_SET_2.CONFIG.clear()
        lab.RULE_SET_2.CONFIG.update(old_r2)
        lab.RULE_SET_2.CONFIG.update(sell)
        lab.RULE_SET_7.CONFIG.clear()
        lab.RULE_SET_7.CONFIG.update(old_r7)
        lab.RULE_SET_7.CONFIG.update(buy)
        os.environ["AT_BACKTEST_RISK_PER_TRADE_PCT"] = risk_pct
        os.environ["AT_MAX_SINGLE_SYMBOL_WEIGHT"] = max_weight
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
        os.environ["AT_BACKTEST_RISK_PER_TRADE_PCT"] = old_risk
        os.environ["AT_MAX_SINGLE_SYMBOL_WEIGHT"] = old_weight


# ── Aggressive Alpha Variant Definitions ──

# Base configs that showed best IS and/or OOS performance
COMBO282 = {"sr_breakout_enabled": 1, "sr_breakout_buffer_pct": 0.005,
             "sr_bounce_enabled": 1, "volume_confirm_mult": 0.85, "adx_strong_min": 18, "ich_cloud_bull": 0}

REGIME30_150 = {"adx_min": 5, "volume_confirm_mult": 0.3, "rsi_floor": 25,
                 "regime_filter_enabled": 1, "regime_ema_fast": 30, "regime_ema_slow": 150,
                 "ich_cloud_bull": 0, "vwap_buy_above": 0, "max_extension_atr": 5.0}

ADX18_ICH = {"adx_strong_min": 18, "ich_cloud_bull": 1, "volume_confirm_mult": 0.75}

# Momentum-only (no SR bounce/mean-reversion, pure trend-following)
MOMENTUM_ONLY = {"sr_bounce_enabled": 0, "sr_breakout_enabled": 0,
                  "adx_min": 20, "adx_strong_min": 25, "volume_confirm_mult": 0.9,
                  "rsi_floor": 45, "ich_cloud_bull": 1, "vwap_buy_above": 0,
                  "supertrend_direction_ok": 1, "supertrend_price_ok": 1,
                  "weekly_trend_ok": 1, "macd_signal_ok": 1, "macd_hist_rising": 1,
                  "max_extension_atr": 2.5}

# Pure breakout (strong trend only)
PURE_BREAKOUT = {"sr_breakout_enabled": 1, "sr_bounce_enabled": 0,
                  "adx_min": 25, "adx_strong_min": 30, "volume_confirm_mult": 1.0,
                  "rsi_floor": 50, "ich_cloud_bull": 1, "vwap_buy_above": 1,
                  "supertrend_direction_ok": 1, "weekly_trend_ok": 1,
                  "high_n_break": 1, "max_extension_atr": 1.5}

VARIANTS = []

# ── Group A: Aggressive sizing on best OOS-robust (regime30) ──
for risk in ["0.08", "0.10", "0.12"]:
    for maxw in ["0.65", "0.80"]:
        for ts in [5, 8]:
            VARIANTS.append({
                "name": f"regime30_risk{risk}_mw{maxw}_ts{ts}",
                "buy": {**REGIME30_150, "sr_bounce_enabled": 1, "sr_near_support_pct": 0.02},
                "sell": {"breakeven_trigger_pct": 2.0, "equity_time_stop_bars": ts},
                "risk_pct": risk, "max_weight": maxw,
                "universe": "expanded",
            })

# ── Group B: Aggressive sizing on best IS (combo282) ──
for risk in ["0.08", "0.10", "0.12"]:
    for maxw in ["0.65", "0.80"]:
        for bep in [1.5, 2.0]:
            VARIANTS.append({
                "name": f"combo282_risk{risk}_mw{maxw}_bep{bep}_ts5",
                "buy": {**COMBO282},
                "sell": {"breakeven_trigger_pct": bep, "equity_time_stop_bars": 5},
                "risk_pct": risk, "max_weight": maxw,
                "universe": "expanded",
            })

# ── Group C: Momentum-only (pure trend-following, no mean reversion) ──
for risk in ["0.05", "0.08", "0.10"]:
    for ts in [5, 8, 10]:
        VARIANTS.append({
            "name": f"momentum_risk{risk}_ts{ts}",
            "buy": {**MOMENTUM_ONLY},
            "sell": {"momentum_exit_rsi": 45.0, "equity_review_rsi": 50.0,
                     "breakeven_trigger_pct": 2.0, "equity_time_stop_bars": ts},
            "risk_pct": risk, "max_weight": "0.65",
            "universe": "expanded",
        })

# ── Group D: Pure breakout (strong trend entries only) ──
for risk in ["0.05", "0.08", "0.10"]:
    for ts in [5, 8, 10]:
        VARIANTS.append({
            "name": f"breakout_risk{risk}_ts{ts}",
            "buy": {**PURE_BREAKOUT},
            "sell": {"momentum_exit_rsi": 50.0, "equity_review_rsi": 55.0,
                     "breakeven_trigger_pct": 1.5, "equity_time_stop_bars": ts},
            "risk_pct": risk, "max_weight": "0.65",
            "universe": "expanded",
        })

# ── Group E: Telegram alpha universe only ──
for buy_cfg, label in [(COMBO282, "c282"), (REGIME30_150, "reg30"), (ADX18_ICH, "adx18ich")]:
    for ts in [5, 8]:
        VARIANTS.append({
            "name": f"tgalp_{label}_ts{ts}",
            "buy": {**buy_cfg},
            "sell": {"breakeven_trigger_pct": 2.0, "equity_time_stop_bars": ts},
            "risk_pct": "0.08", "max_weight": "0.80",
            "universe": "telegram_alpha",
        })

# ── Group F: Telegram alpha + aggressive sizing ──
for buy_cfg, label in [(COMBO282, "c282"), (REGIME30_150, "reg30")]:
    for risk in ["0.10", "0.12"]:
        VARIANTS.append({
            "name": f"tgalp_{label}_risk{risk}_ts5",
            "buy": {**buy_cfg},
            "sell": {"breakeven_trigger_pct": 2.0, "equity_time_stop_bars": 5},
            "risk_pct": risk, "max_weight": "0.80",
            "universe": "telegram_alpha",
        })


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--shard", type=str, default="")
    parser.add_argument("--universe", type=str, default="expanded",
                        choices=["expanded", "telegram_alpha", "full", "curated"])
    parser.add_argument("--walk-forward", action="store_true", help="Run walk-forward validation on top 3")
    args = parser.parse_args()

    # Select universe
    if args.universe == "telegram_alpha":
        symbol_filter = set(TELEGRAM_ALPHA)
    elif args.universe == "curated":
        symbol_filter = set(CURATED_EXPANDED[:39])
    elif args.universe == "expanded":
        symbol_filter = set(CURATED_EXPANDED)
    else:
        symbol_filter = None

    # Filter to only variants matching requested universe scope
    universe_scope = args.universe
    if args.limit == 0 and args.offset == 0:
        all_variants = VARIANTS
    else:
        all_variants = VARIANTS[args.offset:]
        if args.limit > 0:
            all_variants = all_variants[:args.limit]

    shard_label = args.shard or f"aggressive_{args.universe}_o{args.offset}"

    print("=" * 60)
    print(f"SENTINEL 30% AGGRESSIVE — {shard_label}")
    print(f"Universe: {args.universe} | Variants: {len(all_variants)}")
    print("=" * 60)

    data_map = load_kite_symbols(symbol_filter=symbol_filter)
    if not data_map:
        print("ERROR: No symbols loaded")
        return 1

    ranked = []
    best_cagr = 0.0
    best_name = ""

    STATUS_FILE = OUT_DIR / "sentinel_30_aggressive_status.json"
    HISTORY_FILE = OUT_DIR / "sentinel_30_aggressive_history.jsonl"

    for idx, bp in enumerate(all_variants, 1):
        name = bp["name"]
        buy = {k: v for k, v in bp["buy"].items() if not k.startswith("_")}
        sell = bp.get("sell", {})
        risk_pct = bp.get("risk_pct", "0.05")
        max_weight = bp.get("max_weight", "0.65")
        universe_label = bp.get("universe", args.universe)

        t0 = time.time()
        result = run_variant(data_map, buy, sell, universe_label=universe_label,
                             risk_pct=risk_pct, max_weight=max_weight)
        elapsed = time.time() - t0

        result.name = name
        row = asdict(result)
        row["shard"] = shard_label
        row["risk_pct"] = risk_pct
        row["max_weight"] = max_weight
        row["buy"] = dict(buy)
        row["sell"] = dict(sell)
        ranked.append(row)

        if HISTORY_FILE.exists() or idx == 1:
            with open(HISTORY_FILE, "a") as f:
                f.write(json.dumps({**row, "elapsed_s": round(elapsed, 1)}) + "\n")

        if result.cagr_pct > best_cagr:
            best_cagr = result.cagr_pct
            best_name = name

        ts_str = datetime.now().strftime("%H:%M:%S")
        print(f"{ts_str} {idx}/{len(all_variants)} {name} cagr={result.cagr_pct}% "
              f"ret={result.total_return_pct}% dd={result.max_drawdown_pct}% "
              f"trades={result.trades} sharpe={result.sharpe} [{elapsed:.0f}s]", flush=True)

        now_iso = datetime.now().isoformat()
        STATUS_FILE.write_text(json.dumps({
            "generated_at": now_iso,
            "updated_at": now_iso,
            "status": "running" if idx < len(all_variants) else "completed",
            "pid": os.getpid(),
            "shard": shard_label,
            "universe": args.universe,
            "variants_total": len(all_variants),
            "best_variant": best_name,
            "best_cagr_pct": round(best_cagr, 2),
        }, indent=2))

    # ── Final ranking ──
    ranked.sort(key=lambda r: r.get("cagr_pct", 0) or 0, reverse=True)

    ts_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_file = OUT_DIR / f"sentinel_30_aggressive_{shard_label}_{ts_str}.json"
    wf_out_file = OUT_DIR / f"sentinel_30_aggressive_wf_{shard_label}_{ts_str}.json"

    payload = {
        "generated_at": datetime.now().isoformat(),
        "hunt_label": f"Sentinel 30% Aggressive — {shard_label}",
        "data_source": "kite_feather_5y",
        "shard": shard_label,
        "universe": args.universe,
        "symbols_loaded": len(data_map),
        "variants_total": len(all_variants),
        "best_variant": best_name,
        "best_cagr_pct": round(best_cagr, 2),
        "ranked": ranked[:30],
    }

    out_file.write_text(json.dumps(payload, indent=2, default=str))
    print(f"\nSaved: {out_file}")

    STATUS_FILE.write_text(json.dumps({
        "generated_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat(),
        "status": "completed",
        "pid": os.getpid(),
        "shard": shard_label,
        "universe": args.universe,
        "variants_total": len(all_variants),
        "best_variant": best_name,
        "best_cagr_pct": round(best_cagr, 2),
        "report_path": str(out_file),
    }, indent=2))

    # ── Walk-forward validation on top 3 ──
    if args.walk_forward and len(ranked) >= 3:
        from scripts.weekly_strategy_lab import run_walk_forward_validation
        top3 = [r for r in ranked[:3] if not r.get("error")]

        # Re-run with WF for each top candidate
        wf_results = []
        for cand in top3:
            buy = {k: v for k, v in cand["buy"].items() if not k.startswith("_")}
            sell = cand.get("sell", {})
            risk_pct = cand.get("risk_pct", "0.05")
            max_weight = cand.get("max_weight", "0.65")

            # Temporarily apply config
            old_r2 = dict(lab.RULE_SET_2.CONFIG)
            old_r7 = dict(lab.RULE_SET_7.CONFIG)
            old_risk_env = os.environ.get("AT_BACKTEST_RISK_PER_TRADE_PCT", "0.05")
            old_weight_env = os.environ.get("AT_MAX_SINGLE_SYMBOL_WEIGHT", "0.65")
            try:
                lab.RULE_SET_2.CONFIG.clear()
                lab.RULE_SET_2.CONFIG.update(old_r2)
                lab.RULE_SET_2.CONFIG.update(sell)
                lab.RULE_SET_7.CONFIG.clear()
                lab.RULE_SET_7.CONFIG.update(old_r7)
                lab.RULE_SET_7.CONFIG.update(buy)
                os.environ["AT_BACKTEST_RISK_PER_TRADE_PCT"] = risk_pct
                os.environ["AT_MAX_SINGLE_SYMBOL_WEIGHT"] = max_weight

                wf_data = run_walk_forward_validation(data_map, n_splits=5)
                wf_results.append({
                    "name": cand["name"],
                    "n_folds": 5,
                    "mean_oos_return_pct": round(wf_data.get("mean_oos_return_pct", 0), 2),
                    "std_oos_return_pct": round(wf_data.get("std_oos_return_pct", 0), 2),
                    "min_oos_return_pct": round(wf_data.get("min_oos_return_pct", 0), 2),
                    "max_oos_return_pct": round(wf_data.get("max_oos_return_pct", 0), 2),
                    "positive_folds": wf_data.get("positive_folds", 0),
                    "folds": wf_data.get("folds", []),
                })
            except Exception as e:
                wf_results.append({"name": cand["name"], "error": str(e)[:200]})
            finally:
                lab.RULE_SET_2.CONFIG.clear()
                lab.RULE_SET_2.CONFIG.update(old_r2)
                lab.RULE_SET_7.CONFIG.clear()
                lab.RULE_SET_7.CONFIG.update(old_r7)
                os.environ["AT_BACKTEST_RISK_PER_TRADE_PCT"] = old_risk_env
                os.environ["AT_MAX_SINGLE_SYMBOL_WEIGHT"] = old_weight_env

        wf_payload = {
            "generated_at": datetime.now().isoformat(),
            "shard": shard_label,
            "universe": args.universe,
            "wf_results": wf_results,
        }
        wf_out_file.write_text(json.dumps(wf_payload, indent=2, default=str))
        print(f"\nWF saved: {wf_out_file}")

    print(f"\nDone. Best: {best_name} @ {best_cagr:.2f}% CAGR")
    return 0


if __name__ == "__main__":
    sys.exit(main())