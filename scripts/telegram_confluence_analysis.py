#!/usr/bin/env python3
"""Telegram Confluence Analysis for 30% CAGR Lab.

Analyzes how Telegram channel signals align with RS7/ADX buy signals
and identifies potential confluence strategies.
"""
from __future__ import annotations
import json
import os
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "reports"
OUT_DIR.mkdir(parents=True, exist_ok=True)

def main() -> int:
    # Load channel learning scores
    scores_path = OUT_DIR / "channel_learning_scores.json"
    audit_path = OUT_DIR / "telegram_trade_audit_latest.json"
    
    if not scores_path.exists():
        print("No channel_learning_scores.json found")
        return 1
    if not audit_path.exists():
        print("No telegram_trade_audit_latest.json found")
        return 1
    
    scores = json.loads(scores_path.read_text())
    audit = json.loads(audit_path.read_text())
    
    # Extract best symbols from Telegram channels
    telegram_symbols = set()
    channel_stats = {}
    for ch_name, ch in scores.get("channels", {}).items():
        conf = ch.get("confidence", 0)
        eq = ch.get("equity_audit", {})
        opt = ch.get("options_audit", {})
        sizing = ch.get("sizing_mult", 0)
        
        stats = {
            "confidence": conf,
            "sizing_mult": sizing,
            "action": ch.get("action", "?"),
        }
        if eq.get("signals_evaluated"):
            stats["eq_signals"] = eq["signals_evaluated"]
            stats["eq_ret_5d_avg"] = eq.get("ret_5d_avg")
            stats["eq_ret_5d_positive_rate"] = eq.get("ret_5d_positive_rate")
        if opt.get("signals_evaluated"):
            stats["opt_signals"] = opt["signals_evaluated"]
            stats["opt_dir_5d_avg"] = opt.get("dir_ret_5d_avg")
            stats["opt_dir_5d_positive_rate"] = opt.get("dir_ret_5d_positive_rate")
        
        channel_stats[ch_name] = stats
    
    # Collect symbols from all channels
    for ch_name in ["finance_with_sunil", "finance_with_sunil_options", "shortterm01", "milind4profits"]:
        ch = audit.get(ch_name, {})
        for sym in ch.get("summary", {}).get("symbols", []):
            telegram_symbols.add(sym)
    
    # Also from channels in scores
    channels_in_audit = audit.get("channels", {})
    for ch_name, ch in channels_in_audit.items():
        for sym in ch.get("summary", {}).get("symbols", []):
            telegram_symbols.add(sym)
    
    # Analysis
    result = {
        "generated_at": datetime.now().astimezone().isoformat(),
        "channel_stats": channel_stats,
        "telegram_symbols": sorted(telegram_symbols),
        "telegram_symbols_count": len(telegram_symbols),
        "recommendations": [],
    }
    
    # Build confluence recommendations
    for ch_name, stats in channel_stats.items():
        conf = stats.get("confidence", 0)
        if conf >= 50 and stats.get("sizing_mult", 0) < 0.3:
            result["recommendations"].append({
                "channel": ch_name,
                "action": "observe_high_confidence",
                "rationale": f"Confidence {conf} but low sizing - accumulate data before boosting",
                "suggested_sizing": 0.2,
            })
        elif conf < 40:
            result["recommendations"].append({
                "channel": ch_name,
                "action": "skip",
                "rationale": f"Confidence {conf} too low for any position",
                "suggested_sizing": 0.0,
            })
    
    # Telegram confluence feature proposal
    result["confluence_features"] = {
        "telegram_watchlist_boost": {
            "description": "Boost BUY signal confidence for symbols appearing in Telegram channels with conf >= 45",
            "proposed_boost": 0.1,
            "channels": [ch for ch, s in channel_stats.items() if s.get("confidence", 0) >= 45],
            "symbols": sorted(telegram_symbols),
        },
        "telegram_momentum_lead": {
            "description": "Use Telegram signal timestamps to detect momentum before RS7 BUY signal",
            "lookback_days": 5,
            "proposed_weight": 0.05,
        },
        "quick_profit_taking": {
            "description": "High max_favorable but weak close-return patterns suggest quicker trailing stops",
            "channels_with_pattern": [],
        }
    }
    
    # Check max_favorable patterns
    for ch_name, ch in scores.get("channels", {}).items():
        eq = ch.get("equity_audit", {})
        opt = ch.get("options_audit", {})
        max_fav = eq.get("max_20d_avg") or opt.get("max_favorable_20d_avg")
        close_ret = eq.get("ret_5d_avg")
        if max_fav is not None and close_ret is not None:
            if max_fav > 3 and close_ret < 0:
                result["confluence_features"]["quick_profit_taking"]["channels_with_pattern"].append(ch_name)
    
    # Save
    output_path = OUT_DIR / "telegram_confluence_analysis_latest.json"
    output_path.write_text(json.dumps(result, indent=2))
    print(f"Saved confluence analysis to {output_path}")
    print(f"Channels analyzed: {len(channel_stats)}")
    print(f"Telegram symbols: {len(telegram_symbols)}")
    print(f"Recommendations: {len(result['recommendations'])}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
