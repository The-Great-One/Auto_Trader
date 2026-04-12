#!/usr/bin/env python3
"""
Research-only options strategy lab.

Purpose:
- run the existing RULE_SET_7 (BUY) + RULE_SET_2 (SELL) logic against cached
  option OHLCV files under intermediary_files/Hist_Data
- rank parameter variants for options separately from the equity/ETF lab
- do NOT auto-promote into live trading, because the base runtime is not yet
  options-capable end to end

Expected symbol format:
- cached files named like NIFTY24APR24500CE.feather or BANKNIFTY...PE.feather
- explicit symbol overrides via AT_OPTIONS_LAB_SYMBOLS are also supported
"""

from __future__ import annotations

import json
import os
import re
import sys
import tempfile
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd

# Avoid noisy file-handler permission issues during research/backtest runs.
os.environ.setdefault("AT_DISABLE_FILE_LOGGING", "1")

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from Auto_Trader import RULE_SET_2, RULE_SET_7, logger as at_logger
from Auto_Trader import utils as at_utils
from scripts import weekly_strategy_lab as core_lab

at_logger.setLevel("WARNING")

OUT_DIR = ROOT / "reports"
OUT_DIR.mkdir(exist_ok=True)
HIST_DIR = ROOT / "intermediary_files" / "Hist_Data"
OPTION_SYMBOL_RE = re.compile(r"^[A-Z0-9]+\d+(CE|PE)$")


@dataclass
class BacktestResult:
    name: str
    final_value: float
    total_return_pct: float
    trades: int
    win_rate_pct: float
    max_drawdown_pct: float
    params: dict
    symbols_tested: list[str]
    selection_score: float


def _parse_symbol_list(value: str) -> list[str]:
    return [x.strip().upper() for x in str(value or "").split(",") if x.strip()]


def _option_side(symbol: str) -> str:
    text = str(symbol or "").upper()
    if text.endswith("CE"):
        return "CE"
    if text.endswith("PE"):
        return "PE"
    return ""


def _looks_like_option_symbol(symbol: str) -> bool:
    return bool(OPTION_SYMBOL_RE.match(str(symbol or "").upper()))


def discover_option_symbols() -> list[str]:
    explicit = os.getenv("AT_OPTIONS_LAB_SYMBOLS", "").strip()
    if explicit:
        return _parse_symbol_list(explicit)

    underlyings = _parse_symbol_list(
        os.getenv("AT_OPTIONS_LAB_UNDERLYINGS", "NIFTY,BANKNIFTY,FINNIFTY")
    )
    side_filter = os.getenv("AT_OPTIONS_LAB_SIDE", "BOTH").strip().upper()
    max_symbols = max(1, int(os.getenv("AT_OPTIONS_LAB_MAX_SYMBOLS", "12")))

    if not HIST_DIR.exists():
        return []

    candidates: list[str] = []
    for path in sorted(HIST_DIR.glob("*.feather")):
        symbol = path.stem.upper()
        if not _looks_like_option_symbol(symbol):
            continue
        if underlyings and not any(symbol.startswith(u) for u in underlyings):
            continue
        side = _option_side(symbol)
        if side_filter in {"CE", "PE"} and side != side_filter:
            continue
        candidates.append(symbol)

    return candidates[:max_symbols]


def load_option_data() -> tuple[dict[str, pd.DataFrame], dict]:
    symbols = discover_option_symbols()
    min_bars = max(30, int(os.getenv("AT_OPTIONS_LAB_MIN_BARS", "60")))
    data_map: dict[str, pd.DataFrame] = {}
    skipped: dict[str, str] = {}

    for symbol in symbols:
        path = HIST_DIR / f"{symbol}.feather"
        if not path.exists():
            skipped[symbol] = "missing_file"
            continue
        try:
            df = core_lab._normalize_ohlcv(pd.read_feather(path))
        except Exception as exc:
            skipped[symbol] = f"read_failed:{exc}"
            continue

        if df is None or df.empty:
            skipped[symbol] = "empty"
            continue
        if len(df) < min_bars:
            skipped[symbol] = f"too_short:{len(df)}"
            continue

        try:
            data_map[symbol] = at_utils.Indicators(df)
        except Exception as exc:
            skipped[symbol] = f"indicator_failed:{exc}"

    if not data_map:
        raise RuntimeError(
            "Could not load any option symbols with usable history. "
            "Populate intermediary_files/Hist_Data with option feather files or set AT_OPTIONS_LAB_SYMBOLS."
        )

    return data_map, {
        "requested_symbols": symbols,
        "loaded_symbols": list(data_map.keys()),
        "skipped_symbols": skipped,
        "side_filter": os.getenv("AT_OPTIONS_LAB_SIDE", "BOTH").strip().upper(),
        "underlyings": _parse_symbol_list(
            os.getenv("AT_OPTIONS_LAB_UNDERLYINGS", "NIFTY,BANKNIFTY,FINNIFTY")
        ),
        "min_bars": min_bars,
    }


def _set_temp_state(rule2_module, d: str):
    rule2_module.BASE_DIR = d
    rule2_module.HOLDINGS_FILE_PATH = os.path.join(d, "Holdings.json")
    rule2_module.LOCK_FILE_PATH = os.path.join(d, "Holdings.lock")


def _simulate_symbol(symbol: str, df: pd.DataFrame) -> dict[str, float]:
    cash = 100000.0
    qty = 0
    avg = 0.0
    entry_idx = None
    trades = 0
    wins = 0
    equity_curve = []
    warmup = max(20, int(os.getenv("AT_OPTIONS_LAB_WARMUP_BARS", "40")))

    for i in range(min(warmup, len(df)), len(df)):
        part = df.iloc[: i + 1].copy()
        row = part.iloc[-1].to_dict()
        row.setdefault("instrument_token", 1626369)
        price = float(part.iloc[-1]["Close"])

        if qty == 0:
            hold_df = pd.DataFrame(
                columns=[
                    "instrument_token",
                    "tradingsymbol",
                    "average_price",
                    "quantity",
                    "t1_quantity",
                    "bars_in_trade",
                ]
            )
            sig = RULE_SET_7.buy_or_sell(part, row, hold_df)
            if str(sig).upper() == "BUY" and price > 0:
                buy_qty = int(cash // price)
                if buy_qty > 0:
                    qty = buy_qty
                    cash -= qty * price
                    avg = price
                    entry_idx = i
                    trades += 1
        else:
            hold_df = pd.DataFrame(
                [
                    {
                        "instrument_token": int(row.get("instrument_token", 1626369)),
                        "tradingsymbol": symbol,
                        "average_price": avg,
                        "quantity": qty,
                        "t1_quantity": 0,
                        "bars_in_trade": max(0, i - entry_idx) if entry_idx is not None else 0,
                    }
                ]
            )
            sig = RULE_SET_2.buy_or_sell(part, row, hold_df)
            if str(sig).upper() == "SELL":
                cash += qty * price
                if price > avg:
                    wins += 1
                qty = 0
                avg = 0.0
                entry_idx = None
                trades += 1

        port = cash + (qty * price)
        equity_curve.append(port)

    final_val = equity_curve[-1] if equity_curve else 100000.0
    s = pd.Series(equity_curve if equity_curve else [100000.0], dtype=float)
    peak = s.cummax()
    dd = ((s - peak) / peak * 100.0).min()
    return {
        "final_value": float(final_val),
        "trades": int(trades),
        "wins": int(wins),
        "max_drawdown_pct": float(dd),
    }


def run_variant(name: str, data_map: dict[str, pd.DataFrame], buy_params: dict, sell_params: dict) -> BacktestResult:
    at_utils.get_mmi_now = lambda: None

    old_r2 = dict(RULE_SET_2.CONFIG)
    old_r7 = dict(RULE_SET_7.CONFIG)
    RULE_SET_2.CONFIG.update(sell_params)
    RULE_SET_7.CONFIG.update(buy_params)

    try:
        with tempfile.TemporaryDirectory(prefix="at_options_state_") as td:
            _set_temp_state(RULE_SET_2, td)
            total_final_value = 0.0
            total_trades = 0
            total_wins = 0
            worst_dd = 0.0
            tested_symbols: list[str] = []

            for symbol, df in data_map.items():
                stats = _simulate_symbol(symbol, df)
                total_final_value += stats["final_value"]
                total_trades += stats["trades"]
                total_wins += stats["wins"]
                worst_dd = min(worst_dd, stats["max_drawdown_pct"])
                tested_symbols.append(symbol)
    finally:
        RULE_SET_2.CONFIG.clear()
        RULE_SET_2.CONFIG.update(old_r2)
        RULE_SET_7.CONFIG.clear()
        RULE_SET_7.CONFIG.update(old_r7)

    start_capital = 100000.0 * max(1, len(tested_symbols))
    ret = (total_final_value / start_capital - 1.0) * 100.0
    round_trips = max(1, total_trades // 2)
    win_rate = (total_wins / round_trips) * 100.0
    selection_score = float(ret + (0.015 * total_trades) - (0.18 * abs(min(0.0, worst_dd))))

    return BacktestResult(
        name=name,
        final_value=round(float(total_final_value), 2),
        total_return_pct=round(float(ret), 2),
        trades=int(total_trades),
        win_rate_pct=round(float(win_rate), 2),
        max_drawdown_pct=round(float(worst_dd), 2),
        params={"buy": buy_params, "sell": sell_params},
        symbols_tested=tested_symbols,
        selection_score=round(selection_score, 3),
    )


def option_variants(scorecard_context: dict, tradebook_context: dict):
    old = os.environ.get("AT_LAB_MAX_VARIANTS")
    os.environ["AT_LAB_MAX_VARIANTS"] = os.getenv("AT_OPTIONS_LAB_MAX_VARIANTS", "80")
    try:
        return core_lab.variants(scorecard_context, tradebook_context)
    finally:
        if old is None:
            os.environ.pop("AT_LAB_MAX_VARIANTS", None)
        else:
            os.environ["AT_LAB_MAX_VARIANTS"] = old


def main():
    scorecard_context = core_lab.load_scorecard_context()
    tradebook_context = core_lab.load_tradebook_context()
    data_map, data_context = load_option_data()

    results = []
    for name, b, s in option_variants(scorecard_context, tradebook_context):
        results.append(run_variant(name, data_map, b, s))

    rank = sorted(
        results,
        key=lambda r: (r.selection_score, r.total_return_pct, -abs(r.max_drawdown_pct), r.win_rate_pct),
        reverse=True,
    )
    baseline = next(r for r in rank if r.name == "baseline_current")
    best = rank[0]

    recommendation = {
        "generated_at": datetime.now().isoformat(),
        "lab_type": "options_research_only",
        "production_rule_model": "BUY=RULE_SET_7, SELL=RULE_SET_2",
        "supports_live_auto_promotion": False,
        "scorecard_context": scorecard_context,
        "tradebook_context": tradebook_context,
        "data_context": data_context,
        "baseline": asdict(baseline),
        "best": asdict(best),
        "tested_variants": len(rank),
        "improvement_return_pct": round(best.total_return_pct - baseline.total_return_pct, 2),
        "improvement_score": round(best.selection_score - baseline.selection_score, 3),
        "should_promote": False,
        "notes": [
            "Research-only lab. Current live runtime is not options-capable end to end.",
            "Use results to guide future options runtime work, not live promotion.",
        ],
    }

    payload = {
        "recommendation": recommendation,
        "ranked": [asdict(r) for r in rank],
    }

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_json = OUT_DIR / f"options_strategy_lab_{ts}.json"
    out_csv = OUT_DIR / f"options_strategy_lab_{ts}.csv"

    out_json.write_text(json.dumps(payload, indent=2))
    pd.DataFrame([asdict(r) for r in rank]).to_csv(out_csv, index=False)

    print(json.dumps(recommendation, indent=2))
    print(f"Saved: {out_json}")
    print(f"Saved: {out_csv}")


if __name__ == "__main__":
    main()
