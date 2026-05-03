#!/usr/bin/env python3
"""
Research-only NIFTY options strategy lab.

- uses RULE_SET_OPTIONS_1 as the base rule
- runs parameter iterations around that base rule
- consumes the NIFTY options manifest/data fetched by fetch_nifty_options_data.py
- does NOT auto-promote into live trading
"""

from __future__ import annotations

import json
import os
import sys
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd

os.environ.setdefault("AT_DISABLE_FILE_LOGGING", "1")

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from Auto_Trader import RULE_SET_OPTIONS_1, logger as at_logger
from Auto_Trader import options_support as opt_support

at_logger.setLevel("WARNING")

OUT_DIR = ROOT / "reports"
OUT_DIR.mkdir(exist_ok=True)
HIST_DIR = ROOT / "intermediary_files" / "Hist_Data"


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


BASE_CONFIG_KEYS = [
    "underlying_rsi_bull_min",
    "underlying_rsi_bear_max",
    "underlying_adx_min",
    "option_rsi_min",
    "volume_confirm_mult",
    "oi_sma_mult",
    "oi_change_min_pct",
    "atr_pct_min",
    "atr_pct_max",
    "buy_score_min",
    "take_profit_pct",
    "stop_loss_pct",
    "max_hold_bars",
    "exit_rsi",
]


def prioritized_values(values, current):
    current_f = float(current)
    uniq = []
    seen = set()
    for value in values:
        key = float(value)
        if key in seen:
            continue
        seen.add(key)
        uniq.append(value)
    return sorted(uniq, key=lambda v: (abs(float(v) - current_f), float(v)))


def load_option_data() -> tuple[dict[str, pd.DataFrame], dict]:
    symbols = opt_support.discover_option_symbols()
    min_bars = max(12, int(os.getenv("AT_OPTIONS_LAB_MIN_BARS", "15")))
    data_map: dict[str, pd.DataFrame] = {}
    skipped: dict[str, str] = {}

    for symbol in symbols:
        path = HIST_DIR / f"{symbol}.feather"
        if not path.exists():
            skipped[symbol] = "missing_file"
            continue
        try:
            df = opt_support.enrich_option_frame(pd.read_feather(path))
        except Exception as exc:
            skipped[symbol] = f"enrich_failed:{exc}"
            continue
        if df is None or df.empty:
            skipped[symbol] = "empty"
            continue
        if len(df) < min_bars:
            skipped[symbol] = f"too_short:{len(df)}"
            continue
        data_map[symbol] = df.reset_index(drop=True)

    if not data_map:
        raise RuntimeError(
            "Could not load any option symbols with usable history. Run fetch_nifty_options_data.py first."
        )

    return data_map, {
        "requested_symbols": symbols,
        "loaded_symbols": list(data_map.keys()),
        "skipped_symbols": skipped,
        "side_filter": os.getenv("AT_OPTIONS_LAB_SIDE", "BOTH").strip().upper(),
        "underlyings": opt_support.parse_symbol_list(os.getenv("AT_OPTIONS_LAB_UNDERLYINGS", "NIFTY")),
        "manifest_path": str(opt_support.OPTIONS_MANIFEST),
        "min_bars": min_bars,
        "underlying_context_path": str(opt_support.HIST_DIR / "NIFTY50_INDEX.feather"),
    }



def base_params() -> dict:
    return {k: RULE_SET_OPTIONS_1.CONFIG[k] for k in BASE_CONFIG_KEYS}



def build_grids(scorecard_context: dict, tradebook_context: dict) -> dict:
    cfg = RULE_SET_OPTIONS_1.CONFIG
    grid = {
        "underlying_rsi_bull_min": prioritized_values([50, 52, 55, 58, 60], cfg["underlying_rsi_bull_min"]),
        "underlying_rsi_bear_max": prioritized_values([40, 42, 45, 48], cfg["underlying_rsi_bear_max"]),
        "underlying_adx_min": prioritized_values([10, 12, 14, 16, 18, 22], cfg["underlying_adx_min"]),
        "option_rsi_min": prioritized_values([48, 50, 52, 54, 56, 60], cfg["option_rsi_min"]),
        "volume_confirm_mult": prioritized_values([0.9, 0.95, 1.0, 1.1, 1.25], cfg["volume_confirm_mult"]),
        "oi_sma_mult": prioritized_values([1.0, 1.02, 1.05, 1.1], cfg["oi_sma_mult"]),
        "oi_change_min_pct": prioritized_values([-1.0, 0.0, 1.0, 2.0, 3.0], cfg["oi_change_min_pct"]),
        "atr_pct_min": prioritized_values([0.0, 0.02, 0.03, 0.05], cfg["atr_pct_min"]),
        "atr_pct_max": prioritized_values([0.8, 1.0, 1.5, 2.0], cfg["atr_pct_max"]),
        "buy_score_min": prioritized_values([4.5, 5.0, 5.5, 6.0, 6.5], cfg["buy_score_min"]),
        "take_profit_pct": prioritized_values([12.0, 15.0, 18.0, 25.0, 35.0], cfg["take_profit_pct"]),
        "stop_loss_pct": prioritized_values([8.0, 10.0, 12.0, 15.0], cfg["stop_loss_pct"]),
        "max_hold_bars": prioritized_values([1, 2, 3, 4, 6], cfg["max_hold_bars"]),
        "exit_rsi": prioritized_values([38.0, 40.0, 42.0, 45.0, 50.0], cfg["exit_rsi"]),
    }

    if scorecard_context.get("no_trade_day"):
        grid["option_rsi_min"] = prioritized_values([48, *grid["option_rsi_min"]], cfg["option_rsi_min"])
        grid["volume_confirm_mult"] = prioritized_values([0.9, *grid["volume_confirm_mult"]], cfg["volume_confirm_mult"])
        grid["oi_change_min_pct"] = prioritized_values([-1.0, *grid["oi_change_min_pct"]], cfg["oi_change_min_pct"])
        grid["buy_score_min"] = prioritized_values([4.5, *grid["buy_score_min"]], cfg["buy_score_min"])

    if tradebook_context.get("weak_mid_hold_window"):
        grid["take_profit_pct"] = prioritized_values([12.0, 15.0, *grid["take_profit_pct"]], cfg["take_profit_pct"])
        grid["max_hold_bars"] = prioritized_values([1, 2, *grid["max_hold_bars"]], cfg["max_hold_bars"])
        grid["exit_rsi"] = prioritized_values([40.0, *grid["exit_rsi"]], cfg["exit_rsi"])

    return grid



def _variant_key(params: dict) -> str:
    return json.dumps(params, sort_keys=True)



def option_variants(scorecard_context: dict, tradebook_context: dict) -> list[tuple[str, dict]]:
    base = base_params()
    grid = build_grids(scorecard_context, tradebook_context)

    out: list[tuple[str, dict]] = []
    seen: set[str] = set()

    def add(name: str, patch: dict):
        key = _variant_key(patch)
        if key in seen:
            return
        seen.add(key)
        out.append((name, patch))

    add("baseline_current", {})

    for key, values in grid.items():
        for value in values:
            if float(value) == float(base[key]):
                continue
            add(f"{key}_{value}", {key: value})

    focus_groups = [
        ("underlying_rsi_bull_min", "underlying_adx_min", "buy_score_min"),
        ("option_rsi_min", "volume_confirm_mult", "oi_change_min_pct"),
        ("take_profit_pct", "stop_loss_pct", "max_hold_bars"),
        ("atr_pct_min", "atr_pct_max", "exit_rsi"),
    ]

    combo_idx = 0
    for keys in focus_groups:
        value_lists = []
        for key in keys:
            vals = [v for v in grid[key] if float(v) != float(base[key])][:2]
            if not vals:
                vals = [base[key]]
            value_lists.append(vals)
        for a in value_lists[0]:
            for b in value_lists[1]:
                for c in value_lists[2]:
                    combo_idx += 1
                    add(f"focus_combo_{combo_idx:03d}", {keys[0]: a, keys[1]: b, keys[2]: c})

    prebuilt = [
        {"option_rsi_min": 50, "volume_confirm_mult": 0.95, "buy_score_min": 5.0},
        {"underlying_rsi_bull_min": 52, "underlying_rsi_bear_max": 48, "underlying_adx_min": 14},
        {"oi_sma_mult": 1.0, "oi_change_min_pct": 0.0, "buy_score_min": 5.5},
        {"take_profit_pct": 15.0, "stop_loss_pct": 10.0, "max_hold_bars": 2},
        {"take_profit_pct": 35.0, "stop_loss_pct": 8.0, "exit_rsi": 50.0},
        {"atr_pct_min": 0.0, "atr_pct_max": 2.0, "buy_score_min": 5.5},
        # ADX trend + tighter exits (reduce drawdown)
        {"underlying_adx_min": 14, "take_profit_pct": 18.0, "stop_loss_pct": 10.0},
        # ADX + score filter (higher conviction)
        {"underlying_adx_min": 14, "buy_score_min": 6.0, "take_profit_pct": 25.0},
        # ADX + RSI combo
        {"underlying_adx_min": 18, "underlying_rsi_bull_min": 55, "buy_score_min": 5.5},
    ]
    for idx, patch in enumerate(prebuilt, start=1):
        add(f"prebuilt_combo_{idx:02d}", patch)

    adx14_cluster_idx = 0
    for bull_min in [50, 52, 54]:
        for buy_score in [4.5, 5.0, 5.5]:
            for option_rsi in [48, 50, 52]:
                adx14_cluster_idx += 1
                add(
                    f"adx14_cluster_{adx14_cluster_idx:03d}",
                    {
                        "underlying_adx_min": 14,
                        "underlying_rsi_bull_min": bull_min,
                        "buy_score_min": buy_score,
                        "option_rsi_min": option_rsi,
                        "volume_confirm_mult": 0.95,
                        "oi_change_min_pct": 0.0,
                    },
                )

    adx14_exit_idx = 0
    for take_profit in [12.0, 15.0, 18.0, 25.0]:
        for stop_loss in [8.0, 10.0, 12.0]:
            adx14_exit_idx += 1
            add(
                f"adx14_exit_{adx14_exit_idx:03d}",
                {
                    "underlying_adx_min": 14,
                    "take_profit_pct": take_profit,
                    "stop_loss_pct": stop_loss,
                    "max_hold_bars": 2,
                    "buy_score_min": 5.0,
                },
            )

    # Cross entry + exit families. Earlier sweeps found that entry loosening and
    # exits can each help, but testing them separately misses the interaction.
    entry_templates = [
        {"oi_change_min_pct": -1.0},
        {"oi_sma_mult": 1.0, "oi_change_min_pct": 0.0, "buy_score_min": 5.5},
        {"underlying_adx_min": 14, "underlying_rsi_bull_min": 50, "buy_score_min": 4.5, "option_rsi_min": 48, "volume_confirm_mult": 0.95, "oi_change_min_pct": 0.0},
        {"underlying_adx_min": 14, "underlying_rsi_bull_min": 50, "buy_score_min": 5.0, "option_rsi_min": 50, "volume_confirm_mult": 0.95, "oi_change_min_pct": 0.0},
        {"underlying_adx_min": 12, "underlying_rsi_bull_min": 50, "buy_score_min": 5.0, "option_rsi_min": 48, "volume_confirm_mult": 0.9, "oi_sma_mult": 1.0, "oi_change_min_pct": -1.0},
    ]
    exit_templates = [
        {"take_profit_pct": 12.0, "stop_loss_pct": 8.0, "max_hold_bars": 1},
        {"take_profit_pct": 15.0, "stop_loss_pct": 8.0, "max_hold_bars": 2},
        {"take_profit_pct": 18.0, "stop_loss_pct": 10.0, "max_hold_bars": 2},
        {"take_profit_pct": 25.0, "stop_loss_pct": 10.0, "max_hold_bars": 3},
        {"take_profit_pct": 35.0, "stop_loss_pct": 8.0, "exit_rsi": 50.0},
    ]
    entry_exit_idx = 0
    for entry_patch in entry_templates:
        for exit_patch in exit_templates:
            entry_exit_idx += 1
            patch = {**entry_patch, **exit_patch}
            add(f"entry_exit_combo_{entry_exit_idx:03d}", patch)

    max_variants = int(os.getenv("AT_OPTIONS_LAB_MAX_VARIANTS", os.getenv("AT_LAB_MAX_VARIANTS", "200")))
    return out[:max_variants]



def _simulate_symbol(symbol: str, df: pd.DataFrame) -> dict[str, float]:
    cash = 100000.0
    qty = 0
    avg = 0.0
    lot_size = int(float(df.iloc[-1].get("lot_size", 1) or 1))
    entry_idx = None
    trades = 0
    wins = 0
    equity_curve = []
    warmup = max(8, int(os.getenv("AT_OPTIONS_LAB_WARMUP_BARS", "10")))

    for i in range(min(warmup, len(df)), len(df)):
        part = df.iloc[: i + 1].copy().reset_index(drop=True)
        row = part.iloc[-1].to_dict()
        symbol_lot = int(float(row.get("lot_size", lot_size) or lot_size or 1))

        if qty == 0:
            hold_df = pd.DataFrame(columns=["tradingsymbol", "average_price", "quantity", "t1_quantity", "bars_in_trade"])
            sig = RULE_SET_OPTIONS_1.buy_or_sell(part, row, hold_df)
            price = float(part.iloc[-1]["Close"])
            buy_qty = symbol_lot
            if str(sig).upper() == "BUY" and buy_qty > 0 and cash >= buy_qty * price:
                qty = buy_qty
                cash -= qty * price
                avg = price
                entry_idx = i
                trades += 1
        else:
            hold_df = pd.DataFrame(
                [
                    {
                        "tradingsymbol": symbol,
                        "average_price": avg,
                        "quantity": qty,
                        "t1_quantity": 0,
                        "bars_in_trade": max(0, i - entry_idx) if entry_idx is not None else 0,
                    }
                ]
            )
            sig = RULE_SET_OPTIONS_1.buy_or_sell(part, row, hold_df)
            price = float(part.iloc[-1]["Close"])
            if str(sig).upper() == "SELL":
                cash += qty * price
                if price > avg:
                    wins += 1
                qty = 0
                avg = 0.0
                entry_idx = None
                trades += 1

        last_price = float(part.iloc[-1]["Close"])
        equity_curve.append(cash + (qty * last_price))

    if qty > 0:
        final_price = float(df.iloc[-1]["Close"])
        cash += qty * final_price
        if final_price > avg:
            wins += 1
        trades += 1
        equity_curve.append(cash)

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



def run_variant(name: str, data_map: dict[str, pd.DataFrame], params: dict) -> BacktestResult:
    old_config = dict(RULE_SET_OPTIONS_1.CONFIG)
    RULE_SET_OPTIONS_1.CONFIG.update(params)

    try:
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
        RULE_SET_OPTIONS_1.CONFIG.clear()
        RULE_SET_OPTIONS_1.CONFIG.update(old_config)

    start_capital = 100000.0 * max(1, len(tested_symbols))
    ret = (total_final_value / start_capital - 1.0) * 100.0
    round_trips = max(1, total_trades // 2)
    win_rate = (total_wins / round_trips) * 100.0 if round_trips > 0 else 0.0
    selection_score = float(ret + (0.04 * total_trades) - (0.18 * abs(min(0.0, worst_dd))))

    return BacktestResult(
        name=name,
        final_value=round(float(total_final_value), 2),
        total_return_pct=round(float(ret), 2),
        trades=int(total_trades),
        win_rate_pct=round(float(win_rate), 2),
        max_drawdown_pct=round(float(worst_dd), 2),
        params={"options": params},
        symbols_tested=tested_symbols,
        selection_score=round(selection_score, 3),
    )



def _split_data_map(data_map: dict[str, pd.DataFrame], split_pct: float) -> tuple[dict[str, pd.DataFrame], dict[str, pd.DataFrame]]:
    """Chronological train/test split per option symbol.

    Options histories are short, so keep only symbols that have enough bars on
    both sides to run the rule warmup. This avoids selecting params solely on
    the same bars used to report the headline result.
    """
    split_pct = min(0.85, max(0.45, float(split_pct)))
    warmup = max(8, int(os.getenv("AT_OPTIONS_LAB_WARMUP_BARS", "10")))
    train: dict[str, pd.DataFrame] = {}
    test: dict[str, pd.DataFrame] = {}
    for symbol, df in data_map.items():
        if len(df) < (warmup * 2 + 4):
            continue
        cut = int(len(df) * split_pct)
        cut = min(max(cut, warmup + 2), len(df) - warmup - 2)
        left = df.iloc[:cut].copy().reset_index(drop=True)
        right = df.iloc[cut:].copy().reset_index(drop=True)
        if len(left) > warmup and len(right) > warmup:
            train[symbol] = left
            test[symbol] = right
    return train, test



def validate_top_candidates(rank: list[BacktestResult], data_map: dict[str, pd.DataFrame]) -> dict:
    if os.getenv("AT_OPTIONS_LAB_VALIDATE", "1").strip().lower() in {"0", "false", "no"}:
        return {"enabled": False, "reason": "disabled"}
    split_pct = float(os.getenv("AT_OPTIONS_LAB_VALIDATION_SPLIT", "0.65") or 0.65)
    train_map, test_map = _split_data_map(data_map, split_pct)
    if not train_map or not test_map:
        return {"enabled": False, "reason": "insufficient_bars_for_split", "split_pct": split_pct}

    top_n = max(1, int(os.getenv("AT_OPTIONS_LAB_VALIDATE_TOP", "30") or 30))
    candidates = rank[:top_n]
    rows = []
    for cand in candidates:
        params = dict((cand.params or {}).get("options", {}) or {})
        train = run_variant(cand.name, train_map, params)
        test = run_variant(cand.name, test_map, params)
        robust_score = round(
            (0.65 * test.selection_score)
            + (0.25 * train.selection_score)
            + (0.10 * cand.selection_score)
            - (0.20 * max(0.0, abs(test.max_drawdown_pct) - abs(train.max_drawdown_pct))),
            3,
        )
        rows.append({
            "name": cand.name,
            "params": cand.params,
            "full": asdict(cand),
            "train": asdict(train),
            "test": asdict(test),
            "robust_score": robust_score,
        })
    rows.sort(key=lambda r: (r["robust_score"], r["test"]["total_return_pct"], -abs(r["test"]["max_drawdown_pct"])), reverse=True)
    return {
        "enabled": True,
        "split_pct": split_pct,
        "train_symbols": len(train_map),
        "test_symbols": len(test_map),
        "validated_top_n": len(rows),
        "best_validated": rows[0] if rows else None,
        "ranked_validated": rows,
    }



def load_scorecard_context() -> dict:
    explicit = os.getenv("AT_LAB_SCORECARD_PATH", "").strip()
    scorecard_path = Path(explicit) if explicit else None
    if scorecard_path is None:
        matches = sorted(OUT_DIR.glob("daily_scorecard_*.json"))
        scorecard_path = matches[-1] if matches else None

    if not scorecard_path or not scorecard_path.exists():
        return {
            "scorecard_found": False,
            "optimization_focus": ["baseline_search"],
            "code_findings": [],
        }

    raw = json.loads(scorecard_path.read_text())
    log_counts = raw.get("log_counts", {}) or {}
    orders = int(raw.get("orders", 0) or 0)
    trades = int(raw.get("trades", 0) or 0)
    buy_placed = int(log_counts.get("buy_placed", 0) or 0)
    sell_placed = int(log_counts.get("sell_placed", 0) or 0)
    ws_close = int(log_counts.get("ws_close", 0) or 0)
    order_failed = int(log_counts.get("order_failed", 0) or 0)
    market_blocked = int(log_counts.get("market_blocked", 0) or 0)
    tick_size = int(log_counts.get("tick_size", 0) or 0)

    no_trade_day = orders == 0 and trades == 0 and buy_placed == 0 and sell_placed == 0
    optimization_focus = ["expand_buy_sensitivity" if no_trade_day else "balanced_search"]
    code_findings = []
    if ws_close > 0:
        code_findings.append("websocket_reconnect_review")
    if order_failed > 0:
        code_findings.append("order_error_path_review")
    if market_blocked > 0 or tick_size > 0:
        code_findings.append("broker_constraints_review")
    if not code_findings:
        code_findings.append("no_code_issues_detected_from_scorecard")

    return {
        "scorecard_found": True,
        "scorecard_path": str(scorecard_path),
        "date": raw.get("date"),
        "orders": orders,
        "trades": trades,
        "buy_placed": buy_placed,
        "sell_placed": sell_placed,
        "estimated_realized_pnl": raw.get("estimated_realized_pnl"),
        "verdict": raw.get("verdict"),
        "log_counts": log_counts,
        "no_trade_day": no_trade_day,
        "optimization_focus": optimization_focus,
        "code_findings": code_findings,
    }



def load_tradebook_context() -> dict:
    tradebook_path = os.getenv("AT_LAB_TRADEBOOK_PATH", "").strip()
    if not tradebook_path:
        return {
            "tradebook_found": False,
            "optimization_focus": [],
            "code_findings": [],
            "top_symbols": [],
        }

    path = Path(tradebook_path)
    if not path.exists():
        return {
            "tradebook_found": False,
            "tradebook_path": tradebook_path,
            "optimization_focus": [],
            "code_findings": ["tradebook_path_missing"],
            "top_symbols": [],
        }

    try:
        tradebook = pd.read_csv(path)
        tradebook["trade_type"] = tradebook["trade_type"].astype(str).str.upper().str.strip()
        tradebook["symbol"] = tradebook["symbol"].astype(str).str.upper().str.strip()
        tradebook["trade_date"] = pd.to_datetime(tradebook["trade_date"], errors="coerce")
        tradebook["order_execution_time"] = pd.to_datetime(tradebook["order_execution_time"], errors="coerce")
        tradebook["quantity"] = pd.to_numeric(tradebook["quantity"], errors="coerce")
        tradebook["price"] = pd.to_numeric(tradebook["price"], errors="coerce")
        tradebook = tradebook.dropna(subset=["trade_type", "symbol", "order_execution_time", "quantity", "price"])

        grouped = tradebook.groupby(["order_id", "symbol", "trade_type", "order_execution_time"], as_index=False).agg(
            quantity=("quantity", "sum"),
            avg_price=("price", lambda s: (s * tradebook.loc[s.index, "quantity"]).sum() / tradebook.loc[s.index, "quantity"].sum()),
        )
        grouped = grouped.sort_values(["order_execution_time", "order_id"]).reset_index(drop=True)

        open_lots = {}
        closed_rows = []
        for row in grouped.to_dict("records"):
            symbol = row["symbol"]
            open_lots.setdefault(symbol, [])
            if row["trade_type"] == "BUY":
                open_lots[symbol].append({"qty": float(row["quantity"]), "price": float(row["avg_price"]), "ts": row["order_execution_time"]})
                continue

            remaining = float(row["quantity"])
            while remaining > 1e-9 and open_lots[symbol]:
                lot = open_lots[symbol][0]
                matched = min(remaining, lot["qty"])
                hold_days = (row["order_execution_time"] - lot["ts"]).total_seconds() / 86400.0
                closed_rows.append({"symbol": symbol, "hold_days": hold_days, "pnl": (float(row["avg_price"]) - float(lot["price"])) * matched})
                lot["qty"] -= matched
                remaining -= matched
                if lot["qty"] <= 1e-9:
                    open_lots[symbol].pop(0)

        closed = pd.DataFrame(closed_rows)
        if closed.empty:
            return {
                "tradebook_found": True,
                "tradebook_path": str(path),
                "optimization_focus": [],
                "code_findings": ["tradebook_has_no_closed_round_trips"],
                "top_symbols": [],
            }

        buckets = pd.cut(closed["hold_days"], bins=[-1, 2, 5, 10, 30, 9999], labels=["0-2d", "2-5d", "5-10d", "10-30d", "30d+"])
        hold_stats = closed.groupby(buckets, observed=False)["pnl"].sum().to_dict()
        weak_mid_hold = float(hold_stats.get("5-10d", 0.0) or 0.0) < min(float(hold_stats.get("0-2d", 0.0) or 0.0), float(hold_stats.get("2-5d", 0.0) or 0.0))
        top_symbols = closed.groupby("symbol").size().sort_values(ascending=False).head(12).index.tolist()

        optimization_focus = []
        if weak_mid_hold:
            optimization_focus.append("tighten_mid_hold_exits")

        return {
            "tradebook_found": True,
            "tradebook_path": str(path),
            "closed_round_trips": int(len(closed)),
            "hold_bucket_pnl": {k: round(float(v), 2) for k, v in hold_stats.items()},
            "weak_mid_hold_window": bool(weak_mid_hold),
            "optimization_focus": optimization_focus,
            "code_findings": [],
            "top_symbols": top_symbols,
        }
    except Exception as exc:
        return {
            "tradebook_found": False,
            "tradebook_path": str(path),
            "optimization_focus": [],
            "code_findings": [f"tradebook_parse_failed: {exc}"],
            "top_symbols": [],
        }



def main():
    scorecard_context = load_scorecard_context()
    tradebook_context = load_tradebook_context()
    data_map, data_context = load_option_data()

    results = []
    for name, params in option_variants(scorecard_context, tradebook_context):
        results.append(run_variant(name, data_map, params))

    rank = sorted(
        results,
        key=lambda r: (r.selection_score, r.total_return_pct, -abs(r.max_drawdown_pct), r.win_rate_pct),
        reverse=True,
    )
    baseline = next(r for r in rank if r.name == "baseline_current")
    best = rank[0]
    validation = validate_top_candidates(rank, data_map)
    validated_best = (validation.get("best_validated") or {}) if validation.get("enabled") else {}

    recommendation = {
        "generated_at": datetime.now().isoformat(),
        "lab_type": "options_research_only",
        "production_rule_model": "OPTIONS=RULE_SET_OPTIONS_1",
        "supports_live_auto_promotion": False,
        "scorecard_context": scorecard_context,
        "tradebook_context": tradebook_context,
        "data_context": data_context,
        "baseline": asdict(baseline),
        "best": asdict(best),
        "tested_variants": len(rank),
        "improvement_return_pct": round(best.total_return_pct - baseline.total_return_pct, 2),
        "improvement_score": round(best.selection_score - baseline.selection_score, 3),
        "validation": validation,
        "selected_by_validation": validated_best.get("name"),
        "should_promote": False,
        "notes": [
            "Research-only NIFTY options lab using RULE_SET_OPTIONS_1.",
            "Search space is context-aware, similar to the equity lab, but still never auto-promotes into live trading.",
            "Use results to refine options paper trading before any live NFO execution work.",
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
