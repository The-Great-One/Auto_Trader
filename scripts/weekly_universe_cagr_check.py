#!/usr/bin/env python3
"""Weekly 5 year validation pack for the current production equity strategy."""

from __future__ import annotations

import importlib
import json
import math
import os
import sys
import tempfile
from collections import Counter
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from statistics import median

import numpy as np
import pandas as pd

os.environ.setdefault("AT_DISABLE_FILE_LOGGING", "1")
os.environ.setdefault("AT_LAB_HISTORY_PERIOD", os.getenv("AT_WEEKLY_CAGR_HISTORY_PERIOD", "5y"))
os.environ.setdefault("AT_LAB_MIN_BARS", os.getenv("AT_WEEKLY_CAGR_MIN_BARS", "1000"))

ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / "reports"
STATUS_DIR = ROOT / "intermediary_files" / "lab_status"
STATUS_PATH = STATUS_DIR / "weekly_universe_cagr_status.json"
REPORTS.mkdir(exist_ok=True)
STATUS_DIR.mkdir(exist_ok=True)

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from Auto_Trader import utils as at_utils  # noqa: E402
from Auto_Trader.StrongFundamentalsStockList import goodStocks  # noqa: E402

lab = importlib.import_module("scripts.weekly_strategy_lab")


def write_status(**updates) -> dict:
    current = {}
    if STATUS_PATH.exists():
        try:
            current = json.loads(STATUS_PATH.read_text())
        except Exception:
            current = {}
    current.update(updates)
    current["updated_at"] = datetime.now().isoformat()
    STATUS_PATH.write_text(json.dumps(current, indent=2))
    return current


def iso_week_key(now: datetime) -> str:
    iso = now.isocalendar()
    return f"{iso.year}_W{iso.week:02d}"


def build_universe(limit: int | None = None) -> tuple[list[str], dict]:
    df = goodStocks()
    if df is None or df.empty:
        raise RuntimeError("Strong fundamentals universe is empty")

    df["Symbol"] = df["Symbol"].astype(str).str.upper().str.strip()
    df["AssetClass"] = df["AssetClass"].astype(str).str.upper().str.strip()
    include_etfs = os.getenv("AT_WEEKLY_CAGR_INCLUDE_ETFS", "1").strip().lower() not in {"0", "false", "no"}

    if not include_etfs:
        df = df[df["AssetClass"] == "EQUITY"].copy()

    symbols = df["Symbol"].dropna().unique().tolist()
    if limit is not None:
        symbols = symbols[: max(1, int(limit))]
        df = df[df["Symbol"].isin(symbols)].copy()

    counts = df["AssetClass"].value_counts(dropna=False).to_dict()
    return symbols, {
        "include_etfs": include_etfs,
        "requested_symbols": len(symbols),
        "asset_class_counts": counts,
        "sample_symbols": symbols[:20],
    }


def load_data(symbols: list[str], min_history_bars: int) -> tuple[dict[str, pd.DataFrame], dict]:
    data_map: dict[str, pd.DataFrame] = {}
    skipped: dict[str, str] = {}
    spans_years: list[float] = []
    total = max(1, len(symbols))

    for idx, symbol in enumerate(symbols, start=1):
        write_status(
            status="running",
            phase="loading_history",
            current_symbol=symbol,
            symbols_total=total,
            symbols_index=idx,
            symbols_loaded=len(data_map),
            progress_pct=round(((idx - 1) / total) * 100.0, 1),
        )
        df = lab._load_symbol_history(symbol)
        if df is None or df.empty:
            skipped[symbol] = "missing_or_empty"
            continue
        if len(df) < min_history_bars:
            skipped[symbol] = f"too_short:{len(df)}"
            continue
        try:
            ind = at_utils.Indicators(df)
            data_map[symbol] = ind
            span_years = (pd.to_datetime(ind["Date"].iloc[-1]) - pd.to_datetime(ind["Date"].iloc[0])).days / 365.25
            spans_years.append(float(span_years))
        except Exception as exc:
            skipped[symbol] = f"indicator_failed:{exc}"

    skip_reasons = Counter(reason.split(":", 1)[0] for reason in skipped.values())
    return data_map, {
        "loaded_symbols": list(data_map.keys()),
        "skipped_symbols": skipped,
        "skip_reason_counts": dict(skip_reasons),
        "median_span_years": round(float(median(spans_years)), 3) if spans_years else 0.0,
        "min_span_years": round(float(min(spans_years)), 3) if spans_years else 0.0,
        "max_span_years": round(float(max(spans_years)), 3) if spans_years else 0.0,
    }


def _safe_float(value, digits: int = 4):
    if value is None:
        return None
    try:
        f = float(value)
    except Exception:
        return None
    if math.isnan(f) or math.isinf(f):
        return None
    return round(f, digits)


def _safe_ratio(num: float, den: float, digits: int = 4):
    if den in {0, 0.0} or den is None:
        return None
    return _safe_float(num / den, digits=digits)


def _compute_drawdown_pct(equity: pd.Series) -> float:
    if equity.empty:
        return 0.0
    drawdown = equity / equity.cummax() - 1.0
    return float(drawdown.min() * 100.0)


def simulate_symbol_detail(symbol: str, df: pd.DataFrame) -> dict:
    cash = 100000.0
    qty = 0
    avg = 0.0
    entry_idx = None
    entry_date = None
    entry_qty = 0
    trades = 0
    wins = 0
    exposure_bars = 0
    equity_points: list[dict] = []
    closed_trades: list[dict] = []

    for i in range(250, len(df)):
        part = df.iloc[: i + 1].copy()
        row = part.iloc[-1].to_dict()
        row.setdefault("instrument_token", 1626369)
        price = float(part.iloc[-1]["Close"])
        date = pd.to_datetime(part.iloc[-1]["Date"])

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
            sig = lab.RULE_SET_7.buy_or_sell(part, row, hold_df)
            if str(sig).upper() == "BUY":
                buy_qty = int(cash // price)
                if buy_qty > 0:
                    qty = buy_qty
                    cash -= qty * price
                    avg = price
                    entry_idx = i
                    entry_date = date
                    entry_qty = qty
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
            sig = lab.RULE_SET_2.buy_or_sell(part, row, hold_df)
            if str(sig).upper() == "SELL":
                cash += qty * price
                pnl_abs = (price - avg) * qty
                pnl_pct = ((price / avg) - 1.0) * 100.0 if avg > 0 else 0.0
                hold_bars = max(0, i - entry_idx) if entry_idx is not None else 0
                hold_days = (date - entry_date).days if entry_date is not None else hold_bars
                if price > avg:
                    wins += 1
                closed_trades.append(
                    {
                        "symbol": symbol,
                        "entry_date": entry_date.isoformat() if entry_date is not None else None,
                        "exit_date": date.isoformat(),
                        "entry_price": round(float(avg), 4),
                        "exit_price": round(float(price), 4),
                        "qty": int(entry_qty or qty),
                        "pnl_abs": round(float(pnl_abs), 4),
                        "pnl_pct": round(float(pnl_pct), 4),
                        "hold_bars": int(hold_bars),
                        "hold_days": int(hold_days),
                    }
                )
                qty = 0
                avg = 0.0
                entry_idx = None
                entry_date = None
                entry_qty = 0
                trades += 1

        if qty > 0:
            exposure_bars += 1
        port = cash + (qty * price)
        equity_points.append({"Date": date, "Equity": float(port)})

    equity_df = pd.DataFrame(equity_points)
    if equity_df.empty:
        equity_df = pd.DataFrame({"Date": [pd.Timestamp.utcnow().normalize()], "Equity": [100000.0]})
    equity_df["Date"] = pd.to_datetime(equity_df["Date"])
    equity_df = equity_df.sort_values("Date").drop_duplicates(subset=["Date"], keep="last")
    equity = equity_df.set_index("Date")["Equity"].astype(float)
    final_val = float(equity.iloc[-1]) if not equity.empty else 100000.0
    dd = _compute_drawdown_pct(equity)
    hold_days = [row["hold_days"] for row in closed_trades]

    return {
        "final_value": final_val,
        "trades": int(trades),
        "wins": int(wins),
        "max_drawdown_pct": float(dd),
        "equity_curve": equity,
        "closed_trades": closed_trades,
        "exposure_pct": round((exposure_bars / max(1, len(equity_df))) * 100.0, 2),
        "avg_hold_days": round(float(np.mean(hold_days)) if hold_days else 0.0, 2),
    }


def run_baseline_detailed(data_map: dict[str, pd.DataFrame]):
    at_utils.get_mmi_now = lambda: None

    old_r2 = dict(lab.RULE_SET_2.CONFIG)
    old_r7 = dict(lab.RULE_SET_7.CONFIG)
    details: dict[str, dict] = {}

    try:
        with tempfile.TemporaryDirectory(prefix="at_state_") as td:
            lab._set_temp_state(lab.RULE_SET_2, td)
            total_final_value = 0.0
            total_trades = 0
            total_wins = 0
            worst_dd = 0.0
            tested_symbols: list[str] = []

            total = max(1, len(data_map))
            for idx, (symbol, df) in enumerate(data_map.items(), start=1):
                write_status(
                    status="running",
                    phase="backtesting",
                    current_symbol=symbol,
                    tested_symbols=idx - 1,
                    symbols_total=total,
                    symbols_index=idx,
                    progress_pct=round(((idx - 1) / total) * 100.0, 1),
                )
                stats = simulate_symbol_detail(symbol, df)
                details[symbol] = stats
                total_final_value += stats["final_value"]
                total_trades += stats["trades"]
                total_wins += stats["wins"]
                worst_dd = min(worst_dd, stats["max_drawdown_pct"])
                tested_symbols.append(symbol)
    finally:
        lab.RULE_SET_2.CONFIG.clear()
        lab.RULE_SET_2.CONFIG.update(old_r2)
        lab.RULE_SET_7.CONFIG.clear()
        lab.RULE_SET_7.CONFIG.update(old_r7)

    start_capital = 100000.0 * max(1, len(tested_symbols))
    ret = (total_final_value / start_capital - 1.0) * 100.0
    round_trips = max(1, total_trades // 2)
    win_rate = (total_wins / round_trips) * 100.0
    selection_score = float(ret + (0.02 * total_trades) - (0.15 * abs(min(0.0, worst_dd))))

    result = lab.BacktestResult(
        name="baseline_current",
        final_value=round(float(total_final_value), 2),
        total_return_pct=round(float(ret), 2),
        trades=int(total_trades),
        win_rate_pct=round(float(win_rate), 2),
        max_drawdown_pct=round(float(worst_dd), 2),
        params={"buy": {}, "sell": {}, "rnn": {"enabled": False}},
        symbols_tested=tested_symbols,
        selection_score=round(selection_score, 3),
        rnn_enabled=False,
        rnn_avg_test_accuracy=0.0,
    )
    return result, details


def build_validation_curves(details: dict[str, dict]) -> tuple[pd.Series, pd.Series, pd.DataFrame, dict]:
    curves = []
    trade_rows = []
    exposure_pcts = []
    hold_days = []

    for symbol, stats in details.items():
        equity = stats["equity_curve"]
        if equity is None or equity.empty:
            continue
        curves.append(equity.rename(symbol))
        trade_rows.extend(stats.get("closed_trades", []))
        exposure_pcts.append(float(stats.get("exposure_pct", 0.0) or 0.0))
        hold_days.append(float(stats.get("avg_hold_days", 0.0) or 0.0))

    if not curves:
        raise RuntimeError("No equity curves were produced for validation")

    panel = pd.concat(curves, axis=1).sort_index().ffill().dropna(how="any")
    if panel.empty:
        raise RuntimeError("No overlapping portfolio dates available for validation")

    portfolio_equity = panel.sum(axis=1)
    portfolio_daily_returns = portfolio_equity.pct_change().dropna()
    trades_df = pd.DataFrame(trade_rows)
    meta = {
        "common_start": str(panel.index.min().date()),
        "common_end": str(panel.index.max().date()),
        "common_days": int(len(panel)),
        "symbols_in_validation_curve": int(panel.shape[1]),
        "avg_symbol_exposure_pct": round(float(np.mean(exposure_pcts)) if exposure_pcts else 0.0, 2),
        "avg_symbol_hold_days": round(float(np.mean(hold_days)) if hold_days else 0.0, 2),
    }
    return portfolio_equity, portfolio_daily_returns, trades_df, meta


def compute_validation_metrics(portfolio_equity: pd.Series, daily_returns: pd.Series, trades_df: pd.DataFrame) -> dict:
    monthly_equity = portfolio_equity.resample("M").last().dropna()
    monthly_returns = monthly_equity.pct_change().dropna()
    years = len(daily_returns) / 252.0 if len(daily_returns) else 0.0
    total_return = (portfolio_equity.iloc[-1] / portfolio_equity.iloc[0] - 1.0) if len(portfolio_equity) > 1 else 0.0
    cagr = ((portfolio_equity.iloc[-1] / portfolio_equity.iloc[0]) ** (1.0 / years) - 1.0) if years > 0 and len(portfolio_equity) > 1 else 0.0

    vol = float(daily_returns.std(ddof=0) * np.sqrt(252)) if len(daily_returns) else 0.0
    mean_daily = float(daily_returns.mean()) if len(daily_returns) else 0.0
    downside = daily_returns[daily_returns < 0]
    downside_std = float(downside.std(ddof=0) * np.sqrt(252)) if len(downside) else 0.0
    sharpe = (mean_daily / float(daily_returns.std(ddof=0)) * np.sqrt(252)) if len(daily_returns) and float(daily_returns.std(ddof=0)) > 0 else None
    sortino = (mean_daily / float(downside.std(ddof=0)) * np.sqrt(252)) if len(downside) and float(downside.std(ddof=0)) > 0 else None

    drawdown = portfolio_equity / portfolio_equity.cummax() - 1.0
    max_dd_pct = float(drawdown.min() * 100.0) if len(drawdown) else 0.0
    ulcer_index = float(np.sqrt(np.mean(np.square(np.minimum(drawdown, 0.0) * 100.0)))) if len(drawdown) else 0.0
    calmar = (cagr * 100.0 / abs(max_dd_pct)) if max_dd_pct < 0 else None

    gross_profit = float(trades_df.loc[trades_df["pnl_abs"] > 0, "pnl_abs"].sum()) if not trades_df.empty else 0.0
    gross_loss = float(-trades_df.loc[trades_df["pnl_abs"] < 0, "pnl_abs"].sum()) if not trades_df.empty else 0.0
    profit_factor = _safe_ratio(gross_profit, gross_loss, digits=3)
    expectancy_pct = float(trades_df["pnl_pct"].mean()) if not trades_df.empty else 0.0
    avg_win_pct = float(trades_df.loc[trades_df["pnl_pct"] > 0, "pnl_pct"].mean()) if not trades_df.empty and (trades_df["pnl_pct"] > 0).any() else 0.0
    avg_loss_pct = float(trades_df.loc[trades_df["pnl_pct"] < 0, "pnl_pct"].mean()) if not trades_df.empty and (trades_df["pnl_pct"] < 0).any() else 0.0
    payoff_ratio = _safe_ratio(avg_win_pct, abs(avg_loss_pct), digits=3) if avg_loss_pct < 0 else None
    sqn = None
    if not trades_df.empty and len(trades_df) >= 5:
        trade_std = float(trades_df["pnl_pct"].std(ddof=0))
        if trade_std > 0:
            sqn = float(np.sqrt(len(trades_df)) * float(trades_df["pnl_pct"].mean()) / trade_std)

    positive_month_pct = float((monthly_returns > 0).mean() * 100.0) if len(monthly_returns) else 0.0

    return {
        "curve_years": round(float(years), 3),
        "curve_total_return_pct": round(total_return * 100.0, 2),
        "curve_cagr_pct": round(cagr * 100.0, 2),
        "annualized_volatility_pct": round(vol * 100.0, 2),
        "sharpe_ratio": _safe_float(sharpe, digits=3),
        "sortino_ratio": _safe_float(sortino, digits=3),
        "calmar_ratio": _safe_float(calmar, digits=3),
        "ulcer_index": round(ulcer_index, 2),
        "curve_max_drawdown_pct": round(max_dd_pct, 2),
        "positive_month_pct": round(positive_month_pct, 2),
        "best_month_pct": round(float(monthly_returns.max() * 100.0), 2) if len(monthly_returns) else 0.0,
        "worst_month_pct": round(float(monthly_returns.min() * 100.0), 2) if len(monthly_returns) else 0.0,
        "closed_trades": int(len(trades_df)),
        "profit_factor": profit_factor,
        "expectancy_pct": round(expectancy_pct, 3),
        "avg_win_pct": round(avg_win_pct, 3),
        "avg_loss_pct": round(avg_loss_pct, 3),
        "payoff_ratio": payoff_ratio,
        "system_quality_number": _safe_float(sqn, digits=3),
        "monthly_returns": monthly_returns,
    }


def _return_stats_from_monthly(monthly_returns: pd.Series) -> dict:
    if monthly_returns.empty:
        return {
            "months": 0,
            "total_return_pct": 0.0,
            "cagr_pct": 0.0,
            "sharpe_ratio": None,
            "max_drawdown_pct": 0.0,
        }

    equity = (1.0 + monthly_returns).cumprod()
    months = len(monthly_returns)
    years = months / 12.0
    total_return_pct = float((equity.iloc[-1] - 1.0) * 100.0)
    cagr_pct = float(((equity.iloc[-1]) ** (1.0 / years) - 1.0) * 100.0) if years > 0 else 0.0
    std = float(monthly_returns.std(ddof=0))
    sharpe = float(monthly_returns.mean() / std * np.sqrt(12)) if std > 0 else None
    max_dd_pct = _compute_drawdown_pct(equity)
    return {
        "months": int(months),
        "total_return_pct": round(total_return_pct, 2),
        "cagr_pct": round(cagr_pct, 2),
        "sharpe_ratio": _safe_float(sharpe, digits=3),
        "max_drawdown_pct": round(max_dd_pct, 2),
    }


def walkforward_validation(monthly_returns: pd.Series) -> dict:
    train_months = max(12, int(os.getenv("AT_WEEKLY_CAGR_WF_TRAIN_MONTHS", "24")))
    test_months = max(3, int(os.getenv("AT_WEEKLY_CAGR_WF_TEST_MONTHS", "6")))
    step_months = max(1, int(os.getenv("AT_WEEKLY_CAGR_WF_STEP_MONTHS", "3")))

    if len(monthly_returns) < train_months + test_months:
        return {
            "available": False,
            "reason": "not_enough_monthly_history",
            "train_months": train_months,
            "test_months": test_months,
            "step_months": step_months,
            "windows": [],
        }

    rows = []
    idx = 0
    window_num = 0
    while idx + train_months + test_months <= len(monthly_returns):
        train = monthly_returns.iloc[idx : idx + train_months]
        test = monthly_returns.iloc[idx + train_months : idx + train_months + test_months]
        window_num += 1
        rows.append(
            {
                "window": window_num,
                "train_start": str(train.index[0].date()),
                "train_end": str(train.index[-1].date()),
                "test_start": str(test.index[0].date()),
                "test_end": str(test.index[-1].date()),
                **{f"train_{k}": v for k, v in _return_stats_from_monthly(train).items()},
                **{f"test_{k}": v for k, v in _return_stats_from_monthly(test).items()},
                "test_positive": bool((1.0 + test).prod() > 1.0),
            }
        )
        idx += step_months

    test_returns = [row["test_total_return_pct"] for row in rows]
    positive_windows = sum(1 for row in rows if row["test_positive"])
    return {
        "available": True,
        "train_months": train_months,
        "test_months": test_months,
        "step_months": step_months,
        "windows": rows,
        "summary": {
            "window_count": len(rows),
            "positive_windows": positive_windows,
            "positive_window_pct": round((positive_windows / max(1, len(rows))) * 100.0, 2),
            "median_test_return_pct": round(float(np.median(test_returns)), 2) if test_returns else 0.0,
            "best_test_return_pct": round(float(np.max(test_returns)), 2) if test_returns else 0.0,
            "worst_test_return_pct": round(float(np.min(test_returns)), 2) if test_returns else 0.0,
        },
    }


def _block_bootstrap(values: np.ndarray, horizon: int, block_size: int, rng: np.random.Generator) -> np.ndarray:
    if len(values) == 0:
        return np.array([], dtype=float)
    if len(values) <= block_size:
        return rng.choice(values, size=horizon, replace=True)

    out: list[float] = []
    max_start = max(1, len(values) - block_size + 1)
    while len(out) < horizon:
        start = int(rng.integers(0, max_start))
        out.extend(values[start : start + block_size].tolist())
    return np.array(out[:horizon], dtype=float)


def monte_carlo_validation(monthly_returns: pd.Series) -> dict:
    simulations = max(200, int(os.getenv("AT_WEEKLY_CAGR_MC_SIMS", "2000")))
    block_size = max(1, int(os.getenv("AT_WEEKLY_CAGR_MC_BLOCK_MONTHS", "3")))
    seed = int(os.getenv("AT_WEEKLY_CAGR_MC_SEED", "42"))

    if len(monthly_returns) < 24:
        return {
            "available": False,
            "reason": "not_enough_monthly_history",
            "simulations": simulations,
            "block_size_months": block_size,
        }

    rng = np.random.default_rng(seed)
    values = monthly_returns.to_numpy(dtype=float)
    horizon = len(values)
    years = horizon / 12.0
    cagr_paths = []
    final_return_paths = []
    max_dd_paths = []

    for _ in range(simulations):
        sample = _block_bootstrap(values, horizon=horizon, block_size=block_size, rng=rng)
        equity = np.cumprod(1.0 + sample)
        final_multiple = float(equity[-1]) if len(equity) else 1.0
        cagr_pct = (((final_multiple ** (1.0 / years)) - 1.0) * 100.0) if years > 0 else 0.0
        peak = np.maximum.accumulate(equity)
        max_dd_pct = float(np.min((equity / peak - 1.0) * 100.0)) if len(equity) else 0.0
        final_return_paths.append((final_multiple - 1.0) * 100.0)
        cagr_paths.append(cagr_pct)
        max_dd_paths.append(max_dd_pct)

    cagr_arr = np.array(cagr_paths, dtype=float)
    final_arr = np.array(final_return_paths, dtype=float)
    dd_arr = np.array(max_dd_paths, dtype=float)

    return {
        "available": True,
        "simulations": simulations,
        "horizon_months": horizon,
        "block_size_months": block_size,
        "cagr_pct_p05": round(float(np.percentile(cagr_arr, 5)), 2),
        "cagr_pct_p50": round(float(np.percentile(cagr_arr, 50)), 2),
        "cagr_pct_p95": round(float(np.percentile(cagr_arr, 95)), 2),
        "final_return_pct_p05": round(float(np.percentile(final_arr, 5)), 2),
        "final_return_pct_p50": round(float(np.percentile(final_arr, 50)), 2),
        "final_return_pct_p95": round(float(np.percentile(final_arr, 95)), 2),
        "max_drawdown_pct_p50": round(float(np.percentile(dd_arr, 50)), 2),
        "max_drawdown_pct_p95": round(float(np.percentile(dd_arr, 95)), 2),
        "probability_of_loss_pct": round(float((final_arr < 0).mean() * 100.0), 2),
        "probability_of_negative_cagr_pct": round(float((cagr_arr < 0).mean() * 100.0), 2),
        "probability_drawdown_worse_than_20_pct": round(float((dd_arr <= -20.0).mean() * 100.0), 2),
    }


def build_payload(now: datetime, universe_meta: dict, data_context: dict, result, details: dict) -> dict:
    start_capital = 100000.0 * max(1, len(result.symbols_tested))
    years = float(data_context.get("median_span_years") or 0.0)
    ending_value = float(result.final_value)
    cagr_pct = None
    if years > 0 and start_capital > 0 and ending_value > 0:
        cagr_pct = round((((ending_value / start_capital) ** (1.0 / years)) - 1.0) * 100.0, 2)

    coverage_pct = round((len(result.symbols_tested) / max(1, universe_meta["requested_symbols"])) * 100.0, 2)
    portfolio_equity, daily_returns, trades_df, curve_meta = build_validation_curves(details)
    validation_metrics = compute_validation_metrics(portfolio_equity, daily_returns, trades_df)
    monthly_returns = validation_metrics.pop("monthly_returns")
    walkforward = walkforward_validation(monthly_returns)
    monte_carlo = monte_carlo_validation(monthly_returns)

    report_week = os.getenv("AT_WEEKLY_CAGR_REPORT_WEEK", iso_week_key(now))
    return {
        "generated_at": now.isoformat(),
        "report_week": report_week,
        "production_rule_model": "BUY=RULE_SET_7, SELL=RULE_SET_2",
        "history_period": os.getenv("AT_LAB_HISTORY_PERIOD", "5y"),
        "min_history_bars": int(os.getenv("AT_LAB_MIN_BARS", "1000")),
        "universe": {
            **universe_meta,
            "tested_symbols": len(result.symbols_tested),
            "coverage_pct": coverage_pct,
        },
        "data_context": {
            **data_context,
            "loaded_symbols_count": len(data_context.get("loaded_symbols", [])),
            "skipped_symbols_count": len(data_context.get("skipped_symbols", {})),
            "loaded_symbols_sample": data_context.get("loaded_symbols", [])[:25],
        },
        "backtest": asdict(result),
        "annualized_years": round(years, 3),
        "cagr_pct": cagr_pct,
        "validation": {
            "curve_meta": curve_meta,
            "portfolio_metrics": validation_metrics,
            "walkforward": walkforward,
            "monte_carlo": monte_carlo,
        },
    }


def write_reports(payload: dict) -> tuple[Path, Path]:
    week_key = payload["report_week"]
    out_json = REPORTS / f"weekly_universe_cagr_{week_key}.json"
    out_md = REPORTS / f"weekly_universe_cagr_{week_key}.md"

    out_json.write_text(json.dumps(payload, indent=2))

    backtest = payload["backtest"]
    universe = payload["universe"]
    data_context = payload["data_context"]
    validation = payload["validation"]
    metrics = validation["portfolio_metrics"]
    walkforward = validation["walkforward"]
    monte_carlo = validation["monte_carlo"]
    lines = [
        f"# Weekly Universe CAGR Check, {payload['report_week']}",
        "",
        f"- Generated at: **{payload['generated_at']}**",
        f"- Rule model: **{payload['production_rule_model']}**",
        f"- History period: **{payload['history_period']}**",
        f"- Min history bars: **{payload['min_history_bars']}**",
        f"- Requested universe: **{universe['requested_symbols']}**",
        f"- Tested symbols: **{universe['tested_symbols']}** ({universe['coverage_pct']}%)",
        f"- Asset classes: **{universe['asset_class_counts']}**",
        f"- Median history span: **{payload['annualized_years']} years**",
        f"- Strategy total return: **{backtest['total_return_pct']}%**",
        f"- Strategy CAGR: **{payload['cagr_pct']}%**",
        f"- Max drawdown: **{backtest['max_drawdown_pct']}%**",
        f"- Trades: **{backtest['trades']}**",
        f"- Win rate: **{backtest['win_rate_pct']}%**",
        "",
        "## Validation curve",
        f"- Common overlap: **{validation['curve_meta']['common_start']} → {validation['curve_meta']['common_end']}**",
        f"- Curve years: **{metrics['curve_years']}**",
        f"- Curve CAGR: **{metrics['curve_cagr_pct']}%**",
        f"- Curve max drawdown: **{metrics['curve_max_drawdown_pct']}%**",
        f"- Annualized volatility: **{metrics['annualized_volatility_pct']}%**",
        f"- Sharpe: **{metrics['sharpe_ratio']}**",
        f"- Sortino: **{metrics['sortino_ratio']}**",
        f"- Calmar: **{metrics['calmar_ratio']}**",
        f"- Profit factor: **{metrics['profit_factor']}**",
        f"- Expectancy per trade: **{metrics['expectancy_pct']}%**",
        f"- Positive months: **{metrics['positive_month_pct']}%**",
        "",
        "## Walk-forward",
        f"- Available: **{walkforward.get('available')}**",
    ]
    if walkforward.get("available"):
        wf_summary = walkforward.get("summary", {})
        lines.extend(
            [
                f"- Windows: **{wf_summary.get('window_count')}**",
                f"- Positive windows: **{wf_summary.get('positive_window_pct')}%**",
                f"- Median test return: **{wf_summary.get('median_test_return_pct')}%**",
                f"- Worst test return: **{wf_summary.get('worst_test_return_pct')}%**",
            ]
        )
    else:
        lines.append(f"- Reason: **{walkforward.get('reason')}**")

    lines.extend([
        "",
        "## Monte Carlo",
        f"- Available: **{monte_carlo.get('available')}**",
    ])
    if monte_carlo.get("available"):
        lines.extend(
            [
                f"- Simulations: **{monte_carlo.get('simulations')}**",
                f"- CAGR 5/50/95: **{monte_carlo.get('cagr_pct_p05')} / {monte_carlo.get('cagr_pct_p50')} / {monte_carlo.get('cagr_pct_p95')}**",
                f"- Return 5/50/95: **{monte_carlo.get('final_return_pct_p05')} / {monte_carlo.get('final_return_pct_p50')} / {monte_carlo.get('final_return_pct_p95')}**",
                f"- Median / 95th drawdown: **{monte_carlo.get('max_drawdown_pct_p50')} / {monte_carlo.get('max_drawdown_pct_p95')}**",
                f"- Probability of loss: **{monte_carlo.get('probability_of_loss_pct')}%**",
            ]
        )
    else:
        lines.append(f"- Reason: **{monte_carlo.get('reason')}**")

    lines.extend([
        "",
        "## Skips",
        f"- Skip reasons: **{data_context['skip_reason_counts']}**",
    ])
    out_md.write_text("\n".join(lines) + "\n")
    return out_json, out_md


def main() -> int:
    now = datetime.now()
    limit_raw = os.getenv("AT_WEEKLY_CAGR_LIMIT", "").strip()
    limit = int(limit_raw) if limit_raw else None
    min_history_bars = lab.configured_min_history_bars(default=1000)

    write_status(
        status="running",
        phase="building_universe",
        message="building current strong-fundamentals universe",
        history_period=os.getenv("AT_LAB_HISTORY_PERIOD", "5y"),
        min_history_bars=min_history_bars,
    )
    symbols, universe_meta = build_universe(limit=limit)
    write_status(
        status="running",
        phase="loading_history",
        message="loading price history for current universe",
        requested_symbols=universe_meta["requested_symbols"],
        asset_class_counts=universe_meta["asset_class_counts"],
    )
    data_map, data_context = load_data(symbols, min_history_bars=min_history_bars)
    if not data_map:
        raise RuntimeError("No symbols had enough history for the weekly CAGR check")

    write_status(
        status="running",
        phase="backtesting",
        message="running baseline production strategy across current universe",
        tested_symbols=len(data_map),
    )
    result, details = run_baseline_detailed(data_map)
    payload = build_payload(now, universe_meta, data_context, result, details)
    out_json, out_md = write_reports(payload)

    validation_metrics = payload["validation"]["portfolio_metrics"]
    monte_carlo = payload["validation"]["monte_carlo"]
    write_status(
        status="done",
        phase="completed",
        message="weekly universe validation pack completed",
        report_json=str(out_json),
        report_md=str(out_md),
        requested_symbols=payload["universe"]["requested_symbols"],
        tested_symbols=payload["universe"]["tested_symbols"],
        coverage_pct=payload["universe"]["coverage_pct"],
        cagr_pct=payload["cagr_pct"],
        total_return_pct=payload["backtest"]["total_return_pct"],
        max_drawdown_pct=payload["backtest"]["max_drawdown_pct"],
        validation_curve_cagr_pct=validation_metrics.get("curve_cagr_pct"),
        validation_curve_max_drawdown_pct=validation_metrics.get("curve_max_drawdown_pct"),
        monte_carlo_loss_pct=monte_carlo.get("probability_of_loss_pct"),
    )

    print(json.dumps(payload, indent=2))
    print(f"Saved: {out_json}")
    print(f"Saved: {out_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
