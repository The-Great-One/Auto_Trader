#!/usr/bin/env python3
"""
Strategy lab:
- tweaks RULE_SET_7 (BUY) + RULE_SET_2 (SELL)
- backtests variants on a small basket, not just one ETF
- uses latest daily scorecard + tradebook context to bias the search
- writes ranked reports; does NOT auto-deploy winners
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yfinance as yf

# Avoid noisy file-handler permission issues during research/backtest runs.
os.environ.setdefault("AT_DISABLE_FILE_LOGGING", "1")

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from Auto_Trader import RULE_SET_2, RULE_SET_7, logger as at_logger
from Auto_Trader import utils as at_utils

try:
    from Auto_Trader import rnn_lab
except Exception:
    rnn_lab = None

at_logger.setLevel("WARNING")

OUT_DIR = ROOT / "reports"
OUT_DIR.mkdir(exist_ok=True)
HIST_DIR = ROOT / "intermediary_files" / "Hist_Data"
STATUS_DIR = ROOT / "intermediary_files" / "lab_status"
STATUS_DIR.mkdir(exist_ok=True)
STATUS_PATH = STATUS_DIR / "weekly_strategy_lab_status.json"

DEFAULT_LAB_SYMBOLS = [
    # --- Large-cap core ---
    "BHARTIARTL", "SBIN", "TCS", "INFY", "LICI",
    "LT", "MARUTI", "HCLTECH", "ITC", "AXISBANK",
    "NTPC", "M&M", "ONGC", "ADANIPORTS", "POWERGRID",
    "COALINDIA", "VEDL", "BAJAJ-AUTO", "HAL", "ADANIENT",
    "HINDZINC", "WIPRO", "IOC", "HINDALCO", "INDIGO",
    # --- Mid/large cap ---
    "HYUNDAI", "BANKBARODA", "PFC", "UNIONBANK", "DLF",
    "PNB", "BPCL", "CANBK", "INDIANB", "TMPV",
    "MOTHERSON", "INDUSTOWER", "LUPIN", "DRREDDY", "BAJAJHLDNG",
    "HEROMOTOCO", "CIPLA", "AMBUJACEM", "ASHOKLEY", "GAIL",
    "MAZDOCK", "ZYDUSLIFE", "RECLTD", "OIL", "AUROPHARMA",
    "LODHA", "IDBI", "HINDPETRO", "NMDC", "BANKINDIA",
    "NATIONALUM", "GICRE", "FEDERALBNK", "ALKEM", "SAIL",
    "JSL", "SUZLON", "COROMANDEL", "MRF", "OBEROIRLTY",
    # --- Mid cap ---
    "JSWINFRA", "MAHABANK", "GODREJPROP", "PIIND", "AIIL",
    "IRCTC", "EMBASSY", "BALKRISIND", "TATACOMM", "MOTILALOFS",
    "PETRONET", "NLCINDIA", "HUDCO", "CONCOR", "ASTERDM",
    "ESCORTS", "AIAENG", "CENTRALBK", "UCOBANK", "GODFRYPHLP",
    "FORCEMOT", "KPRMILL", "TVSHLTD", "HEXT", "CHOLAHLDNG",
    "LICHSGFIN", "APOLLOTYRE", "KARURVYSYA", "ACC", "EXIDEIND",
    "IRB", "BANDHANBNK", "SUNTV", "GUJGASLTD", "AWL",
    "SHYAMMETL", "PFIZER", "IGL", "WELCORP", "CESC",
    "PNBHOUSING", "GESHIP", "EIHOTEL", "DEEPAKNTR", "KPITTECH",
    "CASTROLIND", "EMAMILTD", "CUB", "SARDAEN", "GPIL",
    "CIEINDIA", "SUNDRMFAST", "NATCOPHARM", "CHAMBLFERT", "REDINGTON",
    "SYNGENE", "VERTIS", "ANANTRAJ", "DCMSHRIRAM", "VTL",
    # --- Small/mid cap ---
    "BRIGADE", "NAVA", "PSB", "AVANTIFEED", "RATNAMANI",
    "CROMPTON", "BHARATCOAL", "GRANULES", "IGIL", "KEC",
    "CEATLTD", "EIDPARRY", "ARE&M", "GSPL", "INOXWIND",
    "SHRIPISTON", "JUBLPHARMA", "KANSAINER", "TRIDENT", "APLLTD",
    "LTFOODS", "FINCABLES", "ZENSARTECH", "J&KBANK", "CAPLIPOINT",
    "INDIAMART", "TECHNOE", "KIRLOSBROS", "ALIVUS", "JINDALSAW",
    "JKTYRE", "JMFINANCIL", "PARADEEP", "GRAPHITE", "DEEPAKFERT",
    "CANFINHOME", "IRCON", "CONCORDBIO", "JWL", "GODREJAGRO",
    "WELSPUNLIV", "FINPIPE", "FIVESTAR", "SCI", "ENGINERSIN",
    "INDGN", "APTUS", "JPPOWER", "AFCONS", "WHIRLPOOL",
    "UJJIVANSFB", "BBTC", "SWANCORP", "BALRAMCHIN", "BSOFT",
    "ACE", "VESUVIUS", "PCBL", "BLS", "ARVIND",
    # --- Small cap ---
    "TMB", "SANDUMA", "MGL", "RITES", "SOUTHBANK",
    "CELLO", "CEMPRO", "CYIENT", "RPOWER", "ELECON",
    "AAVAS", "VOLTAMP", "STAR", "RKFORGE", "NCC",
    "MANYAVAR", "PGINVIT", "GRINFRA", "KTKBANK", "BATAINDIA",
    "TITAGARH", "PGHL", "IIFLCAPS", "GRWRHITECH", "GMRP&UI",
    "ASKAUTOLTD", "TIMETECHNO", "KAMAHOLD", "PNGJL", "BANCOINDIA",
    "SANOFI", "JYOTHYLAB", "MARKSANS", "SKFINDIA", "MAHSEAMLES",
    "JKLAKSHMI", "NESCO", "AKUMS", "GPPL", "KIRLPNU",
    "TCI", "BENGALASM", "ZEEL", "TRANSRAILL", "KRBL",
    "ISGEC", "PGIL", "IMFA", "SAREGAMA", "DBL",
    "EMBDL", "TIPSMUSIC", "PCJEWELLER", "SONATSOFTW", "REDTAPE",
    "BIRLACORPN", "RCF", "SHREMINVIT", "GARFIBRES", "NEWGEN",
    "DODLA", "WELENT", "BLUEJET", "CERA", "EPL",
]


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
    rnn_enabled: bool = False
    rnn_avg_test_accuracy: float = 0.0


def _normalize_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    if hasattr(df.columns, "levels"):
        df.columns = [str(c[0]) for c in df.columns]
    df = df.reset_index()
    cmap = {str(c).lower(): c for c in df.columns}
    use = pd.DataFrame(
        {
            "Date": pd.to_datetime(df[cmap.get("date", "Date")], errors="coerce"),
            "Open": pd.to_numeric(df[cmap.get("open", "Open")], errors="coerce"),
            "High": pd.to_numeric(df[cmap.get("high", "High")], errors="coerce"),
            "Low": pd.to_numeric(df[cmap.get("low", "Low")], errors="coerce"),
            "Close": pd.to_numeric(df[cmap.get("close", "Close")], errors="coerce"),
            "Volume": pd.to_numeric(df.get(cmap.get("volume", "Volume"), 0), errors="coerce").fillna(0),
        }
    ).dropna(subset=["Date", "Open", "High", "Low", "Close"])
    return use.sort_values("Date").reset_index(drop=True)


def _load_symbol_history(symbol: str) -> pd.DataFrame | None:
    symbol = str(symbol or "").strip().upper()
    if not symbol:
        return None

    local_path = HIST_DIR / f"{symbol}.feather"
    if local_path.exists():
        try:
            local_df = _normalize_ohlcv(pd.read_feather(local_path))
            if local_df is not None and not local_df.empty:
                return local_df
        except Exception:
            pass

    y_symbols = []
    if "." in symbol:
        y_symbols.append(symbol)
    else:
        y_symbols.append(f"{symbol}.NS")
    if symbol == "NIFTYETF":
        y_symbols.extend(["NIFTYBEES.NS", "^NSEI"])

    seen = set()
    for y_symbol in y_symbols:
        if y_symbol in seen:
            continue
        seen.add(y_symbol)
        try:
            df = yf.download(
                y_symbol,
                period="3y",
                interval="1d",
                auto_adjust=False,
                progress=False,
            )
            if df is None or df.empty:
                continue
            out = _normalize_ohlcv(df)
            if out is not None and not out.empty:
                return out
        except Exception:
            continue
    return None


def _parse_symbol_list(value: str) -> list[str]:
    return [x.strip().upper() for x in value.split(",") if x.strip()]


def _looks_etf_like(symbol: str) -> bool:
    text = str(symbol or "").upper()
    return ("ETF" in text) or ("BEES" in text)


def build_lab_symbols(tradebook_context: dict, fundamental_context: dict) -> list[str]:
    explicit = os.getenv("AT_LAB_SYMBOLS", "").strip()
    if explicit:
        requested = _parse_symbol_list(explicit)
    else:
        requested = list(DEFAULT_LAB_SYMBOLS)
        requested.extend(tradebook_context.get("top_symbols", [])[:8])

    approved_equities = set(fundamental_context.get("approved_equities", []))
    approved_etfs = set(fundamental_context.get("approved_etfs", []))

    out: list[str] = []
    seen = set()
    for symbol in requested:
        if symbol in seen:
            continue
        seen.add(symbol)

        if fundamental_context.get("fundamentals_found"):
            if _looks_etf_like(symbol) or symbol in approved_etfs:
                out.append(symbol)
                continue
            if symbol not in approved_equities:
                continue

        out.append(symbol)
    return out


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


def _candidate_fallback_symbols(limit: int = 8) -> list[str]:
    candidates: list[tuple[int, str]] = []
    for path in sorted(HIST_DIR.glob("*.feather")):
        symbol = path.stem.upper()
        try:
            df = _normalize_ohlcv(pd.read_feather(path))
            if df is None or len(df) < 260:
                continue
            candidates.append((len(df), symbol))
        except Exception:
            continue
    candidates.sort(reverse=True)
    seen = set()
    out = []
    for _, symbol in candidates:
        if symbol in seen:
            continue
        seen.add(symbol)
        out.append(symbol)
        if len(out) >= limit:
            break
    return out



def load_data(tradebook_context: dict, fundamental_context: dict) -> tuple[dict[str, pd.DataFrame], dict]:
    symbols = build_lab_symbols(tradebook_context, fundamental_context)
    data_map: dict[str, pd.DataFrame] = {}
    skipped: dict[str, str] = {}

    def _try_load(symbol_list: list[str], phase_label: str):
        total_symbols = max(1, len(symbol_list))
        for idx, symbol in enumerate(symbol_list, start=1):
            if symbol in data_map:
                continue
            write_status(
                phase=phase_label,
                current_symbol=symbol,
                symbols_total=total_symbols,
                symbols_loaded=len(data_map),
                symbols_index=idx,
            )
            df = _load_symbol_history(symbol)
            if df is None or df.empty:
                skipped[symbol] = "missing_or_empty"
                continue
            if len(df) < 260:
                skipped[symbol] = f"too_short:{len(df)}"
                continue
            try:
                data_map[symbol] = at_utils.Indicators(df)
            except Exception as exc:
                skipped[symbol] = f"indicator_failed:{exc}"

    _try_load(symbols, "loading_history")

    fallback_symbols = []
    if not data_map:
        fallback_symbols = _candidate_fallback_symbols(limit=8)
        _try_load(fallback_symbols, "loading_history_fallback")

    if not data_map:
        raise RuntimeError("Could not load any lab symbols with usable history")

    return data_map, {
        "requested_symbols": symbols,
        "loaded_symbols": list(data_map.keys()),
        "skipped_symbols": skipped,
        "fallback_symbols": fallback_symbols,
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
    optimization_focus = []
    code_findings = []

    optimization_focus.append("expand_buy_sensitivity" if no_trade_day else "balanced_search")

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


def load_fundamental_context() -> dict:
    try:
        from Auto_Trader.StrongFundamentalsStockList import goodStocks

        df = goodStocks()
        if df is None or df.empty:
            return {
                "fundamentals_found": False,
                "code_findings": ["fundamental_screener_empty"],
                "approved_equities": [],
                "approved_etfs": [],
            }

        df["Symbol"] = df["Symbol"].astype(str).str.upper().str.strip()
        df["AssetClass"] = df["AssetClass"].astype(str).str.upper().str.strip()
        approved_equities = df[df["AssetClass"] == "EQUITY"]["Symbol"].dropna().unique().tolist()
        approved_etfs = df[df["AssetClass"] == "ETF"]["Symbol"].dropna().unique().tolist()
        sector_map = dict(zip(df["Symbol"], df.get("Sector", "")))
        return {
            "fundamentals_found": True,
            "approved_equities": approved_equities,
            "approved_etfs": approved_etfs,
            "approved_symbol_count": int(len(df)),
            "sample_symbols": df["Symbol"].head(12).tolist(),
            "sector_map": sector_map,
            "code_findings": [],
        }
    except Exception as exc:
        return {
            "fundamentals_found": False,
            "code_findings": [f"fundamental_screener_failed: {exc}"],
            "approved_equities": [],
            "approved_etfs": [],
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

        open_lots: dict[str, list[dict[str, Any]]] = {}
        closed_rows: list[dict[str, Any]] = []
        for row in grouped.to_dict("records"):
            symbol = row["symbol"]
            open_lots.setdefault(symbol, [])
            if row["trade_type"] == "BUY":
                open_lots[symbol].append(
                    {
                        "qty": float(row["quantity"]),
                        "price": float(row["avg_price"]),
                        "ts": row["order_execution_time"],
                    }
                )
                continue

            remaining = float(row["quantity"])
            while remaining > 1e-9 and open_lots[symbol]:
                lot = open_lots[symbol][0]
                matched = min(remaining, lot["qty"])
                hold_days = (row["order_execution_time"] - lot["ts"]).total_seconds() / 86400.0
                closed_rows.append(
                    {
                        "symbol": symbol,
                        "hold_days": hold_days,
                        "pnl": (float(row["avg_price"]) - float(lot["price"])) * matched,
                    }
                )
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

        buckets = pd.cut(
            closed["hold_days"],
            bins=[-1, 2, 5, 10, 30, 9999],
            labels=["0-2d", "2-5d", "5-10d", "10-30d", "30d+"],
        )
        hold_stats = closed.groupby(buckets, observed=False)["pnl"].sum().to_dict()
        weak_mid_hold = float(hold_stats.get("5-10d", 0.0) or 0.0) < min(
            float(hold_stats.get("0-2d", 0.0) or 0.0),
            float(hold_stats.get("2-5d", 0.0) or 0.0),
        )

        top_symbols = (
            closed.groupby("symbol").size().sort_values(ascending=False).head(12).index.tolist()
        )

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


def build_grids(scorecard_context: dict, tradebook_context: dict) -> tuple[dict, dict]:
    buy_cfg = RULE_SET_7.CONFIG
    sell_cfg = RULE_SET_2.CONFIG

    buy_grid = {
        "adx_min": prioritized_values([12, 14, 16, 18, 20, 22], buy_cfg["adx_min"]),
        "adx_strong_min": prioritized_values([20, 22, 25, 28, 30], buy_cfg["adx_strong_min"]),
        "max_obv_zscore": prioritized_values([2.0, 2.5, 3.0, 3.5, 4.0], buy_cfg["max_obv_zscore"]),
        "obv_min_zscore": prioritized_values([0.0, 0.25, 0.5, 0.75, 1.0], buy_cfg["obv_min_zscore"]),
        "max_extension_atr": prioritized_values([1.5, 1.8, 2.0, 2.2, 2.5, 2.8, 3.2], buy_cfg["max_extension_atr"]),
        "mmi_risk_off": prioritized_values([60, 62, 65, 68, 70], buy_cfg["mmi_risk_off"]),
        "volume_confirm_mult": prioritized_values([0.95, 1.0, 1.05, 1.1, 1.2, 1.3], buy_cfg["volume_confirm_mult"]),
        "cmf_base_min": prioritized_values([0.02, 0.03, 0.05, 0.08], buy_cfg["cmf_base_min"]),
        "rsi_floor": prioritized_values([40, 42, 45, 48, 50], buy_cfg["rsi_floor"]),
        "stoch_pull_max": prioritized_values([60, 65, 70, 75, 80], buy_cfg["stoch_pull_max"]),
        "stoch_momo_max": prioritized_values([75, 80, 85, 90], buy_cfg["stoch_momo_max"]),
        "min_atr_pct": prioritized_values([0.003, 0.005, 0.006, 0.008], buy_cfg["min_atr_pct"]),
        "max_atr_pct": prioritized_values([0.07, 0.08, 0.09, 0.10], buy_cfg["max_atr_pct"]),
    }
    sell_grid = {
        "momentum_exit_rsi": prioritized_values([35.0, 38.0, 40.0, 42.0, 45.0], sell_cfg["momentum_exit_rsi"]),
        "ema_break_atr_mult": prioritized_values([0.3, 0.4, 0.45, 0.5, 0.7], sell_cfg["ema_break_atr_mult"]),
        "breakeven_trigger_pct": prioritized_values([1.0, 1.5, 2.0, 2.5, 3.0, 3.5], sell_cfg["breakeven_trigger_pct"]),
        "relative_volume_exit": prioritized_values([1.0, 1.1, 1.2, 1.3, 1.5], sell_cfg["relative_volume_exit"]),
        "equity_time_stop_bars": prioritized_values([5, 6, 8, 10, 12], sell_cfg["equity_time_stop_bars"]),
        "equity_review_rsi": prioritized_values([45.0, 48.0, 50.0, 52.0], sell_cfg["equity_review_rsi"]),
        "fund_time_stop_bars": prioritized_values([10, 12, 14, 16, 18], sell_cfg["fund_time_stop_bars"]),
        "fund_time_stop_min_profit_pct": prioritized_values([0.5, 0.75, 1.0, 1.5, 2.0], sell_cfg["fund_time_stop_min_profit_pct"]),
    }

    if scorecard_context.get("no_trade_day"):
        buy_grid["adx_min"] = prioritized_values([10, 12, *buy_grid["adx_min"]], buy_cfg["adx_min"])
        buy_grid["max_obv_zscore"] = prioritized_values([*buy_grid["max_obv_zscore"], 4.0, 5.0], buy_cfg["max_obv_zscore"])
        buy_grid["max_extension_atr"] = prioritized_values([*buy_grid["max_extension_atr"], 3.5], buy_cfg["max_extension_atr"])
        buy_grid["mmi_risk_off"] = prioritized_values([*buy_grid["mmi_risk_off"], 70, 75], buy_cfg["mmi_risk_off"])
        buy_grid["volume_confirm_mult"] = prioritized_values([0.9, 0.95, *buy_grid["volume_confirm_mult"]], buy_cfg["volume_confirm_mult"])

    if tradebook_context.get("weak_mid_hold_window"):
        sell_grid["equity_time_stop_bars"] = prioritized_values([4, 5, 6, *sell_grid["equity_time_stop_bars"]], sell_cfg["equity_time_stop_bars"])
        sell_grid["equity_review_rsi"] = prioritized_values([44.0, 46.0, 48.0, *sell_grid["equity_review_rsi"]], sell_cfg["equity_review_rsi"])
        sell_grid["fund_time_stop_bars"] = prioritized_values([8, *sell_grid["fund_time_stop_bars"]], sell_cfg["fund_time_stop_bars"])
        sell_grid["fund_time_stop_min_profit_pct"] = prioritized_values([0.3, 0.5, 0.75, *sell_grid["fund_time_stop_min_profit_pct"]], sell_cfg["fund_time_stop_min_profit_pct"])

    return buy_grid, sell_grid


def _current_param_values(grid: dict, config: dict) -> dict:
    return {key: config[key] for key in grid.keys()}


def _variant_key(buy_params: dict, sell_params: dict) -> str:
    return json.dumps({"buy": buy_params, "sell": sell_params}, sort_keys=True)


class _DisabledRNNConfig:
    enabled = False
    buy_threshold = 0.56
    sell_threshold = 0.44



def load_rnn_config():
    if rnn_lab is None:
        return _DisabledRNNConfig()
    try:
        return rnn_lab.load_config()
    except Exception:
        return _DisabledRNNConfig()



def build_rnn_models(data_map: dict[str, pd.DataFrame], config):
    if rnn_lab is None or not bool(getattr(config, "enabled", False)):
        return {}
    try:
        return rnn_lab.build_overlay_models(data_map, config=config)
    except Exception:
        return {}



def variants(scorecard_context: dict, tradebook_context: dict) -> list[tuple[str, dict, dict, dict]]:
    buy_grid, sell_grid = build_grids(scorecard_context, tradebook_context)
    base_buy = _current_param_values(buy_grid, RULE_SET_7.CONFIG)
    base_sell = _current_param_values(sell_grid, RULE_SET_2.CONFIG)

    out: list[tuple[str, dict, dict, dict]] = []
    seen: set[str] = set()
    base_rnn_cfg = load_rnn_config()

    def add(name: str, buy_patch: dict, sell_patch: dict, rnn_patch: dict | None = None):
        rnn_patch = rnn_patch or {}
        key = json.dumps({"buy": buy_patch, "sell": sell_patch, "rnn": rnn_patch}, sort_keys=True)
        if key in seen:
            return
        seen.add(key)
        out.append((name, buy_patch, sell_patch, rnn_patch))

    add("baseline_current", {}, {}, {"enabled": False})

    if base_rnn_cfg.enabled:
        add(
            "baseline_with_rnn",
            {},
            {},
            {
                "enabled": True,
                "buy_threshold": base_rnn_cfg.buy_threshold,
                "sell_threshold": base_rnn_cfg.sell_threshold,
            },
        )
        for buy_t in [0.52, 0.54, 0.56, 0.58]:
            for sell_t in [0.42, 0.44, 0.46, 0.48]:
                if buy_t <= sell_t:
                    continue
                add(
                    f"rnn_overlay_b{buy_t:.2f}_s{sell_t:.2f}",
                    {},
                    {},
                    {"enabled": True, "buy_threshold": buy_t, "sell_threshold": sell_t},
                )

    for key, values in buy_grid.items():
        for value in values:
            if float(value) == float(base_buy[key]):
                continue
            add(f"buy_{key}_{value}", {key: value}, {}, {"enabled": False})

    for key, values in sell_grid.items():
        for value in values:
            if float(value) == float(base_sell[key]):
                continue
            add(f"sell_{key}_{value}", {}, {key: value}, {"enabled": False})

    focus_buy = ["adx_min", "volume_confirm_mult", "obv_min_zscore", "cmf_base_min", "rsi_floor", "min_atr_pct"]
    focus_sell = ["equity_time_stop_bars", "equity_review_rsi", "momentum_exit_rsi", "breakeven_trigger_pct", "relative_volume_exit"]

    combo_idx = 0
    for bkey in focus_buy:
        for skey in focus_sell:
            bvals = [v for v in buy_grid[bkey] if float(v) != float(base_buy[bkey])][:2]
            svals = [v for v in sell_grid[skey] if float(v) != float(base_sell[skey])][:2]
            for bval in bvals:
                for sval in svals:
                    combo_idx += 1
                    add(f"focus_combo_{combo_idx:03d}", {bkey: bval}, {skey: sval}, {"enabled": False})

    max_variants = int(os.getenv("AT_LAB_MAX_VARIANTS", "350"))
    return out[:max_variants]


def _set_temp_state(rule2_module, d: str):
    rule2_module.BASE_DIR = d
    rule2_module.HOLDINGS_FILE_PATH = os.path.join(d, "Holdings.json")
    rule2_module.LOCK_FILE_PATH = os.path.join(d, "Holdings.lock")


def _simulate_symbol(symbol: str, df: pd.DataFrame, rnn_model=None, rnn_cfg: dict | None = None) -> dict[str, float]:
    cash = 100000.0
    qty = 0
    avg = 0.0
    entry_idx = None
    trades = 0
    wins = 0
    equity_curve = []
    rnn_cfg = rnn_cfg or {"enabled": False}
    rnn_enabled = bool(rnn_cfg.get("enabled")) and rnn_model is not None
    buy_threshold = float(rnn_cfg.get("buy_threshold", 0.56))
    sell_threshold = float(rnn_cfg.get("sell_threshold", 0.44))

    for i in range(250, len(df)):
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
            if rnn_enabled and str(sig).upper() == "BUY":
                prob_up = rnn_model.prob_at(i)
                if prob_up is None or prob_up < buy_threshold:
                    sig = "HOLD"
            if str(sig).upper() == "BUY":
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
            if rnn_enabled and str(sig).upper() != "SELL":
                prob_up = rnn_model.prob_at(i)
                if prob_up is not None and prob_up <= sell_threshold:
                    sig = "SELL"
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


def run_variant(name: str, data_map: dict[str, pd.DataFrame], buy_params: dict, sell_params: dict, rnn_params: dict | None = None, rnn_models: dict | None = None) -> BacktestResult:
    # avoid DB dependency in RULE_SET_7 market regime check
    at_utils.get_mmi_now = lambda: None

    old_r2 = dict(RULE_SET_2.CONFIG)
    old_r7 = dict(RULE_SET_7.CONFIG)
    RULE_SET_2.CONFIG.update(sell_params)
    RULE_SET_7.CONFIG.update(buy_params)
    rnn_params = rnn_params or {"enabled": False}
    rnn_models = rnn_models or {}

    try:
        with tempfile.TemporaryDirectory(prefix="at_state_") as td:
            _set_temp_state(RULE_SET_2, td)
            total_final_value = 0.0
            total_trades = 0
            total_wins = 0
            worst_dd = 0.0
            tested_symbols: list[str] = []
            rnn_accuracies: list[float] = []

            for symbol, df in data_map.items():
                stats = _simulate_symbol(symbol, df, rnn_model=rnn_models.get(symbol), rnn_cfg=rnn_params)
                total_final_value += stats["final_value"]
                total_trades += stats["trades"]
                total_wins += stats["wins"]
                worst_dd = min(worst_dd, stats["max_drawdown_pct"])
                tested_symbols.append(symbol)
                model = rnn_models.get(symbol)
                if rnn_params.get("enabled") and model is not None:
                    rnn_accuracies.append(float(model.metrics.get("test_accuracy", 0.0)))
    finally:
        RULE_SET_2.CONFIG.clear()
        RULE_SET_2.CONFIG.update(old_r2)
        RULE_SET_7.CONFIG.clear()
        RULE_SET_7.CONFIG.update(old_r7)

    start_capital = 100000.0 * max(1, len(tested_symbols))
    ret = (total_final_value / start_capital - 1.0) * 100.0
    round_trips = max(1, total_trades // 2)
    win_rate = (total_wins / round_trips) * 100.0
    selection_score = float(ret + (0.02 * total_trades) - (0.15 * abs(min(0.0, worst_dd))))
    if rnn_params.get("enabled"):
        selection_score += 0.05 * float(np.mean(rnn_accuracies) if rnn_accuracies else 0.0)

    return BacktestResult(
        name=name,
        final_value=round(float(total_final_value), 2),
        total_return_pct=round(float(ret), 2),
        trades=int(total_trades),
        win_rate_pct=round(float(win_rate), 2),
        max_drawdown_pct=round(float(worst_dd), 2),
        params={"buy": buy_params, "sell": sell_params, "rnn": rnn_params},
        symbols_tested=tested_symbols,
        selection_score=round(selection_score, 3),
        rnn_enabled=bool(rnn_params.get("enabled")),
        rnn_avg_test_accuracy=round(float(np.mean(rnn_accuracies) if rnn_accuracies else 0.0), 4),
    )


def main():
    write_status(status="running", phase="initializing", message="starting weekly strategy lab")
    scorecard_context = load_scorecard_context()
    tradebook_context = load_tradebook_context()
    fundamental_context = load_fundamental_context()
    data_map, data_context = load_data(tradebook_context, fundamental_context)
    rnn_config = load_rnn_config()
    write_status(
        phase="training_rnn",
        message="building RNN overlay models",
        rnn_enabled=bool(rnn_config.enabled),
        universe_size=len(data_map),
        universe_symbols=list(data_map.keys()),
    )
    rnn_models = build_rnn_models(data_map, config=rnn_config)
    variant_list = variants(scorecard_context, tradebook_context)
    write_status(
        phase="evaluating_variants",
        message="running strategy variants",
        variants_total=len(variant_list),
        rnn_models_built=len(rnn_models),
    )
    results = []
    for idx, (name, b, s, rnn_params) in enumerate(variant_list, start=1):
        write_status(
            phase="evaluating_variants",
            current_variant=name,
            variants_done=idx - 1,
            variants_total=len(variant_list),
            progress_pct=round(((idx - 1) / max(1, len(variant_list))) * 100.0, 1),
        )
        results.append(run_variant(name, data_map, b, s, rnn_params=rnn_params, rnn_models=rnn_models))

    rank = sorted(
        results,
        key=lambda r: (r.selection_score, r.total_return_pct, -abs(r.max_drawdown_pct), r.win_rate_pct),
        reverse=True,
    )
    baseline = next(r for r in rank if r.name == "baseline_current")
    best = rank[0]

    recommendation = {
        "generated_at": datetime.now().isoformat(),
        "production_rule_model": "BUY=RULE_SET_7, SELL=RULE_SET_2",
        "scorecard_context": scorecard_context,
        "tradebook_context": tradebook_context,
        "fundamental_context": {
            k: v
            for k, v in fundamental_context.items()
            if k != "sector_map"
        },
        "data_context": data_context,
        "rnn_context": {
            "enabled": bool(rnn_config.enabled),
            "models_built": int(len(rnn_models)),
            "symbols": {k: v.metrics for k, v in rnn_models.items()},
        },
        "baseline": asdict(baseline),
        "best": asdict(best),
        "tested_variants": len(rank),
        "improvement_return_pct": round(best.total_return_pct - baseline.total_return_pct, 2),
        "improvement_score": round(best.selection_score - baseline.selection_score, 3),
        "should_promote": bool(
            best.name != baseline.name
            and best.total_return_pct > baseline.total_return_pct + 0.25
            and best.selection_score > baseline.selection_score + 0.2
            and abs(best.max_drawdown_pct) <= abs(baseline.max_drawdown_pct) + 2.0
        ),
    }

    payload = {
        "recommendation": recommendation,
        "ranked": [asdict(r) for r in rank],
    }

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_json = OUT_DIR / f"strategy_lab_{ts}.json"
    out_csv = OUT_DIR / f"strategy_lab_{ts}.csv"

    out_json.write_text(json.dumps(payload, indent=2))
    pd.DataFrame([asdict(r) for r in rank]).to_csv(out_csv, index=False)

    write_status(
        status="done",
        phase="completed",
        progress_pct=100.0,
        current_variant=None,
        variants_done=len(variant_list),
        variants_total=len(variant_list),
        latest_report_json=str(out_json),
        latest_report_csv=str(out_csv),
        best_variant=best.name,
        best_return_pct=best.total_return_pct,
        best_score=best.selection_score,
        best_rnn_enabled=best.rnn_enabled,
    )

    print(json.dumps(recommendation, indent=2))
    print(f"Saved: {out_json}")
    print(f"Saved: {out_csv}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        write_status(status="failed", phase="failed", error=str(exc))
        raise
