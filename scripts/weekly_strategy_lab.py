#!/usr/bin/env python3
"""
Strategy lab:
- tweaks RULE_SET_7 (BUY) + RULE_SET_2 (SELL)
- backtests variants on a small basket, not just one ETF
- uses latest daily scorecard + tradebook context to bias the search
- writes ranked reports; does NOT auto-deploy winners
"""

from __future__ import annotations

import atexit
import contextlib
import json
import multiprocessing as mp
import os
import signal
import sys
import tempfile
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yfinance as yf

try:
    import skfolio
    from skfolio.optimization import (
        MeanRisk, HierarchicalRiskParity, MaximumDiversification,
        RiskBudgeting, EqualWeighted, InverseVolatility,
    )
    from skfolio.model_selection import WalkForward
    from skfolio import RiskMeasure, RatioMeasure
    from skfolio.pre_selection import DropCorrelated
    SKFOLIO_AVAILABLE = True
except ImportError:
    SKFOLIO_AVAILABLE = False

try:
    from pypfopt import EfficientFrontier, EfficientCVaR
    from pypfopt.risk_models import CovarianceShrinkage
    from pypfopt.expected_returns import mean_historical_return
    PYPFOPT_AVAILABLE = True
except ImportError:
    PYPFOPT_AVAILABLE = False

try:
    import riskfolio as rp
    RISKFOLIO_AVAILABLE = True
except ImportError:
    RISKFOLIO_AVAILABLE = False

try:
    import empyrical as empyrical
    EMPYRICAL_AVAILABLE = True
except ImportError:
    EMPYRICAL_AVAILABLE = False

try:
    import quantstats as qs
    QUANTSTATS_AVAILABLE = True
except ImportError:
    QUANTSTATS_AVAILABLE = False

# Avoid noisy file-handler permission issues during research/backtest runs.
os.environ.setdefault("AT_DISABLE_FILE_LOGGING", "1")

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# In research mode, Auto_Trader skips Kite/broker imports. The lab only needs
# RULE_SET_2/7 and utils.fetch_prices_kite; it does NOT need a live Kite session.
os.environ.setdefault("AT_RESEARCH_MODE", "1")

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
HIST_DIR.mkdir(parents=True, exist_ok=True)
STATUS_DIR = ROOT / "intermediary_files" / "lab_status"
STATUS_DIR.mkdir(exist_ok=True)
STATUS_PATH = STATUS_DIR / "weekly_strategy_lab_status.json"

_WORKER_DATA_MAP: dict[str, pd.DataFrame] | None = None
_WORKER_RNN_MODELS: dict | None = None


def configured_history_period() -> str:
    return os.getenv("AT_LAB_HISTORY_PERIOD", "3y").strip() or "3y"


def configured_min_history_bars(default: int = 260) -> int:
    try:
        return max(1, int(os.getenv("AT_LAB_MIN_BARS", str(default))))
    except Exception:
        return int(default)


def configured_precache_workers() -> int:
    cpu = max(1, int(os.cpu_count() or 1))
    default = min(12, max(4, cpu))
    try:
        return max(1, int(os.getenv("AT_LAB_PRECACHE_WORKERS", str(default))))
    except Exception:
        return int(default)


def configured_variant_workers() -> int:
    cpu = max(1, int(os.cpu_count() or 1))
    default = min(6, max(1, cpu - 2))
    try:
        return max(1, int(os.getenv("AT_LAB_MAX_WORKERS", str(default))))
    except Exception:
        return int(default)

def _portfolio_weight_cap() -> float:
    try:
        return min(1.0, max(0.01, float(os.getenv("AT_LAB_PORTFOLIO_MAX_WEIGHT", "0.15"))))
    except Exception:
        return 0.15


def _normalize_weight_map(weights: dict[str, float], symbols: list[str], *, cap: float | None = None) -> dict[str, float]:
    cap = _portfolio_weight_cap() if cap is None else float(cap)
    clean = {s: max(0.0, float(weights.get(s, 0.0) or 0.0)) for s in symbols}
    if not clean or sum(clean.values()) <= 0:
        return {s: 1.0 / max(1, len(symbols)) for s in symbols}
    clean = {s: min(cap, w) for s, w in clean.items()}
    total = sum(clean.values())
    if total <= 0:
        return {s: 1.0 / max(1, len(symbols)) for s in symbols}
    return {s: float(w / total) for s, w in clean.items() if w > 1e-8}


def _compute_return_metrics(returns: pd.Series | list[float], *, periods_per_year: int = 252) -> dict:
    ser = pd.Series(returns, dtype=float).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    if ser.empty:
        return {}
    equity = (1.0 + ser).cumprod()
    max_dd = float(((equity / equity.cummax()) - 1.0).min())
    total_return = float(equity.iloc[-1] - 1.0)
    years = max(len(ser) / float(periods_per_year), 1.0 / float(periods_per_year))
    cagr = float(equity.iloc[-1] ** (1.0 / years) - 1.0) if equity.iloc[-1] > 0 else -1.0
    vol = float(ser.std(ddof=0) * np.sqrt(periods_per_year))
    sharpe = float((ser.mean() / ser.std(ddof=0)) * np.sqrt(periods_per_year)) if ser.std(ddof=0) > 0 else 0.0
    downside = ser[ser < 0].std(ddof=0)
    sortino = float((ser.mean() / downside) * np.sqrt(periods_per_year)) if downside and downside > 0 else 0.0
    calmar = float(cagr / abs(max_dd)) if max_dd < 0 else 0.0
    metrics = {
        "total_return_pct": round(total_return * 100.0, 3),
        "cagr_pct": round(cagr * 100.0, 3),
        "max_drawdown_pct": round(max_dd * 100.0, 3),
        "annual_volatility_pct": round(vol * 100.0, 3),
        "sharpe": round(sharpe, 4),
        "sortino": round(sortino, 4),
        "calmar": round(calmar, 4),
    }
    if EMPYRICAL_AVAILABLE:
        try:
            metrics.update({
                "empyrical_annual_return_pct": round(float(empyrical.annual_return(ser)) * 100.0, 3),
                "empyrical_max_drawdown_pct": round(float(empyrical.max_drawdown(ser)) * 100.0, 3),
                "empyrical_sharpe": round(float(empyrical.sharpe_ratio(ser)), 4),
            })
        except Exception as exc:
            metrics["empyrical_error"] = str(exc)[:160]
    if QUANTSTATS_AVAILABLE:
        try:
            # quantstats expects a datetime-like index for some annualized ratios.
            qs_ser = ser.copy()
            if not isinstance(qs_ser.index, pd.DatetimeIndex):
                qs_ser.index = pd.bdate_range(end=pd.Timestamp.today().normalize(), periods=len(qs_ser))
            metrics["quantstats_sharpe"] = round(float(qs.stats.sharpe(qs_ser)), 4)
            metrics["quantstats_sortino"] = round(float(qs.stats.sortino(qs_ser)), 4)
            metrics["quantstats_calmar"] = round(float(qs.stats.calmar(qs_ser)), 4)
        except Exception as exc:
            metrics["quantstats_error"] = str(exc)[:160]
    return metrics


def _score_from_metrics(total_return_pct: float, trades: int, win_rate_pct: float, max_drawdown_pct: float, metrics: dict | None = None) -> float:
    metrics = metrics or {}
    trade_bonus = min(5.0, 0.003 * float(trades))
    score = float(total_return_pct + trade_bonus + (0.05 * float(win_rate_pct)) - (0.75 * abs(min(0.0, max_drawdown_pct))))
    if metrics:
        score += 0.35 * float(metrics.get("calmar", 0.0) or 0.0)
        score += 0.20 * float(metrics.get("sortino", 0.0) or 0.0)
        score += 0.15 * float(metrics.get("sharpe", 0.0) or 0.0)
    return score


def configured_variant_batch() -> tuple[int, int | None]:
    try:
        offset = max(0, int(os.getenv("AT_LAB_VARIANT_OFFSET", "0")))
    except Exception:
        offset = 0
    raw_limit = os.getenv("AT_LAB_VARIANT_LIMIT", "").strip()
    if not raw_limit:
        return offset, None
    try:
        limit = max(1, int(raw_limit))
    except Exception:
        limit = None
    return offset, limit

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
    risk_metrics: dict | None = None


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


def _history_cache_path(symbol: str) -> Path:
    return HIST_DIR / f"{str(symbol or '').strip().upper()}.feather"


def _save_symbol_history(symbol: str, df: pd.DataFrame) -> None:
    symbol = str(symbol or "").strip().upper()
    if not symbol or df is None or df.empty:
        return
    out = _normalize_ohlcv(df)
    if out is None or out.empty:
        return
    tmp = _history_cache_path(symbol).with_suffix(".feather.tmp")
    out.reset_index(drop=True).to_feather(tmp)
    os.replace(tmp, _history_cache_path(symbol))


def _download_symbol_history(symbol: str) -> pd.DataFrame | None:
    symbol = str(symbol or "").strip().upper()
    if not symbol:
        return None

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
                period=configured_history_period(),
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


def _load_symbol_history(symbol: str, *, persist_download: bool = True) -> pd.DataFrame | None:
    symbol = str(symbol or "").strip().upper()
    if not symbol:
        return None

    local_path = _history_cache_path(symbol)
    if local_path.exists():
        try:
            local_df = _normalize_ohlcv(pd.read_feather(local_path))
            if local_df is not None and not local_df.empty:
                return local_df
        except Exception:
            pass

    cache_only = os.getenv("AT_LAB_CACHE_ONLY", "0").strip().lower() in {"1", "true", "yes"}
    # On secondary/research runs we must not silently fall back to yfinance:
    # Kite cached feather data is the live-parity source of truth.
    if cache_only:
        return None

    out = _download_symbol_history(symbol)
    if out is not None and not out.empty and persist_download:
        try:
            _save_symbol_history(symbol, out)
        except Exception:
            pass
    return out


def _parse_symbol_list(value: str) -> list[str]:
    return [x.strip().upper() for x in value.split(",") if x.strip()]


def _looks_etf_like(symbol: str) -> bool:
    text = str(symbol or "").upper()
    return ("ETF" in text) or ("BEES" in text)


def build_lab_symbols(tradebook_context: dict, fundamental_context: dict) -> list[str]:
    explicit = os.getenv("AT_LAB_SYMBOLS", "").strip()
    use_approved_universe = os.getenv("AT_LAB_USE_APPROVED_UNIVERSE", "1").strip().lower() not in {"0", "false", "no"}

    approved_equities_list = [str(x).upper().strip() for x in fundamental_context.get("approved_equities", []) if str(x).strip()]
    approved_etfs_list = [str(x).upper().strip() for x in fundamental_context.get("approved_etfs", []) if str(x).strip()]
    approved_equities = set(approved_equities_list)
    approved_etfs = set(approved_etfs_list)

    if explicit:
        requested = _parse_symbol_list(explicit)
    elif use_approved_universe and fundamental_context.get("fundamentals_found"):
        requested = approved_equities_list + approved_etfs_list
        requested.extend(tradebook_context.get("top_symbols", [])[:8])
    else:
        requested = list(DEFAULT_LAB_SYMBOLS)
        requested.extend(tradebook_context.get("top_symbols", [])[:8])

    out: list[str] = []
    seen = set()
    for symbol in requested:
        symbol = str(symbol or "").upper().strip()
        if not symbol or symbol in seen:
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


def _precache_histories(symbols: list[str]) -> dict:
    enabled = os.getenv("AT_LAB_PRECACHE", "1").strip().lower() not in {"0", "false", "no"}
    if not enabled:
        return {
            "enabled": False,
            "requested": len(symbols),
            "cached_before": sum(1 for symbol in symbols if _history_cache_path(symbol).exists()),
            "downloaded": 0,
            "missing_after": sum(1 for symbol in symbols if not _history_cache_path(symbol).exists()),
            "workers": 0,
        }

    missing = [symbol for symbol in symbols if not _history_cache_path(symbol).exists()]
    total = max(1, len(missing))
    if not missing:
        return {
            "enabled": True,
            "requested": len(symbols),
            "cached_before": len(symbols),
            "downloaded": 0,
            "missing_after": 0,
            "workers": 0,
        }

    workers = min(configured_precache_workers(), len(missing))
    downloaded = 0

    def _ensure_cached(symbol: str) -> tuple[str, bool]:
        if _history_cache_path(symbol).exists():
            return symbol, True
        df = _download_symbol_history(symbol)
        if df is None or df.empty:
            return symbol, False
        try:
            _save_symbol_history(symbol, df)
            return symbol, True
        except Exception:
            return symbol, False

    write_status(
        phase="precaching_history",
        message="pre-caching missing symbol history locally",
        symbols_total=len(symbols),
        symbols_loaded=0,
        symbols_index=0,
        symbols_missing=len(missing),
        precache_workers=workers,
    )
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_ensure_cached, symbol): symbol for symbol in missing}
        for idx, future in enumerate(as_completed(futures), start=1):
            _, ok = future.result()
            if ok:
                downloaded += 1
            write_status(
                phase="precaching_history",
                current_symbol=futures[future],
                symbols_total=len(symbols),
                symbols_loaded=downloaded,
                symbols_index=idx,
                symbols_missing=len(missing),
                progress_pct=round((idx / total) * 100.0, 1),
                precache_workers=workers,
            )

    missing_after = sum(1 for symbol in symbols if not _history_cache_path(symbol).exists())
    return {
        "enabled": True,
        "requested": len(symbols),
        "cached_before": len(symbols) - len(missing),
        "downloaded": downloaded,
        "missing_after": missing_after,
        "workers": workers,
    }


def load_data(tradebook_context: dict, fundamental_context: dict) -> tuple[dict[str, pd.DataFrame], dict]:
    symbols = build_lab_symbols(tradebook_context, fundamental_context)
    data_map: dict[str, pd.DataFrame] = {}
    skipped: dict[str, str] = {}
    min_history_bars = configured_min_history_bars()
    precache_stats = _precache_histories(symbols)

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
            if len(df) < min_history_bars:
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
        "requested_symbol_count": len(symbols),
        "loaded_symbols": list(data_map.keys()),
        "loaded_symbol_count": len(data_map),
        "skipped_symbols": skipped,
        "fallback_symbols": fallback_symbols,
        "history_period": configured_history_period(),
        "min_history_bars": min_history_bars,
        "use_approved_universe": os.getenv("AT_LAB_USE_APPROVED_UNIVERSE", "1").strip().lower() not in {"0", "false", "no"},
        "precache": precache_stats,
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
    if os.getenv("AT_LAB_SKIP_FUNDAMENTALS", "0").strip().lower() in {"1", "true", "yes"} or os.getenv("AT_LAB_SYMBOLS", "").strip():
        return {
            "fundamentals_found": False,
            "code_findings": ["fundamental_screener_skipped_for_cache_lab"],
            "approved_equities": [],
            "approved_etfs": [],
        }
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
        "adx_min": prioritized_values([6, 8, 10, 12, 14, 16, 18], buy_cfg["adx_min"]),
        "adx_strong_min": prioritized_values([18, 20, 22, 25, 28, 30], buy_cfg["adx_strong_min"]),
        "max_obv_zscore": prioritized_values([2.0, 2.5, 3.0, 3.5, 4.0, 5.0], buy_cfg["max_obv_zscore"]),
        "obv_min_zscore": prioritized_values([0.0, 0.25, 0.5, 0.75, 1.0], buy_cfg["obv_min_zscore"]),
        "max_extension_atr": prioritized_values([1.5, 1.8, 2.0, 2.2, 2.5, 2.8, 3.2, 3.5], buy_cfg["max_extension_atr"]),
        "mmi_risk_off": prioritized_values([60, 62, 65, 68, 70, 75], buy_cfg["mmi_risk_off"]),
        "volume_confirm_mult": prioritized_values([0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 1.0, 1.05], buy_cfg["volume_confirm_mult"]),
        "cmf_base_min": prioritized_values([0.0, 0.02, 0.03, 0.05, 0.08], buy_cfg["cmf_base_min"]),
        "rsi_floor": prioritized_values([34, 36, 38, 40, 42, 45, 48], buy_cfg["rsi_floor"]),
        "stoch_pull_max": prioritized_values([65, 70, 75, 80, 85, 90, 95], buy_cfg["stoch_pull_max"]),
        "stoch_momo_max": prioritized_values([80, 85, 90, 95], buy_cfg["stoch_momo_max"]),
        "min_atr_pct": prioritized_values([0.001, 0.002, 0.003, 0.005, 0.006, 0.008], buy_cfg["min_atr_pct"]),
        "max_atr_pct": prioritized_values([0.07, 0.08, 0.09, 0.10, 0.12], buy_cfg["max_atr_pct"]),
        "ich_cloud_bull": prioritized_values([0, 1], buy_cfg["ich_cloud_bull"]),
        "vwap_buy_above": prioritized_values([0, 1], buy_cfg["vwap_buy_above"]),
        "cci_buy_min": prioritized_values([-175, -150, -125, -100, -75, -50], buy_cfg["cci_buy_min"]),
    }
    sell_grid = {
        "momentum_exit_rsi": prioritized_values([35.0, 38.0, 40.0, 42.0, 45.0, 48.0], sell_cfg["momentum_exit_rsi"]),
        "ema_break_atr_mult": prioritized_values([0.3, 0.4, 0.45, 0.5, 0.7, 0.9], sell_cfg["ema_break_atr_mult"]),
        "breakeven_trigger_pct": prioritized_values([1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 5.0], sell_cfg["breakeven_trigger_pct"]),
        "relative_volume_exit": prioritized_values([1.0, 1.1, 1.2, 1.3, 1.5], sell_cfg["relative_volume_exit"]),
        "equity_time_stop_bars": prioritized_values([5, 6, 8, 10, 12, 15, 20], sell_cfg["equity_time_stop_bars"]),
        "equity_review_rsi": prioritized_values([42.0, 45.0, 48.0, 50.0, 52.0, 55.0], sell_cfg["equity_review_rsi"]),
        "fund_time_stop_bars": prioritized_values([10, 12, 14, 16, 18, 22, 26], sell_cfg["fund_time_stop_bars"]),
        "fund_time_stop_min_profit_pct": prioritized_values([0.3, 0.5, 0.75, 1.0, 1.5, 2.0], sell_cfg["fund_time_stop_min_profit_pct"]),
    }

    if scorecard_context.get("no_trade_day"):
        buy_grid["adx_min"] = prioritized_values([6, 8, 10, 12, *buy_grid["adx_min"]], buy_cfg["adx_min"])
        buy_grid["max_obv_zscore"] = prioritized_values([*buy_grid["max_obv_zscore"], 4.0, 5.0], buy_cfg["max_obv_zscore"])
        buy_grid["max_extension_atr"] = prioritized_values([*buy_grid["max_extension_atr"], 3.5], buy_cfg["max_extension_atr"])
        buy_grid["mmi_risk_off"] = prioritized_values([*buy_grid["mmi_risk_off"], 70, 75], buy_cfg["mmi_risk_off"])
        buy_grid["volume_confirm_mult"] = prioritized_values([0.7, 0.75, 0.8, 0.9, 0.95, *buy_grid["volume_confirm_mult"]], buy_cfg["volume_confirm_mult"])
        buy_grid["rsi_floor"] = prioritized_values([34, 36, 38, 40, *buy_grid["rsi_floor"]], buy_cfg["rsi_floor"])
        buy_grid["stoch_pull_max"] = prioritized_values([85, 90, 95, *buy_grid["stoch_pull_max"]], buy_cfg["stoch_pull_max"])
        buy_grid["ich_cloud_bull"] = prioritized_values([0, 1], buy_cfg["ich_cloud_bull"])
        buy_grid["vwap_buy_above"] = prioritized_values([0, 1], buy_cfg["vwap_buy_above"])

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

    # High-priority structural candidates: ensure small AT_LAB_MAX_VARIANTS runs
    # actually test sideways-market mean-reversion before broad one-factor sweeps.
    priority_meanrev_buy = [
        {"meanrev_enabled": 1, "meanrev_rsi_oversold": 35, "meanrev_adx_max": 25, "meanrev_bb_pctb_max": 0.3, "rsi_floor": 45, "adx_min": 10},
        {"meanrev_enabled": 1, "meanrev_rsi_oversold": 40, "meanrev_adx_max": 28, "meanrev_bb_pctb_max": 0.35, "meanrev_cci_min": -100, "meanrev_stoch_k_max": 35, "rsi_floor": 45, "adx_min": 10},
        {"meanrev_enabled": 1, "meanrev_rsi_oversold": 30, "meanrev_adx_max": 22, "meanrev_bb_pctb_max": 0.2, "meanrev_stoch_k_max": 25, "rsi_floor": 45, "adx_min": 10},
    ]
    priority_meanrev_sell = [
        {"meanrev_exit_rsi": 60, "meanrev_exit_bb_pctb": 0.8, "meanrev_exit_bars": 5, "equity_time_stop_bars": 20},
        {"meanrev_exit_rsi": 55, "meanrev_exit_bb_pctb": 0.7, "meanrev_exit_bars": 8, "equity_time_stop_bars": 20},
    ]
    priority_idx = 0
    for buy_patch in priority_meanrev_buy:
        for sell_patch in priority_meanrev_sell:
            priority_idx += 1
            add(f"priority_meanrev_{priority_idx:03d}", buy_patch, sell_patch, {"enabled": False})
    portfolio_variant_enabled = os.getenv("AT_LAB_PORTFOLIO_OPT_VARIANTS", "1").strip().lower() not in {"0", "false", "no"}
    if portfolio_variant_enabled and (SKFOLIO_AVAILABLE or PYPFOPT_AVAILABLE or RISKFOLIO_AVAILABLE):
        for buy_patch in priority_meanrev_buy[:2]:
            priority_idx += 1
            for method in ["skfolio_hrp", "riskfolio_hrp_mv", "riskfolio_min_cvar", "riskfolio_cvar_sharpe", "inverse_volatility"]:
                add(
                    f"priority_po_{method}_{priority_idx:03d}",
                    buy_patch,
                    {"meanrev_exit_rsi": 60, "meanrev_exit_bb_pctb": 0.8, "meanrev_exit_bars": 5, "equity_time_stop_bars": 20},
                    {"enabled": False, "portfolio_opt": True, "portfolio_method": method},
                )

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

    focus_buy = [
        "adx_min",
        "volume_confirm_mult",
        "obv_min_zscore",
        "cmf_base_min",
        "rsi_floor",
        "min_atr_pct",
        "max_extension_atr",
        "ich_cloud_bull",
        "vwap_buy_above",
        "stoch_pull_max",
        "cci_buy_min",
    ]
    focus_sell = [
        "equity_time_stop_bars",
        "equity_review_rsi",
        "momentum_exit_rsi",
        "breakeven_trigger_pct",
        "relative_volume_exit",
        "fund_time_stop_bars",
    ]

    combo_idx = 0
    for bkey in focus_buy:
        for skey in focus_sell:
            bvals = [v for v in buy_grid[bkey] if float(v) != float(base_buy[bkey])][:2]
            svals = [v for v in sell_grid[skey] if float(v) != float(base_sell[skey])][:2]
            for bval in bvals:
                for sval in svals:
                    combo_idx += 1
                    add(f"focus_combo_{combo_idx:03d}", {bkey: bval}, {skey: sval}, {"enabled": False})

    curated_buy = [
        # Original curated combos
        {"volume_confirm_mult": 0.9, "ich_cloud_bull": 0},
        {"volume_confirm_mult": 0.9, "vwap_buy_above": 0},
        {"volume_confirm_mult": 0.9, "ich_cloud_bull": 0, "vwap_buy_above": 0},
        {"rsi_floor": 40, "stoch_pull_max": 85, "stoch_momo_max": 90},
        {"rsi_floor": 40, "volume_confirm_mult": 0.9, "stoch_pull_max": 85, "ich_cloud_bull": 0},
        {"adx_min": 12, "volume_confirm_mult": 0.9, "ich_cloud_bull": 0},
        {"adx_min": 12, "volume_confirm_mult": 0.9, "ich_cloud_bull": 0, "vwap_buy_above": 0},
        {"cci_buy_min": -125, "volume_confirm_mult": 0.9, "ich_cloud_bull": 0},
        {"adx_min": 8, "volume_confirm_mult": 0.75, "rsi_floor": 38, "stoch_pull_max": 90, "ich_cloud_bull": 0, "vwap_buy_above": 0},
        {"adx_min": 8, "volume_confirm_mult": 0.75, "max_extension_atr": 3.2, "obv_min_zscore": 0.0, "ich_cloud_bull": 0},
        {"adx_min": 6, "volume_confirm_mult": 0.7, "rsi_floor": 36, "stoch_pull_max": 95, "cci_buy_min": -150, "vwap_buy_above": 0},
        # Ultra-loose combos: maximize signal density (these match best-performing symbols)
        {"adx_min": 6, "volume_confirm_mult": 0.7, "ich_cloud_bull": 0, "vwap_buy_above": 0, "rsi_floor": 34, "stoch_pull_max": 95, "max_extension_atr": 3.5, "max_obv_zscore": 5.0, "cci_buy_min": -175, "cmf_base_min": 0.0, "mmi_risk_off": 75},
        {"adx_min": 6, "volume_confirm_mult": 0.7, "ich_cloud_bull": 0, "vwap_buy_above": 0, "rsi_floor": 36, "stoch_pull_max": 90, "max_extension_atr": 3.2, "obv_min_zscore": 0.0, "cci_buy_min": -150},
        {"adx_min": 8, "volume_confirm_mult": 0.75, "ich_cloud_bull": 0, "vwap_buy_above": 0, "rsi_floor": 34, "stoch_pull_max": 95, "max_extension_atr": 3.5},
        {"adx_min": 6, "volume_confirm_mult": 0.7, "ich_cloud_bull": 0, "rsi_floor": 38, "stoch_pull_max": 85, "cci_buy_min": -125},
        {"adx_min": 8, "volume_confirm_mult": 0.8, "ich_cloud_bull": 0, "vwap_buy_above": 0, "rsi_floor": 36, "stoch_pull_max": 90, "obv_min_zscore": 0.25},
        # High-conviction combos (tighter buy, wider hold)
        {"adx_strong_min": 18, "volume_confirm_mult": 0.85, "ich_cloud_bull": 0, "rsi_floor": 40, "stoch_pull_max": 85},
        {"adx_strong_min": 20, "volume_confirm_mult": 0.9, "ich_cloud_bull": 0, "rsi_floor": 38, "cci_buy_min": -100},
        # Contrarian / mean-reversion adjacent
        {"adx_min": 6, "rsi_floor": 34, "stoch_pull_max": 95, "volume_confirm_mult": 0.7, "ich_cloud_bull": 0, "cci_buy_min": -175, "max_extension_atr": 3.5, "obv_min_zscore": 0.0, "cmf_base_min": 0.0},
        {"adx_min": 8, "rsi_floor": 36, "stoch_pull_max": 90, "volume_confirm_mult": 0.75, "ich_cloud_bull": 0, "vwap_buy_above": 0, "cci_buy_min": -150, "max_extension_atr": 3.2},
    ]
    curated_sell = [
        # Original sell combos
        {},
        {"breakeven_trigger_pct": 4.0},
        {"breakeven_trigger_pct": 5.0, "equity_time_stop_bars": 12},
        {"breakeven_trigger_pct": 4.0, "fund_time_stop_bars": 18},
        {"equity_time_stop_bars": 15, "fund_time_stop_bars": 22},
        {"momentum_exit_rsi": 38.0, "equity_review_rsi": 45.0},
        # Longer holds: let winners run
        {"equity_time_stop_bars": 20, "fund_time_stop_bars": 26, "breakeven_trigger_pct": 5.0},
        {"equity_time_stop_bars": 15, "fund_time_stop_bars": 22, "momentum_exit_rsi": 35.0},
        # Wider breakeven to avoid whipsaw exits
        {"breakeven_trigger_pct": 5.0, "equity_review_rsi": 42.0, "fund_time_stop_min_profit_pct": 0.5},
        {"breakeven_trigger_pct": 3.5, "equity_time_stop_bars": 12, "fund_time_stop_bars": 18},
        # Aggressive momentum capture
        {"momentum_exit_rsi": 35.0, "equity_review_rsi": 42.0, "equity_time_stop_bars": 15},
        {"relative_volume_exit": 1.5, "breakeven_trigger_pct": 4.0, "fund_time_stop_bars": 18},
    ]
    # --- Mean-reversion entry variants ---
    # These enable the meanrev entry mode and tune its parameters
    meanrev_buy_combos = [
        # Basic meanrev: oversold bounce
        {"meanrev_enabled": 1, "rsi_floor": 45, "adx_min": 10},
        # Mean-reversion with low ADX threshold
        {"meanrev_enabled": 1, "meanrev_rsi_oversold": 35, "meanrev_adx_max": 25, "meanrev_bb_pctb_max": 0.3, "rsi_floor": 45, "adx_min": 10},
        # Aggressive meanrev: catch deeper oversold
        {"meanrev_enabled": 1, "meanrev_rsi_oversold": 30, "meanrev_adx_max": 22, "meanrev_bb_pctb_max": 0.2, "meanrev_stoch_k_max": 25, "rsi_floor": 45, "adx_min": 10},
        # Wide meanrev: more signals with relaxed thresholds
        {"meanrev_enabled": 1, "meanrev_rsi_oversold": 40, "meanrev_adx_max": 28, "meanrev_bb_pctb_max": 0.35, "meanrev_cci_min": -100, "meanrev_stoch_k_max": 35, "rsi_floor": 45, "adx_min": 10},
        # Conservative meanrev: only high-confidence reversals
        {"meanrev_enabled": 1, "meanrev_rsi_oversold": 25, "meanrev_adx_max": 20, "meanrev_bb_pctb_max": 0.15, "meanrev_cci_min": -200, "meanrev_stoch_k_max": 20, "rsi_floor": 45, "adx_min": 10},
        # Meanrev + trend pullback hybrid (both modes can trigger)
        {"meanrev_enabled": 1, "meanrev_rsi_oversold": 35, "meanrev_adx_max": 25, "rsi_floor": 40, "adx_min": 8, "volume_confirm_mult": 0.8},
    ]
    meanrev_sell_combos = [
        # Quick mean-reversion exit: take profit fast, cut losses fast
        {"meanrev_exit_rsi": 60, "meanrev_exit_bb_pctb": 0.8, "meanrev_exit_bars": 5},
        # Slower meanrev exit: let the reversal develop
        {"meanrev_exit_rsi": 55, "meanrev_exit_bb_pctb": 0.7, "meanrev_exit_bars": 8, "equity_time_stop_bars": 20},
        # Aggressive meanrev exit: very quick
        {"meanrev_exit_rsi": 65, "meanrev_exit_bb_pctb": 0.85, "meanrev_exit_bars": 3},
        # Patient meanrev exit: hold longer for bigger reversal
        {"meanrev_exit_rsi": 55, "meanrev_exit_bb_pctb": 0.75, "meanrev_exit_bars": 10, "breakeven_trigger_pct": 3.0},
    ]
    meanrev_idx = 0
    for buy_patch in meanrev_buy_combos:
        for sell_patch in meanrev_sell_combos:
            meanrev_idx += 1
            add(f"meanrev_{meanrev_idx:03d}", buy_patch, sell_patch, {"enabled": False})

    curated_idx = 0
    for buy_patch in curated_buy:
        for sell_patch in curated_sell:
            curated_idx += 1
            add(f"curated_combo_{curated_idx:03d}", buy_patch, sell_patch, {"enabled": False})

    # Portfolio-optimized variants: reuse best curated buy combos with portfolio optimization
    # These use skfolio/PyPortfolioOpt to weight symbols by correlation structure
    if portfolio_variant_enabled and (SKFOLIO_AVAILABLE or PYPFOPT_AVAILABLE or RISKFOLIO_AVAILABLE):
        # Top curated combos that should benefit most from portfolio optimization
        po_buy_combos = [
            {"adx_min": 6, "volume_confirm_mult": 0.7, "ich_cloud_bull": 0, "vwap_buy_above": 0, "rsi_floor": 34, "stoch_pull_max": 95, "max_extension_atr": 3.5, "max_obv_zscore": 5.0, "cci_buy_min": -175, "cmf_base_min": 0.0, "mmi_risk_off": 75},
            {"adx_min": 6, "volume_confirm_mult": 0.7, "ich_cloud_bull": 0, "vwap_buy_above": 0, "rsi_floor": 36, "stoch_pull_max": 90, "max_extension_atr": 3.2, "obv_min_zscore": 0.0, "cci_buy_min": -150},
            {"adx_min": 8, "volume_confirm_mult": 0.75, "ich_cloud_bull": 0, "vwap_buy_above": 0, "rsi_floor": 34, "stoch_pull_max": 95, "max_extension_atr": 3.5},
            {"adx_min": 6, "volume_confirm_mult": 0.7, "ich_cloud_bull": 0, "rsi_floor": 38, "stoch_pull_max": 85, "cci_buy_min": -125},
            {"volume_confirm_mult": 0.9, "ich_cloud_bull": 0},
            {"adx_min": 12, "volume_confirm_mult": 0.9, "ich_cloud_bull": 0},
            {"adx_min": 8, "volume_confirm_mult": 0.75, "rsi_floor": 38, "stoch_pull_max": 90, "ich_cloud_bull": 0, "vwap_buy_above": 0},
        ]
        po_sell_combos = [
            {},
            {"breakeven_trigger_pct": 5.0, "equity_time_stop_bars": 12},
            {"equity_time_stop_bars": 20, "fund_time_stop_bars": 26, "breakeven_trigger_pct": 5.0},
            {"momentum_exit_rsi": 35.0, "equity_review_rsi": 42.0, "equity_time_stop_bars": 15},
        ]
        po_idx = 0
        for buy_patch in po_buy_combos:
            for sell_patch in po_sell_combos:
                po_idx += 1
                for method in ["skfolio_hrp", "riskfolio_hrp_mv", "riskfolio_min_cvar", "riskfolio_cvar_sharpe", "pypfopt_max_sharpe", "inverse_volatility"]:
                    add(f"po_{method}_{po_idx:03d}", buy_patch, sell_patch, {"enabled": False, "portfolio_opt": True, "portfolio_method": method})

    max_variants = int(os.getenv("AT_LAB_MAX_VARIANTS", "900"))
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


def _variant_worker_init(data_map: dict[str, pd.DataFrame], rnn_models: dict) -> None:
    global _WORKER_DATA_MAP, _WORKER_RNN_MODELS
    _WORKER_DATA_MAP = data_map
    _WORKER_RNN_MODELS = rnn_models


def _run_variant_worker(task: tuple[str, dict, dict, dict]) -> BacktestResult:
    if _WORKER_DATA_MAP is None:
        raise RuntimeError("Variant worker data not initialized")
    name, buy_params, sell_params, rnn_params = task
    use_portfolio_opt = bool(rnn_params.get("portfolio_opt"))
    if use_portfolio_opt and (SKFOLIO_AVAILABLE or PYPFOPT_AVAILABLE or RISKFOLIO_AVAILABLE):
        return run_portfolio_optimized(
            name,
            _WORKER_DATA_MAP,
            buy_params,
            sell_params,
            rnn_params=rnn_params,
            rnn_models=_WORKER_RNN_MODELS or {},
        )
    return run_variant(
        name,
        _WORKER_DATA_MAP,
        buy_params,
        sell_params,
        rnn_params=rnn_params,
        rnn_models=_WORKER_RNN_MODELS or {},
    )


def _variant_mp_context():
    default_method = "fork" if sys.platform == "darwin" else "spawn"
    method = os.getenv("AT_LAB_MP_START", default_method).strip() or default_method
    try:
        return mp.get_context(method)
    except Exception:
        return mp.get_context(default_method)


@contextlib.contextmanager
def _temporary_env(overrides: dict[str, str | None]):
    old = {k: os.environ.get(k) for k in overrides}
    try:
        for k, v in overrides.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = str(v)
        yield
    finally:
        for k, v in old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def _save_lab_payload(payload: dict, prefix: str = "strategy_lab") -> tuple[Path, Path]:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_json = OUT_DIR / f"{prefix}_{ts}.json"
    out_csv = OUT_DIR / f"{prefix}_{ts}.csv"
    out_json.write_text(json.dumps(payload, indent=2))
    pd.DataFrame(payload.get("ranked", [])).to_csv(out_csv, index=False)
    return out_json, out_csv



def run_variant(name: str, data_map: dict[str, pd.DataFrame], buy_params: dict, sell_params: dict, rnn_params: dict | None = None, rnn_models: dict | None = None) -> BacktestResult:
    # avoid DB dependency in RULE_SET_7 market regime check
    at_utils.get_mmi_now = lambda: None

    # Ensure AT_LAB_MODE is set for lab portfolio relaxation
    lab_mode = os.getenv("AT_LAB_MODE", "1").strip().lower() in {"1", "true", "yes"}
    if not os.getenv("AT_LAB_MODE"):
        os.environ["AT_LAB_MODE"] = "1"

    old_r2 = dict(RULE_SET_2.CONFIG)
    old_r7 = dict(RULE_SET_7.CONFIG)
    RULE_SET_2.CONFIG.update(sell_params)
    RULE_SET_7.CONFIG.update(buy_params)
    # Lab override: if AT_LAB_REGIME_FILTER_ENABLED is set, force regime filter on/off
    lab_regime = os.getenv("AT_LAB_REGIME_FILTER_ENABLED", "").strip()
    if lab_regime:
        RULE_SET_7.CONFIG["regime_filter_enabled"] = int(float(lab_regime))
    rnn_params = rnn_params or {"enabled": False}
    rnn_models = rnn_models or {}
    match_live = os.getenv("AT_LAB_MATCH_LIVE", "1").strip().lower() not in {"0", "false", "no"}

    try:
        if match_live and not rnn_params.get("enabled"):
            from scripts import weekly_universe_cagr_check as parity_pack

            result, _, _ = parity_pack.run_baseline_detailed(data_map)
            result.name = name
            result.params = {
                "buy": buy_params,
                "sell": sell_params,
                "rnn": rnn_params,
                **({"simulation": result.params.get("simulation", {})} if getattr(result, "params", None) else {}),
            }
            result.rnn_enabled = False
            result.rnn_avg_test_accuracy = 0.0
            return result

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
    portfolio_metrics = locals().get("portfolio_metrics", {})
    if not portfolio_metrics and 'symbol_returns' in locals() and symbol_returns:
        try:
            min_len_for_metrics = min(len(v) for v in symbol_returns.values())
            eq_weight_returns = pd.DataFrame({sym: rets[:min_len_for_metrics] for sym, rets in symbol_returns.items()}, dtype=float).mean(axis=1)
            portfolio_metrics = _compute_return_metrics(eq_weight_returns)
        except Exception:
            portfolio_metrics = {}
    selection_score = _score_from_metrics(ret, total_trades, win_rate, worst_dd, portfolio_metrics)
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
        risk_metrics={},
    )


def _simulate_symbol_with_equity(symbol: str, df: pd.DataFrame, rnn_model=None, rnn_cfg: dict | None = None) -> dict:
    """Run simple sim but also return per-bar equity curve for portfolio optimization."""
    cash = 100000.0
    qty = 0
    avg = 0.0
    entry_idx = None
    trades = 0
    wins = 0
    equity_curve = []
    daily_returns = []
    rnn_cfg = rnn_cfg or {"enabled": False}
    rnn_enabled = bool(rnn_cfg.get("enabled")) and rnn_model is not None
    buy_threshold = float(rnn_cfg.get("buy_threshold", 0.56))
    sell_threshold = float(rnn_cfg.get("sell_threshold", 0.44))
    prev_equity = cash

    for i in range(250, len(df)):
        part = df.iloc[: i + 1].copy()
        row = part.iloc[-1].to_dict()
        row.setdefault("instrument_token", 1626369)
        price = float(part.iloc[-1]["Close"])

        if qty == 0:
            hold_df = pd.DataFrame(
                columns=[
                    "instrument_token", "tradingsymbol", "average_price",
                    "quantity", "t1_quantity", "bars_in_trade",
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
        daily_ret = (port - prev_equity) / prev_equity if prev_equity > 0 else 0.0
        daily_returns.append(daily_ret)
        prev_equity = port

    final_val = equity_curve[-1] if equity_curve else 100000.0
    s = pd.Series(equity_curve if equity_curve else [100000.0], dtype=float)
    peak = s.cummax()
    dd = ((s - peak) / peak * 100.0).min()
    return {
        "final_value": float(final_val),
        "trades": int(trades),
        "wins": int(wins),
        "max_drawdown_pct": float(dd),
        "daily_returns": daily_returns,
        "equity_curve": equity_curve,
    }


def run_portfolio_optimized(
    name: str,
    data_map: dict[str, pd.DataFrame],
    buy_params: dict,
    sell_params: dict,
    rnn_params: dict | None = None,
    rnn_models: dict | None = None,
) -> BacktestResult:
    """Run per-symbol sim then optimize portfolio allocation using skfolio/PyPortfolioOpt.

    This addresses the core problem: independent per-symbol sizing ignores correlation.
    skfolio's HRP and MeanRisk optimizers produce better risk-adjusted portfolios.
    """
    at_utils.get_mmi_now = lambda: None
    if not os.getenv("AT_LAB_MODE"):
        os.environ["AT_LAB_MODE"] = "1"

    old_r2 = dict(RULE_SET_2.CONFIG)
    old_r7 = dict(RULE_SET_7.CONFIG)
    RULE_SET_2.CONFIG.update(sell_params)
    RULE_SET_7.CONFIG.update(buy_params)
    lab_regime = os.getenv("AT_LAB_REGIME_FILTER_ENABLED", "").strip()
    if lab_regime:
        RULE_SET_7.CONFIG["regime_filter_enabled"] = int(float(lab_regime))
    rnn_params = rnn_params or {"enabled": False}
    rnn_models = rnn_models or {}

    try:
        with tempfile.TemporaryDirectory(prefix="at_state_") as td:
            _set_temp_state(RULE_SET_2, td)

            # Step 1: Run per-symbol sims and collect daily returns
            symbol_returns: dict[str, list[float]] = {}
            symbol_stats: dict[str, dict] = {}
            total_trades = 0
            total_wins = 0
            worst_dd = 0.0
            tested_symbols: list[str] = []

            for symbol, df in data_map.items():
                stats = _simulate_symbol_with_equity(
                    symbol, df,
                    rnn_model=rnn_models.get(symbol),
                    rnn_cfg=rnn_params,
                )
                symbol_returns[symbol] = stats["daily_returns"]
                symbol_stats[symbol] = stats
                total_trades += stats["trades"]
                total_wins += stats["wins"]
                worst_dd = min(worst_dd, stats["max_drawdown_pct"])
                tested_symbols.append(symbol)

        # Step 2: Build returns DataFrame for portfolio optimization
        min_len = min(len(v) for v in symbol_returns.values()) if symbol_returns else 0
        if min_len < 60:
            # Not enough data for optimization; fall back to average per-symbol equity.
            # Keep capital base at 100k because optimized portfolios are weighted allocations,
            # not N independent 100k accounts.
            total_final = float(np.mean([s["final_value"] for s in symbol_stats.values()])) if symbol_stats else 100000.0
            start_cap = 100000.0
            ret = (total_final / start_cap - 1.0) * 100.0
        else:
            returns_df = pd.DataFrame(
                {sym: rets[:min_len] for sym, rets in symbol_returns.items()},
                dtype=float,
            )

            # Step 3: Optimize portfolio allocation
            opt_result = _optimize_portfolio_weights(returns_df, name, method=str(rnn_params.get("portfolio_method", "auto")))

            # Step 4: Apply optimized weights to the aligned daily return matrix.
            weights = opt_result.get("weights", {})
            fallback_weight = 1.0 / max(1, len(tested_symbols))
            weight_vec = pd.Series({sym: weights.get(sym, fallback_weight) for sym in returns_df.columns}, dtype=float)
            weight_vec = weight_vec / weight_vec.sum() if weight_vec.sum() > 0 else pd.Series(1.0 / len(returns_df.columns), index=returns_df.columns)
            portfolio_returns = returns_df.mul(weight_vec, axis=1).sum(axis=1)
            portfolio_equity = 100000.0 * (1.0 + portfolio_returns).cumprod()
            total_final = float(portfolio_equity.iloc[-1]) if not portfolio_equity.empty else 100000.0
            portfolio_metrics = _compute_return_metrics(portfolio_returns)
            worst_dd = float(portfolio_metrics.get("max_drawdown_pct", worst_dd))
            start_cap = 100000.0
            ret = (total_final / start_cap - 1.0) * 100.0

    finally:
        RULE_SET_2.CONFIG.clear()
        RULE_SET_2.CONFIG.update(old_r2)
        RULE_SET_7.CONFIG.clear()
        RULE_SET_7.CONFIG.update(old_r7)

    start_capital = 100000.0
    total_final = float(total_final) if 'total_final' in dir() else start_capital
    ret = (total_final / start_capital - 1.0) * 100.0
    round_trips = max(1, total_trades // 2)
    win_rate = (total_wins / round_trips) * 100.0 if round_trips > 0 else 0.0
    portfolio_metrics = locals().get("portfolio_metrics", {})
    if not portfolio_metrics and 'symbol_returns' in locals() and symbol_returns:
        try:
            min_len_for_metrics = min(len(v) for v in symbol_returns.values())
            eq_weight_returns = pd.DataFrame({sym: rets[:min_len_for_metrics] for sym, rets in symbol_returns.items()}, dtype=float).mean(axis=1)
            portfolio_metrics = _compute_return_metrics(eq_weight_returns)
        except Exception:
            portfolio_metrics = {}
    selection_score = _score_from_metrics(ret, total_trades, win_rate, worst_dd, portfolio_metrics)

    return BacktestResult(
        name=name,
        final_value=round(float(total_final), 2),
        total_return_pct=round(float(ret), 2),
        trades=int(total_trades),
        win_rate_pct=round(float(win_rate), 2),
        max_drawdown_pct=round(float(worst_dd), 2),
        params={
            "buy": buy_params,
            "sell": sell_params,
            "rnn": rnn_params,
            "portfolio_opt": True,
            "portfolio_method": str(rnn_params.get("portfolio_method", locals().get("opt_result", {}).get("method", "auto"))),
            "portfolio_optimizer_used": locals().get("opt_result", {}).get("method", "none"),
        },
        symbols_tested=tested_symbols,
        selection_score=round(selection_score, 3),
        rnn_enabled=bool(rnn_params.get("enabled")),
        rnn_avg_test_accuracy=0.0,
        risk_metrics=portfolio_metrics or {},
    )


def _optimize_portfolio_weights(returns_df: pd.DataFrame, variant_name: str = "", method: str = "auto") -> dict:
    """Optimize portfolio weights using skfolio HRP or PyPortfolioOpt efficient frontier.

    Returns dict with 'weights' (symbol -> weight), 'method', and 'opt_return'.
    """
    result = {"weights": {}, "method": "none", "opt_return": 0.0}
    symbols = list(returns_df.columns)
    n_symbols = len(symbols)
    if n_symbols < 3:
        result["weights"] = {s: 1.0 / n_symbols for s in symbols}
        result["method"] = "equal_weight_fallback"
        return result

    requested_method = (method or "auto").strip().lower()
    cap = _portfolio_weight_cap()

    if requested_method in {"equal", "equal_weight"}:
        result["weights"] = {s: 1.0 / n_symbols for s in symbols}
        result["method"] = "equal_weight"
        return result

    if requested_method in {"inverse_vol", "inverse_volatility"}:
        vols = returns_df.std()
        inv_vols = 1.0 / vols.replace(0, np.nan)
        raw = (inv_vols / inv_vols.sum()).fillna(1.0 / n_symbols).to_dict()
        result["weights"] = _normalize_weight_map(raw, symbols, cap=cap)
        result["method"] = "inverse_volatility"
        return result

    if RISKFOLIO_AVAILABLE and requested_method in {"auto", "riskfolio_hrp_mv", "riskfolio_hrp", "riskfolio_min_cvar", "riskfolio_cvar_sharpe"}:
        try:
            clean_returns = returns_df.replace([np.inf, -np.inf], np.nan).fillna(0.0)
            if requested_method in {"auto", "riskfolio_hrp_mv", "riskfolio_hrp"}:
                port = rp.HCPortfolio(returns=clean_returns)
                w = port.optimization(model="HRP", codependence="pearson", rm="MV", linkage="single", leaf_order=True)
                raw = w.iloc[:, 0].astype(float).to_dict() if hasattr(w, "iloc") else dict(w)
                result["weights"] = _normalize_weight_map(raw, symbols, cap=cap)
                result["method"] = "riskfolio_hrp_mv"
                return result
            port = rp.Portfolio(returns=clean_returns)
            port.assets_stats(method_mu="hist", method_cov="hist")
            obj = "Sharpe" if requested_method == "riskfolio_cvar_sharpe" else "MinRisk"
            w = port.optimization(model="Classic", rm="CVaR", obj=obj, rf=0, l=0, hist=True)
            raw = w.iloc[:, 0].astype(float).to_dict() if hasattr(w, "iloc") else dict(w)
            result["weights"] = _normalize_weight_map(raw, symbols, cap=cap)
            result["method"] = requested_method
            return result
        except Exception as exc:
            result["riskfolio_error"] = str(exc)[:200]

    # Try skfolio HRP first (handles non-normal returns, no inversion needed)
    if SKFOLIO_AVAILABLE and requested_method in {"auto", "skfolio_hrp"}:
        try:
            hrp = HierarchicalRiskParity(
                risk_measure=RiskMeasure.VARIANCE,
                portfolio_params=dict(name=f"hrp_{variant_name}"),
            )
            hrp.fit(returns_df)
            port = hrp.predict(returns_df)
            weights = {}
            for i, sym in enumerate(symbols):
                weights[sym] = float(hrp.weights_[i]) if hasattr(hrp, 'weights_') else 1.0 / n_symbols
            result["weights"] = _normalize_weight_map(weights, symbols, cap=cap)
            result["method"] = "skfolio_hrp"
            result["opt_return"] = float(port.mean) if hasattr(port, 'mean') else 0.0
            return result
        except Exception:
            pass

    # Fallback: PyPortfolioOpt efficient frontier with shrinkage covariance
    if PYPFOPT_AVAILABLE and requested_method in {"auto", "pypfopt_max_sharpe"}:
        try:
            mu = mean_historical_return(returns_df, compounding=False)
            S = CovarianceShrinkage(returns_df).ledoit_wolf()
            ef = EfficientFrontier(mu, S, weight_bounds=(0.0, 0.15))
            ef.max_sharpe()
            weights = ef.clean_weights()
            result["weights"] = _normalize_weight_map({k: float(v) for k, v in weights.items() if v > 0.001}, symbols, cap=cap)
            result["method"] = "pypfopt_max_sharpe"
            return result
        except Exception:
            pass

    # Last resort: inverse volatility
    vols = returns_df.std()
    inv_vols = 1.0 / vols.replace(0, 1e-10)
    total_iv = inv_vols.sum()
    result["weights"] = _normalize_weight_map({s: float(inv_vols[s] / total_iv) for s in symbols}, symbols, cap=cap)
    result["method"] = "inverse_volatility"
    return result


def run_walk_forward_validation(
    data_map: dict[str, pd.DataFrame],
    buy_params: dict,
    sell_params: dict,
    n_splits: int = 5,
) -> dict:
    """Walk-forward validation: train on expanding window, test on out-of-sample.

    Uses skfolio WalkForward if available, otherwise manual time-series split.
    Returns dict with per-fold results and aggregate metrics.
    """
    at_utils.get_mmi_now = lambda: None
    if not os.getenv("AT_LAB_MODE"):
        os.environ["AT_LAB_MODE"] = "1"

    old_r2 = dict(RULE_SET_2.CONFIG)
    old_r7 = dict(RULE_SET_7.CONFIG)
    RULE_SET_2.CONFIG.update(sell_params)
    RULE_SET_7.CONFIG.update(buy_params)
    lab_regime = os.getenv("AT_LAB_REGIME_FILTER_ENABLED", "").strip()
    if lab_regime:
        RULE_SET_7.CONFIG["regime_filter_enabled"] = int(float(lab_regime))

    fold_results = []
    try:
        with tempfile.TemporaryDirectory(prefix="at_state_") as td:
            _set_temp_state(RULE_SET_2, td)

            # Collect all dates across symbols
            all_dates = set()
            for symbol, df in data_map.items():
                for d in df["Date"]:
                    all_dates.add(pd.to_datetime(d))
            all_dates = sorted(all_dates)
            n_dates = len(all_dates)

            if n_dates < 500:
                return {"error": "insufficient_data", "n_dates": n_dates, "folds": []}

            # Manual walk-forward splits (expanding window)
            fold_size = n_dates // (n_splits + 1)
            for fold_idx in range(n_splits):
                train_end = fold_size * (fold_idx + 2)  # expanding window
                test_start = train_end
                test_end = min(train_end + fold_size, n_dates)

                if test_end <= test_start:
                    continue

                train_end_date = all_dates[train_end - 1]
                test_start_date = all_dates[test_start]
                test_end_date = all_dates[test_end - 1]

                # Keep warmup/training history through test_end, but do not allow
                # signals before test_start. This avoids the old bug where OOS slices
                # lost 250 warmup bars and produced artificial zero-trade folds.
                test_data = {
                    sym: df[pd.to_datetime(df["Date"]) <= test_end_date].copy()
                    for sym, df in data_map.items()
                }
                test_data = {k: v for k, v in test_data.items() if len(v) > 260}

                if not test_data:
                    continue

                from scripts import weekly_universe_cagr_check as parity_pack

                with _temporary_env({
                    "AT_BACKTEST_SIGNAL_START_DATE": str(test_start_date.date()),
                    "AT_BACKTEST_SIGNAL_END_DATE": str(test_end_date.date()),
                    "AT_BACKTEST_STARTING_CAPITAL": os.getenv("AT_BACKTEST_STARTING_CAPITAL", "100000"),
                }):
                    result, details, sim_meta = parity_pack.run_baseline_detailed(test_data)

                total_trades = int(result.trades)
                round_trips = max(1, total_trades // 2)
                ret_pct = float(result.total_return_pct)
                wr = float(result.win_rate_pct)
                worst_dd = float(result.max_drawdown_pct)
                fold_results.append({
                    "fold": fold_idx + 1,
                    "train_end": str(train_end_date.date()),
                    "test_start": str(test_start_date.date()),
                    "test_end": str(test_end_date.date()),
                    "return_pct": round(ret_pct, 2),
                    "trades": total_trades,
                    "win_rate_pct": round(wr, 1),
                    "max_drawdown_pct": round(worst_dd, 2),
                    "symbols_tested": len(getattr(result, "symbols_tested", []) or test_data),
                })
    finally:
        RULE_SET_2.CONFIG.clear()
        RULE_SET_2.CONFIG.update(old_r2)
        RULE_SET_7.CONFIG.clear()
        RULE_SET_7.CONFIG.update(old_r7)

    if not fold_results:
        return {"error": "no_valid_folds", "folds": []}

    oos_returns = [f["return_pct"] for f in fold_results]
    return {
        "n_folds": len(fold_results),
        "mean_oos_return_pct": round(float(np.mean(oos_returns)), 2),
        "std_oos_return_pct": round(float(np.std(oos_returns)), 2),
        "min_oos_return_pct": round(float(np.min(oos_returns)), 2),
        "max_oos_return_pct": round(float(np.max(oos_returns)), 2),
        "positive_folds": sum(1 for r in oos_returns if r > 0),
        "folds": fold_results,
    }


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
    full_variant_list = variants(scorecard_context, tradebook_context)
    batch_offset, batch_limit = configured_variant_batch()
    if batch_limit is None:
        variant_list = full_variant_list[batch_offset:]
    else:
        variant_list = full_variant_list[batch_offset : batch_offset + batch_limit]
    if not variant_list:
        raise RuntimeError(f"No variants selected for batch offset={batch_offset} limit={batch_limit}")

    batch_label = f"offset_{batch_offset}_limit_{batch_limit or 'all'}"
    write_status(
        phase="evaluating_variants",
        message="running strategy variants",
        variants_total=len(variant_list),
        variants_total_full=len(full_variant_list),
        variant_batch_offset=batch_offset,
        variant_batch_limit=batch_limit,
        variant_batch_label=batch_label,
        rnn_models_built=len(rnn_models),
    )
    results = []
    parallel_enabled = os.getenv("AT_LAB_PARALLEL_VARIANTS", "1").strip().lower() not in {"0", "false", "no"}
    can_parallelize = parallel_enabled and not bool(rnn_config.enabled)
    variant_workers = min(configured_variant_workers(), max(1, len(variant_list))) if can_parallelize else 1
    write_status(
        phase="evaluating_variants",
        message="running strategy variants",
        variants_total=len(variant_list),
        variants_total_full=len(full_variant_list),
        variant_batch_offset=batch_offset,
        variant_batch_limit=batch_limit,
        variant_batch_label=batch_label,
        rnn_models_built=len(rnn_models),
        parallel_variants=bool(can_parallelize and variant_workers > 1),
        variant_workers=variant_workers,
    )

    if can_parallelize and variant_workers > 1:
        ctx = _variant_mp_context()
        with ProcessPoolExecutor(
            max_workers=variant_workers,
            mp_context=ctx,
            initializer=_variant_worker_init,
            initargs=(data_map, rnn_models),
        ) as executor:
            future_map = {
                executor.submit(_run_variant_worker, task): task[0]
                for task in variant_list
            }
            for idx, future in enumerate(as_completed(future_map), start=1):
                name = future_map[future]
                write_status(
                    phase="evaluating_variants",
                    current_variant=name,
                    variants_done=idx - 1,
                    variants_total=len(variant_list),
                    variants_total_full=len(full_variant_list),
                    variant_batch_offset=batch_offset,
                    variant_batch_limit=batch_limit,
                    variant_batch_label=batch_label,
                    progress_pct=round(((idx - 1) / max(1, len(variant_list))) * 100.0, 1),
                    parallel_variants=True,
                    variant_workers=variant_workers,
                )
                results.append(future.result())
    else:
        for idx, (name, b, s, rnn_params) in enumerate(variant_list, start=1):
            write_status(
                phase="evaluating_variants",
                current_variant=name,
                variants_done=idx - 1,
                variants_total=len(variant_list),
                variants_total_full=len(full_variant_list),
                variant_batch_offset=batch_offset,
                variant_batch_limit=batch_limit,
                variant_batch_label=batch_label,
                progress_pct=round(((idx - 1) / max(1, len(variant_list))) * 100.0, 1),
                parallel_variants=False,
                variant_workers=variant_workers,
            )
            use_portfolio_opt = bool(rnn_params.get("portfolio_opt"))
            if use_portfolio_opt and (SKFOLIO_AVAILABLE or PYPFOPT_AVAILABLE or RISKFOLIO_AVAILABLE):
                results.append(run_portfolio_optimized(name, data_map, b, s, rnn_params=rnn_params, rnn_models=rnn_models))
            else:
                results.append(run_variant(name, data_map, b, s, rnn_params=rnn_params, rnn_models=rnn_models))

    rank = sorted(
        results,
        key=lambda r: (r.selection_score, r.total_return_pct, -abs(r.max_drawdown_pct), r.win_rate_pct),
        reverse=True,
    )
    baseline = next((r for r in rank if r.name == "baseline_current"), None)
    if baseline is None:
        baseline = run_variant("baseline_current", data_map, {}, {}, rnn_params={"enabled": False}, rnn_models=rnn_models)
        rank.append(baseline)
        rank = sorted(
            rank,
            key=lambda r: (r.selection_score, r.total_return_pct, -abs(r.max_drawdown_pct), r.win_rate_pct),
            reverse=True,
        )
    best = rank[0]

    min_promote_return_gain = float(os.getenv("AT_LAB_MIN_PROMOTE_RETURN_GAIN", "1.0") or 1.0)
    min_promote_score_gain = float(os.getenv("AT_LAB_MIN_PROMOTE_SCORE_GAIN", "0.5") or 0.5)
    min_promote_total_return = float(os.getenv("AT_LAB_MIN_PROMOTE_TOTAL_RETURN", "8.0") or 8.0)
    min_promote_trades = int(os.getenv("AT_LAB_MIN_PROMOTE_TRADES", "5") or 5)
    max_promote_drawdown_slack = float(os.getenv("AT_LAB_MAX_PROMOTE_DRAWDOWN_SLACK", "1.0") or 1.0)

    recommendation = {
        "generated_at": datetime.now().isoformat(),
        "production_rule_model": "BUY=RULE_SET_7, SELL=RULE_SET_2",
        "batch": {
            "offset": batch_offset,
            "limit": batch_limit,
            "tested_variants": len(rank),
            "full_variant_count": len(full_variant_list),
            "label": batch_label,
        },
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
        "promotion_guardrails": {
            "min_return_gain": min_promote_return_gain,
            "min_score_gain": min_promote_score_gain,
            "min_total_return_pct": min_promote_total_return,
            "min_trades": min_promote_trades,
            "max_drawdown_slack_pct": max_promote_drawdown_slack,
        },
        "should_promote": bool(
            best.name != baseline.name
            and best.total_return_pct >= min_promote_total_return
            and best.total_return_pct > baseline.total_return_pct + min_promote_return_gain
            and best.selection_score > baseline.selection_score + min_promote_score_gain
            and best.trades >= min_promote_trades
            and abs(best.max_drawdown_pct) <= abs(baseline.max_drawdown_pct) + max_promote_drawdown_slack
        ),
    }

    # Walk-forward validation on best variant
    wf_result = None
    wf_enabled = os.getenv("AT_LAB_WALK_FORWARD", "1").strip().lower() not in {"0", "false", "no"}
    if wf_enabled and best.name != baseline.name:
        write_status(
            phase="walk_forward_validation",
            message=f"running walk-forward validation on {best.name}",
            current_variant=best.name,
        )
        try:
            best_buy = best.params.get("buy", {})
            best_sell = best.params.get("sell", {})
            wf_result = run_walk_forward_validation(
                data_map, best_buy, best_sell, n_splits=5,
            )
            recommendation["walk_forward"] = wf_result
            # If walk-forward is negative, override promotion
            if wf_result.get("mean_oos_return_pct", 0) <= 0:
                recommendation["should_promote"] = False
                recommendation["walk_forward_override"] = "mean_oos_return_non_positive"
        except Exception as exc:
            recommendation["walk_forward"] = {"error": str(exc)}

    payload = {
        "recommendation": recommendation,
        "ranked": [asdict(r) for r in rank],
    }

    prefix = "strategy_lab" if batch_offset == 0 and batch_limit is None else f"strategy_lab_batch_{batch_offset}_{batch_limit or 'all'}"
    out_json, out_csv = _save_lab_payload(payload, prefix=prefix)

    write_status(
        status="done",
        phase="completed",
        progress_pct=100.0,
        current_variant=None,
        variants_done=len(variant_list),
        variants_total=len(variant_list),
        variants_total_full=len(full_variant_list),
        variant_batch_offset=batch_offset,
        variant_batch_limit=batch_limit,
        variant_batch_label=batch_label,
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


# --- Process cleanup guard ---
# Prevent orphan multiprocessing workers from accumulating and eating RAM.
# When the parent process exits (normally or via signal), kill all child processes
# that were spawned by ProcessPoolExecutor.
_children: set[int] = set()
_original_sigint = signal.getsignal(signal.SIGINT)
_original_sigterm = signal.getsignal(signal.SIGTERM)


def _register_child(pid: int) -> None:
    _children.add(pid)


def _cleanup_children() -> None:
    """Terminate orphan child processes on exit."""
    for pid in _children:
        try:
            os.kill(pid, signal.SIGTERM)
        except OSError:
            pass
    # Give them a moment, then force-kill
    if _children:
        import time
        time.sleep(0.5)
        for pid in list(_children):
            try:
                os.kill(pid, signal.SIGKILL)
            except OSError:
                pass
    _children.clear()


def _signal_handler(signum: int, frame) -> None:
    """Handle SIGTERM/SIGINT by cleaning up children before exiting."""
    _cleanup_children()
    # Restore original handler and re-raise
    signal.signal(signum, _original_sigint if signum == signal.SIGINT else _original_sigterm)
    os.kill(os.getpid(), signum)


# Signal handlers are registered only in the __main__ guard below. Registering
# them at import time breaks multiprocessing spawn workers on Python 3.10+.

# Patch ProcessPoolExecutor to track child PIDs
_OriginalProcessPoolExecutor = ProcessPoolExecutor


class _TrackedProcessPoolExecutor(_OriginalProcessPoolExecutor):
    """ProcessPoolExecutor that registers child PIDs for cleanup on exit."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # PIDs become available after the executor starts
        self._registered = False

    def _register_workers(self):
        if self._registered:
            return
        try:
            for p in self._processes.values():
                _register_child(p.pid)
            self._registered = True
        except Exception:
            pass

    def submit(self, *args, **kwargs):
        fut = super().submit(*args, **kwargs)
        self._register_workers()
        return fut


ProcessPoolExecutor = _TrackedProcessPoolExecutor
# --- End process cleanup guard ---

if __name__ == "__main__":
    atexit.register(_cleanup_children)
    signal.signal(signal.SIGTERM, _signal_handler)
    signal.signal(signal.SIGINT, _signal_handler)
    try:
        main()
    except Exception as exc:
        write_status(status="failed", phase="failed", error=str(exc))
        raise
