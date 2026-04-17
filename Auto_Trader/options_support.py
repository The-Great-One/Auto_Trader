from __future__ import annotations

import json
import os
import re
from pathlib import Path

import numpy as np
import pandas as pd

from . import utils as at_utils

ROOT = Path(__file__).resolve().parents[1]
HIST_DIR = ROOT / "intermediary_files" / "Hist_Data"
OPTIONS_MANIFEST = ROOT / "intermediary_files" / "options" / "nifty_options_universe.json"
OPTION_SYMBOL_RE = re.compile(r"^[A-Z0-9]+\d+(CE|PE)$")


def parse_symbol_list(value: str) -> list[str]:
    return [x.strip().upper() for x in str(value or "").split(",") if x.strip()]



def option_side(symbol: str) -> str:
    text = str(symbol or "").upper()
    if text.endswith("CE"):
        return "CE"
    if text.endswith("PE"):
        return "PE"
    return ""



def looks_like_option_symbol(symbol: str) -> bool:
    return bool(OPTION_SYMBOL_RE.match(str(symbol or "").upper()))



def _normalize_date_series(series: pd.Series) -> pd.Series:
    dt = pd.to_datetime(series, errors="coerce")
    try:
        if getattr(dt.dt, "tz", None) is not None:
            dt = dt.dt.tz_convert(None)
    except Exception:
        try:
            dt = dt.dt.tz_localize(None)
        except Exception:
            pass
    try:
        dt = dt.astype("datetime64[ns]")
    except Exception:
        pass
    return dt



def normalize_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    if hasattr(df.columns, "levels"):
        df.columns = [str(c[0]) for c in df.columns]
    df = df.reset_index(drop=False)
    cmap = {str(c).lower(): c for c in df.columns}
    out = pd.DataFrame(
        {
            "Date": _normalize_date_series(df[cmap.get("date", "Date")]),
            "Open": pd.to_numeric(df[cmap.get("open", "Open")], errors="coerce"),
            "High": pd.to_numeric(df[cmap.get("high", "High")], errors="coerce"),
            "Low": pd.to_numeric(df[cmap.get("low", "Low")], errors="coerce"),
            "Close": pd.to_numeric(df[cmap.get("close", "Close")], errors="coerce"),
            "Volume": pd.to_numeric(df.get(cmap.get("volume", "Volume"), 0), errors="coerce").fillna(0),
        }
    )
    if "oi" in cmap:
        out["OI"] = pd.to_numeric(df[cmap["oi"]], errors="coerce").fillna(0)
    for extra in ["tradingsymbol", "underlying", "expiry", "strike", "option_type", "lot_size"]:
        key = extra.lower()
        if key in cmap:
            out[extra] = df[cmap[key]]
    return out.dropna(subset=["Date", "Open", "High", "Low", "Close"]).sort_values("Date").drop_duplicates(subset=["Date"]).reset_index(drop=True)



def load_manifest() -> dict:
    try:
        if OPTIONS_MANIFEST.exists():
            return json.loads(OPTIONS_MANIFEST.read_text())
    except Exception:
        pass
    return {}



def load_manifest_symbols() -> list[str]:
    payload = load_manifest()
    contracts = payload.get("contracts") or []
    return [str(c.get("tradingsymbol") or "").upper() for c in contracts if c.get("tradingsymbol")]



def discover_option_symbols() -> list[str]:
    explicit = os.getenv("AT_OPTIONS_LAB_SYMBOLS", "").strip()
    if explicit:
        return parse_symbol_list(explicit)

    manifest_symbols = load_manifest_symbols()
    if manifest_symbols:
        return manifest_symbols

    underlyings = parse_symbol_list(os.getenv("AT_OPTIONS_LAB_UNDERLYINGS", "NIFTY"))
    side_filter = os.getenv("AT_OPTIONS_LAB_SIDE", "BOTH").strip().upper()
    max_symbols = max(1, int(os.getenv("AT_OPTIONS_LAB_MAX_SYMBOLS", "12")))

    if not HIST_DIR.exists():
        return []

    candidates = []
    for path in sorted(HIST_DIR.glob("*.feather")):
        symbol = path.stem.upper()
        if not looks_like_option_symbol(symbol):
            continue
        if underlyings and not any(symbol.startswith(u) for u in underlyings):
            continue
        side = option_side(symbol)
        if side_filter in {"CE", "PE"} and side != side_filter:
            continue
        candidates.append(symbol)
    return candidates[:max_symbols]



def load_underlying_context(underlying_symbol: str = "NIFTY50_INDEX") -> pd.DataFrame | None:
    path = HIST_DIR / f"{underlying_symbol}.feather"
    if not path.exists():
        return None
    df = normalize_ohlcv(pd.read_feather(path))
    if df.empty:
        return None
    df = at_utils.Indicators(df)
    df = df.ffill().dropna(subset=["Close"]).reset_index(drop=True)
    keep = pd.DataFrame(
        {
            "Date": _normalize_date_series(df["Date"]),
            "UL_Close": pd.to_numeric(df["Close"], errors="coerce"),
            "UL_EMA10": pd.to_numeric(df.get("EMA10"), errors="coerce"),
            "UL_EMA20": pd.to_numeric(df.get("EMA20"), errors="coerce"),
            "UL_EMA50": pd.to_numeric(df.get("EMA50"), errors="coerce"),
            "UL_RSI": pd.to_numeric(df.get("RSI"), errors="coerce"),
            "UL_MACD_Hist": pd.to_numeric(df.get("MACD_Hist"), errors="coerce"),
            "UL_ADX": pd.to_numeric(df.get("ADX"), errors="coerce"),
            "UL_Supertrend_Direction": df.get("Supertrend_Direction"),
        }
    )
    return keep.dropna(subset=["Date"]).sort_values("Date").reset_index(drop=True)



def enrich_option_frame(raw_df: pd.DataFrame, underlying_symbol: str = "NIFTY50_INDEX") -> pd.DataFrame:
    df = normalize_ohlcv(raw_df)
    if df.empty:
        return df

    if "tradingsymbol" not in df.columns:
        df["tradingsymbol"] = ""
    if "option_type" not in df.columns:
        df["option_type"] = df["tradingsymbol"].astype(str).str.upper().str[-2:]
    if "lot_size" not in df.columns:
        df["lot_size"] = 1

    if "OI" not in df.columns:
        df["OI"] = 0.0
    df["OI"] = pd.to_numeric(df["OI"], errors="coerce").fillna(0.0)
    df["OI_SMA5"] = df["OI"].rolling(5, min_periods=3).mean()
    df["OI_Change"] = df["OI"].diff().fillna(0.0)
    prev_oi = df["OI"].shift(1).replace(0, np.nan)
    df["OI_PctChange"] = ((df["OI"] - prev_oi) / prev_oi * 100.0).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    oi_mean = df["OI"].rolling(5, min_periods=3).mean()
    oi_std = df["OI"].rolling(5, min_periods=3).std(ddof=1).replace(0, np.nan)
    df["OI_ZScore5"] = ((df["OI"] - oi_mean) / oi_std).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    df["RangePct"] = ((df["High"] - df["Low"]) / df["Close"].replace(0, np.nan)).replace([np.inf, -np.inf], np.nan).fillna(0.0)

    df = at_utils.Indicators(df)
    df = df.ffill().dropna(subset=["Close"]).reset_index(drop=True)

    ul = load_underlying_context(underlying_symbol)
    if ul is not None and not ul.empty:
        df = pd.merge_asof(
            df.sort_values("Date"),
            ul.sort_values("Date"),
            on="Date",
            direction="backward",
        )

    return df.ffill().reset_index(drop=True)
