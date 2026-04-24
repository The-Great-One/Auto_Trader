from __future__ import annotations

import datetime as dt
from functools import lru_cache
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

SCHEME_LIST_URL = "https://api.mfapi.in/mf"
NAV_URL_TEMPLATE = "https://api.mfapi.in/mf/{scheme_code}"
NAV_CACHE_DIR = Path(__file__).resolve().parent / ".nav_cache"
NAV_DISK_CACHE_MAX_AGE_HOURS = 24


def _nav_cache_path(scheme_code: int) -> Path:
    return NAV_CACHE_DIR / f"{int(scheme_code)}.csv"


def _load_nav_from_disk_cache(scheme_code: int, max_age_hours: int = NAV_DISK_CACHE_MAX_AGE_HOURS) -> Optional[pd.DataFrame]:
    path = _nav_cache_path(scheme_code)
    if not path.exists():
        return None
    try:
        age_sec = dt.datetime.now().timestamp() - path.stat().st_mtime
        if age_sec > max_age_hours * 3600:
            return None
        df = pd.read_csv(path, parse_dates=["date"])
        if "date" not in df.columns or "nav" not in df.columns:
            return None
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df["nav"] = pd.to_numeric(df["nav"], errors="coerce")
        df = df.dropna(subset=["date", "nav"]).sort_values("date").reset_index(drop=True)
        if df.empty:
            return None
        return df[["date", "nav"]]
    except Exception:
        return None


def _save_nav_to_disk_cache(scheme_code: int, nav_df: pd.DataFrame) -> None:
    if nav_df is None or nav_df.empty:
        return
    try:
        NAV_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        path = _nav_cache_path(scheme_code)
        tmp = path.with_suffix(".tmp")
        nav_df[["date", "nav"]].to_csv(tmp, index=False)
        tmp.replace(path)
    except Exception:
        pass


@lru_cache(maxsize=1)
def fetch_scheme_list() -> pd.DataFrame:
    r = requests.get(SCHEME_LIST_URL, timeout=30)
    r.raise_for_status()
    df = pd.DataFrame(r.json()).rename(columns={"schemeCode": "scheme_code", "schemeName": "scheme_name"})
    df["scheme_code"] = pd.to_numeric(df["scheme_code"], errors="coerce")
    df = df.dropna(subset=["scheme_code", "scheme_name"]).reset_index(drop=True)
    df["scheme_code"] = df["scheme_code"].astype(int)
    df["scheme_name_lc"] = df["scheme_name"].str.lower()
    return df


@lru_cache(maxsize=256)
def fetch_nav_history(scheme_code: int) -> pd.DataFrame:
    cached = _load_nav_from_disk_cache(scheme_code)
    if cached is not None:
        return cached

    r = requests.get(NAV_URL_TEMPLATE.format(scheme_code=scheme_code), timeout=30)
    r.raise_for_status()
    payload = r.json()
    df = pd.DataFrame(payload["data"])
    df["date"] = pd.to_datetime(df["date"], format="%d-%m-%Y", errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    df["nav"] = pd.to_numeric(df["nav"], errors="coerce")
    df = df.dropna(subset=["nav"]).reset_index(drop=True)
    out = df[["date", "nav"]]
    _save_nav_to_disk_cache(scheme_code, out)
    return out


def normalize_nav(nav_df: pd.DataFrame, base: float = 10.0) -> pd.DataFrame:
    if nav_df.empty:
        return nav_df.copy()
    start = float(nav_df["nav"].iloc[0])
    if start <= 0:
        return nav_df.copy()
    out = nav_df.copy()
    out["nav_norm"] = out["nav"] * (base / start)
    return out


def filter_nav_timeframe(nav_df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    if nav_df.empty or timeframe == "Full":
        return nav_df.copy()
    last_date = pd.Timestamp(nav_df["date"].iloc[-1])
    start_map = {
        "YTD": pd.Timestamp(year=last_date.year, month=1, day=1),
        "1Y": last_date - pd.DateOffset(years=1),
        "3Y": last_date - pd.DateOffset(years=3),
        "5Y": last_date - pd.DateOffset(years=5),
        "10Y": last_date - pd.DateOffset(years=10),
    }
    cutoff = start_map.get(timeframe)
    if cutoff is None:
        return nav_df.copy()
    return nav_df[nav_df["date"] >= cutoff].reset_index(drop=True)
