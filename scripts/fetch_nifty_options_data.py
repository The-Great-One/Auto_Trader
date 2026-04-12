#!/usr/bin/env python3
"""
Fetch research data for NIFTY index options.

What it stores:
- selected NIFTY option contract OHLCV + OI feather files in intermediary_files/Hist_Data
- NIFTY underlying index OHLCV feather file for context and future signal work
- a manifest json describing the selected contracts, strikes, expiries, and spot used

This is research plumbing for the options lab. It does not enable live options trading.
"""

from __future__ import annotations

import json
import math
import os
import sys
import time
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable

import pandas as pd
import yfinance as yf
from dateutil.relativedelta import relativedelta
from kiteconnect import KiteConnect
from kiteconnect.exceptions import NetworkException

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from Auto_Trader.FetchPricesKite import INTERVAL_LIMITS
from Auto_Trader.my_secrets import API_KEY
from Auto_Trader.utils import read_session_data

HIST_DIR = ROOT / "intermediary_files" / "Hist_Data"
OPTIONS_DIR = ROOT / "intermediary_files" / "options"
MANIFEST_PATH = OPTIONS_DIR / "nifty_options_universe.json"
NFO_CACHE_PATH = ROOT / "intermediary_files" / "nfo_instruments_cache.json"

DEFAULT_INTERVAL = os.getenv("AT_OPTIONS_FETCH_INTERVAL", "day").strip().lower()
if DEFAULT_INTERVAL not in INTERVAL_LIMITS:
    DEFAULT_INTERVAL = "day"


@dataclass
class OptionContract:
    tradingsymbol: str
    instrument_token: int
    exchange: str
    segment: str
    expiry: str
    strike: float
    option_type: str
    lot_size: int
    tick_size: float



def _ensure_dirs():
    HIST_DIR.mkdir(parents=True, exist_ok=True)
    OPTIONS_DIR.mkdir(parents=True, exist_ok=True)



def _load_json(path: Path, default):
    try:
        if path.exists():
            return json.loads(path.read_text())
    except Exception:
        pass
    return default



def _save_json(path: Path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")



def _chunk_date_range(start_dt, end_dt, max_days):
    cursor = start_dt
    while cursor < end_dt:
        chunk_end = min(cursor + relativedelta(days=max_days), end_dt)
        yield cursor, chunk_end
        cursor = chunk_end



def _interval_to_timedelta(interval: str) -> timedelta:
    if interval == "day":
        return timedelta(days=1)
    if interval == "minute":
        return timedelta(minutes=1)
    if interval.endswith("minute"):
        return timedelta(minutes=int(interval.replace("minute", "")))
    return timedelta(days=1)



def _is_intraday_interval(interval: str) -> bool:
    return interval != "day"



def _normalize_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    if hasattr(df.columns, "levels"):
        df.columns = [str(c[0]) for c in df.columns]
    df = df.reset_index(drop=False)
    cmap = {str(c).lower(): c for c in df.columns}
    out = pd.DataFrame(
        {
            "Date": pd.to_datetime(df[cmap.get("date", "Date")], errors="coerce"),
            "Open": pd.to_numeric(df[cmap.get("open", "Open")], errors="coerce"),
            "High": pd.to_numeric(df[cmap.get("high", "High")], errors="coerce"),
            "Low": pd.to_numeric(df[cmap.get("low", "Low")], errors="coerce"),
            "Close": pd.to_numeric(df[cmap.get("close", "Close")], errors="coerce"),
            "Volume": pd.to_numeric(df.get(cmap.get("volume", "Volume"), 0), errors="coerce").fillna(0),
        }
    )
    if cmap.get("oi") or "oi" in cmap:
        out["OI"] = pd.to_numeric(df.get(cmap.get("oi", "oi"), 0), errors="coerce").fillna(0)
    out = out.dropna(subset=["Date", "Open", "High", "Low", "Close"]).sort_values("Date")
    out = out.drop_duplicates(subset=["Date"]).reset_index(drop=True)
    return out



def _get_kite() -> KiteConnect:
    kite = KiteConnect(api_key=API_KEY)
    kite.set_access_token(read_session_data())
    return kite



def _load_nfo_instruments(kite: KiteConnect, refresh: bool = False) -> pd.DataFrame:
    if not refresh and NFO_CACHE_PATH.exists():
        cached = _load_json(NFO_CACHE_PATH, {})
        rows = cached.get("rows") or []
        if rows:
            return pd.DataFrame(rows)

    try:
        instruments = kite.instruments("NFO")
    except Exception:
        instruments = kite.instruments()

    df = pd.DataFrame(instruments)
    if df.empty:
        raise RuntimeError("No instruments returned from Kite for NFO")

    keep = [
        "instrument_token",
        "exchange_token",
        "tradingsymbol",
        "name",
        "last_price",
        "expiry",
        "strike",
        "tick_size",
        "lot_size",
        "instrument_type",
        "segment",
        "exchange",
    ]
    df = df[[c for c in keep if c in df.columns]].copy()
    if "expiry" in df.columns:
        df["expiry"] = pd.to_datetime(df["expiry"], errors="coerce")
    if "strike" in df.columns:
        df["strike"] = pd.to_numeric(df["strike"], errors="coerce")
    payload = {
        "generated_at": datetime.now().isoformat(),
        "rows": df.to_dict(orient="records"),
    }
    _save_json(NFO_CACHE_PATH, payload)
    return df



def _get_nifty_spot(kite: KiteConnect) -> float:
    override = os.getenv("AT_NIFTY_SPOT_OVERRIDE", "").strip()
    if override:
        return float(override)

    try:
        ltp = kite.ltp(["NSE:NIFTY 50"])
        if isinstance(ltp, dict) and ltp.get("NSE:NIFTY 50"):
            value = float(ltp["NSE:NIFTY 50"].get("last_price") or 0)
            if value > 0:
                return value
    except Exception:
        pass

    df = yf.download("^NSEI", period="5d", interval="1d", auto_adjust=False, progress=False)
    if df is None or df.empty:
        raise RuntimeError("Unable to resolve NIFTY spot via Kite or yfinance")
    if hasattr(df.columns, "levels"):
        df.columns = [str(c[0]) for c in df.columns]
    close_col = next((c for c in df.columns if str(c).lower() == "close"), None)
    value = float(df[close_col].dropna().iloc[-1])
    if value <= 0:
        raise RuntimeError("Resolved non-positive NIFTY spot")
    return value



def _pick_strike_step(strikes: Iterable[float]) -> int:
    ordered = sorted({int(round(float(s))) for s in strikes if float(s) > 0})
    diffs = [b - a for a, b in zip(ordered, ordered[1:]) if (b - a) > 0]
    if not diffs:
        return 50
    return int(min(diffs))



def _select_nifty_contracts(df: pd.DataFrame, spot: float) -> list[OptionContract]:
    side_filter = os.getenv("AT_NIFTY_OPTIONS_SIDE", "BOTH").strip().upper()
    expiry_count = max(1, int(os.getenv("AT_NIFTY_OPTIONS_EXPIRY_COUNT", "2")))
    strikes_each_side = max(0, int(os.getenv("AT_NIFTY_OPTIONS_STRIKES_EACH_SIDE", "3")))

    nifty = df[
        (df["name"].astype(str).str.upper() == "NIFTY")
        & (df["instrument_type"].astype(str).str.upper().isin(["CE", "PE"]))
        & (df["exchange"].astype(str).str.upper() == "NFO")
    ].copy()
    if nifty.empty:
        raise RuntimeError("No NIFTY option instruments found in NFO master")

    nifty = nifty[nifty["expiry"].notna() & (nifty["expiry"].dt.date >= date.today())]
    if nifty.empty:
        raise RuntimeError("No current/future NIFTY option expiries found")

    expiries = sorted(nifty["expiry"].dt.date.unique())[:expiry_count]
    selected: list[OptionContract] = []

    for expiry in expiries:
        exp_df = nifty[nifty["expiry"].dt.date == expiry].copy()
        strikes = sorted(exp_df["strike"].dropna().unique())
        if not strikes:
            continue
        step = _pick_strike_step(strikes)
        atm = min(strikes, key=lambda x: abs(float(x) - float(spot)))
        atm = int(round(float(atm) / step) * step)
        target_strikes = {atm + (offset * step) for offset in range(-strikes_each_side, strikes_each_side + 1)}

        filtered = exp_df[exp_df["strike"].round().astype(int).isin({int(x) for x in target_strikes})].copy()
        if side_filter in {"CE", "PE"}:
            filtered = filtered[filtered["instrument_type"].astype(str).str.upper() == side_filter]

        filtered = filtered.sort_values(["strike", "instrument_type"])
        for _, row in filtered.iterrows():
            selected.append(
                OptionContract(
                    tradingsymbol=str(row["tradingsymbol"]).upper(),
                    instrument_token=int(row["instrument_token"]),
                    exchange=str(row["exchange"]).upper(),
                    segment=str(row.get("segment") or ""),
                    expiry=str(pd.Timestamp(row["expiry"]).date()),
                    strike=float(row["strike"]),
                    option_type=str(row["instrument_type"]).upper(),
                    lot_size=int(row.get("lot_size") or 0),
                    tick_size=float(row.get("tick_size") or 0.05),
                )
            )

    if not selected:
        raise RuntimeError("No NIFTY option contracts selected after expiry/strike filtering")
    return selected



def _history_start(interval: str) -> datetime | date:
    if _is_intraday_interval(interval):
        lookback_days = max(5, int(os.getenv("AT_NIFTY_OPTIONS_INTRADAY_LOOKBACK_DAYS", "45")))
        return datetime.now() - timedelta(days=lookback_days)
    years = max(1, int(os.getenv("AT_NIFTY_OPTIONS_DAILY_LOOKBACK_YEARS", "2")))
    return date.today() - relativedelta(years=years)



def _fetch_contract_history(kite: KiteConnect, contract: OptionContract, interval: str) -> tuple[bool, str, int]:
    feather_path = HIST_DIR / f"{contract.tradingsymbol}.feather"
    start_date = _history_start(interval)

    if feather_path.exists():
        try:
            existing = pd.read_feather(feather_path)
            existing = _normalize_ohlcv(existing)
            if not existing.empty:
                last_ts = pd.to_datetime(existing["Date"], errors="coerce").max()
                if pd.notna(last_ts):
                    start_date = last_ts.to_pydatetime() + _interval_to_timedelta(interval) if _is_intraday_interval(interval) else last_ts.date() + timedelta(days=1)
        except Exception:
            pass

    end_date = datetime.now() if _is_intraday_interval(interval) else date.today()
    if start_date >= end_date:
        return True, contract.tradingsymbol, 0

    frames = []
    for from_date, to_date in _chunk_date_range(start_date, end_date, INTERVAL_LIMITS[interval]):
        success = False
        for attempt in range(3):
            try:
                data = kite.historical_data(
                    contract.instrument_token,
                    from_date=from_date,
                    to_date=to_date,
                    interval=interval,
                    oi=True,
                )
                frames.append(pd.DataFrame(data))
                success = True
                break
            except NetworkException:
                time.sleep(2.0 * (attempt + 1))
            except Exception as exc:
                return False, f"{contract.tradingsymbol}:fetch_failed:{exc}", 0
        if not success:
            return False, f"{contract.tradingsymbol}:rate_limit_retries_exhausted", 0
        time.sleep(0.35)

    if not frames:
        return True, contract.tradingsymbol, 0

    try:
        df = pd.concat(frames, ignore_index=True)
        df = _normalize_ohlcv(df)
        df["tradingsymbol"] = contract.tradingsymbol
        df["underlying"] = "NIFTY"
        df["expiry"] = contract.expiry
        df["strike"] = contract.strike
        df["option_type"] = contract.option_type
        df["lot_size"] = contract.lot_size

        if feather_path.exists():
            try:
                existing = _normalize_ohlcv(pd.read_feather(feather_path))
                for col in ["tradingsymbol", "underlying", "expiry", "strike", "option_type", "lot_size"]:
                    if col in df.columns and col not in existing.columns:
                        existing[col] = df[col].iloc[0]
                df = pd.concat([existing, df], ignore_index=True)
                df = df.drop_duplicates(subset=["Date"], keep="last").sort_values("Date").reset_index(drop=True)
            except Exception:
                pass

        df.to_feather(feather_path)
        return True, contract.tradingsymbol, len(df)
    except Exception as exc:
        return False, f"{contract.tradingsymbol}:save_failed:{exc}", 0



def _fetch_underlying_context(interval: str) -> dict:
    path = HIST_DIR / "NIFTY50_INDEX.feather"
    period = "2y" if interval == "day" else "60d"
    yf_interval = "1d" if interval == "day" else "60m"
    df = yf.download("^NSEI", period=period, interval=yf_interval, auto_adjust=False, progress=False)
    if df is None or df.empty:
        return {"ok": False, "reason": "yfinance_empty", "path": str(path)}
    norm = _normalize_ohlcv(df)
    norm["tradingsymbol"] = "NIFTY50_INDEX"
    norm.to_feather(path)
    return {
        "ok": True,
        "path": str(path),
        "rows": int(len(norm)),
        "last_close": float(norm["Close"].iloc[-1]),
        "interval": interval,
    }



def main():
    _ensure_dirs()
    interval = DEFAULT_INTERVAL
    kite = _get_kite()
    instruments = _load_nfo_instruments(kite, refresh=os.getenv("AT_REFRESH_NFO_CACHE", "0") == "1")
    spot = _get_nifty_spot(kite)
    contracts = _select_nifty_contracts(instruments, spot)

    fetched = []
    failed = []
    for contract in contracts:
        ok, name_or_error, rows = _fetch_contract_history(kite, contract, interval)
        if ok:
            fetched.append({"tradingsymbol": name_or_error, "rows": rows})
        else:
            failed.append(name_or_error)

    underlying = _fetch_underlying_context(interval)
    manifest = {
        "generated_at": datetime.now().isoformat(),
        "purpose": "research_only",
        "underlying": "NIFTY",
        "spot_used": round(float(spot), 2),
        "interval": interval,
        "expiry_count": max(1, int(os.getenv("AT_NIFTY_OPTIONS_EXPIRY_COUNT", "2"))),
        "strikes_each_side": max(0, int(os.getenv("AT_NIFTY_OPTIONS_STRIKES_EACH_SIDE", "3"))),
        "side_filter": os.getenv("AT_NIFTY_OPTIONS_SIDE", "BOTH").strip().upper(),
        "contracts": [asdict(c) for c in contracts],
        "fetched": fetched,
        "failed": failed,
        "underlying_context": underlying,
    }
    _save_json(MANIFEST_PATH, manifest)

    print(json.dumps(
        {
            "manifest": str(MANIFEST_PATH),
            "interval": interval,
            "spot_used": manifest["spot_used"],
            "contracts_selected": len(contracts),
            "contracts_fetched": len(fetched),
            "contracts_failed": failed,
            "underlying_context": underlying,
        },
        indent=2,
    ))


if __name__ == "__main__":
    main()
