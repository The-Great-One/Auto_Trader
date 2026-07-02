#!/usr/bin/env python3
"""Kite WebSocket fallback publisher for RSI Momentum paper-ledger MTM.

This script is intentionally *not* a replacement for ``wednesday.py``.
It is a safety net:

1. Stay dormant while ``wednesday.py`` / ``rt_compute.py`` is writing fresh
   Kite prices to ``reports/live_prices.json``.
2. If that primary feed goes stale during market hours, open a lightweight Kite
   WebSocket subscription and write the same live_prices.json format.
3. As soon as the primary feed becomes fresh again, stop the fallback WebSocket
   so we do not run duplicate Kite tickers.

Important safety rule: this fallback never creates a Kite access token. If the
cached token is missing/stale, it logs and stays dormant. Wednesday/main service
is the only component allowed to perform login/token refresh.
"""

from __future__ import annotations

import json
import logging
import os
import queue as queue_mod
import sys
import time
import traceback
from datetime import datetime
from multiprocessing import Process, Queue
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

LIVE_PRICE_PATH = ROOT / "reports" / "live_prices.json"
PAPER_STATE_PATH = ROOT / "reports" / "paper_ledger_rsi_momentum_state.json"
TOKEN_PATH = ROOT / "intermediary_files" / "access_token.json"
MANUAL_LOGIN_FLAG = ROOT / "intermediary_files" / "kite_manual_login_required.json"
INSTRUMENTS_FEATHER_PATH = ROOT / "intermediary_files" / "Instruments.feather"
INSTRUMENTS_CACHE_PATH = ROOT / "intermediary_files" / "instruments_cache.json"

FALLBACK_SOURCE = "kite_ws_fallback"
PRIMARY_SOURCE = "wednesday_kite_ticker"

# Primary writes every ~5s when healthy. Give it slack for quiet ticks.
PRIMARY_FRESH_SEC = int(os.getenv("AT_KITE_WS_FALLBACK_PRIMARY_FRESH_SEC", "60"))
LIVE_PRICE_INTERVAL = int(os.getenv("AT_LIVE_PRICE_INTERVAL", "5"))
DORMANT_CHECK_SEC = int(os.getenv("AT_KITE_WS_FALLBACK_CHECK_SEC", "15"))
NO_TICK_RESTART_SEC = int(os.getenv("AT_KITE_WS_FALLBACK_NO_TICK_RESTART_SEC", "180"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)
logger = logging.getLogger("kite_ws_price_fallback")
_last_dump_ts = 0.0


def _parse_live_time(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value))
    except Exception:
        return None
    # Existing project timestamps are local naive datetimes. If a future writer
    # stores timezone-aware timestamps, compare in local wall-clock terms.
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone().replace(tzinfo=None)
    return parsed


def _live_age_sec(payload: dict[str, Any], *, now: datetime | None = None) -> float | None:
    live_time = _parse_live_time(payload.get("time"))
    if live_time is None:
        return None
    now = now or datetime.now()
    return max(0.0, (now - live_time).total_seconds())


def _is_primary_payload_fresh(payload: dict[str, Any], *, max_age_sec: int = PRIMARY_FRESH_SEC) -> bool:
    """Return True when live_prices.json was freshly written by the primary feed.

    Backward compatibility: an older fresh file without a ``source`` field is
    treated as primary, because before this fallback existed only Wednesday wrote
    this file.
    """

    age = _live_age_sec(payload)
    if age is None or age > max_age_sec:
        return False
    source = str(payload.get("source") or PRIMARY_SOURCE)
    return source != FALLBACK_SOURCE


def primary_price_feed_healthy() -> bool:
    try:
        payload = json.loads(LIVE_PRICE_PATH.read_text())
    except Exception:
        return False
    if not isinstance(payload, dict):
        return False
    return _is_primary_payload_fresh(payload)


def _load_wanted_symbols() -> set[str]:
    try:
        state = json.loads(PAPER_STATE_PATH.read_text())
    except Exception:
        return set()
    positions = state.get("positions", {}) if isinstance(state, dict) else {}
    if not isinstance(positions, dict):
        return set()
    return {str(sym).strip().upper() for sym in positions if str(sym).strip()}


def _lookup_instrument(instruments_dict: dict[Any, dict[str, Any]], token: Any) -> dict[str, Any]:
    if token in instruments_dict:
        return instruments_dict[token]
    try:
        token_int = int(token)
    except Exception:
        return {}
    return instruments_dict.get(token_int, {})


def _extract_tick_prices(
    ticks: list[dict[str, Any]],
    instruments_dict: dict[Any, dict[str, Any]],
    wanted: set[str],
) -> dict[str, float]:
    prices: dict[str, float] = {}
    for tick in ticks:
        if not isinstance(tick, dict):
            continue
        token = tick.get("instrument_token")
        instrument = _lookup_instrument(instruments_dict, token)
        symbol = (
            tick.get("Symbol")
            or tick.get("tradingsymbol")
            or instrument.get("Symbol")
            or instrument.get("tradingsymbol")
            or ""
        )
        symbol = str(symbol).strip().upper()
        if symbol not in wanted:
            continue
        try:
            price = float(tick.get("last_price", 0.0) or 0.0)
        except Exception:
            continue
        if price > 0:
            prices[symbol] = price
    return prices


def _write_live_prices(prices: dict[str, float], wanted: set[str]) -> None:
    existing: dict[str, Any] = {}
    try:
        existing = json.loads(LIVE_PRICE_PATH.read_text())
        if not isinstance(existing, dict):
            existing = {}
    except Exception:
        existing = {}

    now_str = datetime.now().isoformat(timespec="seconds")
    merged_prices = existing.get("prices", {}) if isinstance(existing.get("prices"), dict) else {}
    price_times = existing.get("price_times", {}) if isinstance(existing.get("price_times"), dict) else {}

    for symbol, price in prices.items():
        merged_prices[symbol] = price
        price_times[symbol] = now_str

    merged_prices = {sym: px for sym, px in merged_prices.items() if sym in wanted}
    price_times = {sym: ts for sym, ts in price_times.items() if sym in wanted}

    payload = {
        "time": now_str,
        "prices": merged_prices,
        "price_times": price_times,
        "source": FALLBACK_SOURCE,
        "source_pid": os.getpid(),
    }
    LIVE_PRICE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = LIVE_PRICE_PATH.with_suffix(".json.tmp")
    tmp_path.write_text(json.dumps(payload, separators=(",", ":")))
    tmp_path.replace(LIVE_PRICE_PATH)


def publish_fallback_prices(ticks: list[dict[str, Any]], instruments_dict: dict[Any, dict[str, Any]]) -> str:
    """Publish fallback prices from a tick batch.

    Returns a short status string for logging/tests.
    """

    global _last_dump_ts

    if primary_price_feed_healthy():
        return "primary_healthy"

    now = time.time()
    if now - _last_dump_ts < LIVE_PRICE_INTERVAL:
        return "throttled"

    wanted = _load_wanted_symbols()
    if not wanted:
        return "no_positions"

    prices = _extract_tick_prices(ticks, instruments_dict, wanted)
    if not prices:
        return "no_wanted_ticks"

    _write_live_prices(prices, wanted)
    _last_dump_ts = now
    return f"published:{len(prices)}"


def _today_str() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def _read_cached_access_token() -> str | None:
    """Read today's Kite access token without attempting login/refresh."""

    if MANUAL_LOGIN_FLAG.exists() and not TOKEN_PATH.exists():
        try:
            flag = json.loads(MANUAL_LOGIN_FLAG.read_text())
            reason = flag.get("message") or flag.get("reason") or "manual login required"
        except Exception:
            reason = "manual login required"
        logger.warning("No Kite token available; %s", reason)
        return None

    try:
        payload = json.loads(TOKEN_PATH.read_text())
    except FileNotFoundError:
        logger.warning("No Kite access token at %s; fallback WS not started", TOKEN_PATH)
        return None
    except Exception as exc:
        logger.warning("Could not read Kite token %s: %s; fallback WS not started", TOKEN_PATH, exc)
        return None

    access_token = str(payload.get("access_token") or "").strip()
    token_date = str(payload.get("date") or "").strip()
    if not access_token:
        logger.warning("Kite token file has no access_token; fallback WS not started")
        return None
    if token_date != _today_str():
        logger.warning(
            "Kite token date is %s, expected %s; fallback WS not started",
            token_date or "missing",
            _today_str(),
        )
        return None
    return access_token


def _build_cached_subscription() -> tuple[list[int], dict[Any, dict[str, Any]]]:
    """Build subscription tokens from cached files only; never call Kite REST."""

    import pandas as pd

    tokens: list[int] = []
    instruments_dict: dict[Any, dict[str, Any]] = {}

    if INSTRUMENTS_FEATHER_PATH.exists():
        df = pd.read_feather(INSTRUMENTS_FEATHER_PATH)
        for _, row in df.iterrows():
            try:
                token = int(row["instrument_token"])
            except Exception:
                continue
            symbol = str(row.get("Symbol") or row.get("tradingsymbol") or "").strip().upper()
            if token not in tokens:
                tokens.append(token)
            instruments_dict[token] = {"Symbol": symbol, "tradingsymbol": symbol}
    else:
        logger.warning("Cached Instruments.feather missing at %s", INSTRUMENTS_FEATHER_PATH)

    wanted = _load_wanted_symbols()
    cache: dict[str, Any] = {}
    if INSTRUMENTS_CACHE_PATH.exists():
        try:
            cache = json.loads(INSTRUMENTS_CACHE_PATH.read_text())
            if not isinstance(cache, dict):
                cache = {}
        except Exception as exc:
            logger.warning("Could not read %s: %s", INSTRUMENTS_CACHE_PATH, exc)

    # Ensure current paper-ledger symbols are subscribed even if not in the normal watchlist.
    for symbol in sorted(wanted):
        try:
            token = int(cache.get(symbol) or cache.get(symbol.upper()) or 0)
        except Exception:
            token = 0
        if token <= 0:
            logger.warning("No cached instrument token for paper-ledger symbol %s", symbol)
            continue
        if token not in tokens:
            tokens.append(token)
        instruments_dict[token] = {"Symbol": symbol, "tradingsymbol": symbol}

    # Preserve order while removing accidental duplicates.
    tokens = list(dict.fromkeys(tokens))
    if not tokens:
        raise RuntimeError("No cached subscription tokens available")
    return tokens, instruments_dict


def _market_is_open() -> bool:
    if os.getenv("AT_KITE_WS_FALLBACK_IGNORE_MARKET", "0").strip().lower() in {"1", "true", "yes"}:
        return True
    try:
        from Auto_Trader.tickertape_data import is_market_open_via_tickertape

        status = is_market_open_via_tickertape("IN")
        if status is not None:
            return bool(status)
    except Exception:
        logger.warning("Tickertape market-open check failed", exc_info=True)

    # Calendar fallback without importing Auto_Trader.utils (which can touch Kite token paths).
    try:
        import pandas_market_calendars as mcal

        now = datetime.now()
        schedule = mcal.get_calendar("NSE").schedule(start_date=now.date(), end_date=now.date())
        if schedule.empty:
            return False
        open_ts = schedule.iloc[0]["market_open"].tz_convert("Asia/Kolkata").replace(tzinfo=None)
        close_ts = schedule.iloc[0]["market_close"].tz_convert("Asia/Kolkata").replace(tzinfo=None)
        return open_ts <= now <= close_ts
    except Exception:
        logger.warning("Calendar market-open check failed; assuming open for fallback safety", exc_info=True)
        return True


def _stop_process(proc: Process | None) -> None:
    if proc is None:
        return
    if proc.is_alive():
        proc.terminate()
        proc.join(timeout=10)
    if proc.is_alive():
        logger.warning("Fallback ticker did not terminate cleanly; killing pid=%s", proc.pid)
        proc.kill()
        proc.join(timeout=5)


def _run_cached_token_ticker(tokens: list[int], q: Queue, api_key: str, access_token: str) -> None:
    """KiteTicker worker using only the already-cached access token."""

    from kiteconnect import KiteTicker

    kws = KiteTicker(api_key=api_key, access_token=access_token)

    def on_ticks(_ws, ticks):
        q.put(ticks)

    def on_connect(ws, _response):
        logger.info("Fallback Kite ticker connected; subscribing to %s tokens", len(tokens))
        ws.subscribe(tokens)
        ws.set_mode(ws.MODE_QUOTE, tokens)

    def on_close(ws, code, reason):
        logger.warning("Fallback Kite WebSocket closed code=%s reason=%s", code, reason)
        try:
            ws.stop()
        except Exception:
            pass

    def on_error(_ws, code, reason):
        logger.error("Fallback Kite WebSocket error code=%s reason=%s", code, reason)

    kws.on_ticks = on_ticks
    kws.on_connect = on_connect
    kws.on_close = on_close
    kws.on_error = on_error
    kws.connect()


def run_fallback_until_primary_recovers() -> None:
    access_token = _read_cached_access_token()
    if not access_token:
        return

    logger.warning("Primary live price feed stale; starting fallback Kite WebSocket")
    q: Queue = Queue()
    tokens, instruments_dict = _build_cached_subscription()

    from Auto_Trader.my_secrets import API_KEY

    ticker_proc = Process(target=_run_cached_token_ticker, args=(tokens, q, API_KEY, access_token), daemon=True)
    ticker_proc.start()
    logger.info("Fallback Kite ticker started pid=%s tokens=%s", ticker_proc.pid, len(tokens))

    last_tick_ts = time.time()
    try:
        while _market_is_open():
            if primary_price_feed_healthy():
                logger.info("Primary live price feed is fresh again; stopping fallback Kite WebSocket")
                return
            if not ticker_proc.is_alive():
                logger.warning("Fallback ticker process exited; restarting")
                return
            try:
                ticks = q.get(timeout=2)
            except queue_mod.Empty:
                if time.time() - last_tick_ts > NO_TICK_RESTART_SEC:
                    logger.warning("No fallback ticks for %ss; restarting fallback ticker", NO_TICK_RESTART_SEC)
                    return
                continue
            if ticks is None:
                logger.info("Fallback ticker queue received shutdown sentinel")
                return
            last_tick_ts = time.time()
            status = publish_fallback_prices(ticks, instruments_dict)
            if status.startswith("published"):
                logger.info("Fallback live price update %s", status)
            elif status == "primary_healthy":
                logger.info("Primary recovered before fallback write; stopping fallback")
                return
    finally:
        _stop_process(ticker_proc)
        logger.info("Fallback Kite ticker stopped")


def main() -> None:
    logger.info(
        "Kite WS fallback supervisor started primary_fresh_sec=%s check_sec=%s",
        PRIMARY_FRESH_SEC,
        DORMANT_CHECK_SEC,
    )
    while True:
        try:
            if not _market_is_open():
                logger.info("Market closed; fallback dormant")
                time.sleep(max(DORMANT_CHECK_SEC, 60))
                continue
            if primary_price_feed_healthy():
                logger.info("Primary live price feed healthy; fallback dormant")
                time.sleep(DORMANT_CHECK_SEC)
                continue
            run_fallback_until_primary_recovers()
            time.sleep(DORMANT_CHECK_SEC)
        except KeyboardInterrupt:
            raise
        except Exception as exc:
            logger.error("Fallback supervisor error: %s\n%s", exc, traceback.format_exc())
            time.sleep(max(DORMANT_CHECK_SEC, 30))


if __name__ == "__main__":
    main()
