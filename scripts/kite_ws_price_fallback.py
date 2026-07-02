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


def _market_is_open() -> bool:
    if os.getenv("AT_KITE_WS_FALLBACK_IGNORE_MARKET", "0").strip().lower() in {"1", "true", "yes"}:
        return True
    try:
        from Auto_Trader.utils import is_Market_Open

        return bool(is_Market_Open())
    except Exception:
        logger.warning("Market-open check failed; assuming open for fallback safety", exc_info=True)
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


def _build_subscription_tokens() -> list[int]:
    from Auto_Trader.Build_Master import create_master

    dummy_mq: Queue = Queue()
    tokens = create_master(dummy_mq)
    # Drop startup messages; this script has no Telegram sender.
    while not dummy_mq.empty():
        try:
            dummy_mq.get_nowait()
        except Exception:
            break
    if not tokens:
        raise RuntimeError("create_master returned no subscription tokens")
    return [int(token) for token in tokens]


def run_fallback_until_primary_recovers() -> None:
    logger.warning("Primary live price feed stale; starting fallback Kite WebSocket")
    q: Queue = Queue()
    tokens = _build_subscription_tokens()

    from Auto_Trader.kite_ticker import run_ticker
    from Auto_Trader.utils import load_instruments_data

    instruments_dict = load_instruments_data()
    ticker_proc = Process(target=run_ticker, args=(tokens, q), daemon=True)
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
        except KeyboardInterrupt:
            raise
        except Exception as exc:
            logger.error("Fallback supervisor error: %s\n%s", exc, traceback.format_exc())
            time.sleep(max(DORMANT_CHECK_SEC, 30))


if __name__ == "__main__":
    main()
