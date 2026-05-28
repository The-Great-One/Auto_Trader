"""Supplemental Tickertape data access for live Auto_Trader.

The live trader keeps Kite/Zerodha as the source of truth for execution and
Indian historical data. This module centralizes public Tickertape web endpoints
behind the `tickertape-api-client` package for low-rate supplemental data such
as market-open status, MMI, public quotes, US data, and mutual-fund holdings.

All helpers are fail-soft by design: callers should have broker/calendar
fallbacks and must not place orders solely because a public web endpoint says so.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Sequence

logger = logging.getLogger("Auto_Trade_Logger")

_CLIENT: Any | None = None
_LAST_MARKET_STATUS: dict[str, Any] | None = None
_LAST_MARKET_STATUS_FETCH = 0.0
_LAST_MMI: Any | None = None
_LAST_MMI_FETCH = 0.0

MARKET_STATUS_TTL = 60  # seconds
MMI_TTL = 1800  # 30 minutes


def _get_client() -> Any | None:
    """Return a singleton TickertapeClient, or None if dependency is absent."""

    global _CLIENT
    if _CLIENT is not None:
        return _CLIENT

    try:
        from tickertape_api import TickertapeClient
    except Exception as exc:  # pragma: no cover - dependency absence path
        logger.warning("tickertape-api-client is unavailable: %s", exc)
        return None

    _CLIENT = TickertapeClient(timeout=5.0)
    return _CLIENT


def get_market_status(market: str = "IN", *, force_refresh: bool = False) -> dict[str, Any] | None:
    """Fetch public Tickertape market status for `IN` or `US` with short cache."""

    global _LAST_MARKET_STATUS, _LAST_MARKET_STATUS_FETCH

    now = time.time()
    if (
        not force_refresh
        and _LAST_MARKET_STATUS is not None
        and now - _LAST_MARKET_STATUS_FETCH <= MARKET_STATUS_TTL
        and _LAST_MARKET_STATUS.get("market", market).upper() == market.upper()
    ):
        return _LAST_MARKET_STATUS

    client = _get_client()
    if client is None:
        return None

    try:
        status = client.market_status(market.upper())
        if isinstance(status, dict):
            _LAST_MARKET_STATUS = status
            _LAST_MARKET_STATUS_FETCH = now
            return status
    except Exception as exc:
        logger.warning("Tickertape market status fetch failed for %s: %s", market, exc)
    return None


def is_market_open_via_tickertape(market: str = "IN") -> bool | None:
    """Return Tickertape market-open flag, or None when unavailable."""

    status = get_market_status(market)
    if not isinstance(status, dict) or "isOpen" not in status:
        return None
    return bool(status.get("isOpen"))


def get_mmi_indicator(*, force_refresh: bool = False) -> Any | None:
    """Fetch Tickertape MMI indicator via the packaged client with cache."""

    global _LAST_MMI, _LAST_MMI_FETCH

    now = time.time()
    if not force_refresh and _LAST_MMI is not None and now - _LAST_MMI_FETCH <= MMI_TTL:
        return _LAST_MMI

    client = _get_client()
    if client is None:
        return None

    try:
        payload = client.mmi_now()
        if isinstance(payload, dict):
            # Historical direct endpoint usage expected data.indicator. The
            # wrapper returns unwrapped data, so preserve that public function
            # contract when the field exists.
            _LAST_MMI = payload.get("indicator", payload)
        else:
            _LAST_MMI = payload
        _LAST_MMI_FETCH = now
        return _LAST_MMI
    except Exception as exc:
        logger.warning("Tickertape MMI fetch failed: %s", exc)
        return None


def get_india_quotes(sids: Sequence[str] | str) -> dict[str, Any] | None:
    """Fetch public Indian quote payload for supplemental diagnostics.

    `sids` are Tickertape SIDs, which usually but not always equal NSE trading
    symbols. Do not use this for order execution without an explicit SID map.
    """

    client = _get_client()
    if client is None:
        return None
    try:
        payload = client.india_quotes(sids)
        return payload if isinstance(payload, dict) else None
    except Exception as exc:
        logger.warning("Tickertape India quote fetch failed: %s", exc)
        return None


def get_mutual_fund_holdings(mf_id: str) -> dict[str, Any] | None:
    """Fetch public Tickertape mutual-fund holdings/portfolio composition."""

    client = _get_client()
    if client is None:
        return None
    try:
        payload = client.mutual_fund_holdings(mf_id)
        return payload if isinstance(payload, dict) else None
    except Exception as exc:
        logger.warning("Tickertape MF holdings fetch failed for %s: %s", mf_id, exc)
        return None
