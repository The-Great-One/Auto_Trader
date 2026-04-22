from kiteconnect import KiteConnect
from Auto_Trader.my_secrets import API_KEY
from Auto_Trader.utils import read_session_data, fetch_holdings, get_mmi_now
from collections import defaultdict
from math import floor
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import threading
from kiteconnect.exceptions import (
    GeneralException,
    TokenException,
    PermissionException,
    OrderException,
    InputException,
    DataException,
    NetworkException,
)
import logging
import traceback
import os
from typing import Dict, List

# Initialize KiteConnect
kite = KiteConnect(api_key=API_KEY)
kite.set_access_token(read_session_data())

logger = logging.getLogger("Auto_Trade_Logger")

# --- tiny helpers ---
_ACTIVE_ORDER_STATUSES = {
    "OPEN",
    "TRIGGER PENDING",
    "PUT ORDER REQ RECEIVED",
    "VALIDATION PENDING",
    "PENDING",
    "MODIFY VALIDATION PENDING",
    "AMO REQ RECEIVED",
}
_FILLED_ORDER_STATUSES = {"COMPLETE", "PARTIALLY FILLED"}
_ORDER_DEDUPE_WINDOW_S = max(5, int(os.getenv("AT_ORDER_DEDUPE_WINDOW_S", "180")))
_ORDER_STATE_LOCK = threading.Lock()
_ORDER_INFLIGHT_KEYS: set[tuple[str, str]] = set()
_ORDER_RECENT_TS: Dict[tuple[str, str], float] = {}
_PORTFOLIO_TARGET_EQUITY = float(os.getenv("AT_TARGET_EQUITY", "0.75"))
_PORTFOLIO_TARGET_ETF = float(os.getenv("AT_TARGET_ETF", "0.25"))
_PORTFOLIO_BAND = float(os.getenv("AT_PORTFOLIO_BAND", "0.05"))
_MAX_SINGLE_SYMBOL_WEIGHT = float(os.getenv("AT_MAX_SINGLE_SYMBOL_WEIGHT", "0.15"))
_MMI_NEUTRAL = float(os.getenv("AT_MMI_NEUTRAL", "50"))
_MMI_FULL_SCALE = max(1.0, float(os.getenv("AT_MMI_FULL_SCALE", "20")))
_MMI_MAX_SKEW = max(0.0, float(os.getenv("AT_MMI_MAX_SKEW", "0.20")))
_MMI_EQUITY_MIN = max(0.0, float(os.getenv("AT_MMI_EQUITY_MIN", "0.20")))
_MMI_EQUITY_MAX = min(1.0, float(os.getenv("AT_MMI_EQUITY_MAX", "0.90")))


def _norm_status(s: str) -> str:
    return (s or "").replace("_", " ").strip().upper()


def _sleep_backoff(attempt: int, base: float = 0.4, cap: float = 4.0):
    time.sleep(min(cap, base * (2**attempt) + 0.05 * attempt))


def _safe_float(x, default=None):
    try:
        return float(x)
    except Exception:
        return default


def _env_float(primary: str, fallback: str | None, default: float) -> float:
    raw = os.getenv(primary)
    if raw in {None, ""} and fallback:
        raw = os.getenv(fallback)
    return float(raw if raw not in {None, ""} else default)


def _env_flag(primary: str, fallback: str | None = None, default: bool = False) -> bool:
    raw = os.getenv(primary)
    if raw in {None, ""} and fallback:
        raw = os.getenv(fallback)
    if raw in {None, ""}:
        return default
    return str(raw).strip().lower() not in {"0", "false", "no"}


def _live_position_sizing_config() -> dict:
    return {
        "enabled": _env_flag("AT_LIVE_VOL_SIZING_ENABLED", "AT_BACKTEST_VOL_SIZING_ENABLED", False),
        "risk_per_trade_pct": max(
            0.0,
            _env_float("AT_LIVE_RISK_PER_TRADE_PCT", "AT_BACKTEST_RISK_PER_TRADE_PCT", 0.01),
        ),
        "atr_stop_mult": max(
            0.1,
            _env_float("AT_LIVE_ATR_STOP_MULT", "AT_BACKTEST_ATR_STOP_MULT", 2.5),
        ),
        "max_position_notional_pct": max(
            0.01,
            _env_float(
                "AT_LIVE_MAX_POSITION_NOTIONAL_PCT",
                "AT_BACKTEST_MAX_POSITION_NOTIONAL_PCT",
                0.25,
            ),
        ),
        "fallback_allocation": max(0.0, _env_float("FUND_ALLOCATION", None, 20000.0)),
    }


def _calc_buy_quantity(
    price: float,
    atr: float | None,
    *,
    available_cash: float,
    portfolio_value: float,
    sizing_cfg: dict,
) -> tuple[int, dict]:
    if price <= 0 or available_cash <= 0:
        return 0, {"method": "invalid_inputs"}

    fallback_notional = min(max(0.0, available_cash), sizing_cfg["fallback_allocation"])
    if not sizing_cfg.get("enabled"):
        qty = int(floor(fallback_notional / price))
        return qty, {
            "method": "fixed_allocation",
            "target_notional": fallback_notional,
            "available_cash": available_cash,
        }

    max_notional = max(0.0, portfolio_value) * sizing_cfg["max_position_notional_pct"]
    if max_notional <= 0:
        max_notional = fallback_notional

    atr_value = _safe_float(atr)
    stop_distance = None
    if atr_value is not None and atr_value > 0:
        stop_distance = atr_value * sizing_cfg["atr_stop_mult"]

    risk_budget = max(0.0, portfolio_value) * sizing_cfg["risk_per_trade_pct"]
    if stop_distance is None or stop_distance <= 0 or risk_budget <= 0:
        target_notional = min(available_cash, max_notional, fallback_notional)
        qty = int(floor(target_notional / price))
        return qty, {
            "method": "vol_sizing_fallback",
            "target_notional": target_notional,
            "available_cash": available_cash,
            "max_notional": max_notional,
            "risk_budget": risk_budget,
            "stop_distance": stop_distance,
        }

    risk_qty = risk_budget / stop_distance
    target_notional = min(available_cash, max_notional, risk_qty * price)
    qty = int(floor(target_notional / price))
    return qty, {
        "method": "atr_risk",
        "target_notional": target_notional,
        "available_cash": available_cash,
        "max_notional": max_notional,
        "risk_budget": risk_budget,
        "stop_distance": stop_distance,
    }


def _order_key(symbol: str, side: str) -> tuple[str, str]:
    return ((symbol or "").upper(), (side or "").upper())


def _cleanup_recent_order_cache(now_ts: float) -> None:
    stale = [
        key
        for key, ts in _ORDER_RECENT_TS.items()
        if (now_ts - ts) > (_ORDER_DEDUPE_WINDOW_S * 2)
    ]
    for key in stale:
        _ORDER_RECENT_TS.pop(key, None)


def _reserve_order_slot(symbol: str, side: str) -> tuple[bool, tuple[str, str]]:
    key = _order_key(symbol, side)
    now_ts = time.time()
    with _ORDER_STATE_LOCK:
        _cleanup_recent_order_cache(now_ts)
        if key in _ORDER_INFLIGHT_KEYS:
            return False, key
        last_ts = _ORDER_RECENT_TS.get(key)
        if last_ts is not None and (now_ts - last_ts) < _ORDER_DEDUPE_WINDOW_S:
            return False, key
        _ORDER_INFLIGHT_KEYS.add(key)
        return True, key


def _release_order_slot(key: tuple[str, str], *, mark_recent: bool = False) -> None:
    with _ORDER_STATE_LOCK:
        _ORDER_INFLIGHT_KEYS.discard(key)
        if mark_recent:
            _ORDER_RECENT_TS[key] = time.time()


def _build_order_tag(symbol: str, side: str) -> str:
    clean_symbol = "".join(ch for ch in (symbol or "").upper() if ch.isalnum())[:8]
    bucket = int(time.time() // _ORDER_DEDUPE_WINDOW_S)
    return f"AT{(side or '')[:1].upper()}{clean_symbol}{bucket}"[:20]


def _parse_order_timestamp(order: dict):
    for key in ("order_timestamp", "exchange_update_timestamp", "exchange_timestamp"):
        raw = order.get(key)
        if raw:
            ts = pd.to_datetime(raw, errors="coerce")
            if not pd.isna(ts):
                return ts.tz_localize(None)
    return None


def _has_recent_same_side_order(
    symbol: str, side: str, *, within_seconds: int | None = None
) -> bool:
    symbol_upper = (symbol or "").upper()
    side_upper = (side or "").upper()
    window_s = within_seconds if within_seconds is not None else _ORDER_DEDUPE_WINDOW_S
    cutoff = pd.Timestamp.now().tz_localize(None) - pd.Timedelta(seconds=window_s)
    try:
        for order in kite.orders() or []:
            if (order.get("tradingsymbol") or "").upper() != symbol_upper:
                continue
            if (order.get("transaction_type") or "").upper() != side_upper:
                continue

            status = _norm_status(order.get("status"))
            if status in _ACTIVE_ORDER_STATUSES:
                return True

            if status in _FILLED_ORDER_STATUSES:
                ts = _parse_order_timestamp(order)
                if ts is None or ts >= cutoff:
                    return True
    except Exception as e:
        logger.warning(
            "Orderbook check failed for %s %s: %s", side_upper, symbol_upper, e
        )
    return False


def get_active_order_symbols(side: str | None = None) -> set[str]:
    want_side = (side or "").upper() if side else None
    symbols: set[str] = set()
    try:
        for order in kite.orders() or []:
            status = _norm_status(order.get("status"))
            if status not in _ACTIVE_ORDER_STATUSES:
                continue
            txn = (order.get("transaction_type") or "").upper()
            if want_side and txn != want_side:
                continue
            symbol = order.get("tradingsymbol")
            if symbol:
                symbols.add(symbol)
    except Exception as e:
        logger.error(
            "Error retrieving active order symbols: %s, Traceback: %s",
            e,
            traceback.format_exc(),
        )
    return symbols


def _load_symbol_metadata() -> Dict[str, dict]:
    try:
        df = pd.read_feather("intermediary_files/Instruments.feather")
        if "Symbol" not in df.columns:
            return {}
        out = {}
        for _, row in df.iterrows():
            symbol = str(row.get("Symbol", "")).strip()
            if not symbol:
                continue
            out[symbol] = {
                "AssetClass": str(row.get("AssetClass", "EQUITY")).upper(),
                "ETFTheme": str(row.get("ETFTheme", "")).upper(),
            }
        return out
    except Exception:
        return {}


def _normalize_targets(mmi_value=None) -> Dict[str, float]:
    eq = max(0.0, _PORTFOLIO_TARGET_EQUITY)
    etf = max(0.0, _PORTFOLIO_TARGET_ETF)
    s = eq + etf
    if s <= 0:
        base_eq, base_etf = 0.75, 0.25
    else:
        base_eq, base_etf = (eq / s), (etf / s)

    if mmi_value is None:
        return {"EQUITY": base_eq, "ETF": base_etf}

    try:
        mmi = float(mmi_value)
    except (TypeError, ValueError):
        return {"EQUITY": base_eq, "ETF": base_etf}

    # Low MMI => risk-on (equity overweight), high MMI => risk-off (ETF overweight).
    z = (_MMI_NEUTRAL - mmi) / _MMI_FULL_SCALE
    z = max(-1.0, min(1.0, z))
    skewed_eq = base_eq + (z * _MMI_MAX_SKEW)
    skewed_eq = max(_MMI_EQUITY_MIN, min(_MMI_EQUITY_MAX, skewed_eq))
    skewed_etf = max(0.0, 1.0 - skewed_eq)
    return {"EQUITY": skewed_eq, "ETF": skewed_etf}


def _classify_asset_class(
    symbol: str, decision_asset_class, metadata: Dict[str, dict]
) -> str:
    if decision_asset_class:
        value = str(decision_asset_class).upper()
        if value in {"EQUITY", "ETF"}:
            return value

    meta = metadata.get(symbol, {})
    meta_class = str(meta.get("AssetClass", "")).upper()
    if meta_class in {"EQUITY", "ETF"}:
        return meta_class

    symbol_u = (symbol or "").upper()
    if "ETF" in symbol_u or "BEES" in symbol_u:
        return "ETF"
    return "EQUITY"


def _compute_portfolio_exposure(
    hdf: pd.DataFrame, symbol_metadata: Dict[str, dict]
) -> tuple[Dict[str, float], Dict[str, float], float]:
    class_notional = defaultdict(float)
    symbol_notional = defaultdict(float)
    total = 0.0
    for symbol, row in hdf.iterrows():
        qty = _safe_float(row.get("quantity"), 0.0)
        avg_price = _safe_float(row.get("average_price"), 0.0)
        if qty <= 0 or avg_price <= 0:
            continue
        notional = float(qty) * float(avg_price)
        symbol_notional[symbol] += notional
        asset_class = _classify_asset_class(symbol, None, symbol_metadata)
        class_notional[asset_class] += notional
        total += notional
    return class_notional, symbol_notional, total


def _portfolio_allows_buy(
    symbol: str,
    asset_class: str,
    notional: float,
    *,
    base_portfolio_value: float,
    current_class_notional: Dict[str, float],
    current_symbol_notional: Dict[str, float],
    planned_class_notional: Dict[str, float],
    planned_symbol_notional: Dict[str, float],
    targets: Dict[str, float],
) -> tuple[bool, str]:
    total_base = max(base_portfolio_value, 1.0)
    class_cap = targets.get(asset_class, 0.0) + _PORTFOLIO_BAND
    if class_cap <= 0:
        return False, f"{asset_class} target disabled"

    projected_class = (
        current_class_notional.get(asset_class, 0.0)
        + planned_class_notional.get(asset_class, 0.0)
        + notional
    )
    projected_class_weight = projected_class / total_base
    if projected_class_weight > class_cap:
        return (
            False,
            f"{asset_class} allocation cap hit ({projected_class_weight:.2%} > {class_cap:.2%})",
        )

    projected_symbol = (
        current_symbol_notional.get(symbol, 0.0)
        + planned_symbol_notional.get(symbol, 0.0)
        + notional
    )
    if (projected_symbol / total_base) > _MAX_SINGLE_SYMBOL_WEIGHT:
        return (
            False,
            f"symbol concentration cap hit ({projected_symbol / total_base:.2%} > {_MAX_SINGLE_SYMBOL_WEIGHT:.2%})",
        )

    return True, "ok"


def trigger(
    message_queue,
    symbol,
    exchange,
    trans_quantity,
    order_type,
    close_price,
    contributing_rules,
):
    """
    Places a (by default) safer MARKET sell / LIMIT buy order and notifies.
    Keeps signature unchanged. Uses retries on transient failures.
    """
    reserved, order_slot_key = _reserve_order_slot(symbol, order_type)
    if not reserved:
        logger.info(
            "Skipping duplicate %s for %s (in-flight/cooldown dedupe window active).",
            order_type,
            symbol,
        )
        return

    order_confirmed = False
    logger.info(
        f"Triggering {order_type} for {symbol} on {exchange} qty={trans_quantity} px={close_price}"
    )
    try:
        trigg_exchange = (
            kite.EXCHANGE_NSE
            if (exchange or "").upper() == "NSE"
            else kite.EXCHANGE_BSE
        )
        txn = (
            kite.TRANSACTION_TYPE_BUY
            if order_type == "BUY"
            else kite.TRANSACTION_TYPE_SELL
        )

        qty = int(trans_quantity or 0)
        if qty <= 0:
            logger.warning(
                "Skipping %s for %s because quantity=%s is not valid.",
                order_type,
                symbol,
                trans_quantity,
            )
            return

        if _has_recent_same_side_order(symbol, order_type):
            logger.info(
                "Skipping %s for %s because a recent matching order already exists.",
                order_type,
                symbol,
            )
            order_confirmed = True
            return

        # Strategy: use MARKET for SELL (to exit), LIMIT for BUY (protect slippage)
        # If you want both MARKET, set use_market_buy=True and skip price.
        use_market_buy = False
        use_market_sell = True

        # Build order args
        variety = kite.VARIETY_REGULAR
        product = kite.PRODUCT_CNC
        validity = kite.VALIDITY_DAY

        # For LIMIT buys we bias slightly above last price to improve fill probability.
        price = _safe_float(close_price)
        limit_price = None
        if order_type == "BUY":
            if use_market_buy:
                order_type_k = kite.ORDER_TYPE_MARKET
            else:
                order_type_k = kite.ORDER_TYPE_LIMIT
                # +0.2% bump to improve fill; tweak as you like
                if price is None or price <= 0:
                    raise ValueError(f"Missing/invalid close_price for BUY {symbol}")
                limit_price = round(price * 1.002, 2)
        else:
            if use_market_sell:
                order_type_k = kite.ORDER_TYPE_MARKET
            else:
                order_type_k = kite.ORDER_TYPE_LIMIT
                if price is None or price <= 0:
                    raise ValueError(f"Missing/invalid close_price for SELL {symbol}")
                # -0.2% to improve sell fill
                limit_price = round(max(0.05, price * 0.998), 2)

        order_tag = _build_order_tag(symbol, order_type)

        # Retry wrapper
        max_attempts = 4
        last_err = None
        for attempt in range(max_attempts):
            try:
                kwargs = dict(
                    variety=variety,
                    tradingsymbol=symbol,
                    exchange=trigg_exchange,
                    transaction_type=txn,
                    quantity=qty,
                    product=product,
                    validity=validity,
                    order_type=order_type_k,
                    tag=order_tag,
                )
                if order_type_k == kite.ORDER_TYPE_LIMIT:
                    kwargs["price"] = limit_price

                order_id = kite.place_order(**kwargs)
                order_confirmed = True

                # Notify
                message = (
                    f"Symbol: {symbol}\n"
                    f"Quantity: {qty}\n"
                    f"Price: {limit_price if order_type_k == kite.ORDER_TYPE_LIMIT else 'MARKET'}\n"
                    f"Type: {order_type}\n"
                    f"Order ID: {order_id}\n"
                    f"Contributing Rules: {contributing_rules}"
                )
                try:
                    message_queue.put(message)
                except Exception as me:
                    logger.warning(f"Failed to enqueue message for {symbol}: {me}")

                logger.info(f"{order_type} placed: {symbol} (Order ID: {order_id})")
                return
            except (NetworkException, GeneralException, DataException) as e:
                last_err = e
                logger.warning(
                    f"[Attempt {attempt + 1}/{max_attempts}] Transient error for {symbol}: {e}"
                )
                # Defensive idempotency: if broker accepted despite a transient client error,
                # treat it as placed and stop retrying to avoid duplicates.
                if _has_recent_same_side_order(symbol, order_type):
                    logger.warning(
                        "Detected recent matching order for %s after transient error; stopping retries.",
                        symbol,
                    )
                    order_confirmed = True
                    return
                _sleep_backoff(attempt)
            except (
                TokenException,
                PermissionException,
                InputException,
                OrderException,
            ) as e:
                # Non-retryable in most cases
                logger.error(f"Order placement failed for {symbol}: {e}")
                return
            except Exception as e:
                last_err = e
                logger.error(f"Unexpected error while placing order for {symbol}: {e}")
                if _has_recent_same_side_order(symbol, order_type):
                    logger.warning(
                        "Detected recent matching order for %s after unexpected error; stopping retries.",
                        symbol,
                    )
                    order_confirmed = True
                    return
                _sleep_backoff(attempt)

        if last_err:
            logger.error(
                f"Giving up placing order for {symbol} after {max_attempts} attempts: {last_err}"
            )
    finally:
        _release_order_slot(order_slot_key, mark_recent=order_confirmed)


def get_positions() -> Dict[str, int]:
    """
    Returns net quantities by tradingsymbol for current positions (non-zero only).
    """
    try:
        pos = kite.positions()  # dict with 'net' and 'day' lists
        net = pos.get("net", []) if isinstance(pos, dict) else []
        out = {}
        for p in net:
            qty = int(p.get("quantity", 0) or 0)
            tsym = p.get("tradingsymbol")
            if tsym and qty != 0:
                out[tsym] = out.get(tsym, 0) + qty
        return out
    except Exception as e:
        logger.error(
            f"Error retrieving positions: {e}, Traceback: {traceback.format_exc()}"
        )
        return {}


def get_holdings() -> Dict[str, int]:
    """
    Returns holdings quantities by tradingsymbol (non-zero only).
    Includes t1_quantity if you prefer to count sellable T+1—toggle behavior below.
    """
    count_t1_as_sellable = (
        False  # set True if your broker allows/you want to include T1
    )
    try:
        holds = kite.holdings()  # list of dicts
        out = {}
        for h in holds or []:
            tsym = h.get("tradingsymbol")
            qty = int(h.get("quantity", 0) or 0)
            t1 = int(h.get("t1_quantity", 0) or 0)
            total = qty + (t1 if count_t1_as_sellable else 0)
            if tsym and total != 0:
                out[tsym] = out.get(tsym, 0) + total
        return out
    except Exception as e:
        logger.error(
            f"Error retrieving holdings: {e}, Traceback: {traceback.format_exc()}"
        )
        return {}


def is_symbol_in_order_book(symbol: str) -> bool:
    """
    True if symbol has any active (non-terminal) order in the order book.
    """
    try:
        for o in kite.orders() or []:
            if (o.get("tradingsymbol") == symbol) and (
                _norm_status(o.get("status")) in _ACTIVE_ORDER_STATUSES
            ):
                return True
        return False
    except Exception as e:
        logger.error(
            f"Error checking order book for {symbol}: {e}, Traceback: {traceback.format_exc()}"
        )
        return False


def should_place_buy_order(symbol: str) -> bool:
    """
    Place a buy only if not already held/positioned and no active order exists.
    """
    positions = get_positions()
    holdings = get_holdings()

    if symbol in positions:
        return False
    if symbol in holdings:
        return False
    if is_symbol_in_order_book(symbol):
        return False
    if _has_recent_same_side_order(symbol, "BUY"):
        return False
    return True


def handle_decisions(message_queue, decisions: List[dict]):
    """
    Executes SELLs first (to free funds), then BUYs (respecting funds & rate limits).
    Keeps signature unchanged.
    """
    # Your custom helper; keep fallback if it returns empty/shape mismatch
    try:
        hdf = fetch_holdings()
        if isinstance(hdf, pd.DataFrame) and not hdf.empty:
            if "tradingsymbol" in hdf.columns:
                hdf = hdf.set_index("tradingsymbol")
            else:
                hdf = pd.DataFrame(columns=["tradingsymbol", "quantity"]).set_index(
                    "tradingsymbol"
                )
        else:
            hdf = pd.DataFrame(columns=["tradingsymbol", "quantity"]).set_index(
                "tradingsymbol"
            )
    except Exception as e:
        logger.warning(
            f"fetch_holdings() failed, falling back to API-only holdings: {e}"
        )
        # Build minimal DF from API
        api_holds = get_holdings()
        hdf = (
            pd.DataFrame(
                [
                    {"tradingsymbol": k, "quantity": v, "average_price": 0.0}
                    for k, v in api_holds.items()
                ]
            ).set_index("tradingsymbol")
            if api_holds
            else pd.DataFrame(
                columns=["tradingsymbol", "quantity", "average_price"]
            ).set_index("tradingsymbol")
        )

    if "quantity" not in hdf.columns:
        hdf["quantity"] = 0
    if "average_price" not in hdf.columns:
        hdf["average_price"] = 0.0
    hdf["quantity"] = pd.to_numeric(hdf["quantity"], errors="coerce").fillna(0)
    hdf["average_price"] = pd.to_numeric(hdf["average_price"], errors="coerce").fillna(
        0.0
    )

    symbol_metadata = _load_symbol_metadata()
    mmi_value = get_mmi_now()
    portfolio_targets = _normalize_targets(mmi_value)
    class_notional, symbol_notional, holdings_notional = _compute_portfolio_exposure(
        hdf, symbol_metadata
    )
    logger.info(
        "Portfolio targets from MMI=%s => EQUITY %.1f%%, ETF %.1f%%",
        mmi_value,
        portfolio_targets["EQUITY"] * 100.0,
        portfolio_targets["ETF"] * 100.0,
    )

    symbols_held = set(hdf.index)

    # Remove duplicate decisions for the same symbol+side within this cycle.
    deduped_decisions = []
    seen_decision_keys = set()
    for d in decisions:
        symbol = d.get("Symbol")
        side = (d.get("Decision") or "").upper()
        if not symbol or side not in {"BUY", "SELL"}:
            continue
        key = (symbol, side)
        if key in seen_decision_keys:
            continue
        seen_decision_keys.add(key)
        deduped_decisions.append(d)

    # Partition sell/buy
    sell_decisions = [
        d
        for d in deduped_decisions
        if d.get("Decision") == "SELL" and d.get("Symbol") in symbols_held
    ]
    sell_symbols = {d.get("Symbol") for d in sell_decisions}
    buy_decisions = [
        d
        for d in deduped_decisions
        if d.get("Decision") == "BUY"
        and d.get("Symbol") not in symbols_held
        and d.get("Symbol") not in sell_symbols
    ]

    positions_now = get_positions()
    holdings_now = get_holdings()
    pending_buy_symbols = get_active_order_symbols("BUY")
    blocked_buy_symbols = (
        set(positions_now) | set(holdings_now) | symbols_held | pending_buy_symbols
    )

    # Concurrency & gentle rate-limit parameters
    max_workers = 3
    inter_request_gap = 0.35  # seconds between placements best-effort

    # --- SELLs first ---
    sell_futures = []
    last_call = 0.0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for d in sell_decisions:
            symbol = d["Symbol"]
            exchange = d["Exchange"]
            price = d.get("Close")
            rules = d.get("ContributingRules")

            qty = int(hdf.loc[symbol, "quantity"]) if symbol in hdf.index else 0
            if qty <= 0:
                continue

            # Spread out requests
            now = time.monotonic()
            if now - last_call < inter_request_gap:
                time.sleep(inter_request_gap - (now - last_call))
            last_call = time.monotonic()

            sell_futures.append(
                executor.submit(
                    trigger, message_queue, symbol, exchange, qty, "SELL", price, rules
                )
            )

        for f in as_completed(sell_futures):
            try:
                f.result()
            except Exception as e:
                logger.error(
                    f"Error in executing sell order: {e}, Traceback: {traceback.format_exc()}"
                )

    # --- BUYs next ---
    buy_futures = []
    sizing_cfg = _live_position_sizing_config()
    # Track local committed spend to avoid over-alloc during concurrency
    committed = 0.0
    planned_class_notional = defaultdict(float)
    planned_symbol_notional = defaultdict(float)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for d in buy_decisions:
            symbol = d["Symbol"]
            exchange = d["Exchange"]
            price = _safe_float(d.get("Close"))
            atr = _safe_float(d.get("ATR"))
            rules = d.get("ContributingRules")
            asset_class = _classify_asset_class(
                symbol, d.get("AssetClass"), symbol_metadata
            )

            if symbol in blocked_buy_symbols:
                continue

            if price is None or price <= 0:
                logger.warning(f"Invalid price for {symbol}, skipping buy.")
                continue

            # Live funds
            try:
                funds = _safe_float(
                    kite.margins("equity")["available"]["live_balance"], 0.0
                )
            except Exception as e:
                logger.error(f"Failed to fetch margins; skipping {symbol}: {e}")
                continue

            available_cash = max(0.0, funds - committed)
            if available_cash <= 0:
                logger.warning(
                    "Insufficient funds to place more buy orders. Stopping buy order processing."
                )
                break

            if not should_place_buy_order(symbol):
                blocked_buy_symbols.add(symbol)
                continue

            portfolio_base = holdings_notional + max(funds, 0.0)
            qty, sizing_meta = _calc_buy_quantity(
                price,
                atr,
                available_cash=available_cash,
                portfolio_value=portfolio_base,
                sizing_cfg=sizing_cfg,
            )
            if qty <= 0:
                logger.warning(
                    "Calculated qty %s for %s at %.2f is not positive. Sizing meta=%s",
                    qty,
                    symbol,
                    price,
                    sizing_meta,
                )
                continue

            # Optional: guardrail – minimum notional (e.g., ₹1000)
            min_notional = 500.0
            if qty * price < min_notional:
                logger.info(
                    f"Order value too small for {symbol} ({qty * price:.2f} < {min_notional}). Skipping."
                )
                continue

            order_notional = qty * price
            logger.info(
                "BUY sizing %s qty=%s notional=%.2f method=%s atr=%s meta=%s",
                symbol,
                qty,
                order_notional,
                sizing_meta.get("method"),
                atr,
                sizing_meta,
            )
            can_buy, reason = _portfolio_allows_buy(
                symbol,
                asset_class,
                order_notional,
                base_portfolio_value=portfolio_base,
                current_class_notional=class_notional,
                current_symbol_notional=symbol_notional,
                planned_class_notional=planned_class_notional,
                planned_symbol_notional=planned_symbol_notional,
                targets=portfolio_targets,
            )
            if not can_buy:
                logger.info(
                    "Skipping BUY %s due to portfolio manager: %s", symbol, reason
                )
                blocked_buy_symbols.add(symbol)
                continue

            committed += order_notional
            planned_class_notional[asset_class] += order_notional
            planned_symbol_notional[symbol] += order_notional

            # Rate-limit spacing
            now = time.monotonic()
            if now - last_call < inter_request_gap:
                time.sleep(inter_request_gap - (now - last_call))
            last_call = time.monotonic()

            buy_futures.append(
                executor.submit(
                    trigger, message_queue, symbol, exchange, qty, "BUY", price, rules
                )
            )
            blocked_buy_symbols.add(symbol)

        for f in as_completed(buy_futures):
            try:
                f.result()
            except Exception as e:
                logger.error(
                    f"Error in executing buy order: {e}, Traceback: {traceback.format_exc()}"
                )

    # End-of-cycle tiny cool-off
    time.sleep(0.1)
