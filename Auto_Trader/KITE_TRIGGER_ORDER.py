from kiteconnect import KiteConnect
from Auto_Trader.my_secrets import API_KEY
from Auto_Trader.utils import read_session_data, fetch_holdings
from math import floor
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
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
import math
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

def _norm_status(s: str) -> str:
    return (s or "").replace("_", " ").strip().upper()

def _sleep_backoff(attempt: int, base: float = 0.4, cap: float = 4.0):
    time.sleep(min(cap, base * (2 ** attempt) + 0.05 * attempt))

def _safe_float(x, default=None):
    try:
        return float(x)
    except Exception:
        return default

def trigger(message_queue, symbol, exchange, trans_quantity, order_type, close_price, contributing_rules):
    """
    Places a (by default) safer MARKET sell / LIMIT buy order and notifies.
    Keeps signature unchanged. Uses retries on transient failures.
    """
    logger.info(f"Triggering {order_type} for {symbol} on {exchange} qty={trans_quantity} px={close_price}")
    trigg_exchange = kite.EXCHANGE_NSE if (exchange or "").upper() == "NSE" else kite.EXCHANGE_BSE
    txn = kite.TRANSACTION_TYPE_BUY if order_type == "BUY" else kite.TRANSACTION_TYPE_SELL

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
                quantity=int(trans_quantity),
                product=product,
                validity=validity,
                order_type=order_type_k,
            )
            if order_type_k == kite.ORDER_TYPE_LIMIT:
                kwargs["price"] = limit_price

            order_id = kite.place_order(**kwargs)

            # Notify
            message = (
                f"Symbol: {symbol}\n"
                f"Quantity: {trans_quantity}\n"
                f"Price: {limit_price if order_type_k==kite.ORDER_TYPE_LIMIT else 'MARKET'}\n"
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
            logger.warning(f"[Attempt {attempt+1}/{max_attempts}] Transient error for {symbol}: {e}")
            _sleep_backoff(attempt)
        except (TokenException, PermissionException, InputException, OrderException) as e:
            # Non-retryable in most cases
            logger.error(f"Order placement failed for {symbol}: {e}")
            return
        except Exception as e:
            last_err = e
            logger.error(f"Unexpected error while placing order for {symbol}: {e}")
            _sleep_backoff(attempt)

    if last_err:
        logger.error(f"Giving up placing order for {symbol} after {max_attempts} attempts: {last_err}")


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
        logger.error(f"Error retrieving positions: {e}, Traceback: {traceback.format_exc()}")
        return {}

def get_holdings() -> Dict[str, int]:
    """
    Returns holdings quantities by tradingsymbol (non-zero only).
    Includes t1_quantity if you prefer to count sellable T+1—toggle behavior below.
    """
    count_t1_as_sellable = False  # set True if your broker allows/you want to include T1
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
        logger.error(f"Error retrieving holdings: {e}, Traceback: {traceback.format_exc()}")
        return {}

def is_symbol_in_order_book(symbol: str) -> bool:
    """
    True if symbol has any active (non-terminal) order in the order book.
    """
    try:
        for o in kite.orders() or []:
            if (o.get("tradingsymbol") == symbol) and (_norm_status(o.get("status")) in _ACTIVE_ORDER_STATUSES):
                return True
        return False
    except Exception as e:
        logger.error(f"Error checking order book for {symbol}: {e}, Traceback: {traceback.format_exc()}")
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
                hdf = pd.DataFrame(columns=["tradingsymbol", "quantity"]).set_index("tradingsymbol")
        else:
            hdf = pd.DataFrame(columns=["tradingsymbol", "quantity"]).set_index("tradingsymbol")
    except Exception as e:
        logger.warning(f"fetch_holdings() failed, falling back to API-only holdings: {e}")
        # Build minimal DF from API
        api_holds = get_holdings()
        hdf = pd.DataFrame(
            [{"tradingsymbol": k, "quantity": v} for k, v in api_holds.items()]
        ).set_index("tradingsymbol") if api_holds else pd.DataFrame(columns=["tradingsymbol","quantity"]).set_index("tradingsymbol")

    symbols_held = list(hdf.index)

    # Partition sell/buy
    sell_decisions = [d for d in decisions if d.get("Decision") == "SELL" and d.get("Symbol") in symbols_held]
    buy_decisions  = [d for d in decisions if d.get("Decision") == "BUY"  and d.get("Symbol") not in symbols_held]

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
            price   = d.get("Close")
            rules   = d.get("ContributingRules")

            qty = int(hdf.loc[symbol, "quantity"]) if symbol in hdf.index else 0
            if qty <= 0:
                continue

            # Spread out requests
            now = time.monotonic()
            if now - last_call < inter_request_gap:
                time.sleep(inter_request_gap - (now - last_call))
            last_call = time.monotonic()

            sell_futures.append(
                executor.submit(trigger, message_queue, symbol, exchange, qty, "SELL", price, rules)
            )

        for f in as_completed(sell_futures):
            try:
                f.result()
            except Exception as e:
                logger.error(f"Error in executing sell order: {e}, Traceback: {traceback.format_exc()}")

    # --- BUYs next ---
    buy_futures = []
    # Session fund allocation per buy
    per_buy_allocation = int(os.environ.get("FUND_ALLOCATION", 20000))
    # Track local committed spend to avoid over-alloc during concurrency
    committed = 0.0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for d in buy_decisions:
            symbol  = d["Symbol"]
            exchange= d["Exchange"]
            price   = _safe_float(d.get("Close"))
            rules   = d.get("ContributingRules")

            if price is None or price <= 0:
                logger.warning(f"Invalid price for {symbol}, skipping buy.")
                continue

            # Live funds
            try:
                funds = _safe_float(kite.margins("equity")["available"]["live_balance"], 0.0)
            except Exception as e:
                logger.error(f"Failed to fetch margins; skipping {symbol}: {e}")
                continue

            # Check available vs allocation & committed
            if funds - committed <= per_buy_allocation:
                logger.warning("Insufficient funds to place more buy orders. Stopping buy order processing.")
                break

            if not should_place_buy_order(symbol):
                continue

            qty = int(floor(per_buy_allocation / price))
            if qty <= 0:
                logger.warning(f"Calculated qty {qty} for {symbol} at {price} is not positive. Skipping.")
                continue

            # Optional: guardrail – minimum notional (e.g., ₹1000)
            min_notional = 500.0
            if qty * price < min_notional:
                logger.info(f"Order value too small for {symbol} ({qty*price:.2f} < {min_notional}). Skipping.")
                continue

            committed += qty * price

            # Rate-limit spacing
            now = time.monotonic()
            if now - last_call < inter_request_gap:
                time.sleep(inter_request_gap - (now - last_call))
            last_call = time.monotonic()

            buy_futures.append(
                executor.submit(trigger, message_queue, symbol, exchange, qty, "BUY", price, rules)
            )

        for f in as_completed(buy_futures):
            try:
                f.result()
            except Exception as e:
                logger.error(f"Error in executing buy order: {e}, Traceback: {traceback.format_exc()}")

    # End-of-cycle tiny cool-off
    time.sleep(0.1)