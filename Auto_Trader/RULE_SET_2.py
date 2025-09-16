import logging
from filelock import FileLock, Timeout
import json
import os
import time
import random
import talib
import numpy as np

logger = logging.getLogger("Auto_Trade_Logger")

# ---------- Paths ----------
BASE_DIR = os.path.abspath(os.getenv("AT_STATE_DIR", "./intermediary_files"))
os.makedirs(BASE_DIR, exist_ok=True)

HOLDINGS_FILE_PATH = os.path.join(BASE_DIR, "Holdings.json")
LOCK_FILE_PATH = os.path.join(BASE_DIR, "Holdings.lock")


# ---------- Helpers ----------
def _atomic_write(data: dict, path: str):
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, indent=4)
    os.replace(tmp, path)


def _finite(val, default=0.0):
    try:
        val = float(val)
        if np.isfinite(val):
            return val
        return default
    except Exception:
        return default


def load_stop_loss_json():
    for attempt in range(3):
        try:
            with FileLock(LOCK_FILE_PATH, timeout=30):
                if not os.path.exists(HOLDINGS_FILE_PATH):
                    return {}
                with open(HOLDINGS_FILE_PATH, "r") as f:
                    data = json.load(f)
                out = {}
                for k, v in data.items():
                    try:
                        out[k] = float(v)
                    except Exception:
                        logger.warning("Non-numeric stop-loss for %s; setting None", k)
                        out[k] = None
                return out
        except Timeout:
            logger.warning("Timeout acquiring lock (read), retry %d", attempt + 1)
            time.sleep(0.2 * (attempt + 1) + random.random() * 0.1)
        except Exception:
            logger.exception("Load stop-loss JSON failed")
            return {}
    logger.error("Failed to load stop-loss JSON after retries")
    return {}


def update_stop_loss_json(tradingsymbol, stop_loss):
    try:
        stop_loss = round(float(stop_loss), 2)
    except Exception:
        logger.error("Invalid stop_loss for %s: %r", tradingsymbol, stop_loss)
        return

    for attempt in range(3):
        try:
            with FileLock(LOCK_FILE_PATH, timeout=30):
                data = {}
                if os.path.exists(HOLDINGS_FILE_PATH):
                    try:
                        with open(HOLDINGS_FILE_PATH, "r") as f:
                            data = json.load(f)
                    except json.JSONDecodeError:
                        logger.error("Corrupted JSON; starting fresh")
                data[tradingsymbol] = stop_loss
                _atomic_write(data, HOLDINGS_FILE_PATH)
                logger.info("Updated stop-loss for %s to %.2f", tradingsymbol, stop_loss)
                return
        except Timeout:
            logger.warning("Timeout acquiring lock (write), retry %d", attempt + 1)
            time.sleep(0.2 * (attempt + 1) + random.random() * 0.1)
        except Exception:
            logger.exception("Update stop-loss JSON failed")
            return
    logger.error("Failed to update stop-loss JSON after retries")


def handle_sell(tradingsymbol):
    try:
        stop_loss_data = load_stop_loss_json()
        with FileLock(LOCK_FILE_PATH, timeout=30):
            stop_loss_data.pop(tradingsymbol, None)
            _atomic_write(stop_loss_data, HOLDINGS_FILE_PATH)
        logger.info("Removed %s from stop-loss JSON after selling", tradingsymbol)
    except Exception:
        logger.exception("Error while removing %s from JSON:", tradingsymbol)
    return "SELL"


# ---------- Main strategy ----------
def buy_or_sell(df, row, holdings):
    try:
        instrument_token = int(row["instrument_token"])
        holdings = holdings.assign(instrument_token=holdings["instrument_token"].astype(int))
    except Exception:
        logger.exception("Row/holdings missing instrument_token")
        return "HOLD"

    try:
        holdings_symbol_data = holdings[holdings["instrument_token"] == instrument_token]
    except Exception:
        logger.exception("Error filtering holdings by instrument_token")
        return "HOLD"

    if holdings_symbol_data.empty:
        logger.debug("No holdings data for instrument_token %s. HOLD", instrument_token)
        return "HOLD"

    try:
        tradingsymbol = holdings_symbol_data["tradingsymbol"].iloc[0]
        average_price = float(holdings_symbol_data["average_price"].iloc[0])
    except Exception:
        logger.exception("Error extracting tradingsymbol/average_price")
        return "HOLD"

    try:
        last_row = df.iloc[-1]
        last_price = float(last_row["Close"])
    except Exception:
        logger.exception("Error extracting last price from df")
        return "HOLD"

    # Load stop-loss
    stop_loss_data = load_stop_loss_json()
    stop_loss = stop_loss_data.get(tradingsymbol, None)
    if stop_loss is not None:
        stop_loss = _finite(stop_loss, None)

    # Indicators with safe fallbacks
    last_atr = _finite(last_row.get("ATR", 0.0), 0.0)
    if not (last_atr > 0):
        last_atr = max(0.01, 0.02 * last_price)

    def safe_get(series, col, default=0.0):
        return _finite(series.get(col, default), default)

    last_rsi = safe_get(last_row, "RSI", 50)
    last_macd = safe_get(last_row, "MACD", 0.0)
    last_macd_signal = safe_get(last_row, "MACD_Signal", 0.0)
    macd_histogram = safe_get(last_row, "MACD_Hist", 0.0)
    current_volume = safe_get(last_row, "Volume", 0.0)

    avg_volume_20 = (
        df["Volume"].rolling(window=20).mean().iloc[-1]
        if len(df) >= 20
        else current_volume
    )
    relative_volume = current_volume / avg_volume_20 if avg_volume_20 else 1.0

    ema_10 = safe_get(last_row, "EMA10")
    ema_50 = safe_get(last_row, "EMA50")
    fib_38_2 = safe_get(last_row, "Fibonacci_38_2")
    fib_61_8 = safe_get(last_row, "Fibonacci_61_8")

    try:
        upper_band, middle_band, lower_band = talib.BBANDS(
            df["Close"].astype(float), timeperiod=20, nbdevup=2, nbdevdn=2
        )
    except Exception:
        logger.exception("Error computing Bollinger Bands")
        return "HOLD"

    try:
        profit_percent = ((last_price - average_price) / average_price) * 100.0
    except Exception:
        logger.exception("Error calculating profit_percent")
        return "HOLD"

    # --- Main logic ---
    new_stop_loss = stop_loss if stop_loss is not None else last_price - (2.0 * last_atr)

    try:
        if last_price < average_price:
            # Loss scenario
            if last_rsi < 50:
                new_stop_loss = max(new_stop_loss, last_price - (1.5 * last_atr))
            if last_rsi < 45:
                return handle_sell(tradingsymbol)
            if last_macd < last_macd_signal:
                new_stop_loss = max(new_stop_loss, last_price - (1.0 * last_atr))
            if talib.CDLENGULFING(df["Open"], df["High"], df["Low"], df["Close"]).iloc[-1] != 0:
                new_stop_loss = last_price - (1.0 * last_atr)
            if last_price < fib_61_8:
                return handle_sell(tradingsymbol)
        else:
            # Profit scenario
            if profit_percent > 30:
                tmp_sl = last_price - (0.2 * last_atr)
                new_stop_loss = max(new_stop_loss, tmp_sl)
            if profit_percent > 20:
                tmp_sl = max(last_price - (0.3 * last_atr), average_price * 1.15)
                new_stop_loss = max(new_stop_loss, tmp_sl)
            elif profit_percent > 15:
                tmp_sl = max(last_price - (0.5 * last_atr), average_price * 1.10)
                new_stop_loss = max(new_stop_loss, tmp_sl)
            elif profit_percent > 10:
                tmp_sl = max(last_price - (0.8 * last_atr), average_price * 1.07)
                new_stop_loss = max(new_stop_loss, tmp_sl)
            elif profit_percent > 5:
                tmp_sl = max(last_price - (1.2 * last_atr), average_price * 1.05)
                new_stop_loss = max(new_stop_loss, tmp_sl)
            elif profit_percent >= 0:
                tmp_sl = max(last_price - (1.5 * last_atr), average_price * 0.98)
                new_stop_loss = max(new_stop_loss, tmp_sl)

            if last_rsi > 75:
                new_stop_loss = max(new_stop_loss, last_price - (0.5 * last_atr))
            elif last_rsi > 70:
                new_stop_loss = max(new_stop_loss, last_price - (0.8 * last_atr))
            elif last_rsi > 65:
                new_stop_loss = max(new_stop_loss, last_price - (1.0 * last_atr))

            if last_macd < last_macd_signal:
                new_stop_loss = max(new_stop_loss, last_price - (0.8 * last_atr))
            if len(df) > 1 and _finite(df["MACD_Hist"].iloc[-2], 0.0) > macd_histogram:
                new_stop_loss = max(new_stop_loss, last_price - (1.0 * last_atr))

            if talib.CDLENGULFING(df["Open"], df["High"], df["Low"], df["Close"]).iloc[-1] != 0:
                new_stop_loss = max(new_stop_loss, last_price - (0.8 * last_atr))
            if talib.CDLSHOOTINGSTAR(df["Open"], df["High"], df["Low"], df["Close"]).iloc[-1] != 0:
                new_stop_loss = max(new_stop_loss, last_price - (0.7 * last_atr))
            if talib.CDLDOJI(df["Open"], df["High"], df["Low"], df["Close"]).iloc[-1] != 0:
                new_stop_loss = max(new_stop_loss, last_price - (0.6 * last_atr))

            if last_price < fib_38_2 and last_rsi < 50:
                return handle_sell(tradingsymbol)
            elif last_price < fib_61_8 and last_rsi < 45:
                return handle_sell(tradingsymbol)

            if last_price >= upper_band.iloc[-1] and last_rsi > 60:
                new_stop_loss = max(new_stop_loss, last_price - (0.9 * last_atr))
            if last_price >= upper_band.iloc[-1] and last_rsi > 65:
                new_stop_loss = max(new_stop_loss, last_price - (0.8 * last_atr))
            if last_price >= upper_band.iloc[-1] and last_rsi > 70:
                new_stop_loss = max(new_stop_loss, last_price - (0.7 * last_atr))

            if last_price < ema_10 and last_rsi < 55:
                return handle_sell(tradingsymbol)
            if last_price < ema_50 and relative_volume > 1.5:
                return handle_sell(tradingsymbol)
    except Exception:
        logger.exception("Error applying trading logic")
        return "HOLD"

    # --- Final SL check & update ---
    try:
        if new_stop_loss is not None:
            if new_stop_loss > last_price:
                logger.warning(
                    "New SL (%.2f) > last_price (%.2f) for %s. Clamp to last_price.",
                    new_stop_loss, last_price, tradingsymbol,
                )
                new_stop_loss = last_price

            if stop_loss is None or new_stop_loss > stop_loss:
                update_stop_loss_json(tradingsymbol, new_stop_loss)

            if last_price <= new_stop_loss:
                return handle_sell(tradingsymbol)

        return "HOLD"
    except Exception:
        logger.exception("Error finalizing stop-loss and sell check")
        return "HOLD"