import pandas as pd
import numpy as np
import logging
import traceback
from filelock import FileLock, Timeout
import json
import os
from datetime import datetime
import talib

logger = logging.getLogger("Auto_Trade_Logger")

# Define the holdings file path and lock file path as constants
HOLDINGS_FILE_PATH = 'intermediary_files/Holdings.json'
LOCK_FILE_PATH = 'intermediary_files/Holdings.lock'

def safe_float(value, default=None):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default

def load_stop_loss_json():
    """
    Load the stop-loss data from the JSON file. If the file is corrupted,
    attempts to return an empty dictionary.
    Ensures that all loaded values are floats.
    """
    lock = FileLock(LOCK_FILE_PATH)
    try:
        with lock.acquire(timeout=10):
            if not os.path.exists(HOLDINGS_FILE_PATH):
                return {}

            with open(HOLDINGS_FILE_PATH, 'r') as json_file:
                try:
                    data = json.load(json_file)
                    # Ensure all values are floats
                    for k, v in data.items():
                        data[k] = safe_float(v, default=None)
                    return data
                except json.JSONDecodeError as e:
                    logger.error(f"JSONDecodeError: {e}. The JSON file may be corrupted.")
                    return {}
    except Timeout:
        logger.error(f"Timeout while trying to acquire the lock for {HOLDINGS_FILE_PATH}.")
        return {}
    except Exception as e:
        logger.error(f"Error loading stop-loss from JSON: {str(e)}")
        return {}

def update_stop_loss_json(tradingsymbol, stop_loss):
    """
    Update the stop-loss for a specific trading symbol in the JSON file.
    Rounds the stop-loss to 2 decimal places and ensures it's a float.
    """
    lock = FileLock(LOCK_FILE_PATH)
    try:
        with lock.acquire(timeout=10):
            if not os.path.exists(HOLDINGS_FILE_PATH):
                holdings_dict = {}
            else:
                with open(HOLDINGS_FILE_PATH, 'r') as json_file:
                    try:
                        holdings_dict = json.load(json_file)
                    except json.JSONDecodeError:
                        logger.error("Corrupted JSON file. Starting fresh.")
                        holdings_dict = {}

            # Ensure floats and rounding
            stop_loss = round(float(stop_loss), 2)

            # Update the stop-loss for the trading symbol
            holdings_dict[tradingsymbol] = stop_loss

            # Write the updated dictionary back to the JSON file
            with open(HOLDINGS_FILE_PATH, 'w') as json_file:
                json.dump(holdings_dict, json_file, indent=4)
            logger.info(f"Updated stop-loss for {tradingsymbol} to {stop_loss:.2f} in JSON.")
    except Timeout:
        logger.error(f"Timeout while trying to acquire the lock for {HOLDINGS_FILE_PATH}.")
    except Exception as e:
        logger.error(f"Error updating stop-loss in JSON: {str(e)}")

def handle_sell(tradingsymbol):
    """
    Handles the cleanup for a sell event by removing the trading symbol
    from the stop-loss JSON and then returning 'SELL'.
    """
    try:
        # Load JSON data and remove the trading symbol
        stop_loss_data = load_stop_loss_json()
        if tradingsymbol in stop_loss_data:
            del stop_loss_data[tradingsymbol]
            with FileLock(LOCK_FILE_PATH).acquire(timeout=10):
                with open(HOLDINGS_FILE_PATH, 'w') as json_file:
                    json.dump(stop_loss_data, json_file, indent=4)
        logger.info(f"Removed {tradingsymbol} from stop-loss JSON after selling.")
    except Exception as e:
        logger.error(f"Error while removing {tradingsymbol} from JSON: {str(e)}")
    return "SELL"

def buy_or_sell(df, row, holdings):
    """
    Comprehensive strategy for profit booking and stop-loss management.
    """
    try:
        instrument_token = row['instrument_token']
        holdings_symbol_data = holdings[holdings["instrument_token"] == instrument_token]

        if holdings_symbol_data.empty:
            logger.info(f"No holdings data for instrument_token {instrument_token}. Returning HOLD.")
            return "HOLD"

        tradingsymbol = holdings_symbol_data['tradingsymbol'].iloc[0]
        average_price = float(holdings_symbol_data['average_price'].iloc[0])

        last_row = df.iloc[-1]
        last_price = float(last_row['Close'])

        # Load existing stop-loss
        stop_loss_data = load_stop_loss_json()
        stop_loss = stop_loss_data.get(tradingsymbol, None)
        if stop_loss is not None:
            stop_loss = safe_float(stop_loss, default=None)

        # Extract indicators safely, ensuring float
        def safe_get(row, col, default=0.0):
            val = row.get(col, default)
            return safe_float(val, default)

        last_atr = safe_get(last_row, "ATR")
        last_rsi = safe_get(last_row, "RSI")
        last_macd = safe_get(last_row, "MACD")
        last_macd_signal = safe_get(last_row, "MACD_Signal")
        macd_histogram = safe_get(last_row, "MACD_Hist")
        current_volume = safe_get(last_row, "Volume")
        avg_volume_20 = df['Volume'].rolling(window=20).mean().iloc[-1] if len(df) >= 20 else current_volume
        relative_volume = current_volume / avg_volume_20 if avg_volume_20 != 0 else 1.0

        ema_10 = safe_get(last_row, "EMA10")
        ema_50 = safe_get(last_row, "EMA50")
        fib_38_2 = safe_get(last_row, "Fibonacci_38_2")
        fib_61_8 = safe_get(last_row, "Fibonacci_61_8")

        # Compute Bollinger Bands using TA-Lib
        upper_band, middle_band, lower_band = talib.BBANDS(
            df['Close'].astype(float), timeperiod=20, nbdevup=2, nbdevdn=2
        )

        profit_percent = ((last_price - average_price) / average_price) * 100.0
        new_stop_loss = stop_loss

        # Begin logic
        if last_price < average_price:
            # Set initial stop-loss if None
            if new_stop_loss is None:
                new_stop_loss = last_price - (2.0 * last_atr)

            # Tighten stop-loss if indicators show weakening momentum
            if last_rsi < 50:
                new_stop_loss = max(new_stop_loss, last_price - (1.5 * last_atr))
            if last_rsi < 45:
                logger.info(f"RSI below 45 for {tradingsymbol}. Exiting to prevent further losses.")
                return handle_sell(tradingsymbol)
            if last_macd < last_macd_signal:
                new_stop_loss = max(new_stop_loss, last_price - (1.0 * last_atr))
            if talib.CDLENGULFING(df['Open'], df['High'], df['Low'], df['Close']).iloc[-1] != 0:
                new_stop_loss = last_price - (1.0 * last_atr)

            # Check for Fibonacci support levels
            if last_price < fib_61_8:
                logger.info(f"Price below 61.8% Fibonacci level for {tradingsymbol}. Exiting position.")
                return handle_sell(tradingsymbol)

        else:
            # Profit scenario
            if profit_percent > 20:
                tmp_sl = max(last_price - (0.5 * last_atr), average_price * 1.15)
                # Ensure stop-loss is not above last_price
                new_stop_loss = tmp_sl
            elif profit_percent > 15:
                tmp_sl = max(last_price - (0.8 * last_atr), average_price * 1.10)
                new_stop_loss = tmp_sl
            elif profit_percent > 10:
                tmp_sl = max(last_price - (1.0 * last_atr), average_price * 1.07)
                new_stop_loss = tmp_sl
            elif profit_percent > 5:
                tmp_sl = max(last_price - (1.2 * last_atr), average_price * 1.05)
                new_stop_loss = tmp_sl
            elif profit_percent > 0:
                tmp_sl = max(last_price - (1.5 * last_atr), average_price * 0.98)
                new_stop_loss = tmp_sl

            # RSI adjustments
            if last_rsi > 75:
                new_stop_loss = max(new_stop_loss, last_price - (0.5 * last_atr))
            elif last_rsi > 70:
                new_stop_loss = max(new_stop_loss, last_price - (0.8 * last_atr))
            elif last_rsi > 65:
                new_stop_loss = max(new_stop_loss, last_price - (1.0 * last_atr))

            # MACD adjustments
            if last_macd < last_macd_signal:
                new_stop_loss = max(new_stop_loss, last_price - (0.8 * last_atr))
            if len(df) > 1 and safe_float(df["MACD_Hist"].iloc[-2], 0) > macd_histogram:
                new_stop_loss = max(new_stop_loss, last_price - (1.0 * last_atr))

            # Chart patterns
            if talib.CDLENGULFING(df['Open'], df['High'], df['Low'], df['Close']).iloc[-1] != 0:
                new_stop_loss = max(new_stop_loss, last_price - (0.8 * last_atr))
            if talib.CDLSHOOTINGSTAR(df['Open'], df['High'], df['Low'], df['Close']).iloc[-1] != 0:
                new_stop_loss = max(new_stop_loss, last_price - (0.7 * last_atr))
            if talib.CDLDOJI(df['Open'], df['High'], df['Low'], df['Close']).iloc[-1] != 0:
                new_stop_loss = max(new_stop_loss, last_price - (0.6 * last_atr))

            # Fibonacci checks
            if last_price < fib_38_2 and last_rsi < 50:
                logger.info(f"Price below 38.2% Fibonacci level with RSI < 50 for {tradingsymbol}. Exiting position.")
                return handle_sell(tradingsymbol)
            elif last_price < fib_61_8 and last_rsi < 45:
                logger.info(f"Price below 61.8% Fibonacci level with RSI < 45 for {tradingsymbol}. Exiting position.")
                return handle_sell(tradingsymbol)

            # Bollinger Bands checks
            if last_price >= upper_band.iloc[-1] and last_rsi > 65:
                new_stop_loss = max(new_stop_loss, last_price - (0.9 * last_atr))
            if last_price >= upper_band.iloc[-1] and last_rsi > 70:
                new_stop_loss = max(new_stop_loss, last_price - (0.8 * last_atr))

            # EMA checks
            if last_price < ema_10 and last_rsi < 55:
                logger.info(f"Price below EMA 10 with RSI < 55 for {tradingsymbol}. Exiting position.")
                return handle_sell(tradingsymbol)
            if last_price < ema_50 and relative_volume > 1.5:
                logger.info(f"Price below EMA 50 with significant volume spike for {tradingsymbol}. Exiting position.")
                return handle_sell(tradingsymbol)

        # Ensure new_stop_loss is valid float
        if new_stop_loss is not None:
            new_stop_loss = float(new_stop_loss)
            # Ensure trailing stop is not above current price (to avoid immediate unintended sell)
            if new_stop_loss > last_price:
                logger.warning(f"New stop-loss ({new_stop_loss}) > last_price ({last_price}) for {tradingsymbol}. Adjusting.")
                new_stop_loss = last_price

            # Update stop-loss if it improved (increased) from old stop-loss or old was None
            if stop_loss is None or new_stop_loss > stop_loss:
                update_stop_loss_json(tradingsymbol, new_stop_loss)

            # Final check for stop-loss hit
            if last_price <= new_stop_loss:
                logger.info(f"Stop-loss hit for {tradingsymbol}. Returning SELL.")
                return handle_sell(tradingsymbol)

        return "HOLD"

    except Exception as e:
        logger.error(f"Error processing {row['instrument_token']}: Traceback: {traceback.format_exc()}")
        return "HOLD"