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

def load_stop_loss_json():
    """
    Load the stop-loss data from the JSON file. If the file is corrupted,
    attempts to return an empty dictionary.
    """
    lock = FileLock(LOCK_FILE_PATH)
    try:
        with lock.acquire(timeout=10):
            if not os.path.exists(HOLDINGS_FILE_PATH):
                return {}

            with open(HOLDINGS_FILE_PATH, 'r') as json_file:
                try:
                    return json.load(json_file)
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
        

def buy_or_sell(df, row, holdings):
    """
    Comprehensive strategy for profit booking and stop-loss management.
    Handles both profitable and loss-making positions.
    Uses profit percentage, RSI, MACD, chart patterns, Fibonacci levels, ATR, volume, Bollinger Bands, and EMAs.
    """
    try:
        # Extract instrument_token and filter holdings
        instrument_token = row['instrument_token']
        holdings_symbol_data = holdings[holdings["instrument_token"] == instrument_token]

        if holdings_symbol_data.empty:
            logger.info(f"No holdings data for instrument_token {instrument_token}. Returning HOLD.")
            return "HOLD"

        tradingsymbol = holdings_symbol_data['tradingsymbol'].iloc[0]
        average_price = holdings_symbol_data['average_price'].iloc[0]

        # Extract the latest row data directly using iloc[-1]
        last_row = df.iloc[-1]
        last_price = last_row['Close']

        # Load existing stop-loss from JSON
        stop_loss_data = load_stop_loss_json()
        stop_loss = stop_loss_data.get(tradingsymbol, None)

        last_atr = last_row["ATR"]
        last_rsi = last_row["RSI"]
        last_macd = last_row["MACD"]
        last_macd_signal = last_row["MACD_Signal"]
        macd_histogram = last_row["MACD_Hist"]
        current_volume = last_row['Volume']

        avg_volume_20 = df['Volume'].rolling(window=20).mean().iloc[-1]
        relative_volume = current_volume / avg_volume_20

        # Moving Averages
        ema_10 = last_row["EMA10"]
        ema_50 = last_row["EMA50"]

        # Bollinger Bands
        upper_band, middle_band, lower_band = talib.BBANDS(df['Close'], timeperiod=20, nbdevup=2, nbdevdn=2)

        fib_38_2 = last_row["Fibonacci_38_2"]
        fib_61_8 = last_row["Fibonacci_61_8"]

        profit_percent = ((last_price - average_price) / average_price) * 100
        new_stop_loss = stop_loss

        # 1. Handle Loss-Making Positions
        if last_price < average_price:
            # Set a looser initial stop-loss using ATR
            if stop_loss is None:
                new_stop_loss = last_price - (2.0 * last_atr)
            
            # Tighten stop-loss if indicators show weakening momentum
            if last_rsi < 50:
                new_stop_loss = max(new_stop_loss, last_price - (1.5 * last_atr))
            if last_rsi < 45:
                logger.info(f"RSI below 45 for {tradingsymbol}. Exiting to prevent further losses.")
                return "SELL"
            if last_macd < last_macd_signal:
                new_stop_loss = max(new_stop_loss, last_price - (1.0 * last_atr))
            if talib.CDLENGULFING(df['Open'], df['High'], df['Low'], df['Close']).iloc[-1] != 0:
                new_stop_loss = last_price - (1.0 * last_atr)
            
            # Check for Fibonacci support levels
            if last_price < fib_61_8:
                logger.info(f"Price below 61.8% Fibonacci level for {tradingsymbol}. Exiting position.")
                return "SELL"

        # 2. Profit Percentage-Based Stop-Loss for Profitable Positions
        else:
            if profit_percent > 20:
                new_stop_loss = max(last_price - (0.5 * last_atr), average_price * 1.15)
            elif profit_percent > 15:
                new_stop_loss = max(last_price - (0.8 * last_atr), average_price * 1.10)
            elif profit_percent > 10:
                new_stop_loss = max(last_price - (1.0 * last_atr), average_price * 1.07)
            elif profit_percent > 5:
                new_stop_loss = max(last_price - (1.2 * last_atr), average_price * 1.05)

            # 3. RSI-Based Adjustments for Profitable Positions
            if last_rsi > 75:
                new_stop_loss = last_price - (0.5 * last_atr)
            elif last_rsi > 70:
                new_stop_loss = last_price - (0.8 * last_atr)
            elif last_rsi > 65:
                new_stop_loss = last_price - (1.0 * last_atr)

            # 4. MACD Crossovers and Histogram Analysis
            if last_macd < last_macd_signal:
                new_stop_loss = max(new_stop_loss, last_price - (0.8 * last_atr))
            if len(df) > 1 and df["MACD_Hist"].iloc[-2] > macd_histogram:
                new_stop_loss = max(new_stop_loss, last_price - (1.0 * last_atr))

            # 5. Chart Patterns Detection (Bearish Reversal Patterns)
            if talib.CDLENGULFING(df['Open'], df['High'], df['Low'], df['Close']).iloc[-1] != 0:
                new_stop_loss = last_price - (0.8 * last_atr)
            if talib.CDLSHOOTINGSTAR(df['Open'], df['High'], df['Low'], df['Close']).iloc[-1] != 0:
                new_stop_loss = last_price - (0.7 * last_atr)
            if talib.CDLDOJI(df['Open'], df['High'], df['Low'], df['Close']).iloc[-1] != 0:
                new_stop_loss = last_price - (0.6 * last_atr)

            # 6. Fibonacci Levels Confirmation
            if last_price < fib_38_2 and last_rsi < 50:
                logger.info(f"Price below 38.2% Fibonacci level with RSI < 50 for {tradingsymbol}. Exiting position.")
                return "SELL"
            elif last_price < fib_61_8 and last_rsi < 45:
                logger.info(f"Price below 61.8% Fibonacci level with RSI < 45 for {tradingsymbol}. Exiting position.")
                return "SELL"

            # 7. Bollinger Bands Overextension Check
            if last_price >= upper_band.iloc[-1] and last_rsi > 70:
                new_stop_loss = max(new_stop_loss, last_price - (0.8 * last_atr))

            # 8. Moving Averages Confirmation (EMA 10 & EMA 50)
            if last_price < ema_10 and last_rsi < 55:
                logger.info(f"Price below EMA 10 with RSI < 55 for {tradingsymbol}. Exiting position.")
                return "SELL"
            if last_price < ema_50 and relative_volume > 1.5:
                logger.info(f"Price below EMA 50 with significant volume spike for {tradingsymbol}. Exiting position.")
                return "SELL"

        # Save updated stop-loss if applicable
        if stop_loss is None or new_stop_loss > stop_loss:
            update_stop_loss_json(tradingsymbol, new_stop_loss)

        # Execute Sell if Stop-Loss is Hit
        if last_price <= new_stop_loss:
            logger.info(f"Stop-loss hit for {tradingsymbol}. Returning SELL.")
            return "SELL"
        return "HOLD"

    except Exception as e:
        logger.error(f"Error processing {row['instrument_token']}: {str(e)}")
        return "HOLD"