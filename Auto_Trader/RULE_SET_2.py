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
    Determine whether to sell based on dynamically calculated stop-loss.
    Returns "SELL" if the stop-loss is triggered, otherwise "HOLD".
    Updates the stop-loss in the JSON file when necessary.
    """
    try:
        # Extract instrument_token from the row
        instrument_token = row['instrument_token']
                
        # Filter holdings using instrument_token
        holdings_symbol_data = holdings[holdings["instrument_token"] == instrument_token]

        if holdings_symbol_data.empty:
            logger.debug(f"No holdings data for instrument_token {instrument_token}. Returning HOLD.")
            return "HOLD"
        # Extract the tradingsymbol from holdings
        tradingsymbol = holdings_symbol_data['tradingsymbol'].iloc[0]
        average_price = holdings_symbol_data['average_price'].iloc[0]
        
        # Extract the last price
        last_price = df['Close'].iloc[-1]

        # Debug: Log extracted values
        logger.debug(f"Processing {tradingsymbol} with instrument_token {instrument_token}")
        logger.debug(f"Average Price: {average_price}, Last Price: {last_price}")

        if average_price == 0:
            logger.warning(f"Average price is zero for {tradingsymbol}. Returning HOLD.")
            return "HOLD"

        # Load existing stop-loss from JSON
        stop_loss_data = load_stop_loss_json()
        stop_loss = stop_loss_data.get(tradingsymbol, None)
        

        # Debug: Log loaded stop-loss
        logger.debug(f"Loaded stop-loss for {tradingsymbol}: {stop_loss}")
        # Extract indicators
        last_atr = df["ATR"].iloc[-1]
        last_rsi = df["RSI"].iloc[-1]
        last_macd = df["MACD"].iloc[-1]
        last_macd_signal = df["MACD_Signal"].iloc[-1]
        last_ema_10 = df["EMA10"].iloc[-1]
        last_ema_50 = df["EMA50"].iloc[-1]

        # Debug: Log indicator values
        logger.debug(f"Indicators for {tradingsymbol}: ATR={last_atr}, RSI={last_rsi}, MACD={last_macd}, MACD_SIGNAL={last_macd_signal}, EMA10={last_ema_10}, EMA50={last_ema_50}")

        is_profit = last_price > average_price
        new_stop_loss = stop_loss

        # Debug: Log initial stop-loss and profit status
        logger.debug(f"Initial stop-loss: {new_stop_loss}, Is Profit: {is_profit}")

        # Calculate initial stop-loss if not set
        if stop_loss is None:
            if is_profit:
                new_stop_loss = max(last_price - (1.0 * last_atr), average_price)
                logger.debug(f"Calculated initial stop-loss for profit: {new_stop_loss}")
            else:
                new_stop_loss = last_price - (1.5 * last_atr)
                logger.debug(f"Calculated initial stop-loss for loss: {new_stop_loss}")
        else:
            # Stop-loss adjustment for profit-making stocks
            if is_profit:
                if last_rsi >= 75 or (last_macd < last_macd_signal and last_rsi > 65):
                    potential_sl = last_price - (0.4 * last_atr)
                elif last_rsi >= 70 or (last_macd < last_macd_signal):
                    potential_sl = last_price - (0.6 * last_atr)
                elif last_rsi >= 65:
                    potential_sl = last_price - (0.8 * last_atr)
                else:
                    if last_price > last_ema_10 and last_price > last_ema_50:
                        potential_sl = last_price - (1.0 * last_atr)
                    else:
                        potential_sl = new_stop_loss
                new_stop_loss = max(new_stop_loss, potential_sl)
                logger.debug(f"Adjusted stop-loss for profit: {new_stop_loss}")
            else:
                # Stop-loss adjustment for loss-making stocks
                if last_rsi <= 45 or last_macd < last_macd_signal:
                    logger.debug(f"Sell triggered for {tradingsymbol} due to RSI/MACD conditions.")
                    return "SELL"
                elif last_rsi <= 50:
                    potential_sl = last_price - (0.4 * last_atr)
                elif last_rsi <= 55:
                    potential_sl = last_price - (0.6 * last_atr)
                else:
                    potential_sl = new_stop_loss
                new_stop_loss = max(new_stop_loss, potential_sl)
                logger.debug(f"Adjusted stop-loss for loss: {new_stop_loss}")

        # Validate new_stop_loss before saving
        if pd.isnull(new_stop_loss) or new_stop_loss <= 0:
            logger.error(f"Calculated stop-loss is NaN or invalid for {tradingsymbol}. Returning HOLD.")
            return "HOLD"

        # Update the stop-loss if the new SL is higher than the existing SL
        if stop_loss is None or new_stop_loss > stop_loss:
            update_stop_loss_json(tradingsymbol, new_stop_loss)
            logger.debug(f"Updated stop-loss for {tradingsymbol} to {new_stop_loss:.2f}")

        # Check if SL is hit
        if last_price <= new_stop_loss:
            logger.debug(f"Stop-loss hit for {tradingsymbol}. Returning SELL.")
            return "SELL"
        return "HOLD"

    except Exception as e:
        logger.error(f"Error processing instrument_token {row['instrument_token']}: {str(e)}. Returning HOLD.\nTraceback: {traceback.format_exc()}")
        return "HOLD"
