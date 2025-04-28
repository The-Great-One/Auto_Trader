import logging
from filelock import FileLock, Timeout
import json
import os
import talib

logger = logging.getLogger("Auto_Trade_Logger")

HOLDINGS_FILE_PATH = 'intermediary_files/Holdings.json'
LOCK_FILE_PATH = 'intermediary_files/Holdings.lock'

def load_stop_loss_json():
    lock = FileLock(LOCK_FILE_PATH)
    try:
        with lock.acquire(timeout=10):
            if not os.path.exists(HOLDINGS_FILE_PATH):
                return {}
            with open(HOLDINGS_FILE_PATH, 'r') as json_file:
                try:
                    data = json.load(json_file)
                    # Ensure floats
                    for k, v in data.items():
                        try:
                            data[k] = float(v)
                        except (TypeError, ValueError):
                            logger.warning(f"Non-numeric stop-loss value for {k}, setting to None.")
                            data[k] = None
                    return data
                except json.JSONDecodeError as e:
                    logger.error(f"JSONDecodeError: {e}. The JSON file may be corrupted.")
                    return {}
    except Timeout:
        logger.error(f"Timeout while trying to acquire the lock for {HOLDINGS_FILE_PATH}.")
        return {}
    except Exception as e:
        logger.exception("Error loading stop-loss from JSON:")
        return {}


def update_stop_loss_json(tradingsymbol, stop_loss):
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
            try:
                stop_loss = round(float(stop_loss), 2)
            except (TypeError, ValueError):
                logger.error(f"Invalid stop_loss value for {tradingsymbol}: {stop_loss}")
                return
            
            holdings_dict[tradingsymbol] = stop_loss
            with open(HOLDINGS_FILE_PATH, 'w') as json_file:
                json.dump(holdings_dict, json_file, indent=4)
            logger.info(f"Updated stop-loss for {tradingsymbol} to {stop_loss:.2f} in JSON.")
    except Timeout:
        logger.error(f"Timeout while trying to acquire the lock for {HOLDINGS_FILE_PATH}.")
    except Exception as e:
        logger.exception("Error updating stop-loss in JSON:")


def handle_sell(tradingsymbol):
    try:
        stop_loss_data = load_stop_loss_json()
        if tradingsymbol in stop_loss_data:
            with FileLock(LOCK_FILE_PATH).acquire(timeout=10):
                stop_loss_data.pop(tradingsymbol, None)
                with open(HOLDINGS_FILE_PATH, 'w') as json_file:
                    json.dump(stop_loss_data, json_file, indent=4)
        logger.info(f"Removed {tradingsymbol} from stop-loss JSON after selling.")
    except Exception as e:
        logger.exception(f"Error while removing {tradingsymbol} from JSON:")
    return "SELL"


def buy_or_sell(df, row, holdings):
    """
    Comprehensive strategy for profit booking and stop-loss management.
    """
    # ------------------------------
    # 1. Preliminary validations
    # ------------------------------
    try:
        instrument_token = row['instrument_token']
    except KeyError:
        logger.error("Row data does not contain 'instrument_token'. Returning HOLD.")
        return "HOLD"
    except Exception as e:
        logger.exception("Unexpected error reading 'instrument_token' from row:")
        return "HOLD"

    try:
        holdings_symbol_data = holdings[holdings["instrument_token"] == instrument_token]
    except Exception as e:
        logger.exception("Error filtering holdings by instrument_token:")
        return "HOLD"

    if holdings_symbol_data.empty:
        logger.info(f"No holdings data for instrument_token {instrument_token}. Returning HOLD.")
        return "HOLD"

    try:
        tradingsymbol = holdings_symbol_data['tradingsymbol'].iloc[0]
        average_price = float(holdings_symbol_data['average_price'].iloc[0])
    except (KeyError, IndexError, ValueError, TypeError):
        logger.exception("Error extracting tradingsymbol or average_price from holdings:")
        return "HOLD"

    try:
        last_row = df.iloc[-1]
        last_price = float(last_row['Close'])
    except (IndexError, KeyError, TypeError, ValueError):
        logger.exception("Error extracting last price from df:")
        return "HOLD"

    # -----------------------------------
    # 2. Load stop_loss from JSON
    # -----------------------------------
    try:
        stop_loss_data = load_stop_loss_json()
        stop_loss = stop_loss_data.get(tradingsymbol, None)
        if stop_loss is not None:
            stop_loss = float(stop_loss)
    except Exception as e:
        logger.exception("Error loading stop_loss from JSON:")
        return "HOLD"

    # --------------------------------------------------
    # 3. Ensure baseline SL and extract indicators
    # --------------------------------------------------
    try:
        # Ensure we always have a starting SL: if none, set 2×ATR behind last_price
        last_atr = float(last_row.get("ATR", 0.0))
        new_stop_loss = stop_loss
        if new_stop_loss is None:
            new_stop_loss = last_price - (2.0 * last_atr)

        def safe_get(series, col, default=0.0):
            try:
                return float(series.get(col, default))
            except (TypeError, ValueError):
                logger.warning(f"Invalid value for {col}. Using default {default}")
                return default

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
    except Exception as e:
        logger.exception("Error initializing stop-loss or extracting indicators:")
        return "HOLD"

    try:
        upper_band, middle_band, lower_band = talib.BBANDS(
            df['Close'].astype(float), timeperiod=20, nbdevup=2, nbdevdn=2
        )
    except Exception as e:
        logger.exception("Error computing Bollinger Bands with TA-Lib:")
        return "HOLD"

    try:
        profit_percent = ((last_price - average_price) / average_price) * 100.0
    except Exception as e:
        logger.exception("Error calculating profit_percent:")
        return "HOLD"

    # -----------------------------------------
    # 4. Main logic: Loss vs Profit scenarios
    # -----------------------------------------
    try:
        if last_price < average_price:
            # Loss scenario
            if last_rsi < 50:
                new_stop_loss = max(new_stop_loss, last_price - (1.5 * last_atr))
            if last_rsi < 45:
                return handle_sell(tradingsymbol)
            if last_macd < last_macd_signal:
                new_stop_loss = max(new_stop_loss, last_price - (1.0 * last_atr))
            if talib.CDLENGULFING(df['Open'], df['High'], df['Low'], df['Close']).iloc[-1] != 0:
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
            if len(df) > 1 and float(df["MACD_Hist"].iloc[-2]) > macd_histogram:
                new_stop_loss = max(new_stop_loss, last_price - (1.0 * last_atr))

            if talib.CDLENGULFING(df['Open'], df['High'], df['Low'], df['Close']).iloc[-1] != 0:
                new_stop_loss = max(new_stop_loss, last_price - (0.8 * last_atr))
            if talib.CDLSHOOTINGSTAR(df['Open'], df['High'], df['Low'], df['Close']).iloc[-1] != 0:
                new_stop_loss = max(new_stop_loss, last_price - (0.7 * last_atr))
            if talib.CDLDOJI(df['Open'], df['High'], df['Low'], df['Close']).iloc[-1] != 0:
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
    except Exception as e:
        logger.exception("Error applying trading logic:")
        return "HOLD"

    # -------------------------------------------
    # 5. Final stop-loss check + JSON update
    # -------------------------------------------
    try:
        logger.debug(
            f"[Pre-Final-Check] {tradingsymbol}: old_stop_loss={stop_loss}, "
            f"new_stop_loss={new_stop_loss}, last_price={last_price}"
        )

        if new_stop_loss is not None:
            if new_stop_loss > last_price:
                logger.warning(
                    f"New stop-loss ({new_stop_loss}) > last_price ({last_price}) for {tradingsymbol}. "
                    "Clamping to last_price => immediate SELL."
                )
                new_stop_loss = last_price

            if stop_loss is None or new_stop_loss > stop_loss:
                update_stop_loss_json(tradingsymbol, new_stop_loss)

            if last_price <= new_stop_loss:
                return handle_sell(tradingsymbol)

        logger.debug(f"[Final] {tradingsymbol} => HOLD. SL={new_stop_loss}, last_price={last_price}")
        return "HOLD"

    except Exception as e:
        logger.exception("Error finalizing stop-loss and sell check:")
        return "HOLD"
