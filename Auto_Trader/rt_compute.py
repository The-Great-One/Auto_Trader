from multiprocessing import Pool, cpu_count
import os
import pandas as pd
import sys
from Auto_Trader.KITE_TRIGGER_ORDER import handle_decisions
from Auto_Trader.utils import process_stock_and_decide, load_instruments_data
import logging
import traceback
import queue  # Import Python's queue module for handling empty exceptions

logger = logging.getLogger("Auto_Trade_Logger")
TRADING_MODE = os.getenv("AT_TRADING_MODE", "DAILY").strip().upper()
BAR_MINUTES = max(1, int(os.getenv("AT_BAR_MINUTES", "5")))


def _resolve_bar_timestamp(stock_data):
    ts = (
        stock_data.get("exchange_timestamp")
        or stock_data.get("last_trade_time")
        or stock_data.get("timestamp")
    )
    parsed = pd.to_datetime(ts, errors="coerce")
    if pd.isna(parsed):
        parsed = pd.Timestamp.now(tz="Asia/Kolkata")

    if TRADING_MODE == "INTRADAY":
        return parsed.floor(f"{BAR_MINUTES}min").tz_localize(None)
    return parsed.normalize().tz_localize(None)


def _update_intraday_bar(stock_data, bar_ts, bar_state, last_cum_volume):
    token = stock_data.get("instrument_token")
    price = float(stock_data.get("last_price", 0.0) or 0.0)
    if token is None or price <= 0:
        return

    cum_volume = float(stock_data.get("volume_traded", 0.0) or 0.0)
    prev_cum = last_cum_volume.get(token, cum_volume)
    delta_volume = max(0.0, cum_volume - prev_cum)
    last_cum_volume[token] = cum_volume

    prev_bar = bar_state.get(token)
    if prev_bar is None or prev_bar["ts"] != bar_ts:
        curr_bar = {
            "ts": bar_ts,
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "volume": delta_volume,
        }
    else:
        curr_bar = prev_bar
        curr_bar["high"] = max(curr_bar["high"], price)
        curr_bar["low"] = min(curr_bar["low"], price)
        curr_bar["close"] = price
        curr_bar["volume"] += delta_volume
    bar_state[token] = curr_bar

    ohlc = stock_data.get("ohlc") or {}
    ohlc["open"] = curr_bar["open"]
    ohlc["high"] = curr_bar["high"]
    ohlc["low"] = curr_bar["low"]
    ohlc["close"] = curr_bar["close"]
    stock_data["ohlc"] = ohlc
    stock_data["volume_traded"] = curr_bar["volume"]


def Apply_Rules(q, message_queue):
    """
    Continuously processes stock data from a queue, applies trading rules,
    and handles decisions to buy or sell stocks using multiprocessing.

    Parameters:
        q (multiprocessing.Queue): A queue containing stock data dictionaries for all stocks in a tick.
    """
    cpu_cores = cpu_count()  # Use all cores

    # Convert instruments_df to a dictionary where key is instrument_token
    instruments_dict = load_instruments_data()
    intraday_bar_state = {}
    last_cum_volume = {}
    with Pool(processes=cpu_cores) as pool:
        while True:
            try:
                # Get data from queue
                data = q.get()  # Assume data is a list of dictionaries
                if data is None:
                    logger.warning("Received shutdown signal. Exiting Apply_Rules.")
                    break  # Exit the loop if None is received (signal to stop)

                # Keep only the most recent queued snapshot to avoid stale processing.
                while True:
                    try:
                        newer = q.get_nowait()
                        if newer is None:
                            logger.warning(
                                "Received shutdown signal while draining queue."
                            )
                            return
                        data = newer
                    except queue.Empty:
                        break

                # Process the data by enriching it with instruments data
                for stock_data in data:
                    instrument_token = stock_data.get("instrument_token")

                    # Merge instruments data into stock data
                    instrument_data = instruments_dict.get(instrument_token, {})
                    stock_data.update(
                        instrument_data
                    )  # Add instrument details to stock data

                    bar_ts = _resolve_bar_timestamp(stock_data)
                    stock_data["Date"] = bar_ts
                    if TRADING_MODE == "INTRADAY":
                        _update_intraday_bar(
                            stock_data,
                            bar_ts,
                            intraday_bar_state,
                            last_cum_volume,
                        )

                # Use pool.map to process each stock in parallel
                chunk_size = max(1, len(data) // (cpu_cores * 4))
                results = pool.map(process_stock_and_decide, data, chunksize=chunk_size)

                # Filter out None results
                decisions = [decision for decision in results if decision is not None]

                # Handle the decisions
                if decisions:
                    handle_decisions(message_queue, decisions=decisions)

            except queue.Empty:
                # If the queue is empty, log a message and continue
                logger.info("No new data in the queue. Waiting for next tick.")
                continue
            except Exception as e:
                if isinstance(e, KeyboardInterrupt):
                    logger.info("Manual interrupt detected. Exiting gracefully.")
                    break
                else:
                    logger.error(
                        f"An error occurred while processing data: {e}, Traceback: {traceback.format_exc()}"
                    )
                    sys.exit(1)
