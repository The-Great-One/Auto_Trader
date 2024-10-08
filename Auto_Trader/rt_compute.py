from multiprocessing import Pool, cpu_count
import pandas as pd
import sys
from Auto_Trader.KITE_TRIGGER_ORDER import handle_decisions
from Auto_Trader.utils import process_stock_and_decide
import logging

logger = logging.getLogger("Auto_Trade_Logger")

def Apply_Rules(q, message_queue):
    """
    Continuously processes stock data from a queue, applies trading rules,
    and handles decisions to buy or sell stocks using multiprocessing.

    Parameters:
        q (multiprocessing.Queue): A queue containing stock data dictionaries.
    """
    # Read instruments data once, outside the loop
    try:
        instruments_df = pd.read_csv("intermediary_files/Instruments.csv")
    except Exception as e:
        logger.error(f"Failed to read Instruments.csv: {e}")
        return

    # Initialize multiprocessing pool
    cpu_cores = max(cpu_count() - 1, 1)
    with Pool(processes=cpu_cores) as pool:
        while True:
            try:
                data = q.get()
                if data is None:
                    logger.warning("Received shutdown signal. Exiting Apply_Rules.")
                    break  # Exit the loop if None is received (signal to stop)

                data_df = pd.DataFrame(data)[["last_price", "volume_traded", "instrument_token", "ohlc"]]
                data_df = pd.merge(data_df, instruments_df, on="instrument_token", how="inner")
                data_df['Date'] = pd.Timestamp.today().strftime('%Y-%m-%d')
                
                # Convert DataFrame rows to dictionaries for pickling
                rows = data_df.to_dict(orient='records')

                # Use pool.map to process stocks in parallel
                results = pool.map(process_stock_and_decide, rows)

                # Filter out None results
                decisions = [decision for decision in results if decision is not None]

                if decisions:
                    handle_decisions(message_queue, decisions=decisions)
                else:
                    pass
            except Exception as e:
                logger.error(f"An error occurred while processing data: {e}")
                sys.exit(1)
