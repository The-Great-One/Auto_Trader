from multiprocessing import Pool, cpu_count
import pandas as pd
import sys
from Auto_Trader.KITE_TRIGGER_ORDER import handle_decisions
from Auto_Trader.utils import process_stock_and_decide, load_instruments_data
import logging
import traceback
import queue  # Import Python's queue module for handling empty exceptions

logger = logging.getLogger("Auto_Trade_Logger")

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
    
    with Pool(processes=cpu_cores) as pool:
        while True:
            try:
                # Get data from queue
                data = q.get(timeout=1)  # Assume data is a list of dictionaries
                if data is None:
                    logger.warning("Received shutdown signal. Exiting Apply_Rules.")
                    break  # Exit the loop if None is received (signal to stop)

                # Process the data by enriching it with instruments data
                for stock_data in data:
                    instrument_token = stock_data.get("instrument_token")
                    
                    # Merge instruments data into stock data
                    instrument_data = instruments_dict.get(instrument_token, {})
                    stock_data.update(instrument_data)  # Add instrument details to stock data
                    
                    # Add today's date
                    stock_data['Date'] = pd.Timestamp.today().strftime('%Y-%m-%d')

                # Use pool.map to process each stock in parallel
                results = pool.map(process_stock_and_decide, data)  # data is now a list of enriched dicts

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
                    logger.error(f"An error occurred while processing data: {e}, Traceback: {traceback.format_exc()}")
                    sys.exit(1)