import glob
import pandas as pd
import traceback
from .utils import fetch_instruments_list, get_instrument_token, fetch_holdings
from Auto_Trader import logging
from .StrongFundamentalsStockList import goodStocks
from .FetchPricesYfinance import download_historical_quotes
import sys

logger = logging.getLogger("Auto_Trade_Logger")

def create_master():
    """
    Creates a master list of instruments with their respective tokens, downloads historical quotes,
    and saves the merged data to a CSV file.

    Returns:
        List[int]: A list of instrument tokens that have been processed.
    """
    try:
        # Fetch instrument master list and good stocks list
        try:
            instrument_master = fetch_instruments_list()
            holdings = fetch_holdings()
            ticker_tape_list = goodStocks()
            logger.info("Fetched instrument master, holdings, and good stock lists successfully.")
        except Exception as e:
            logger.error(f"Error fetching data: {str(e)}\n{traceback.format_exc()}")
            sys.exit(1)

        # Rename columns in holdings to match those in merged_df
        try:
            holdings.rename(columns={"tradingsymbol": "Symbol"}, inplace=True)
        except Exception as e:
            logger.error(f"Error renaming columns in holdings: {str(e)}\n{traceback.format_exc()}")
            sys.exit(1)

        # Map the good stock list with instrument tokens
        try:
            mapped_df = get_instrument_token(
                good_stock_list_df=ticker_tape_list, instruments_df=instrument_master
            )
            logger.info("Mapped good stock list with instrument tokens.")
        except Exception as e:
            logger.error(f"Error mapping instrument tokens: {str(e)}\n{traceback.format_exc()}")
            sys.exit(1)

        # Concatenate the holdings DataFrame to the mapped_df
        try:
            mapped_df = pd.concat([mapped_df, holdings], ignore_index=True)
            # Drop duplicates based on the 'Symbol' column
            mapped_df.drop_duplicates(subset=['Symbol'], inplace=True)
            logger.info("Merged holdings with mapped data and removed duplicates.")
        except Exception as e:
            logger.error(f"Error merging holdings data: {str(e)}\n{traceback.format_exc()}")
            sys.exit(1)

        # Download historical quotes for the mapped instruments
        try:
            download_historical_quotes(df=mapped_df)
            logger.info("Downloaded historical quotes for mapped instruments.")
        except Exception as e:
            logger.error(f"Error downloading historical quotes: {str(e)}\n{traceback.format_exc()}")
            sys.exit(1)

        # Fetch all files from the Hist_Data directory
        try:
            files = glob.glob('intermediary_files/Hist_Data/*')
            # Extract file names (symbols) without extensions and directory path
            fetched_symbols = [file.split('/')[2].split('.')[0] for file in files]
            # Create DataFrame from fetched symbols
            fetched_data = pd.DataFrame(fetched_symbols, columns=["Symbol"])
            logger.info("Fetched data from downloaded historical quotes.")
        except Exception as e:
            logger.error(f"Error processing fetched files: {str(e)}\n{traceback.format_exc()}")
            sys.exit(1)

        # Merge fetched data with mapped data on the 'Symbol' column
        try:
            merged_df = pd.merge(fetched_data, mapped_df, on='Symbol', how='inner')
            # Save the final DataFrame to a CSV file
            merged_df.to_csv("intermediary_files/Instruments.csv", index=False)
            logger.info("Merged fetched data with mapped data and saved to Instruments.csv.")
        except Exception as e:
            logger.error(f"Error merging data and saving CSV: {str(e)}\n{traceback.format_exc()}")
            sys.exit(1)

        # Return the list of instrument tokens if available
        if merged_df["instrument_token"].to_list():
            return merged_df["instrument_token"].to_list()
        else:
            logger.error("Error: No instrument tokens found in the merged DataFrame.\n" + traceback.format_exc())
            sys.exit(1)

    except Exception as e:
        logger.error(f"An unexpected error occurred: {str(e)}\n{traceback.format_exc()}")
        sys.exit(1)