import glob
import pandas as pd
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
    # Fetch instrument master list and good stocks list
    instrument_master = fetch_instruments_list()
    holdings = fetch_holdings()
    ticker_tape_list = goodStocks()
        
    # Rename columns in holdings to match those in merged_df
    holdings.rename(columns={"tradingsymbol": "Symbol"}, inplace=True)

    # Map the good stock list with instrument tokens
    mapped_df = get_instrument_token(
        good_stock_list_df=ticker_tape_list, instruments_df=instrument_master
    )
    
    # Concatenate the holdings DataFrame to the merged_df
    mapped_df = pd.concat([mapped_df, holdings], ignore_index=True)

    # Drop duplicates based on the 'Symbol' column
    mapped_df.drop_duplicates(subset=['Symbol'], inplace=True)

    # Download historical quotes for the mapped instruments
    download_historical_quotes(df=mapped_df)

    # Fetch all files from the Hist_Data directory
    files = glob.glob('intermediary_files/Hist_Data/*')

    # Extract file names (symbols) without extensions and directory path
    fetched_symbols = [file.split('/')[2].split('.')[0] for file in files]

    # Create DataFrame from fetched symbols
    fetched_data = pd.DataFrame(fetched_symbols, columns=["Symbol"])

    # Merge fetched data with mapped data on the 'Symbol' column
    merged_df = pd.merge(fetched_data, mapped_df, on='Symbol', how='inner')

    # Save the final DataFrame to a CSV file
    merged_df.to_csv("intermediary_files/Instruments.csv", index=False)
    
    if merged_df["instrument_token"].to_list():
        # Return the list of instrument tokens
        return merged_df["instrument_token"].to_list()
    else:
        logger.error("Error Building Master")
        sys.exit()