from Fundamentals import Tickertape
import logging
import numpy as np
import pandas as pd
import traceback


logger = logging.getLogger("Auto_Trade_Logger")

def goodStocks():
    ttp = Tickertape()
    
    try:
        # Fetch only the necessary columns to improve performance
        filtered_list_df = ttp.get_equity_screener_data(
            filters = [
                "mrktCapf",  # Market Cap
                "apef",  # P/E Ratio
                "indpe", # Sector PE
            ],
            sortby='mrktCapf',  # Sorting by market capitalization
            number_of_records=7000 # Number of records to fetch
        )

        filtered_list_df = filtered_list_df[
            (filtered_list_df['advancedRatios.apef'] <= 40) &
            (filtered_list_df['advancedRatios.apef'] > 0) &
            (filtered_list_df['advancedRatios.mrktCapf'] >= 500) &
            (filtered_list_df['advancedRatios.apef'] <= filtered_list_df['advancedRatios.indpe'])
        ]
        
        # Select specific columns by name: 'info.ticker' (renamed to 'Symbol') and 'sid'
        filtered_list_df = filtered_list_df[['info.ticker', 'sid']]
        filtered_list_df = filtered_list_df.rename(columns={'info.ticker': 'Symbol'})
        
        return filtered_list_df[['Symbol']]
        
    except Exception as e:
        logger.error(f"An error occurred: {e}, Traceback: {traceback.format_exc()}")
        raise e