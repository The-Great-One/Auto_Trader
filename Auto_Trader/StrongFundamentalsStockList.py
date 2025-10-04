from Fundamentals import Tickertape
import logging
import numpy as np
import pandas as pd
import traceback

logger = logging.getLogger("Auto_Trade_Logger")

def goodStocks():
    ttp = Tickertape()

    try:
        # Fetch required columns, including info.sector
        filtered_list_df = ttp.get_equity_screener_data(
            filters=[
                "mrktCapf",   # Market Cap
                "apef",       # P/E Ratio
                "indpe"      # Sector PE
            ],
            sortby='mrktCapf',
            number_of_records=7000
        )

        # Keep ETFs aside first (sector name contains 'ETF')
        etf_df = filtered_list_df[
            filtered_list_df['info.sector'].str.contains('ETF', case=False, na=False)
        ][['info.ticker', 'sid', 'info.sector']]

        # Apply numeric filters for non-ETF stocks
        non_etf_df = filtered_list_df[
            (filtered_list_df['advancedRatios.apef'] <= 40) &
            (filtered_list_df['advancedRatios.apef'] > 0) &
            (filtered_list_df['advancedRatios.mrktCapf'] >= 500) &
            (filtered_list_df['advancedRatios.apef'] <= filtered_list_df['advancedRatios.indpe'])
        ][['info.ticker', 'sid', 'info.sector']]

        # Combine both
        combined_df = pd.concat([non_etf_df, etf_df], ignore_index=True)

        # Rename for clarity
        combined_df = combined_df.rename(columns={'info.ticker': 'Symbol', 'info.sector': 'Sector'})

        # Final clean output
        return combined_df[['Symbol']]

    except Exception as e:
        logger.error(f"An error occurred: {e}, Traceback: {traceback.format_exc()}")
        raise e