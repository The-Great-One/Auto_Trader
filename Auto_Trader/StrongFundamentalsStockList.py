from Fundamentals import Tickertape
from functools import lru_cache
import logging

logger = logging.getLogger("Auto_Trade_Logger")

@lru_cache(maxsize=10)  # Caching to optimize repeated data fetching
def goodStocks(debt_to_equity_threshold=1.5, eps_growth_threshold=3, 
               profit_margin_threshold=8, current_ratio_threshold=1.5, cagr_threshold=5):
    ttp = Tickertape()
    
    try:
        # Fetch only the necessary columns to improve performance
        filtered_list_df = ttp.get_equity_screener_data(
            filters=["epsGwth", "5yCagrPct", "mrktCapf", "pftMrg", "rtnAsts", "dbtEqt", "qcur"],
            sortby='mrktCapf',  # Sorting by market capitalization
            number_of_records=1000
        )
        
        # # Apply dynamic filtering conditions based on user-defined thresholds
        # filtered_list_df = filtered_list_df[
        #     (filtered_list_df['advancedRatios.dbtEqt'] < debt_to_equity_threshold) &
        #     (filtered_list_df['advancedRatios.epsGwth'] > eps_growth_threshold) &
        #     (filtered_list_df['advancedRatios.pftMrg'] > profit_margin_threshold) &
        #     (filtered_list_df['advancedRatios.qcur'] > current_ratio_threshold) &
        #     (filtered_list_df['advancedRatios.5yCagrPct'] > cagr_threshold)
        # ]
        
        # Select specific columns by name: 'info.ticker' (renamed to 'Symbol') and 'sid'
        filtered_list_df = filtered_list_df[['info.ticker', 'sid']]
        
        # Rename columns for better readability
        filtered_list_df = filtered_list_df.rename(columns={'info.ticker': 'Symbol'})
        
        # Return only the 'Symbol' column
        return filtered_list_df[['Symbol']]
    
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise e
