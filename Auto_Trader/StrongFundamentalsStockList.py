from Fundamentals import Tickertape
import logging
import numpy as np
import pandas as pd
import traceback


logger = logging.getLogger("Auto_Trade_Logger")

def goodStocks():
    ttp = Tickertape()
    
    # Define the columns that must always exist in the final dataframe
    mandatory_columns = ['Entry point', 'Growth', 'Performance', 'Profitability', 'Red flags', 'Valuation']
    
    try:
        # Fetch only the necessary columns to improve performance
        filtered_list_df = ttp.get_equity_screener_data(
            filters = [
                "epsg",  # 1Y EPS Growth
                "5YrevChg",  # 5Y Historical Revenue Growth
                "mrktCapf",  # Market Cap
                "pftMrg",  # Net Profit Margin
                "rtnAsts",  # Return on Assets
                "dbtEqt",  # Debt to Equity
                "qcur",  # Current Ratio
                "roe", # Return on Equity
                "rvng",  # 1Y Revenue Growth
                "beta",  # Beta (volatility compared to the market)
                "incEps",  # Incremental EPS
                "incTrev",  # Total Revenue
                "aopm",  # EBITDA Margin
                "12mVol",  # Volatility
                "12mpctN",  # 1Y Return vs Nifty
                "relVol",  # Relative Volume
                "divDps",  # Dividend Yield
                "apef",  # P/E Ratio
                "5Yaroi",  # 5Y Avg Return on Investment
                "cafCfoaMgn",  # Cash Flow Margin
                "evebit",  # EV/EBIT Ratio
                "qIncTrevK", # Last Quarter Revenue
                "qIncEbiK", # Last Quarter EBITDA
            ],
            sortby='mrktCapf',  # Sorting by market capitalization
            number_of_records=3000  # Increased number to fetch more records
        )

        filtered_list_df = filtered_list_df[
            (filtered_list_df['advancedRatios.apef'] <= 40)
        ]


        # Combine the results for all bins
        # filtered_list_df = pd.concat([large_cap_stocks, mid_cap_stocks_final, small_cap_stocks_final]).drop_duplicates()

        
        # Select specific columns by name: 'info.ticker' (renamed to 'Symbol') and 'sid'
        filtered_list_df = filtered_list_df[['info.ticker', 'sid']]
        filtered_list_df = filtered_list_df.rename(columns={'info.ticker': 'Symbol'})
        
        # Initialize a list to store dataframes with pivoted scorecards
        final_dataframes = []
        
        # # Iterate through each stock and fetch its scorecard
        # for index, row in filtered_list_df.iterrows():
        #     ticker = row['sid']
            
        #     try:
        #         # Fetch the scorecard for the stock (only 'name' and 'tag' columns)
        #         score_card = ttp.get_score_card(ticker)[["name", "tag"]]
                
        #         # If the score_card is empty, add NaN placeholders for the mandatory columns
        #         if score_card.empty:
        #             print(f"Scorecard for {row['Symbol']} is empty. Adding NaN placeholders...")
        #             empty_df = pd.DataFrame(columns=mandatory_columns)
        #             empty_df.loc[0] = [np.nan] * len(mandatory_columns)  # Fill with NaN
        #             empty_df['sid'] = ticker
        #             combined_df = row.to_frame().T.merge(empty_df, on='sid', how='left')
        #             final_dataframes.append(combined_df)
        #             continue
                
        #         # Pivot the scorecard using pivot_table without an explicit index
        #         pivoted_score_card = score_card.pivot_table(index=None, columns='name', values='tag', aggfunc='first')
                
        #         # Ensure all mandatory columns are present, fill missing with NaN
        #         for col in mandatory_columns:
        #             if col not in pivoted_score_card:
        #                 pivoted_score_card[col] = np.nan
                
        #         # Add a column for the ticker sid
        #         pivoted_score_card['sid'] = ticker
                
        #         # Join the pivoted scorecard with the current row's data
        #         combined_df = row.to_frame().T.merge(pivoted_score_card, on='sid')
                
        #         # Append the result to the list
        #         final_dataframes.append(combined_df)
            
            # except Exception as e:
            #     # Print a message and preserve the row with NaN placeholders for mandatory columns
            #     print(f"Error processing scorecard for {row['Symbol']}: {e}. Adding NaN placeholders...")
            #     error_df = pd.DataFrame(columns=mandatory_columns)
            #     error_df.loc[0] = [np.nan] * len(mandatory_columns)  # Fill with NaN
            #     error_df['sid'] = ticker
            #     combined_df = row.to_frame().T.merge(error_df, on='sid', how='left')
            #     final_dataframes.append(combined_df)
            #     continue
        
        # Concatenate all dataframes into a final dataframe
        if final_dataframes:
            result_df = pd.concat(final_dataframes, ignore_index=True)
        elif not filtered_list_df.empty:
            return filtered_list_df[['Symbol']]
        else:
            result_df = pd.DataFrame()  # Return an empty dataframe if no results

        result_df[
            # (result_df['Growth'] != "Low") &
            # (result_df['Entry point'] == "Good") &
            # (result_df['Performance'] != "Low") &
            (result_df['Profitability'] != "Low") #&
            # (result_df['Red flags'] == "Low") #&
            # (result_df['Valuation'] != "High")
        ]
        
        return result_df[['Symbol']]
    
    except Exception as e:
        logger.error(f"An error occurred: {e}, Traceback: {traceback.format_exc()}")
        raise e