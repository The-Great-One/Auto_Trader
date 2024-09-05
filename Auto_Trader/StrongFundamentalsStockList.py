from Fundamentals import Tickertape

def goodStocks():
    ttp = Tickertape()
    
    try:
        # Fetch the filtered equity screener data
        filtered_list_df = ttp.get_equity_screener_data(
            filters=["epsGwth", "5yCagrPct", "mrktCapf", "pftMrg", "rtnAsts", "dbtEqt", "qcur"], 
            sortby='mrktCapf', 
            number_of_records=1000
        )
        
        # Use .iloc to select specific columns by position (info.ticker, sid, and advancedRatios.roe)
        filtered_list_df = filtered_list_df.iloc[:, [4, 8]]
        
        filtered_list_df = filtered_list_df.rename(columns={'info.ticker' : 'Symbol'})
        
        # Save the filtered DataFrame to CSV
        return filtered_list_df[["Symbol"]]
    
    except Exception as e:
        print(f"An error occurred: {e}")