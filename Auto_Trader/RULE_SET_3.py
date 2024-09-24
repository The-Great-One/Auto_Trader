from Auto_Trader import pd

def buy_or_sell(df, row, holdings):
    """
    Determine whether to sell based on a trailing stop-loss of 3% from the day's high.
    """
    try:
        # Convert holdings to a DataFrame and filter for the specific instrument token
        holdings = pd.DataFrame(holdings)
        holdings_symbol_data = holdings.loc[holdings["instrument_token"] == row["instrument_token"], ["average_price", "tradingsymbol"]]
        
        # If no holdings for the instrument token, return HOLD
        if holdings_symbol_data.empty:
            return "HOLD"
        
        # Extract relevant data
        average_price = holdings_symbol_data['average_price'].iloc[-1]
        last_price = df['Close'].iloc[-1]
        day_high_price = row.get("ohlc", {}).get("high")
        
        # Check if required data is valid
        if average_price == 0 or day_high_price is None:
            return "HOLD"
        
        # Check trailing stop-loss: if the current price falls 3% or more from the day's high, return SELL
        if (day_high_price - last_price) / day_high_price * 100 >= 3.0:
            return "SELL"
        else:
            return "HOLD"
        
    except Exception as e:
        print(f"Error processing {row['instrument_token']}: {str(e)}")
        return "HOLD"
