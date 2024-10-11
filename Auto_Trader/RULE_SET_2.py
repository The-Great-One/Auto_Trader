from Auto_Trader import pd, time, logging, traceback
import Auto_Trader

logger = logging.getLogger("Auto_Trade_Logger")

def buy_or_sell(df, row, holdings):
    """
    Determine whether to sell based on Stop Loss of -5% (NetChange -5% from Purchase Price).
    Returns "SELL" if the stop loss is triggered, otherwise "HOLD".
    """
    holdings = pd.DataFrame(holdings)[["tradingsymbol", "instrument_token", "exchange", "average_price", "quantity"]]
    try:
        # Filter holdings for the specific instrument token
        holdings_symbol_data = holdings[holdings["instrument_token"] == row["instrument_token"]]
        
        # Ensure there's data for the symbol
        if holdings_symbol_data.empty:
            logger.debug(f"No holdings data for instrument_token {row['instrument_token']}. Returning HOLD.")
            return "HOLD"
        
        # Extract average price from the holdings
        average_price = holdings_symbol_data['average_price'].iloc[-1]
        
        # Extract last closing price from the dataframe
        last_price = df['Close'].iloc[-1]
        
        # Avoid divide-by-zero error
        if average_price == 0:
            logger.warning(f"Average price is zero for {holdings_symbol_data['tradingsymbol'].iloc[-1]}. Returning HOLD.")
            return "HOLD"
        
        # Calculate profit percentage
        profit_percent = ((last_price - average_price) / average_price) * 100
        
        if profit_percent <= -5.0:
            logger.info(f"Stop loss triggered for {holdings_symbol_data['tradingsymbol'].iloc[-1]}. Returning SELL.")
            return "SELL"
        else:
            logger.info(f"Profit percentage is {profit_percent:.2f}% for {holdings_symbol_data['tradingsymbol'].iloc[-1]}. Returning HOLD.")
            return "HOLD"
        
    except Exception as e:
        logger.error(f"Error processing {row['instrument_token']}: {str(e)}. Returning HOLD, Traceback: {traceback.format_exc()}")
        return "HOLD"
