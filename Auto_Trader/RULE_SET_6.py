def buy_or_sell(df, row, holdings):
    """
    Sell if RSI is greater than 78
    """
    
    if df['RSI'].iloc[-1] >= 78:
        return "SELL"
    else:
        return "HOLD"
