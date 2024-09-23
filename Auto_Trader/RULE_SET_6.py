def buy_or_sell(df, row, holdings):
    """
    Sell if RSI is greater than 80
    """
    
    if df['RSI'].iloc[-1] > 80:
        return "SELL"
    else:
        return "HOLD"
