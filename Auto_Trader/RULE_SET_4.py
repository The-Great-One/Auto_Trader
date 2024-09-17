def buy_or_sell(df, row, holdings):
    """
    Determine whether to buy, sell, or hold based on technical indicators and trailing stop-loss.
    Remove from profit tracker when a stock is sold and no longer held.
    """
    
    if (df['EMA10'].iloc[-1] > df['EMA20'].iloc[-1]) and (df['RSI'].iloc[-1] > 60) and (df['RSI'].iloc[-1] < 65) and (df['MACD_Hist'].iloc[-1] > 0.5):
        return "BUY"
    elif (df['EMA10'].iloc[-1] < df['EMA20'].iloc[-1]) and (df['RSI'].iloc[-1] < 55) and (df['MACD_Hist'].iloc[-1] < -1):
        return "SELL"
    else:
        return "HOLD"
