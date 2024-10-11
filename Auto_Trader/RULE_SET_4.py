def buy_or_sell(df, row, holdings):
    """
    Determine whether to buy, sell, or hold based on technical indicators.
    """

    # Buy condition with refined RSI threshold and volume requirement
    if (
        (df['EMA10'].iloc[-1] > df['EMA20'].iloc[-1])
        and (df['RSI'].iloc[-1] >= 60)
        and (df['RSI'].iloc[-1] <= 66)
        and (df['RSI'].shift(1).iloc[-1] < df['RSI'].iloc[-1])
        and (df['MACD_Hist'].iloc[-1] >= 5)
        and (df['MACD_Hist'].iloc[-1] >= df['MACD_Hist'].shift(1).iloc[-1])
        and (df['Volume'].iloc[-1] > 1.5 * df['SMA_20_Volume'].iloc[-1])
    ):
        return "BUY"

    # Sell condition with RSI threshold or MACD trigger
    elif (
        (df['EMA10'].iloc[-1] < df['EMA20'].iloc[-1])
        or ((df['RSI'].shift(1).iloc[-1] - df['RSI'].iloc[-1]) >= 3)
        or (df['MACD_Hist'].iloc[-1] < 0)
        or (df['MACD'].iloc[-1] < df['MACD_Signal'].iloc[-1])
    ):
        return "SELL"

    else:
        return "HOLD"