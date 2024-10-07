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
        and (df['MACD_Hist'].shift(1).iloc[-1] > 0)
        and (df['MACD_Hist'].iloc[-1] >= df['MACD_Hist'].shift(1).iloc[-1])
        and (df['Volume'].iloc[-1] > 1.5 * df['Volume'].rolling(window=10).mean().iloc[-1])
    ):
        return "BUY"

    # Adaptive sell condition with dynamic RSI threshold based on market volatility
    elif (
        (df['EMA10'].iloc[-1] < df['EMA20'].iloc[-1])
        or (df['RSI'].iloc[-1] >= (76 if 'Volatility' in df.columns and df['Volatility'].iloc[-1] > df['Volatility'].mean() else 78))
        or ((df['RSI'].shift(1).iloc[-1] - df['RSI'].iloc[-1]) >= 3)
        or (df['MACD_Hist'].iloc[-1] < 0)
        or (df['MACD'].iloc[-1] < df['MACD_Signal'].iloc[-1])
    ):
        return "SELL"

    else:
        return "HOLD"