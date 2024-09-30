def buy_or_sell(df, row, holdings):
    """
    Determine whether to buy, sell, or hold based on technical indicators.
    """

    if (
    (df["EMA10"].iloc[-1] > df["EMA20"].iloc[-1])  # EMA10 is greater than EMA20, confirming an uptrend
    and (df["RSI"].iloc[-1] >= 60)  # RSI above 60 to capture the upward momentum earlier
    and (df["RSI"].iloc[-1] <= 70)  # RSI less than 70 to avoid overbought conditions
    and (df["RSI"].shift(1).iloc[-1] < df["RSI"].iloc[-1])  # Previous RSI is lower, indicating increasing momentum
    and (df["MACD_Hist"].iloc[-1] >= 5)  # MACD histogram indicating positive momentum
    and (df["MACD_Hist"].shift(1).iloc[-1] > 0)  # MACD histogram is increasing
    and (df["MACD_Hist"].iloc[-1] >= df["MACD_Hist"].shift(1).iloc[-1])  # MACD histogram is increasing
    and (df["Volume"].iloc[-1] > df["Volume"].mean())  # Ensure there's sufficient volume confirming market interest
    ):
        return "BUY"

    elif (
    (df["EMA10"].iloc[-1] < df["EMA20"].iloc[-1])  # EMA10 is below EMA20, indicating a potential trend reversal
    or (df["RSI"].iloc[-1] >= 76)  # RSI above 76, indicating an overbought condition where profit-taking is safer
    or ((df["RSI"].shift(1).iloc[-1] - df["RSI"].iloc[-1]) >= 3)  # A slight RSI drop (2 points) indicating early weakening momentum
    or (df["MACD_Hist"].iloc[-1] < 0)  # MACD histogram turns negative, indicating loss of bullish momentum
    or (df["MACD"].iloc[-1] < df["Signal_Line"].iloc[-1])  # MACD line crosses below the signal line, further confirming weakening momentum
    ):
        return "SELL"
    else:
        return "HOLD"
