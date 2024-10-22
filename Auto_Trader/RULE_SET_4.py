def buy_or_sell(df, row, holdings):
    """
    Determine whether to buy, sell, or hold based on technical indicators.
    """

    # Additional Close Conditions for Buy
    buy_close_condition = (
        (df['Close'].iloc[-1] > df['SMA_20_Close'].iloc[-1]) and
        (df['Close'].iloc[-1] >= df['SMA_10_Close'].iloc[-1] * 1.01) and
        (df['Close'].iloc[-1] <= df['SMA_10_Close'].iloc[-1] * 1.08)
    )
    
    # Define a MACD crossover in the last 3 days
    macd_crossover_last_3_days = (
        ((df['MACD'] > df['MACD_Signal']) & (df['MACD'].shift(1) <= df['MACD_Signal'].shift(1)))  # MACD crosses above MACD_Signal
        .tail(3)  # Look at the last 3 days
        .any()  # Check if crossover happened on any of the last 3 days
    )

    # Refined buy condition with MACD crossover check
    if (
        (df['EMA10'].iloc[-1] > df['EMA20'].iloc[-1])  # EMA10 is above EMA20
        and (df['RSI'].iloc[-1] >= 60)  # RSI is at least 60
        and (df['RSI'].iloc[-1] <= 66)  # RSI is no more than 66
        and (df['RSI'].shift(1).iloc[-1] < df['RSI'].iloc[-1])  # RSI is increasing
        and (df['MACD_Hist'].iloc[-1] >= 5)  # MACD histogram is at least 5
        and (df['MACD_Hist'].iloc[-1] >= df['MACD_Hist'].shift(1).iloc[-1])  # MACD histogram is increasing
        and (df['Volume'].iloc[-1] > 1.5 * df['SMA_20_Volume'].iloc[-1])  # Volume is greater than 1.5x SMA_20 volume
        and macd_crossover_last_3_days  # Check if MACD crossover happened in the last 3 days
    ) and buy_close_condition:
        return "BUY"


    # Sell condition with RSI threshold or MACD trigger
    # elif (
    #     (df['EMA10'].iloc[-1] < df['EMA20'].iloc[-1])
    #     # or ((df['RSI'].shift(1).iloc[-1] - df['RSI'].iloc[-1]) >= 3)
    #     or (df['MACD_Hist'].iloc[-1] < 0)
    #     or (df['MACD'].iloc[-1] < df['MACD_Signal'].iloc[-1])
    # ):
    #     return "SELL"

    else:
        return "HOLD"