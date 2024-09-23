def buy_or_sell(df, row, holdings):
    """
    Determines whether to generate a 'BUY', 'SELL', or 'HOLD' signal based on technical indicators (MACD, RSI, EMAs).

    Parameters:
    -----------
    df : pd.DataFrame
        The DataFrame containing the historical data, including necessary technical indicators.
    row : int
        The latest row for evaluation.
    holdings : dict
        A dictionary representing the current stock holdings.

    Returns:
    --------    
    str
        Returns 'BUY', 'SELL', or 'HOLD' signal based on technical indicator evaluation.
    """
    
    # Buy signal conditions
    if (
        (df['MACD'].iloc[-1] > df['MACD_Signal'].iloc[-1]) and
        (df['MACD'].shift(1).iloc[-1] <= df['MACD_Signal'].shift(1).iloc[-1]) and
        (df['MACD_Hist'].iloc[-1] > 0) and
        (df['MACD'].iloc[-1] > 0) and
        (df['MACD_Signal'].iloc[-1] > 0) and
        (df["RSI"].iloc[-1] >= 60) and
        (df["Close"].iloc[-1] >= df["EMA20"].iloc[-1]) and
        (df["Close"].iloc[-1] >= df["EMA50"].iloc[-1]) and
        (df["Close"].iloc[-1] >= df["EMA100"].iloc[-1]) and
        (df["Close"].iloc[-1] >= df["EMA200"].iloc[-1])
    ):
        return "BUY"

    # Sell signal conditions
    elif (
        ((df['RSI'].iloc[-1] < 60) or (df['RSI'].iloc[-1] > 75)) and
        (df['RSI'].iloc[-1] <= df['RSI'].shift(1).iloc[-1]) and
        (df['RSI'].shift(1).iloc[-1] <= df['RSI'].shift(2).iloc[-1]) and
        (df['RSI'].shift(2).iloc[-1] <= df['RSI'].shift(3).iloc[-1]) and
        (df['MACD'].iloc[-1] <= df['MACD_Signal'].iloc[-1]) and
        (df['MACD_Hist'].iloc[-1] < 0) and
        (df["Close"].iloc[-1] <= df["EMA20"].iloc[-1]) and
        (df["Close"].iloc[-1] <= df["EMA50"].iloc[-1]) and
        (df["Close"].iloc[-1] <= df["EMA100"].iloc[-1]) and
        (df["Close"].iloc[-1] <= df["EMA200"].iloc[-1])
    ):
        return "SELL"

    # Default to HOLD if no conditions are met
    else:
        return "HOLD"
