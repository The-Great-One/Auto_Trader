def buy_or_sell(df, row, holdings):
    """
    Refined swing trading strategy for Indian stocks on daily timeframe.

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

    if (
        (df["EMA9"].iloc[-1] > df["EMA21"].iloc[-1] > df["EMA50"].iloc[-1])
        and (df["RSI"].iloc[-1] > 60)
        and (df["MACD_Hist"].iloc[-1] > 0)
        and df["VolumeConfirmed"].iloc[-1]
    ):
        return "BUY"  # Buy Signal
    elif (
        (df["EMA9"].iloc[-1] < df["EMA21"].iloc[-1] < df["EMA50"].iloc[-1])
        and (df["RSI"].iloc[-1] < 40)
        and (df["MACD_Hist"].iloc[-1] < 0)
        and df["VolumeConfirmed"].iloc[-1]
    ):
        return "SELL"  # Sell Signal
    else:
        return "HOLD"  # No action
