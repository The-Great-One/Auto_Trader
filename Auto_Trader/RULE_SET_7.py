def buy_or_sell(df, row, holdings):
    """
    Refined swing trading strategy with bulletproof momentum conditions.

    Parameters:
    -----------
    df : pd.DataFrame
        The DataFrame containing historical data and necessary technical indicators.
    row : int
        The latest row for evaluation.
    holdings : dict
        Dictionary representing current stock holdings (can be used for position sizing).

    Returns:
    --------
    str
        'BUY', 'SELL', or 'HOLD' based on the technical indicator evaluation.
    """

    # ---------------------------------
    # 1. Price Breakout Zone Condition
    # ---------------------------------
    price_breakout_zone = (
        (df["Close"].iloc[-1] > df["SMA_10_Close"].iloc[-1] * 1.01) and
        (df["Close"].iloc[-1] < df["SMA_10_Close"].iloc[-1] * 1.08)
    )

    # -----------------------------
    # 2. Strong Trend Confirmation
    # -----------------------------
    trend_strong = (
        (df["EMA20"].iloc[-1] > df["EMA50"].iloc[-1]) and
        (df["EMA50"].iloc[-1] > df["EMA100"].iloc[-1]) and
        (df["EMA100"].iloc[-1] > df["EMA200"].iloc[-1]) and
        (df["Close"].iloc[-1] > df["EMA20"].iloc[-1])  # Price above short-term EMA
    )

    # ---------------------------
    # 3. Bulletproof MACD Check
    # ---------------------------
    macd_strong_momentum = (
        (df["MACD"].iloc[-1] > df["MACD_Signal"].iloc[-1]) and  # Bullish crossover
        ((df["MACD"].iloc[-1] - df["MACD_Signal"].iloc[-1]) > 0.15) and  # Sufficient distance to avoid flat cross
        ((df["MACD"].iloc[-1] - df["MACD"].shift(1).iloc[-1]) > 0.1) and  # Strong upward slope of MACD line
        ((df["MACD_Hist"].iloc[-1] - df["MACD_Hist"].shift(1).iloc[-1]) > 0.1) and  # Rapid histogram acceleration
        (df["MACD_Hist"].iloc[-1] > 0)  # Ensure histogram is positive
    )

    # ---------------------------
    # 4. RSI Confirmation Check
    # ---------------------------
    rsi_strong = (
        (df["RSI"].iloc[-1] > 60) and
        ((df["RSI"].iloc[-1] - df["RSI"].shift(1).iloc[-1]) > 2)  # RSI is rising, not flat
    )
    rsi_recent_cross = (
        (df["RSI"].shift(1).iloc[-1] < 60) and (df["RSI"].iloc[-1] > 60)
    )
    rsi_confirm = rsi_strong or rsi_recent_cross

    # -------------------------------
    # 5. Volume Surge Confirmation
    # -------------------------------
    volume_surge = df["Volume"].iloc[-1] > 1.2 * df["SMA_20_Volume"].iloc[-1]

    # -------------------------
    # Final BUY Signal Trigger
    # -------------------------
    if macd_strong_momentum and rsi_confirm and volume_surge and trend_strong and price_breakout_zone:
        return "BUY"

    # -----------------------------------
    # SELL signal conditions (as provided)
    # -----------------------------------
    elif (
        (df["EMA9"].iloc[-1] < df["EMA21"].iloc[-1] * 0.99 < df["EMA50"].iloc[-1] * 0.99)
        and (df["RSI"].iloc[-1] < 45)
        and (df["MACD_Hist"].iloc[-1] < 0)
        and (df["Volume"].iloc[-1] > 1.5 * df["SMA_20_Volume"].iloc[-1])
    ):
        return "SELL"

    return "HOLD"
