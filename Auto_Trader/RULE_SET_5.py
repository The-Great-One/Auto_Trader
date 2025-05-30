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
    # Extract the latest row of data for convenience
    latest_data = df.iloc[-1]
    previous_data = df.shift(1).iloc[-1]

    # Additional Close Conditions for Buy
    buy_close_condition = (
        (latest_data['Close'] > latest_data['SMA_20_Close']) and
        (latest_data['Close'] >= latest_data['SMA_10_Close'] * 1.01) and
        (latest_data['Close'] <= latest_data['SMA_10_Close'] * 1.08)
    )
    
    # Define a MACD crossover in the last 5 days
    macd_crossover_last_3_days = (
        (df['MACD_Hist'] > 0) & (df['MACD_Hist'].shift(1) <= 0)
    ).iloc[-5:].any()

    # Buy signal conditions (tightened to improve win rate)
    buy_condition = (
        (latest_data['MACD'] > 0) and
        (latest_data['RSI'] >= 60) and  # Reduced RSI threshold for earlier entry
        (latest_data['RSI'] <= 70) and  # Avoid overbought conditions
        (latest_data['RSI'] > previous_data['RSI']) and  # Ensure today's RSI is greater than yesterday's RSI
        (latest_data['Close'] > latest_data['EMA20']) and
        (latest_data['Close'] > latest_data['EMA50']) and
        (latest_data['EMA20'] > latest_data['EMA50']) and
        (latest_data['Volume'] > df['SMA_20_Volume'].iloc[-1]) and # Volume confirmation for strong trend
        (latest_data["ADX"] > 20)
    )

    # Determine the action based on conditions
    if buy_condition and buy_close_condition and macd_crossover_last_3_days:
        return "BUY"
    else:
        return "HOLD"