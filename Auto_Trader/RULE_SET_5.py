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
    previous_data = df.iloc[-2]

    # Buy signal conditions (tightened to improve win rate)
    buy_condition = (
        (latest_data['MACD'] > latest_data['MACD_Signal']) and
        (previous_data['MACD'] <= previous_data['MACD_Signal']) and  # MACD crossover
        (latest_data['MACD_Hist'] > 0) and
        (latest_data['MACD'] > 0) and
        (latest_data['RSI'] >= 55) and  # Reduced RSI threshold for earlier entry
        (latest_data['RSI'] < 70) and  # Avoid overbought conditions
        (latest_data['Close'] > latest_data['EMA20']) and
        (latest_data['Close'] > latest_data['EMA50']) and
        (latest_data['Close'] > latest_data['EMA100']) and
        (latest_data['Close'] > latest_data['EMA200']) and
        (latest_data['Volume'] > df['Volume'].rolling(window=10).mean().iloc[-1])  # Volume confirmation for strong trend
    )

    # Sell signal conditions (tightened to lock in profits)
    sell_condition = (
        (latest_data['RSI'] >= 75) or  # Sell when RSI is in overbought region
        (latest_data['RSI'] < previous_data['RSI'] and latest_data['RSI'] > 50) or  # RSI declining from high levels
        (latest_data['MACD'] < latest_data['MACD_Signal']) or  # MACD bearish crossover
        (latest_data['MACD_Hist'] < 0) or  # MACD histogram turning negative
        (latest_data['Close'] < latest_data['EMA20']) or  # Price falling below EMA20
        (latest_data['Close'] < latest_data['EMA50'])  # Price falling below EMA50
    )

    # Determine the action based on conditions
    if buy_condition:
        return "BUY"
    elif sell_condition:
        return "SELL"
    else:
        return "HOLD"
