def buy_or_sell(df, row, holdings):
    """
    Determines whether to generate a 'BUY', 'SELL', or 'HOLD' signal based on technical indicators.

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

   # --- Buy Conditions ---
    # Liquidity and Bracket Conditions for Buy
    buy_liquidity_condition = (
        (latest_data['Close'] * latest_data['Volume'] > 20_000_000) and
        (latest_data['Close'] * latest_data['Volume'] >
         latest_data['SMA_20_Close'] * latest_data['SMA_20_Volume'])
    )

    # SMA Conditions for Buy
    buy_sma_condition = (
        (latest_data['SMA_20_Close'] > latest_data['SMA_200_Close']) and
        (latest_data['Weekly_SMA_20'] > latest_data['Weekly_SMA_200']) and
        (latest_data['Weekly_SMA_200'] > latest_data['Weekly_SMA_200_1w']) and
        (latest_data['Weekly_SMA_200_1w'] > latest_data['Weekly_SMA_200_2w']) and
        (latest_data['Weekly_SMA_200_2w'] > latest_data['Weekly_SMA_200_3w']) and
        (latest_data['Weekly_SMA_200_3w'] > latest_data['Weekly_SMA_200_4w'])
    )

    # MACD Conditions for Buy
    buy_macd_condition = (
        (latest_data['MACD_Rule_8'] > 0) and
        (latest_data['MACD_Rule_8_Signal'] > 0) and
        (previous_data['MACD_Rule_8'] <= previous_data['MACD_Rule_8_Signal'] and latest_data['MACD_Rule_8'] > latest_data['MACD_Rule_8_Signal'])
    )

    # Additional Close Conditions for Buy
    buy_close_condition = (
        (latest_data['Close'] > latest_data['SMA_20_Close']) and
        (latest_data['Close'] >= latest_data['SMA_10_Close'] * 1.01) and
        (latest_data['Close'] <= latest_data['SMA_10_Close'] * 1.08)
    )

    # "Pass any" Conditions for Buy
    buy_any_condition = (
        (previous_data['Close'] < previous_data['SMA_20_High'] and latest_data['Close'] > latest_data['SMA_20_High']) or
        (previous_data['RSI'] <= 60 and latest_data['RSI'] >= 60) or
        (previous_data['Close'] < previous_data['Supertrend'] and latest_data['Close'] > latest_data['Supertrend']) or
        (previous_data['EMA5'] <= previous_data['EMA13'] and latest_data['EMA5'] > latest_data['EMA13'])
    )

    # Final Buy Condition
    buy_condition = buy_liquidity_condition and buy_sma_condition and buy_macd_condition and buy_close_condition and buy_any_condition

    # --- Sell Conditions ---
    # Liquidity and Bracket Conditions for Sell
    sell_liquidity_condition = (
        (latest_data['Close'] * latest_data['Volume'] > 350_000_000) and
        (latest_data['Close'] * latest_data['Volume'] >
         latest_data['SMA_20_Close'] * latest_data['SMA_20_Volume'])
    )

    # SMA Volume Condition for Sell
    # sell_sma_volume_condition = latest_data['SMA_20_Volume'] > latest_data['SMA_200_Volume']

    # "Pass any" Conditions for Sell
    sell_any_condition = (
        (previous_data['Close'] >= previous_data['SMA_20_Close'] and latest_data['Close'] < latest_data['SMA_20_Low']) or
        (previous_data['RSI'] >= 40 and latest_data['RSI'] < 40) or
        (previous_data['Close'] >= previous_data['Supertrend_Rule_8_Exit'] and latest_data['Close'] < latest_data['Supertrend_Rule_8_Exit']) or
        (latest_data['Close'] >= latest_data['SMA_10_Close'] * 1.14)
    )

    # Final Sell Condition
    sell_condition = sell_liquidity_condition and sell_any_condition #and sell_sma_volume_condition

    # Determine the action
    if buy_condition:
        return "BUY"
    elif sell_condition:
        return "SELL"
    else:
        return "HOLD"