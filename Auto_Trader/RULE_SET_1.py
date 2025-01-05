import logging

logger = logging.getLogger("Auto_Trade_Logger")

def buy_or_sell(df, row, holdings):
    from Auto_Trader.utils import get_params_grid
    
    params_dict = get_params_grid()
    Symbol = row.get('Symbol', 'Unknown')
        
    try:
        params = params_dict[Symbol]
        logger.debug(f"Retrieved parameters for {Symbol}: {params}")
    except KeyError as e:
        logger.error(f"Symbol {Symbol} not found in params_dict: {e}")
        return "HOLD"
    except Exception as e:
        logger.exception("Unexpected error reading 'instrument_token' from row:")
        return "HOLD"
    
    # Extract latest row for evaluation
    try:
        latest_row = df.iloc[-1]
        logger.debug(f"Latest data for {Symbol}: {latest_row.to_dict()}")
        previous_row = df.iloc[-2] if len(df) > 1 else None
    except IndexError as e:
        logger.error(f"DataFrame for {Symbol} is empty or contains insufficient rows: {e}")
        return "HOLD"

    # Precompute reusable variables for the latest row
    ema_diff = latest_row['EMA10'] - latest_row['EMA20']
    obv_change = (
        latest_row['OBV'] - (previous_row['OBV'] if previous_row is not None else 0)
    )
    logger.debug(f"EMA diff for {Symbol}: {ema_diff}, OBV change: {obv_change}")

    # Buy conditions
    buy_signal = (
        (latest_row['RSI'] < params['rsi_buy_threshold']) &
        (ema_diff > params['ema_diff']) &
        (latest_row['Volume'] > latest_row['Volume_MA20'] * params['vol_mult_buy']) &
        (latest_row['Close'] < latest_row['LowerBand'] * params['bb_multiplier']) &
        (latest_row['Stochastic_%K'] < params['stochastic_buy_threshold']) &
        (latest_row['ADX'] > params['adx_threshold'])
    )
    logger.debug(f"Buy signal for {Symbol}: {buy_signal}")

    # Sell conditions
    sell_signal = (
        (latest_row['RSI'] > params['rsi_sell_threshold']) &
        (ema_diff < -params['ema_diff']) &
        (latest_row['Volume'] > latest_row['Volume_MA20'] * params['vol_mult_sell']) &
        (latest_row['Close'] > latest_row['UpperBand'] * params['bb_multiplier']) &
        (latest_row['Stochastic_%K'] > params['stochastic_sell_threshold']) &
        (latest_row['ADX'] > params['adx_threshold'])
    )
    logger.debug(f"Sell signal for {Symbol}: {sell_signal}")

    # MACD histogram cross conditions
    if params['use_macd_hist_cross'] and previous_row is not None:
        macd_cross_up = (latest_row['MACD_Hist'] > 0) and (previous_row['MACD_Hist'] <= 0)
        macd_cross_down = (latest_row['MACD_Hist'] < 0) and (previous_row['MACD_Hist'] >= 0)
        buy_signal &= macd_cross_up
        sell_signal &= macd_cross_down
        logger.debug(f"MACD cross conditions for {Symbol}: Up: {macd_cross_up}, Down: {macd_cross_down}")

    # OBV trend filtering
    if params['obv_trend'] == 'up':
        buy_signal &= (obv_change > 0)
        sell_signal &= (obv_change < 0)
    elif params['obv_trend'] == 'down':
        buy_signal &= (obv_change < 0)
        sell_signal &= (obv_change > 0)
    logger.debug(f"Final buy and sell signals after OBV filtering for {Symbol}: Buy: {buy_signal}, Sell: {sell_signal}")

    # Determine final signal
    if buy_signal:
        return "BUY"
    elif sell_signal:
        return "SELL"
    else:
        return "HOLD"