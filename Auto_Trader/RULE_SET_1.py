# Define global constants for periods and thresholds
EMA_SHORT_PERIOD = 10
EMA_LONG_PERIOD = 20
EMA_LONGER_PERIOD = 50
RSI_PERIOD = 14
MACD_FAST_PERIOD = 12
MACD_SLOW_PERIOD = 26
MACD_SIGNAL_PERIOD = 9
VOLUME_MA_PERIOD = 20
RSI_THRESHOLD = 65  # Lowered to allow more opportunities while maintaining accuracy
MACD_HIST_THRESHOLD = 7  # Kept high to ensure strong momentum
COOLDOWN_PERIOD = 3  # Reduced cooldown period to allow faster re-entries
VOLUME_MULTIPLIER = 1.5  # Reduced to allow more opportunities


def check_ema_crossover(df, short_period=EMA_SHORT_PERIOD, long_period=EMA_LONG_PERIOD, period=7):
    """
    Check if EMA10 has crossed above EMA20 and stayed above for 'period' consecutive periods.
    """
    ema_short = df['EMA10']
    ema_long = df['EMA20']
    
    if len(df) < period + 1:
        return False
    
    crossover = (ema_short.iloc[-period - 1] <= ema_long.iloc[-period - 1]) and (ema_short.iloc[-period] > ema_long.iloc[-period])
    stayed_above = (ema_short[-period:] > ema_long[-period:]).all()
    return crossover and stayed_above


def check_rsi_trend(df, period=3, rsi_threshold=RSI_THRESHOLD):
    """
    Check if RSI has been rising for 'period' periods and crossed above the given threshold (default: 65).
    """
    rsi = df['RSI']
    
    if len(df) < period + 1:
        return False
    
    crossed_above_threshold = (rsi.iloc[-period - 1] < rsi_threshold) and (rsi.iloc[-period] >= rsi_threshold)
    rsi_diff = rsi[-period:].diff()
    has_been_rising = (rsi_diff[1:] > 0).all()
    return crossed_above_threshold and has_been_rising


def check_macd_trend(df, period=2, macd_hist_threshold=MACD_HIST_THRESHOLD):
    """
    Check if MACD line has crossed above the signal line and MACD Histogram is greater than the specified threshold.
    """
    macd = df['MACD']
    signal = df['MACD_Signal']
    hist = df['MACD_Hist']
    
    if len(df) < period + 1:
        return False
    
    macd_cross = (macd.iloc[-period - 1] <= signal.iloc[-period - 1]) and (macd.iloc[-period] > signal.iloc[-period])
    hist_above_threshold = (hist.iloc[-period:] > macd_hist_threshold).all()
    return macd_cross and hist_above_threshold


def check_volume_trend(df, period=5, volume_multiplier=VOLUME_MULTIPLIER):
    """
    Check if Volume is significantly above its 20-period moving average (volume spike).
    """
    volume = df['Volume']
    volume_ma = df['SMA_20_Volume']
    
    if len(df) < period + 1:
        return False
    
    volume_spike = (volume.iloc[-period:] > volume_multiplier * volume_ma.iloc[-period:]).all()
    return volume_spike


def buy_signal(df):
    """
    Determine if all buy conditions are met with updated RSI and MACD conditions.
    """
    ema_signal = check_ema_crossover(df)
    rsi_signal = check_rsi_trend(df, rsi_threshold=RSI_THRESHOLD)
    macd_signal = check_macd_trend(df, macd_hist_threshold=MACD_HIST_THRESHOLD)
    volume_signal = check_volume_trend(df)
    
    # All conditions must be met
    return ema_signal and rsi_signal and macd_signal and volume_signal


def check_ema_crossunder(df, short_period=EMA_SHORT_PERIOD, long_period=EMA_LONG_PERIOD, period=2):
    """
    Check if EMA10 has crossed below EMA20 and stayed below for 'period' consecutive periods.
    """
    ema_short = df['EMA10']
    ema_long = df['EMA20']
    
    if len(df) < period + 1:
        return False
    
    crossunder = (ema_short.iloc[-period - 1] >= ema_long.iloc[-period - 1]) and (ema_short.iloc[-period] < ema_long.iloc[-period])
    stayed_below = (ema_short[-period:] < ema_long[-period:]).all()
    return crossunder and stayed_below


def check_rsi_downtrend(df, period=3, rsi_threshold=55):
    """
    Check if RSI has crossed below the given threshold (default: 55).
    """
    rsi = df['RSI']
    
    if len(df) < period + 1:
        return False
    
    crossed_below_threshold = (rsi.iloc[-period - 1] > rsi_threshold) and (rsi.iloc[-period] <= rsi_threshold)
    return crossed_below_threshold


def check_macd_downtrend(df, period=3, macd_hist_threshold=-5):
    """
    Check if MACD line has crossed below the signal line.
    """
    macd = df['MACD']
    signal = df['MACD_Signal']
    
    if len(df) < period + 1:
        return False
    
    macd_cross = (macd.iloc[-period - 1] >= signal.iloc[-period - 1]) and (macd.iloc[-period] < signal.iloc[-period])
    return macd_cross


def sell_signal(df):
    """
    Determine if any sell condition is met.
    """
    ema_signal = check_ema_crossunder(df)
    rsi_signal = check_rsi_downtrend(df)
    macd_signal = check_macd_downtrend(df)
    
    # Any condition being met is enough to trigger a sell
    return ema_signal or rsi_signal or macd_signal


def buy_or_sell(df, row, holdings, last_trade=None, cooldown_period=COOLDOWN_PERIOD):
    """
    Determine whether to buy, sell, or hold based on the improved trading rules and cooldown period.
    """
    if last_trade and (row['date'] - last_trade).days < cooldown_period:
        return "HOLD"
    
    # Determine buy or sell signals
    if buy_signal(df):
        return "BUY"
    elif sell_signal(df):
        return "SELL"
    else:
        return "HOLD"