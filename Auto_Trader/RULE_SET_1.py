import pandas as pd
import numpy as np

# Define global constants for periods
EMA_SHORT_PERIOD = 10
EMA_LONG_PERIOD = 20
EMA_LONGER_PERIOD = 50
RSI_PERIOD = 14
MACD_FAST_PERIOD = 12
MACD_SLOW_PERIOD = 26
MACD_SIGNAL_PERIOD = 9
VOLUME_MA_PERIOD = 20


def check_ema_crossover(df, period=3):
    """
    Check if EMA10 has crossed above EMA20 and stayed above for 'period' consecutive periods.
    """
    ema_short = df['EMA10']
    ema_long = df['EMA20']
    
    # Ensure we have enough data
    if len(df) < period + 1:
        return False
    
    # Check for crossover at the beginning of the period
    crossover = (ema_short.iloc[-period - 1] <= ema_long.iloc[-period - 1]) and (ema_short.iloc[-period] > ema_long.iloc[-period])
    # Confirm EMA10 has stayed above EMA20 since the crossover
    stayed_above = (ema_short[-period:] > ema_long[-period:]).all()
    return crossover and stayed_above

def check_rsi_trend(df, period=5):
    """
    Check if RSI has been rising for 'period' periods and crossed above 50 from below.
    """
    rsi = df['RSI']
    
    # Ensure we have enough data
    if len(df) < period + 1:
        return False
    
    # Check if RSI crossed above 50 at the beginning of the period
    crossed_above_50 = (rsi.iloc[-period - 1] < 50) and (rsi.iloc[-period] >= 50)
    # Check if RSI has been rising since the crossover
    rsi_diff = rsi[-period:].diff()
    has_been_rising = (rsi_diff[1:] > 0).all()  # Skip NaN at position 0 due to diff()
    return crossed_above_50 and has_been_rising

def check_macd_trend(df, period=3):
    """
    Check if MACD line has crossed above the signal line and MACD Histogram has been increasing.
    """
    macd = df['MACD']
    signal = df['MACD_Signal']
    hist = df['MACD_Hist']
    
    # Ensure we have enough data
    if len(df) < period + 1:
        return False
    
    # Check for MACD line crossing above signal line at the beginning of the period
    macd_cross = (macd.iloc[-period - 1] <= signal.iloc[-period - 1]) and (macd.iloc[-period] > signal.iloc[-period])
    # Confirm MACD Histogram has been increasing since the crossover
    hist_diff = hist[-period:].diff()
    hist_increasing = (hist_diff[1:] > 0).all()
    return macd_cross and hist_increasing

def check_volume_trend(df, period=3):
    """
    Check if Volume is above its 20-period moving average and has been increasing over 'period' periods.
    """
    volume = df['Volume']
    volume_ma = df['Volume_MA']
    
    # Ensure we have enough data
    if len(df) < period + 1:
        return False
    
    # Check if Volume crossed above its moving average at the beginning of the period
    volume_above_ma = (volume.iloc[-period - 1] <= volume_ma.iloc[-period - 1]) and (volume.iloc[-period] > volume_ma.iloc[-period])
    # Check if Volume has been increasing
    volume_diff = volume[-period:].diff()
    volume_increasing = (volume_diff[1:] > 0).all()
    return volume_above_ma and volume_increasing

def check_bullish_divergence(df):
    """
    Optional: Check for bullish divergence between price and RSI.
    """
    price = df['Close']
    rsi = df['RSI']
    
    # Ensure we have enough data
    if len(df) < 7:
        return False
    
    # Price makes lower lows
    price_low_recent = price.iloc[-3:].min()
    price_low_prev = price.iloc[-6:-3].min()
    price_making_lower_lows = price_low_recent < price_low_prev
    
    # RSI makes higher lows
    rsi_low_recent = rsi.iloc[-3:].min()
    rsi_low_prev = rsi.iloc[-6:-3].min()
    rsi_making_higher_lows = rsi_low_recent > rsi_low_prev
    
    return price_making_lower_lows and rsi_making_higher_lows

def buy_signal(df):
    """
    Determine if all buy conditions are met.
    """
    ema_signal = check_ema_crossover(df)
    rsi_signal = check_rsi_trend(df)
    macd_signal = check_macd_trend(df)
    volume_signal = check_volume_trend(df)
    # divergence_signal = check_bullish_divergence(df)  # Optional
    
    # All conditions must be met
    return ema_signal and rsi_signal and macd_signal and volume_signal  # and divergence_signal

def check_ema_crossunder(df, period=3):
    """
    Check if EMA10 has crossed below EMA20 and stayed below for 'period' consecutive periods.
    """
    ema_short = df['EMA10']
    ema_long = df['EMA20']
    
    # Ensure we have enough data
    if len(df) < period + 1:
        return False
    
    # Check for crossunder at the beginning of the period
    crossunder = (ema_short.iloc[-period - 1] >= ema_long.iloc[-period - 1]) and (ema_short.iloc[-period] < ema_long.iloc[-period])
    # Confirm EMA10 has stayed below EMA20 since the crossunder
    stayed_below = (ema_short[-period:] < ema_long[-period:]).all()
    return crossunder and stayed_below

def check_rsi_downtrend(df, period=5):
    """
    Check if RSI has been falling for 'period' periods and crossed below 50 from above.
    """
    rsi = df['RSI']
    
    # Ensure we have enough data
    if len(df) < period + 1:
        return False
    
    # Check if RSI crossed below 50 at the beginning of the period
    crossed_below_50 = (rsi.iloc[-period - 1] > 50) and (rsi.iloc[-period] <= 50)
    # Check if RSI has been falling since the crossover
    rsi_diff = rsi[-period:].diff()
    has_been_falling = (rsi_diff[1:] < 0).all()
    return crossed_below_50 and has_been_falling

def check_macd_downtrend(df, period=3):
    """
    Check if MACD line has crossed below the signal line and MACD Histogram has been decreasing.
    """
    macd = df['MACD']
    signal = df['MACD_Signal']
    hist = df['MACD_Hist']
    
    # Ensure we have enough data
    if len(df) < period + 1:
        return False
    
    # Check for MACD line crossing below signal line at the beginning of the period
    macd_cross = (macd.iloc[-period - 1] >= signal.iloc[-period - 1]) and (macd.iloc[-period] < signal.iloc[-period])
    # Confirm MACD Histogram has been decreasing since the crossover
    hist_diff = hist[-period:].diff()
    hist_decreasing = (hist_diff[1:] < 0).all()
    return macd_cross and hist_decreasing

def check_volume_downtrend(df, period=3):
    """
    Check if Volume is above its 20-period moving average and has been increasing over 'period' periods.
    """
    volume = df['Volume']
    volume_ma = df['Volume_MA']
    
    # Ensure we have enough data
    if len(df) < period + 1:
        return False
    
    # For sell signal, volume is above average and increasing (indicating strong selling pressure)
    volume_above_ma = (volume.iloc[-period:] > volume_ma.iloc[-period:]).all()
    volume_diff = volume[-period:].diff()
    volume_increasing = (volume_diff[1:] > 0).all()
    return volume_above_ma and volume_increasing

def check_bearish_divergence(df):
    """
    Optional: Check for bearish divergence between price and RSI.
    """
    price = df['Close']
    rsi = df['RSI']
    
    # Ensure we have enough data
    if len(df) < 7:
        return False
    
    # Price makes higher highs
    price_high_recent = price.iloc[-3:].max()
    price_high_prev = price.iloc[-6:-3].max()
    price_making_higher_highs = price_high_recent > price_high_prev
    
    # RSI makes lower highs
    rsi_high_recent = rsi.iloc[-3:].max()
    rsi_high_prev = rsi.iloc[-6:-3].max()
    rsi_making_lower_highs = rsi_high_recent < rsi_high_prev
    
    return price_making_higher_highs and rsi_making_lower_highs

def sell_signal(df):
    """
    Determine if all sell conditions are met.
    """
    ema_signal = check_ema_crossunder(df)
    rsi_signal = check_rsi_downtrend(df)
    macd_signal = check_macd_downtrend(df)
    volume_signal = check_volume_downtrend(df)
    # divergence_signal = check_bearish_divergence(df)  # Optional
    
    # All conditions must be met
    return ema_signal and rsi_signal and macd_signal and volume_signal  # and divergence_signal

def buy_or_sell(df, row, holdings):
    """
    Determine whether to buy, sell, or hold based on the improved trading rules.
    """
    # Determine the maximum look-back period required
    required_periods = max(50, RSI_PERIOD + 1, MACD_SLOW_PERIOD + MACD_SIGNAL_PERIOD)
    if len(df) < required_periods:
        return "HOLD"
    
    if buy_signal(df):
        return "BUY"
    elif sell_signal(df):
        return "SELL"
    else:
        return "HOLD"