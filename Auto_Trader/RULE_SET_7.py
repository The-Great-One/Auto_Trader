from . import logging, np

logger = logging.getLogger("Auto_Trade_Logger")

def buy_or_sell(df, row, holdings):
    from . import get_mmi_now

    latest = df.iloc[-1]
    prev   = df.iloc[-2]

    # --- helper for slope ---
    def slope_up(series, win=3):
        if len(series) < win:
            return False
        x = np.arange(win)
        y = np.array(series[-win:], dtype=float)
        cov = np.cov(x, y, bias=True)[0, 1]
        var = np.var(x)
        return (cov / var) > 0 if var > 0 else False

    # --- signals ---
    trend_ok   = latest["Close"] > latest["EMA20"]

    adx        = latest["ADX"]
    adx_ok     = adx > 20
    adx_strong = adx >= 25

    macd       = latest["MACD"]
    macd_sig   = latest["MACD_Signal"]
    macd_rising = latest["MACD_Hist"] > prev["MACD_Hist"]

    # Volume
    vol     = latest["Volume"]
    vol_sma = latest["SMA_20_Volume"]
    vol_ok  = vol > 1.1 * vol_sma

    # CMF regime-aware
    cmf = latest["CMF"]
    if adx_strong:
        cmf_ok = (cmf >= 0.03) and (cmf > prev["CMF"])
    elif not adx_ok:
        cmf_ok = (cmf >= 0.10) and (cmf > prev["CMF"])
    else:
        cmf_ok = (cmf >= 0.05) and (cmf > prev["CMF"])

    # OBV
    z         = latest.get("OBV_ZScore20", np.nan)
    obv_trend = latest["OBV"] > latest["OBV_EMA20"]
    obv_slope = slope_up(df["OBV_EMA20"].values)
    obv_ok    = (np.isfinite(z) and z >= 0.5 and obv_trend) or (obv_trend and obv_slope)

    # Breakouts
    prior_high_break = latest["Close"] > prev["High"]
    highN_break      = latest["Close"] >= latest.get("HHV_20", latest["Close"])

    # RSI with adaptive gates
    rsi          = latest["RSI"]
    rsi_slope_up = rsi >= prev["RSI"]

    if rsi < 45:
        return "HOLD"
    if np.isfinite(z) and z >= 2.0 and rsi >= 75:
        return "HOLD"

    rsi_pull_gate = 55
    rsi_momo_gate = 60

    strong_regime = trend_ok and adx_strong and (cmf >= 0.05) and obv_trend
    if strong_regime:
        rsi_pull_gate = 50
        rsi_momo_gate = 55

    rsi_pullback_trigger = (prev["RSI"] < rsi_pull_gate) and (rsi >= rsi_pull_gate) and rsi_slope_up
    rsi_momo_trigger     = (prev["RSI"] < rsi_momo_gate) and (rsi >= rsi_momo_gate) and rsi_slope_up

    # --- Extra safeguard: always demand MACD > Signal ---
    if macd <= macd_sig:
        return "HOLD"

    # Market regime (MMI) guard
    mmi = get_mmi_now()
    if mmi is not None and mmi >= 70:
        return "HOLD"

    # --- Modes ---
    pullback_mode = all((trend_ok, adx_ok, vol_ok, cmf_ok, obv_ok, macd_rising, rsi_pullback_trigger))
    breakout_mode = all((trend_ok, adx_strong, cmf_ok, obv_ok, (rsi_momo_trigger or highN_break or prior_high_break)))

    if pullback_mode or breakout_mode:
        return "BUY"

    return "HOLD"