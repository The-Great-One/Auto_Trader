import logging
import os

import numpy as np

logger = logging.getLogger("Auto_Trade_Logger")

CONFIG = {
    "adx_min": float(os.getenv("AT_BUY_ADX_MIN", "18")),
    "adx_strong_min": float(os.getenv("AT_BUY_ADX_STRONG_MIN", "25")),
    "mmi_risk_off": float(os.getenv("AT_BUY_MMI_RISK_OFF", "65")),
    "min_atr_pct": float(os.getenv("AT_BUY_MIN_ATR_PCT", "0.006")),
    "max_atr_pct": float(os.getenv("AT_BUY_MAX_ATR_PCT", "0.09")),
    "max_extension_atr": float(os.getenv("AT_BUY_MAX_EXTENSION_ATR", "2.2")),
    "max_obv_zscore": float(os.getenv("AT_BUY_MAX_OBV_ZSCORE", "2.5")),
}


def buy_or_sell(df, row, holdings):
    from .utils import get_mmi_now

    if len(df) < 3:
        return "HOLD"

    latest = df.iloc[-1]
    prev = df.iloc[-2]

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
    close = float(latest["Close"])
    ema20 = float(latest["EMA20"])
    ema50 = float(latest["EMA50"])
    ema200 = latest.get("EMA200", np.nan)
    trend_ok = close > ema20 > ema50
    if np.isfinite(ema200):
        trend_ok = trend_ok and (ema50 > float(ema200))
    trend_slope_ok = (ema20 >= float(prev["EMA20"])) and (ema50 >= float(prev["EMA50"]))

    adx = latest["ADX"]
    adx_ok = adx >= CONFIG["adx_min"]
    adx_strong = adx >= CONFIG["adx_strong_min"]

    macd = latest["MACD"]
    macd_sig = latest["MACD_Signal"]
    macd_rising = latest["MACD_Hist"] > prev["MACD_Hist"]

    # Volume
    vol = latest["Volume"]
    vol_sma = latest["SMA_20_Volume"]
    vol_ok = vol > 1.1 * vol_sma

    # CMF regime-aware
    cmf = latest["CMF"]
    if adx_strong:
        cmf_ok = (cmf >= 0.03) and (cmf > prev["CMF"])
    elif not adx_ok:
        cmf_ok = (cmf >= 0.10) and (cmf > prev["CMF"])
    else:
        cmf_ok = (cmf >= 0.05) and (cmf > prev["CMF"])

    # OBV
    z = latest.get("OBV_ZScore20", np.nan)
    obv_trend = latest["OBV"] > latest["OBV_EMA20"]
    obv_slope = slope_up(df["OBV_EMA20"].values)
    obv_ok = (np.isfinite(z) and z >= 0.5 and obv_trend) or (obv_trend and obv_slope)
    if np.isfinite(z) and z > CONFIG["max_obv_zscore"]:
        return "HOLD"

    # Breakouts
    prior_high_break = close > float(prev["High"])
    hhv20 = latest.get("HHV_20", np.nan)
    prev_hhv20 = prev.get("HHV_20", np.nan)
    highN_break = False
    if np.isfinite(hhv20) and np.isfinite(prev_hhv20):
        highN_break = (close > float(hhv20)) and (
            float(prev["Close"]) <= float(prev_hhv20)
        )

    # RSI with adaptive gates
    rsi = latest["RSI"]
    rsi_slope_up = rsi >= prev["RSI"]

    if rsi < 45:
        return "HOLD"
    if np.isfinite(z) and z >= 2.0 and rsi >= 75:
        return "HOLD"

    atr = latest.get("ATR", np.nan)
    if np.isfinite(atr) and close > 0:
        atr_pct = float(atr) / close
        if not (CONFIG["min_atr_pct"] <= atr_pct <= CONFIG["max_atr_pct"]):
            return "HOLD"
        extension_atr = (close - ema20) / max(float(atr), 1e-9)
        if extension_atr > CONFIG["max_extension_atr"]:
            return "HOLD"

    rsi_pull_gate = 55
    rsi_momo_gate = 60

    strong_regime = trend_ok and adx_strong and (cmf >= 0.05) and obv_trend
    if strong_regime:
        rsi_pull_gate = 50
        rsi_momo_gate = 55

    rsi_pullback_trigger = (
        (prev["RSI"] < rsi_pull_gate) and (rsi >= rsi_pull_gate) and rsi_slope_up
    )
    rsi_momo_trigger = (
        (prev["RSI"] < rsi_momo_gate) and (rsi >= rsi_momo_gate) and rsi_slope_up
    )

    # --- Extra safeguard: always demand MACD > Signal ---
    if macd <= macd_sig:
        return "HOLD"

    # Market regime (MMI) guard
    mmi = get_mmi_now()
    if mmi is not None and mmi >= CONFIG["mmi_risk_off"]:
        return "HOLD"

    # --- Modes ---
    pullback_mode = all(
        (
            trend_ok,
            trend_slope_ok,
            adx_ok,
            vol_ok,
            cmf_ok,
            obv_ok,
            macd_rising,
            rsi_pullback_trigger,
            close >= ema20,
        )
    )
    breakout_mode = all(
        (
            trend_ok,
            trend_slope_ok,
            adx_strong,
            vol_ok,
            cmf_ok,
            obv_ok,
            macd_rising,
            (rsi_momo_trigger or highN_break or prior_high_break),
        )
    )

    if pullback_mode or breakout_mode:
        return "BUY"

    return "HOLD"
