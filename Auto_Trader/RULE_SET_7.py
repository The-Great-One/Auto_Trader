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
    "max_obv_zscore": float(os.getenv("AT_BUY_MAX_OBV_ZSCORE", "3.5")),
    "obv_min_zscore": float(os.getenv("AT_BUY_OBV_MIN_ZSCORE", "0.5")),
    "volume_confirm_mult": float(os.getenv("AT_BUY_VOLUME_CONFIRM_MULT", "1.1")),
    "cmf_strong_min": float(os.getenv("AT_BUY_CMF_STRONG_MIN", "0.03")),
    "cmf_base_min": float(os.getenv("AT_BUY_CMF_BASE_MIN", "0.05")),
    "cmf_weak_min": float(os.getenv("AT_BUY_CMF_WEAK_MIN", "0.10")),
    "rsi_floor": float(os.getenv("AT_BUY_RSI_FLOOR", "45")),
    "stoch_pull_max": float(os.getenv("AT_BUY_STOCH_PULL_MAX", "75")),
    "stoch_momo_max": float(os.getenv("AT_BUY_STOCH_MOMO_MAX", "85")),
    # --- New indicator gates ---
    "cci_buy_min": float(os.getenv("AT_BUY_CCI_BUY_MIN", "-100")),     # CCI above -100 = bullish zone
    "willr_oversold_max": float(os.getenv("AT_BUY_WILLR_OVERSOLD_MAX", "-20")),  # Williams %R above -20 = overbought
    "vwap_buy_above": float(os.getenv("AT_BUY_VWAP_BUY_ABOVE", "1")),  # 1 = must be above VWAP, 0 = skip
    "ich_cloud_bull": float(os.getenv("AT_BUY_ICH_CLOUD_BULL", "1")),  # 1 = must be above Ichimoku cloud, 0 = skip
    "sar_buy_enabled": float(os.getenv("AT_BUY_SAR_ENABLED", "0")),      # 1 = price must be above SAR, 0 = skip gate
    "di_plus_min": float(os.getenv("AT_BUY_DI_PLUS_MIN", "0")),        # minimum DI+ (0 = skip)
    "di_cross_enabled": float(os.getenv("AT_BUY_DI_CROSS_ENABLED", "0")), # 1 = DI+ must be > DI-, 0 = skip
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
    vol_ok = vol > CONFIG["volume_confirm_mult"] * vol_sma

    # CMF regime-aware
    cmf = latest["CMF"]
    if adx_strong:
        cmf_gate = CONFIG["cmf_strong_min"]
    elif not adx_ok:
        cmf_gate = CONFIG["cmf_weak_min"]
    else:
        cmf_gate = CONFIG["cmf_base_min"]
    cmf_ok = (cmf >= cmf_gate) and (cmf > prev["CMF"])

    # OBV
    z = latest.get("OBV_ZScore20", np.nan)
    obv_trend = latest["OBV"] > latest["OBV_EMA20"]
    obv_slope = slope_up(df["OBV_EMA20"].values)
    obv_ok = (np.isfinite(z) and z >= CONFIG["obv_min_zscore"] and obv_trend) or (obv_trend and obv_slope)
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
    stoch_k = latest.get("Stochastic_%K", np.nan)
    supertrend_dir = latest.get("Supertrend_Direction", True)
    supertrend = latest.get("Supertrend", np.nan)
    weekly_sma_20 = latest.get("Weekly_SMA_20", np.nan)
    weekly_sma_200 = latest.get("Weekly_SMA_200", np.nan)

    if rsi < CONFIG["rsi_floor"]:
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

    if np.isfinite(supertrend) and close < float(supertrend):
        return "HOLD"
    if not bool(supertrend_dir):
        return "HOLD"
    if np.isfinite(weekly_sma_20) and np.isfinite(weekly_sma_200) and weekly_sma_20 < weekly_sma_200:
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

    # --- NEW: Additional indicator gates (disabled by default, activated via env vars) ---
    # VWAP: price must be above VWAP (intraday benchmark)
    if CONFIG["vwap_buy_above"] >= 1:
        vwap = latest.get("VWAP", np.nan)
        if np.isfinite(vwap) and close < float(vwap):
            return "HOLD"

    # Ichimoku cloud: price must be above the cloud
    if CONFIG["ich_cloud_bull"] >= 1:
        ich_bull = latest.get("ICH_CLOUD_BULL", np.nan)
        if np.isfinite(ich_bull) and not bool(ich_bull):
            return "HOLD"

    # Parabolic SAR: price must be above SAR
    if CONFIG["sar_buy_enabled"] >= 1:
        sar = latest.get("SAR", np.nan)
        if np.isfinite(sar) and close < float(sar):
            return "HOLD"

    # CCI: must be above oversold zone
    cci = latest.get("CCI", np.nan)
    if np.isfinite(cci) and float(cci) < CONFIG["cci_buy_min"]:
        return "HOLD"

    # Williams %R: not deeply oversold (above threshold = not oversold)
    # Note: Williams %R is negative, -20 is overbought, -80 is oversold
    willr = latest.get("Williams_%R", latest.get("Williams_R", np.nan))
    if np.isfinite(willr) and float(willr) < -80:
        # Deeply oversold — but for buy this could be a contrarian signal, so only block if extreme
        pass  # Don't block on oversold; allow for bounce

    # DMI+/DMI-: if enabled, require DI+ > DI-
    if CONFIG["di_cross_enabled"] >= 1:
        plus_di = latest.get("PLUS_DI", np.nan)
        minus_di = latest.get("MINUS_DI", np.nan)
        if np.isfinite(plus_di) and np.isfinite(minus_di) and float(plus_di) < float(minus_di):
            return "HOLD"

    # Minimum DI+ requirement
    di_plus_min = CONFIG["di_plus_min"]
    if di_plus_min > 0:
        plus_di = latest.get("PLUS_DI", np.nan)
        if np.isfinite(plus_di) and float(plus_di) < di_plus_min:
            return "HOLD"

    # --- Modes ---
    stoch_pull_ok = (not np.isfinite(stoch_k)) or (stoch_k <= CONFIG["stoch_pull_max"])
    stoch_momo_ok = (not np.isfinite(stoch_k)) or (stoch_k <= CONFIG["stoch_momo_max"])

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
            stoch_pull_ok,
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
            stoch_momo_ok,
            (rsi_momo_trigger or highN_break or prior_high_break),
        )
    )

    if pullback_mode or breakout_mode:
        return "BUY"

    return "HOLD"
