import logging
import os

import numpy as np

logger = logging.getLogger("Auto_Trade_Logger")

CONFIG = {
    "adx_min": float(os.getenv("AT_BUY_ADX_MIN", "10")),
    "adx_strong_min": float(os.getenv("AT_BUY_ADX_STRONG_MIN", "25")),
    "mmi_risk_off": float(os.getenv("AT_BUY_MMI_RISK_OFF", "65")),
    "regime_filter_enabled": float(os.getenv("AT_BUY_REGIME_FILTER_ENABLED", "1")),
    "regime_ema_fast": float(os.getenv("AT_BUY_REGIME_EMA_FAST", "50")),
    "regime_ema_slow": float(os.getenv("AT_BUY_REGIME_EMA_SLOW", "200")),
    "regime_atr_pct_max": float(os.getenv("AT_BUY_REGIME_ATR_PCT_MAX", "0")),
    "min_atr_pct": float(os.getenv("AT_BUY_MIN_ATR_PCT", "0.006")),
    "max_atr_pct": float(os.getenv("AT_BUY_MAX_ATR_PCT", "0.09")),
    "max_extension_atr": float(os.getenv("AT_BUY_MAX_EXTENSION_ATR", "2.2")),
    "max_obv_zscore": float(os.getenv("AT_BUY_MAX_OBV_ZSCORE", "3.5")),
    "obv_min_zscore": float(os.getenv("AT_BUY_OBV_MIN_ZSCORE", "0.5")),
    "volume_confirm_mult": float(os.getenv("AT_BUY_VOLUME_CONFIRM_MULT", "0.85")),
    "cmf_strong_min": float(os.getenv("AT_BUY_CMF_STRONG_MIN", "0.03")),
    "cmf_base_min": float(os.getenv("AT_BUY_CMF_BASE_MIN", "0.05")),
    "cmf_weak_min": float(os.getenv("AT_BUY_CMF_WEAK_MIN", "0.10")),
    "rsi_floor": float(os.getenv("AT_BUY_RSI_FLOOR", "45")),
    "stoch_pull_max": float(os.getenv("AT_BUY_STOCH_PULL_MAX", "75")),
    "stoch_momo_max": float(os.getenv("AT_BUY_STOCH_MOMO_MAX", "85")),
    "cci_buy_min": float(os.getenv("AT_BUY_CCI_BUY_MIN", "-100")),
    "willr_oversold_max": float(os.getenv("AT_BUY_WILLR_OVERSOLD_MAX", "-20")),
    "vwap_buy_above": float(os.getenv("AT_BUY_VWAP_BUY_ABOVE", "1")),
    "ich_cloud_bull": float(os.getenv("AT_BUY_ICH_CLOUD_BULL", "0")),
    "sar_buy_enabled": float(os.getenv("AT_BUY_SAR_ENABLED", "0")),
    "di_plus_min": float(os.getenv("AT_BUY_DI_PLUS_MIN", "0")),
    "di_cross_enabled": float(os.getenv("AT_BUY_DI_CROSS_ENABLED", "0")),
    # --- Mean-reversion entry mode ---
    "meanrev_enabled": float(os.getenv("AT_BUY_MEANREV_ENABLED", "1")),
    "meanrev_rsi_oversold": float(os.getenv("AT_BUY_MEANREV_RSI_OVERSOLD", "35")),
    "meanrev_rsi_max": float(os.getenv("AT_BUY_MEANREV_RSI_MAX", "50")),
    "meanrev_bb_pctb_max": float(os.getenv("AT_BUY_MEANREV_BB_PCTB_MAX", "0.3")),
    "meanrev_adx_max": float(os.getenv("AT_BUY_MEANREV_ADX_MAX", "25")),
    "meanrev_cci_min": float(os.getenv("AT_BUY_MEANREV_CCI_MIN", "-150")),
    "meanrev_stoch_k_max": float(os.getenv("AT_BUY_MEANREV_STOCH_K_MAX", "30")),
}


def _slope_up(series, win=3):
    if len(series) < win:
        return False
    x = np.arange(win)
    y = np.array(series[-win:], dtype=float)
    cov = np.cov(x, y, bias=True)[0, 1]
    var = np.var(x)
    return (cov / var) > 0 if var > 0 else False



def _safe_metric(value, digits=4):
    try:
        out = float(value)
    except Exception:
        return None
    if not np.isfinite(out):
        return None
    return round(out, digits)



def _uniq(items):
    seen = set()
    out = []
    for item in items:
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out



def evaluate_signal(df, row, holdings):
    from .utils import get_mmi_now

    core_gate_names = [
        "trend_ok",
        "trend_slope_ok",
        "adx_ok",
        "volume_confirm",
        "cmf_ok",
        "obv_ok",
        "macd_signal_ok",
        "macd_hist_rising",
        "rsi_floor_ok",
        "atr_band_ok",
        "extension_ok",
        "supertrend_price_ok",
        "supertrend_direction_ok",
        "weekly_trend_ok",
        "mmi_ok",
        "vwap_ok",
        "ich_cloud_ok",
        "sar_ok",
        "cci_ok",
        "di_cross_ok",
        "di_plus_ok",
    ]

    if len(df) < 3:
        return "HOLD", {
            "entry_gate_failures": ["short_history"],
            "hard_blocks": ["short_history"],
            "hard_block_count": 1,
            "nearest_mode": None,
            "nearest_mode_missing": ["short_history"],
            "nearest_mode_missing_count": 1,
            "readiness_score_pct": 0.0,
            "score_gap_to_buy": None,
            "blocker_pressure": {"hard_blocks": 1, "nearest_mode_missing": 1},
            "gate_status": {"enough_history": False},
            "metric_snapshot": {},
            "threshold_snapshot": {
                "adx_min": CONFIG["adx_min"],
                "adx_strong_min": CONFIG["adx_strong_min"],
                "rsi_floor": CONFIG["rsi_floor"],
            },
            "reason": ["short_history"],
        }

    latest = df.iloc[-1]
    prev = df.iloc[-2]

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
    macd_hist = latest.get("MACD_Hist", np.nan)
    prev_macd_hist = prev.get("MACD_Hist", np.nan)
    macd_rising = macd_hist > prev_macd_hist
    macd_signal_ok = macd > macd_sig

    vol = latest["Volume"]
    vol_sma = latest["SMA_20_Volume"]
    vol_ok = vol > CONFIG["volume_confirm_mult"] * vol_sma

    cmf = latest["CMF"]
    prev_cmf = prev["CMF"]
    if adx_strong:
        cmf_gate = CONFIG["cmf_strong_min"]
    elif not adx_ok:
        cmf_gate = CONFIG["cmf_weak_min"]
    else:
        cmf_gate = CONFIG["cmf_base_min"]
    cmf_ok = (cmf >= cmf_gate) and (cmf > prev_cmf)

    z = latest.get("OBV_ZScore20", np.nan)
    obv = latest.get("OBV", np.nan)
    obv_ema20 = latest.get("OBV_EMA20", np.nan)
    obv_trend = obv > obv_ema20
    obv_slope = _slope_up(df["OBV_EMA20"].values)
    obv_ok = (np.isfinite(z) and z >= CONFIG["obv_min_zscore"] and obv_trend) or (obv_trend and obv_slope)
    obv_overextended = np.isfinite(z) and z > CONFIG["max_obv_zscore"]

    prior_high_break = close > float(prev["High"])
    hhv20 = latest.get("HHV_20", np.nan)
    prev_hhv20 = prev.get("HHV_20", np.nan)
    highN_break = False
    if np.isfinite(hhv20) and np.isfinite(prev_hhv20):
        highN_break = (close > float(hhv20)) and (float(prev["Close"]) <= float(prev_hhv20))

    rsi = latest["RSI"]
    prev_rsi = prev["RSI"]
    rsi_floor_ok = rsi >= CONFIG["rsi_floor"]
    rsi_slope_up = rsi >= prev_rsi
    overbought_guard = np.isfinite(z) and z >= 2.0 and rsi >= 75
    stoch_k = latest.get("Stochastic_%K", np.nan)
    bb_pctb = latest.get("BB_PercentB", np.nan)
    if not np.isfinite(bb_pctb):
        upper_band = latest.get("UpperBand", latest.get("BB_Upper", np.nan))
        lower_band = latest.get("LowerBand", latest.get("BB_Lower", np.nan))
        if np.isfinite(upper_band) and np.isfinite(lower_band):
            bb_pctb = (close - float(lower_band)) / max(1e-9, float(upper_band) - float(lower_band))
    supertrend_dir = bool(latest.get("Supertrend_Direction", True))
    supertrend = latest.get("Supertrend", np.nan)
    weekly_sma_20 = latest.get("Weekly_SMA_20", np.nan)
    weekly_sma_200 = latest.get("Weekly_SMA_200", np.nan)

    atr = latest.get("ATR", np.nan)
    atr_pct = np.nan
    extension_atr = np.nan
    atr_band_ok = True
    extension_ok = True
    if np.isfinite(atr) and close > 0:
        atr_pct = float(atr) / close
        atr_band_ok = CONFIG["min_atr_pct"] <= atr_pct <= CONFIG["max_atr_pct"]
        extension_atr = (close - ema20) / max(float(atr), 1e-9)
        extension_ok = extension_atr <= CONFIG["max_extension_atr"]

    supertrend_price_ok = (not np.isfinite(supertrend)) or close >= float(supertrend)
    weekly_trend_ok = True
    if np.isfinite(weekly_sma_20) and np.isfinite(weekly_sma_200):
        weekly_trend_ok = weekly_sma_20 >= weekly_sma_200

    rsi_pull_gate = 55
    rsi_momo_gate = 60
    strong_regime = trend_ok and adx_strong and (cmf >= 0.05) and obv_trend
    if strong_regime:
        rsi_pull_gate = 50
        rsi_momo_gate = 55

    rsi_pullback_trigger = (prev_rsi < rsi_pull_gate) and (rsi >= rsi_pull_gate) and rsi_slope_up
    rsi_momo_trigger = (prev_rsi < rsi_momo_gate) and (rsi >= rsi_momo_gate) and rsi_slope_up

    mmi = get_mmi_now()
    mmi_ok = mmi is None or mmi < CONFIG["mmi_risk_off"]

    vwap = latest.get("VWAP", np.nan)
    vwap_ok = True
    if CONFIG["vwap_buy_above"] >= 1 and np.isfinite(vwap):
        vwap_ok = close >= float(vwap)

    ich_bull = latest.get("ICH_CLOUD_BULL", np.nan)
    ich_cloud_ok = True
    if CONFIG["ich_cloud_bull"] >= 1 and np.isfinite(ich_bull):
        ich_cloud_ok = bool(ich_bull)

    sar = latest.get("SAR", np.nan)
    sar_ok = True
    if CONFIG["sar_buy_enabled"] >= 1 and np.isfinite(sar):
        sar_ok = close >= float(sar)

    cci = latest.get("CCI", np.nan)
    cci_ok = (not np.isfinite(cci)) or (float(cci) >= CONFIG["cci_buy_min"])

    willr = latest.get("Williams_%R", latest.get("Williams_R", np.nan))
    plus_di = latest.get("PLUS_DI", np.nan)
    minus_di = latest.get("MINUS_DI", np.nan)

    di_cross_ok = True
    if CONFIG["di_cross_enabled"] >= 1 and np.isfinite(plus_di) and np.isfinite(minus_di):
        di_cross_ok = float(plus_di) >= float(minus_di)

    di_plus_ok = True
    if CONFIG["di_plus_min"] > 0 and np.isfinite(plus_di):
        di_plus_ok = float(plus_di) >= CONFIG["di_plus_min"]

    stoch_pull_ok = (not np.isfinite(stoch_k)) or (stoch_k <= CONFIG["stoch_pull_max"])
    stoch_momo_ok = (not np.isfinite(stoch_k)) or (stoch_k <= CONFIG["stoch_momo_max"])

    pullback_checks = {
        "trend_ok": bool(trend_ok),
        "trend_slope_ok": bool(trend_slope_ok),
        "adx_ok": bool(adx_ok),
        "volume_confirm": bool(vol_ok),
        "cmf_ok": bool(cmf_ok),
        "obv_ok": bool(obv_ok),
        "macd_hist_rising": bool(macd_rising),
        "rsi_pullback_trigger": bool(rsi_pullback_trigger),
        "stoch_pull_ok": bool(stoch_pull_ok),
        "close_above_ema20": bool(close >= ema20),
    }
    breakout_checks = {
        "trend_ok": bool(trend_ok),
        "trend_slope_ok": bool(trend_slope_ok),
        "adx_strong": bool(adx_strong),
        "volume_confirm": bool(vol_ok),
        "cmf_ok": bool(cmf_ok),
        "obv_ok": bool(obv_ok),
        "macd_hist_rising": bool(macd_rising),
        "stoch_momo_ok": bool(stoch_momo_ok),
        "breakout_trigger": bool(rsi_momo_trigger or highN_break or prior_high_break),
    }
    # --- Mean-reversion entry mode ---
    # Buys oversold bounces in sideways/choppy markets
    # Conditions: low RSI, price near lower BB, low ADX (non-trending), oversold CCI
    meanrev_rsi_oversold = np.isfinite(rsi) and rsi <= CONFIG["meanrev_rsi_oversold"]
    meanrev_rsi_max = np.isfinite(rsi) and rsi <= CONFIG["meanrev_rsi_max"]
    meanrev_bb_bounce = np.isfinite(bb_pctb) and bb_pctb <= CONFIG["meanrev_bb_pctb_max"] and rsi_slope_up
    meanrev_adx_low = adx <= CONFIG["meanrev_adx_max"]
    meanrev_cci_oversold = not np.isfinite(cci) or cci <= CONFIG["meanrev_cci_min"]
    meanrev_stoch_oversold = not np.isfinite(stoch_k) or stoch_k <= CONFIG["meanrev_stoch_k_max"]
    meanrev_checks = {
        "rsi_oversold": bool(meanrev_rsi_oversold),
        "bb_bounce": bool(meanrev_bb_bounce),
        "adx_low": bool(meanrev_adx_low),
        "cci_oversold": bool(meanrev_cci_oversold),
        "stoch_oversold": bool(meanrev_stoch_oversold),
        "rsi_max": bool(meanrev_rsi_max),
        "rsi_slope_up": bool(rsi_slope_up),
    }

    pullback_mode = all(pullback_checks.values())
    breakout_mode = all(breakout_checks.values())
    # Mean-reversion: at least 4 of 7 conditions (allows some flexibility)
    meanrev_score = sum(meanrev_checks.values())
    meanrev_mode = CONFIG["meanrev_enabled"] >= 1 and meanrev_score >= 4 and meanrev_checks.get("rsi_oversold") and meanrev_checks.get("rsi_slope_up")
    pullback_missing = [name for name, ok in pullback_checks.items() if not ok]
    breakout_missing = [name for name, ok in breakout_checks.items() if not ok]
    meanrev_missing = [name for name, ok in meanrev_checks.items() if not ok]
    # Pick nearest mode
    candidates = [
        ("pullback", pullback_missing),
        ("breakout", breakout_missing),
    ]
    if CONFIG["meanrev_enabled"] >= 1:
        candidates.append(("meanrev", meanrev_missing))
    nearest_mode, nearest_mode_missing = min(candidates, key=lambda x: len(x[1]))

    hard_blocks = []
    if obv_overextended:
        hard_blocks.append("obv_overextended")
    if not rsi_floor_ok and not meanrev_mode:
        hard_blocks.append("rsi_floor")
    if overbought_guard:
        hard_blocks.append("overbought_guard")
    if not atr_band_ok:
        hard_blocks.append("atr_band")
    if not extension_ok:
        hard_blocks.append("extension_atr")
    if not supertrend_price_ok and not meanrev_mode:
        hard_blocks.append("supertrend_price")
    if not supertrend_dir and not meanrev_mode:
        hard_blocks.append("supertrend_direction")
    if not weekly_trend_ok and not meanrev_mode:
        hard_blocks.append("weekly_trend")
    if not macd_signal_ok and not meanrev_mode:
        hard_blocks.append("macd_signal_cross")
    if not mmi_ok:
        hard_blocks.append("mmi_risk_off")
    if not vwap_ok and not meanrev_mode:
        hard_blocks.append("vwap")
    if not ich_cloud_ok and not meanrev_mode:
        hard_blocks.append("ich_cloud")
    if not sar_ok and not meanrev_mode:
        hard_blocks.append("sar")
    if not cci_ok and not meanrev_mode:
        hard_blocks.append("cci")
    if not di_cross_ok and not meanrev_mode:
        hard_blocks.append("di_cross")
    if not di_plus_ok and not meanrev_mode:
        hard_blocks.append("di_plus")

    decision = "BUY" if (not hard_blocks and (pullback_mode or breakout_mode or meanrev_mode)) else "HOLD"
    entry_gate_failures = _uniq(hard_blocks + nearest_mode_missing)

    gate_status = {
        "trend_ok": bool(trend_ok),
        "trend_slope_ok": bool(trend_slope_ok),
        "adx_ok": bool(adx_ok),
        "adx_strong": bool(adx_strong),
        "volume_confirm": bool(vol_ok),
        "cmf_ok": bool(cmf_ok),
        "obv_ok": bool(obv_ok),
        "obv_overextended": not bool(obv_overextended),
        "macd_signal_ok": bool(macd_signal_ok),
        "macd_hist_rising": bool(macd_rising),
        "rsi_floor_ok": bool(rsi_floor_ok),
        "atr_band_ok": bool(atr_band_ok),
        "extension_ok": bool(extension_ok),
        "supertrend_price_ok": bool(supertrend_price_ok),
        "supertrend_direction_ok": bool(supertrend_dir),
        "weekly_trend_ok": bool(weekly_trend_ok),
        "mmi_ok": bool(mmi_ok),
        "vwap_ok": bool(vwap_ok),
        "ich_cloud_ok": bool(ich_cloud_ok),
        "sar_ok": bool(sar_ok),
        "cci_ok": bool(cci_ok),
        "di_cross_ok": bool(di_cross_ok),
        "di_plus_ok": bool(di_plus_ok),
        "stoch_pull_ok": bool(stoch_pull_ok),
        "stoch_momo_ok": bool(stoch_momo_ok),
        "meanrev_rsi_oversold": bool(meanrev_rsi_oversold),
        "meanrev_bb_bounce": bool(meanrev_bb_bounce),
        "meanrev_adx_low": bool(meanrev_adx_low),
        "meanrev_cci_oversold": bool(meanrev_cci_oversold),
        "meanrev_stoch_oversold": bool(meanrev_stoch_oversold),
        "rsi_pullback_trigger": bool(rsi_pullback_trigger),
        "rsi_momo_trigger": bool(rsi_momo_trigger),
        "prior_high_break": bool(prior_high_break),
        "highN_break": bool(highN_break),
        "pullback_mode": bool(pullback_mode),
        "breakout_mode": bool(breakout_mode),
        "meanrev_mode": bool(meanrev_mode),
    }

    passed_core_gates = sum(1 for name in core_gate_names if gate_status.get(name))
    readiness_score_pct = round(100.0 * passed_core_gates / len(core_gate_names), 1)
    score_gap_to_buy = round((1.25 * len(hard_blocks)) + (0.75 * len(nearest_mode_missing)), 3)
    blocker_pressure = {
        "hard_blocks": len(hard_blocks),
        "nearest_mode_missing": len(nearest_mode_missing),
        "alternate_mode_missing": len(breakout_missing if nearest_mode == "pullback" else pullback_missing),
    }
    blocker_margins = {
        "adx_gap": _safe_metric(max(0.0, CONFIG["adx_min"] - float(adx))),
        "adx_strong_gap": _safe_metric(max(0.0, CONFIG["adx_strong_min"] - float(adx))),
        "volume_gap_ratio": _safe_metric(max(0.0, CONFIG["volume_confirm_mult"] - (float(vol) / max(float(vol_sma), 1e-9)))),
        "cmf_gap": _safe_metric(max(0.0, float(cmf_gate) - float(cmf))),
        "rsi_floor_gap": _safe_metric(max(0.0, CONFIG["rsi_floor"] - float(rsi))),
        "extension_gap_atr": _safe_metric(max(0.0, float(extension_atr) - CONFIG["max_extension_atr"])) if np.isfinite(extension_atr) else None,
        "atr_below_min_gap": _safe_metric(max(0.0, CONFIG["min_atr_pct"] - float(atr_pct))) if np.isfinite(atr_pct) else None,
        "atr_above_max_gap": _safe_metric(max(0.0, float(atr_pct) - CONFIG["max_atr_pct"])) if np.isfinite(atr_pct) else None,
        "vwap_gap_pct": _safe_metric(max(0.0, (float(vwap) - close) / max(close, 1e-9))) if np.isfinite(vwap) else None,
        "cci_gap": _safe_metric(max(0.0, CONFIG["cci_buy_min"] - float(cci))) if np.isfinite(cci) else None,
        "di_plus_gap": _safe_metric(max(0.0, CONFIG["di_plus_min"] - float(plus_di))) if np.isfinite(plus_di) else None,
    }

    metric_snapshot = {
        "close": _safe_metric(close),
        "ema20": _safe_metric(ema20),
        "ema50": _safe_metric(ema50),
        "ema200": _safe_metric(ema200),
        "adx": _safe_metric(adx),
        "macd": _safe_metric(macd),
        "macd_signal": _safe_metric(macd_sig),
        "macd_hist": _safe_metric(macd_hist),
        "volume": _safe_metric(vol, 2),
        "volume_sma20": _safe_metric(vol_sma, 2),
        "cmf": _safe_metric(cmf),
        "obv": _safe_metric(obv, 2),
        "obv_ema20": _safe_metric(obv_ema20, 2),
        "obv_zscore20": _safe_metric(z),
        "rsi": _safe_metric(rsi),
        "bb_percent_b": _safe_metric(bb_pctb),
        "stochastic_k": _safe_metric(stoch_k),
        "atr": _safe_metric(atr),
        "atr_pct": _safe_metric(atr_pct),
        "extension_atr": _safe_metric(extension_atr),
        "supertrend": _safe_metric(supertrend),
        "weekly_sma_20": _safe_metric(weekly_sma_20),
        "weekly_sma_200": _safe_metric(weekly_sma_200),
        "vwap": _safe_metric(vwap),
        "ich_cloud_bull": None if not np.isfinite(ich_bull) else bool(ich_bull),
        "sar": _safe_metric(sar),
        "cci": _safe_metric(cci),
        "williams_r": _safe_metric(willr),
        "plus_di": _safe_metric(plus_di),
        "minus_di": _safe_metric(minus_di),
        "mmi": _safe_metric(mmi),
        "cmf_gate": _safe_metric(cmf_gate),
        "rsi_pull_gate": _safe_metric(rsi_pull_gate),
        "rsi_momo_gate": _safe_metric(rsi_momo_gate),
    }

    threshold_snapshot = {
        "adx_min": CONFIG["adx_min"],
        "adx_strong_min": CONFIG["adx_strong_min"],
        "mmi_risk_off": CONFIG["mmi_risk_off"],
        "min_atr_pct": CONFIG["min_atr_pct"],
        "max_atr_pct": CONFIG["max_atr_pct"],
        "max_extension_atr": CONFIG["max_extension_atr"],
        "max_obv_zscore": CONFIG["max_obv_zscore"],
        "obv_min_zscore": CONFIG["obv_min_zscore"],
        "volume_confirm_mult": CONFIG["volume_confirm_mult"],
        "cmf_gate": round(float(cmf_gate), 4),
        "rsi_floor": CONFIG["rsi_floor"],
        "stoch_pull_max": CONFIG["stoch_pull_max"],
        "stoch_momo_max": CONFIG["stoch_momo_max"],
        "cci_buy_min": CONFIG["cci_buy_min"],
        "vwap_buy_above": CONFIG["vwap_buy_above"],
        "ich_cloud_bull": CONFIG["ich_cloud_bull"],
        "sar_buy_enabled": CONFIG["sar_buy_enabled"],
        "di_plus_min": CONFIG["di_plus_min"],
        "di_cross_enabled": CONFIG["di_cross_enabled"],
        "meanrev_enabled": CONFIG["meanrev_enabled"],
        "meanrev_rsi_oversold": CONFIG["meanrev_rsi_oversold"],
        "meanrev_rsi_max": CONFIG["meanrev_rsi_max"],
        "meanrev_bb_pctb_max": CONFIG["meanrev_bb_pctb_max"],
        "meanrev_adx_max": CONFIG["meanrev_adx_max"],
        "meanrev_cci_min": CONFIG["meanrev_cci_min"],
        "meanrev_stoch_k_max": CONFIG["meanrev_stoch_k_max"],
        "rsi_pull_gate": rsi_pull_gate,
        "rsi_momo_gate": rsi_momo_gate,
    }

    reason = []
    if decision == "BUY":
        if pullback_mode:
            reason.append("pullback_mode")
        if breakout_mode:
            reason.append("breakout_mode")
        if meanrev_mode:
            reason.append("meanrev_mode")
    else:
        if hard_blocks:
            reason.extend(hard_blocks)
        reason.append(f"nearest_mode:{nearest_mode}")

    return decision, {
        "entry_gate_failures": entry_gate_failures,
        "hard_blocks": hard_blocks,
        "hard_block_count": len(hard_blocks),
        "nearest_mode": nearest_mode,
        "nearest_mode_missing": nearest_mode_missing,
        "nearest_mode_missing_count": len(nearest_mode_missing),
        "alternate_mode_missing": breakout_missing if nearest_mode == "pullback" else pullback_missing,
        "readiness_score_pct": readiness_score_pct,
        "score_gap_to_buy": score_gap_to_buy,
        "blocker_pressure": blocker_pressure,
        "blocker_margins": blocker_margins,
        "gate_status": gate_status,
        "metric_snapshot": metric_snapshot,
        "threshold_snapshot": threshold_snapshot,
        "mode_diagnostics": {
            "pullback_missing": pullback_missing,
            "breakout_missing": breakout_missing,
            "meanrev_missing": meanrev_missing,
        },
        "reason": reason,
    }



def buy_or_sell(df, row, holdings):
    decision, _ = evaluate_signal(df, row, holdings)
    return decision
