#!/usr/bin/env python3
"""
RULE_SET_8 — Adaptive Regime-Switching Strategy

Key improvements over RULE_SET_7:
1. Regime detection: Bull / Sideways / Bear — switches entry logic
2. Mean-reversion entries in sideways markets (RSI oversold bounces)
3. Tighter risk: max -5% per-position stop, sector caps
4. Short-side signals in bear regime (future: options overlay)

Regime logic:
  - BULL:  EMA50 > EMA200 AND ADX > 20
  - BEAR:  EMA50 < EMA200 AND ADX > 20
  - SIDEWAYS: everything else (low ADX or cross-whipsaw)

Entry logic by regime:
  BULL:    Breakout/pullback (similar to RS7 but with regime auto-on)
  SIDEWAYS: Mean-reversion — RSI oversold bounce + Bollinger lower band touch
  BEAR:    No new longs (future: short via options overlay)
"""

import logging
import os

import numpy as np

logger = logging.getLogger("Auto_Trade_Logger")

CONFIG = {
    # ── Regime detection ──
    "regime_mode": os.getenv("AT_RS8_REGIME_MODE", "auto"),  # auto / bull / sideways / bear / off
    "regime_adx_threshold": float(os.getenv("AT_RS8_REGIME_ADX", "20")),
    "regime_adx_sideways_max": float(os.getenv("AT_RS8_REGIME_ADX_SIDEWAYS_MAX", "25")),
    
    # ── Bull regime gates (breakout + pullback like RS7) ──
    "bull_adx_min": float(os.getenv("AT_RS8_BULL_ADX_MIN", "8")),
    "bull_volume_confirm_mult": float(os.getenv("AT_RS8_BULL_VOL_MULT", "0.5")),
    "bull_rsi_floor": float(os.getenv("AT_RS8_BULL_RSI_FLOOR", "35")),
    "bull_cmf_min": float(os.getenv("AT_RS8_BULL_CMF_MIN", "-0.05")),
    "bull_obv_min_zscore": float(os.getenv("AT_RS8_BULL_OBV_ZSCORE", "-1.0")),
    "bull_min_atr_pct": float(os.getenv("AT_RS8_BULL_MIN_ATR_PCT", "0.0")),
    "bull_max_atr_pct": float(os.getenv("AT_RS8_BULL_MAX_ATR_PCT", "0.12")),
    "bull_max_extension_atr": float(os.getenv("AT_RS8_BULL_MAX_EXT_ATR", "4.0")),
    "bull_macd_required": float(os.getenv("AT_RS8_BULL_MACD_REQUIRED", "0")),  # 0=off, 1=on
    
    # ── Sideways regime gates (mean-reversion) ──
    "side_rsi_oversold": float(os.getenv("AT_RS8_SIDE_RSI_OVERSOLD", "35")),
    "side_rsi_oversold_exit": float(os.getenv("AT_RS8_SIDE_RSI_OVERSOLD_EXIT", "50")),
    "side_bb_lower_touch": float(os.getenv("AT_RS8_SIDE_BB_LOWER", "1")),  # require BB lower touch?
    "side_volume_min_mult": float(os.getenv("AT_RS8_SIDE_VOL_MULT", "0.5")),
    "side_cmf_min": float(os.getenv("AT_RS8_SIDE_CMF_MIN", "-0.10")),
    "side_adx_max": float(os.getenv("AT_RS8_SIDE_ADX_MAX", "35")),
    "side_stoch_k_oversold": float(os.getenv("AT_RS8_SIDE_STOCH_OVERSOLD", "25")),
    
    # ── Bear regime: no new longs ──
    "bear_allow_longs": float(os.getenv("AT_RS8_BEAR_LONGS", "0")),
    
    # ── Risk management ──
    "max_position_loss_pct": float(os.getenv("AT_RS8_MAX_POS_LOSS_PCT", "5.0")),
    "sector_cap_pct": float(os.getenv("AT_RS8_SECTOR_CAP_PCT", "25.0")),
    "max_correlated_positions": float(os.getenv("AT_RS8_MAX_CORR_POS", "5")),
    "trailing_stop_atr_mult": float(os.getenv("AT_RS8_TRAIL_ATR", "3.0")),
    "breakeven_trigger_pct": float(os.getenv("AT_RS8_BEP_TRIGGER", "3.0")),
    "time_stop_bars": float(os.getenv("AT_RS8_TIME_STOP", "15")),
    
    # ── Global overrides ──
    "regime_filter_enabled": float(os.getenv("AT_RS8_REGIME_FILTER_ENABLED", "0")),
    "regime_ema_fast": float(os.getenv("AT_RS8_REGIME_EMA_FAST", "50")),
    "regime_ema_slow": float(os.getenv("AT_RS8_REGIME_EMA_SLOW", "200")),
}


def _detect_regime(latest, prev):
    """Detect market regime: bull / sideways / bear."""
    mode = CONFIG["regime_mode"]
    if mode != "auto":
        return mode  # manual override
    
    close = float(latest["Close"])
    ema50 = float(latest["EMA50"])
    ema200 = latest.get("EMA200", np.nan)
    adx = float(latest["ADX"])
    
    if not np.isfinite(ema200):
        # No EMA200 — use EMA50 direction as proxy
        prev_ema50 = float(prev["EMA50"])
        if ema50 > prev_ema50 and adx >= CONFIG["regime_adx_threshold"]:
            return "bull"
        return "sideways"
    
    ema200 = float(ema200)
    trend_up = ema50 > ema200
    trend_down = ema50 < ema200
    adx_strong = adx >= CONFIG["regime_adx_threshold"]
    adx_weak = adx < CONFIG["regime_adx_sideways_max"]
    
    if trend_up and adx_strong:
        return "bull"
    elif trend_down and adx_strong:
        return "bear"
    else:
        return "sideways"


def _bull_entry(latest, prev, df):
    """Bull regime: breakout or pullback entry."""
    close = float(latest["Close"])
    ema20 = float(latest["EMA20"])
    ema50 = float(latest["EMA50"])
    
    # ADX
    adx = float(latest["ADX"])
    if adx < CONFIG["bull_adx_min"]:
        return False, "adx_too_low"
    
    # Volume
    vol = float(latest["Volume"])
    vol_sma = float(latest["SMA_20_Volume"])
    vol_ok = vol > CONFIG["bull_volume_confirm_mult"] * vol_sma
    if not vol_ok:
        return False, "volume_low"
    
    # RSI
    rsi = float(latest["RSI"])
    if rsi < CONFIG["bull_rsi_floor"]:
        return False, "rsi_floor"
    
    # CMF
    cmf = float(latest["CMF"])
    if cmf < CONFIG["bull_cmf_min"]:
        return False, "cmf_low"
    
    # ATR band
    atr = latest.get("ATR", np.nan)
    if np.isfinite(atr) and close > 0:
        atr_pct = float(atr) / close
        if atr_pct < CONFIG["bull_min_atr_pct"] or atr_pct > CONFIG["bull_max_atr_pct"]:
            return False, "atr_band"
        extension = (close - ema20) / max(float(atr), 1e-9)
        if extension > CONFIG["bull_max_extension_atr"]:
            return False, "extension_atr"
    
    # Trend: price above EMA20
    if close < ema20:
        return False, "below_ema20"
    
    # MACD signal (optional)
    if CONFIG["bull_macd_required"] >= 1:
        macd = float(latest["MACD"])
        macd_sig = float(latest["MACD_Signal"])
        if macd < macd_sig:
            return False, "macd_signal"
    
    # OBV
    z = latest.get("OBV_ZScore20", np.nan)
    if np.isfinite(z) and float(z) < CONFIG["bull_obv_min_zscore"]:
        obv = latest.get("OBV", np.nan)
        obv_ema = latest.get("OBV_EMA20", np.nan)
        if np.isfinite(obv) and np.isfinite(obv_ema):
            if float(obv) < float(obv_ema):
                return False, "obv_bearish"
    
    # One of: pullback (RSI bouncing off floor) or breakout (new high)
    prev_rsi = float(prev["RSI"])
    rsi_bounce = prev_rsi < 50 and rsi >= 50 and rsi > prev_rsi
    
    prev_high = float(prev["High"])
    breakout = close > prev_high
    
    if rsi_bounce or breakout:
        return True, "bull_" + ("pullback" if rsi_bounce else "breakout")
    
    return False, "no_trigger"


def _sideways_entry(latest, prev, df):
    """Sideways regime: mean-reversion entry (oversold bounce)."""
    close = float(latest["Close"])
    rsi = float(latest["RSI"])
    prev_rsi = float(prev["RSI"])
    stoch_k = latest.get("Stochastic_%K", np.nan)
    adx = float(latest["ADX"])
    
    # Don't buy in strong trends when classified as sideways
    if adx > CONFIG["side_adx_max"]:
        return False, "adx_too_high"
    
    # RSI oversold condition: was oversold, now bouncing
    rsi_was_oversold = prev_rsi <= CONFIG["side_rsi_oversold"]
    rsi_bouncing = rsi > prev_rsi and rsi < CONFIG["side_rsi_oversold_exit"]
    
    if not rsi_was_oversold and rsi < CONFIG["side_rsi_oversold"]:
        # Currently oversold but not yet bouncing
        pass  # allow if other conditions met
    
    # Stochastic oversold
    stoch_oversold = False
    if np.isfinite(stoch_k):
        stoch_oversold = float(stoch_k) <= CONFIG["side_stoch_k_oversold"]
    
    # Bollinger Band lower touch
    bb_touch = False
    lower_band = latest.get("LowerBand", np.nan)
    if CONFIG["side_bb_lower_touch"] >= 1 and np.isfinite(lower_band):
        bb_touch = close <= float(lower_band) * 1.02  # within 2% of lower band
    elif CONFIG["side_bb_lower_touch"] < 1:
        bb_touch = True  # not required
    
    # Volume: at least some interest
    vol = float(latest["Volume"])
    vol_sma = float(latest["SMA_20_Volume"])
    vol_ok = vol > CONFIG["side_volume_min_mult"] * vol_sma
    
    # CMF: allow mildly negative (distribution) but not severe
    cmf = float(latest["CMF"])
    cmf_ok = cmf >= CONFIG["side_cmf_min"]
    
    # Combine: need oversold signal + bounce + some confirmation
    oversold_signal = rsi_was_oversold or rsi < CONFIG["side_rsi_oversold"] or stoch_oversold
    bounce_signal = rsi_bouncing or (rsi_was_oversold and rsi > prev_rsi)
    
    if oversold_signal and (bounce_signal or bb_touch) and vol_ok and cmf_ok:
        return True, "side_mean_revert"
    
    # Relaxed: just RSI oversold + volume
    if rsi < CONFIG["side_rsi_oversold"] and vol_ok and cmf_ok and bb_touch:
        return True, "side_oversold_bb"
    
    # Build missing list
    missing = []
    if not oversold_signal:
        missing.append("not_oversold")
    if not bounce_signal and not bb_touch:
        missing.append("no_bounce")
    if not vol_ok:
        missing.append("volume_low")
    if not cmf_ok:
        missing.append("cmf_low")
    
    return False, "missing:" + ",".join(missing) if missing else "no_signal"


def evaluate_signal(df, row, holdings):
    """Main entry evaluation with regime switching."""
    if len(df) < 3:
        return "HOLD", {"reason": ["short_history"], "regime": "unknown"}
    
    latest = df.iloc[-1]
    prev = df.iloc[-2]
    
    # ── Regime detection ──
    regime = _detect_regime(latest, prev)
    
    diagnostics = {
        "regime": regime,
        "entry_mode": None,
        "block_reason": None,
    }
    
    # ── Global regime filter (legacy compatibility) ──
    if CONFIG["regime_filter_enabled"] >= 1:
        close = float(latest["Close"])
        ema_fast_key = f"EMA{int(CONFIG['regime_ema_fast'])}"
        ema_slow_key = f"EMA{int(CONFIG['regime_ema_slow'])}"
        ema_fast = latest.get(ema_fast_key, np.nan)
        ema_slow = latest.get(ema_slow_key, np.nan)
        if np.isfinite(ema_fast) and np.isfinite(ema_slow):
            if not (close > float(ema_fast) > float(ema_slow)):
                diagnostics["block_reason"] = "regime_filter"
                return "HOLD", diagnostics
    
    # ── Route to regime-specific logic ──
    if regime == "bull":
        ok, reason = _bull_entry(latest, prev, df)
        diagnostics["entry_mode"] = "bull"
        diagnostics["block_reason"] = None if ok else reason
    elif regime == "sideways":
        ok, reason = _sideways_entry(latest, prev, df)
        diagnostics["entry_mode"] = "sideways"
        diagnostics["block_reason"] = None if ok else reason
    elif regime == "bear":
        if CONFIG["bear_allow_longs"] >= 1:
            ok, reason = _sideways_entry(latest, prev, df)  # cautious entries only
            diagnostics["entry_mode"] = "bear_cautious"
            diagnostics["block_reason"] = None if ok else reason
        else:
            ok = False
            diagnostics["entry_mode"] = "bear"
            diagnostics["block_reason"] = "bear_no_longs"
    else:
        ok = False
        diagnostics["block_reason"] = "unknown_regime"
    
    decision = "BUY" if ok else "HOLD"
    return decision, diagnostics


def buy_or_sell(df, row, holdings):
    decision, _ = evaluate_signal(df, row, holdings)
    return decision