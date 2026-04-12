import logging
import os

import numpy as np
import pandas as pd

logger = logging.getLogger("Auto_Trade_Logger")

CONFIG = {
    "underlying_rsi_bull_min": float(os.getenv("AT_OPTIONS_UL_RSI_BULL_MIN", "55")),
    "underlying_rsi_bear_max": float(os.getenv("AT_OPTIONS_UL_RSI_BEAR_MAX", "45")),
    "underlying_adx_min": float(os.getenv("AT_OPTIONS_UL_ADX_MIN", "18")),
    "option_rsi_min": float(os.getenv("AT_OPTIONS_RSI_MIN", "56")),
    "volume_confirm_mult": float(os.getenv("AT_OPTIONS_VOLUME_CONFIRM_MULT", "1.1")),
    "oi_sma_mult": float(os.getenv("AT_OPTIONS_OI_SMA_MULT", "1.02")),
    "oi_change_min_pct": float(os.getenv("AT_OPTIONS_OI_CHANGE_MIN_PCT", "1.0")),
    "atr_pct_min": float(os.getenv("AT_OPTIONS_ATR_PCT_MIN", "0.03")),
    "atr_pct_max": float(os.getenv("AT_OPTIONS_ATR_PCT_MAX", "1.5")),
    "buy_score_min": float(os.getenv("AT_OPTIONS_BUY_SCORE_MIN", "6.0")),
    "take_profit_pct": float(os.getenv("AT_OPTIONS_TAKE_PROFIT_PCT", "25.0")),
    "stop_loss_pct": float(os.getenv("AT_OPTIONS_STOP_LOSS_PCT", "12.0")),
    "max_hold_bars": int(os.getenv("AT_OPTIONS_MAX_HOLD_BARS", "4")),
    "exit_rsi": float(os.getenv("AT_OPTIONS_EXIT_RSI", "45.0")),
}


def _finite(val, default=np.nan):
    try:
        out = float(val)
        return out if np.isfinite(out) else default
    except Exception:
        return default



def _side(row) -> str:
    side = str(row.get("option_type") or "").upper().strip()
    if side in {"CE", "PE"}:
        return side
    symbol = str(row.get("tradingsymbol") or "").upper().strip()
    if symbol.endswith("CE"):
        return "CE"
    if symbol.endswith("PE"):
        return "PE"
    return ""



def _holding_for_symbol(holdings: pd.DataFrame, symbol: str) -> pd.DataFrame:
    if holdings is None or holdings.empty or "tradingsymbol" not in holdings.columns:
        return pd.DataFrame()
    return holdings[holdings["tradingsymbol"].astype(str).str.upper() == str(symbol).upper()]



def evaluate_signal(df, row, holdings):
    if len(df) < 10:
        return "HOLD", {"score": 0.0, "reason": ["short_history"]}

    latest = df.iloc[-1]
    prev = df.iloc[-2]
    symbol = str(row.get("tradingsymbol") or latest.get("tradingsymbol") or "").upper()
    side = _side(latest)

    close = _finite(latest.get("Close"))
    prev_close = _finite(prev.get("Close"))
    ema5 = _finite(latest.get("EMA5"))
    ema10 = _finite(latest.get("EMA10"))
    prev_ema10 = _finite(prev.get("EMA10"))
    rsi = _finite(latest.get("RSI"))
    prev_rsi = _finite(prev.get("RSI"))
    macd_hist = _finite(latest.get("MACD_Hist"))
    prev_macd_hist = _finite(prev.get("MACD_Hist"))
    atr = _finite(latest.get("ATR"))
    volume = _finite(latest.get("Volume"), 0.0)
    volume_sma = _finite(latest.get("SMA_20_Volume"), 0.0)
    oi = _finite(latest.get("OI"), 0.0)
    oi_sma5 = _finite(latest.get("OI_SMA5"), 0.0)
    oi_pct = _finite(latest.get("OI_PctChange"), 0.0)

    ul_close = _finite(latest.get("UL_Close"))
    ul_ema20 = _finite(latest.get("UL_EMA20"))
    ul_ema50 = _finite(latest.get("UL_EMA50"))
    ul_rsi = _finite(latest.get("UL_RSI"))
    ul_adx = _finite(latest.get("UL_ADX"))
    ul_macd_hist = _finite(latest.get("UL_MACD_Hist"))
    ul_supertrend_dir = bool(latest.get("UL_Supertrend_Direction", True))

    bullish_underlying = all(
        (
            side == "CE",
            np.isfinite(ul_close),
            np.isfinite(ul_ema20),
            np.isfinite(ul_ema50),
            ul_close > ul_ema20 > ul_ema50,
            ul_rsi >= CONFIG["underlying_rsi_bull_min"],
            ul_adx >= CONFIG["underlying_adx_min"],
            ul_macd_hist >= 0,
            ul_supertrend_dir,
        )
    )
    bearish_underlying = all(
        (
            side == "PE",
            np.isfinite(ul_close),
            np.isfinite(ul_ema20),
            np.isfinite(ul_ema50),
            ul_close < ul_ema20 < ul_ema50,
            ul_rsi <= CONFIG["underlying_rsi_bear_max"],
            ul_adx >= CONFIG["underlying_adx_min"],
            ul_macd_hist <= 0,
            not ul_supertrend_dir,
        )
    )
    underlying_ok = bullish_underlying or bearish_underlying

    atr_pct = atr / close if np.isfinite(atr) and close > 0 else np.nan
    price_momo = np.isfinite(close) and np.isfinite(ema10) and close > ema10 and close >= prev_close and ema10 >= prev_ema10
    ema_stack = np.isfinite(ema5) and np.isfinite(ema10) and close > ema5 > ema10
    rsi_ok = np.isfinite(rsi) and rsi >= CONFIG["option_rsi_min"] and rsi >= prev_rsi
    macd_ok = np.isfinite(macd_hist) and macd_hist > 0 and macd_hist >= prev_macd_hist
    rsi_available = np.isfinite(rsi)
    macd_available = np.isfinite(macd_hist)
    volume_ok = volume_sma <= 0 or volume >= CONFIG["volume_confirm_mult"] * volume_sma
    oi_ok = (oi_sma5 <= 0 and oi > 0) or (oi_sma5 > 0 and oi >= CONFIG["oi_sma_mult"] * oi_sma5 and oi_pct >= CONFIG["oi_change_min_pct"])
    atr_ok = np.isfinite(atr_pct) and CONFIG["atr_pct_min"] <= atr_pct <= CONFIG["atr_pct_max"]
    breakout_ok = np.isfinite(prev.get("High", np.nan)) and close > _finite(prev.get("High"), np.inf)

    score = 0.0
    reasons = []
    if underlying_ok:
        score += 3.0
        reasons.append("underlying_alignment")
    if price_momo:
        score += 1.5
        reasons.append("price_above_ema10")
    if ema_stack:
        score += 1.0
        reasons.append("ema_stack")
    if rsi_ok:
        score += 1.0
        reasons.append("option_rsi")
    elif not rsi_available:
        reasons.append("rsi_unavailable")
    if macd_ok:
        score += 1.0
        reasons.append("macd_hist_rising")
    elif not macd_available:
        reasons.append("macd_unavailable")
    if volume_ok:
        score += 1.0
        reasons.append("volume_confirm")
    if oi_ok:
        score += 1.0
        reasons.append("oi_confirm")
    if breakout_ok:
        score += 0.5
        reasons.append("breakout")
    if not atr_ok:
        reasons.append("atr_filter_fail")

    holding = _holding_for_symbol(holdings, symbol)
    in_position = not holding.empty and int(_finite(holding.iloc[0].get("quantity"), 0)) > 0

    if in_position:
        avg = _finite(holding.iloc[0].get("average_price"), close)
        bars_in_trade = int(_finite(holding.iloc[0].get("bars_in_trade"), 0) or 0)
        profit_pct = ((close - avg) / avg * 100.0) if avg > 0 else 0.0
        adverse_underlying = (side == "CE" and not bullish_underlying) or (side == "PE" and not bearish_underlying)
        momentum_lost = (np.isfinite(ema10) and close < ema10) or (np.isfinite(rsi) and rsi <= CONFIG["exit_rsi"])
        should_sell = any(
            (
                profit_pct >= CONFIG["take_profit_pct"],
                profit_pct <= -CONFIG["stop_loss_pct"],
                bars_in_trade >= CONFIG["max_hold_bars"],
                adverse_underlying and momentum_lost,
                score < max(3.0, CONFIG["buy_score_min"] - 2.0) and momentum_lost,
            )
        )
        return (
            "SELL" if should_sell else "HOLD",
            {
                "score": round(score, 3),
                "side": side,
                "profit_pct": round(profit_pct, 3),
                "bars_in_trade": bars_in_trade,
                "reason": reasons,
            },
        )

    should_buy = all(
        (
            underlying_ok,
            price_momo,
            volume_ok,
            oi_ok,
            atr_ok,
            score >= CONFIG["buy_score_min"],
            (rsi_ok or not rsi_available),
            (macd_ok or not macd_available),
        )
    )
    return (
        "BUY" if should_buy else "HOLD",
        {
            "score": round(score, 3),
            "side": side,
            "profit_pct": None,
            "bars_in_trade": 0,
            "reason": reasons,
        },
    )



def buy_or_sell(df, row, holdings):
    decision, _ = evaluate_signal(df, row, holdings)
    return decision
