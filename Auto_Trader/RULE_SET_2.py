import json
import logging
import os
import random
import time

import numpy as np
import talib
from filelock import FileLock, Timeout

logger = logging.getLogger("Auto_Trade_Logger")

# ---------- Tunables (sane defaults) ----------
CONFIG = {
    "lock_timeout_s": 30,
    "retry_sleep_base_s": 0.2,
    "donch_period": 20,
    "bb_period": 20,
    "adx_period": 14,
    "trend_adx_min": 20.0,  # regime: trending if ADX >= this
    "ema_break_atr_mult": 0.5,  # close below EMA10 by > 0.5*ATR
    "ema_confirm_bars": 2,  # need 2 closes below EMA10
    "hist_bearish_threshold": 0.0,  # MACD histogram < 0 is bearish
    "relative_volume_exit": 1.3,  # exits that need RVOL
    "time_stop_bars": 20,  # exit if bars_in_trade >= N and profit < time_stop_min_profit
    "time_stop_min_profit_pct": 3.0,
    "min_atr_fallback_frac": 0.02,  # if ATR missing, fallback = max(0.01, frac * price)
    "breakeven_trigger_pct": 2.5,  # once crossed, SL should protect principal
    "breakeven_buffer_pct": 0.2,  # lock at least +0.2% above avg after trigger
    "momentum_exit_rsi": 45.0,
    "profit_ladder": [  # (profit% threshold, trail = max(last - k*ATR, entry*floor_mult))
        (30, {"k": 0.25, "floor_mult": 1.18}),
        (20, {"k": 0.40, "floor_mult": 1.12}),
        (15, {"k": 0.70, "floor_mult": 1.10}),
        (10, {"k": 1.00, "floor_mult": 1.07}),
        (5, {"k": 1.40, "floor_mult": 1.05}),
        (0, {"k": 1.70, "floor_mult": 0.98}),
    ],
}

# ---------- Paths ----------
BASE_DIR = os.path.abspath(os.getenv("AT_STATE_DIR", "./intermediary_files"))
os.makedirs(BASE_DIR, exist_ok=True)

HOLDINGS_FILE_PATH = os.path.join(BASE_DIR, "Holdings.json")
LOCK_FILE_PATH = os.path.join(BASE_DIR, "Holdings.lock")


# ---------- Helpers ----------
def _atomic_write(data: dict, path: str):
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, indent=4)
    os.replace(tmp, path)


def _finite(val, default=np.nan):
    try:
        v = float(val)
        return v if np.isfinite(v) else default
    except Exception:
        return default


def _is_finite_pos(x) -> bool:
    return x is not None and np.isfinite(x) and x > 0


def _get_float(container, key, default=np.nan):
    try:
        return _finite(container[key], default)
    except Exception:
        return default


def _read_json_unlocked(path: str) -> dict:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except json.JSONDecodeError:
        logger.error("Corrupted JSON at %s; resetting to empty.", path)
        return {}
    except Exception:
        logger.exception("Unexpected error reading JSON")
        return {}


def _with_lock(timeout, fn):
    # Simple wrapper for lock-acquire / backoff retries
    for attempt in range(3):
        try:
            with FileLock(LOCK_FILE_PATH, timeout=timeout):
                return fn()
        except Timeout:
            logger.warning("Timeout acquiring lock, retry %d", attempt + 1)
            time.sleep(
                CONFIG["retry_sleep_base_s"] * (attempt + 1) + random.random() * 0.1
            )
        except Exception:
            logger.exception("Lock-guarded operation failed")
            break
    return None


# ---------- Public JSON API ----------
def load_stop_loss_json():
    def _do():
        data = _read_json_unlocked(HOLDINGS_FILE_PATH)
        out = {}
        for k, v in data.items():
            fv = _finite(v, np.nan)
            out[k] = None if np.isnan(fv) else float(fv)
        return out

    res = _with_lock(CONFIG["lock_timeout_s"], _do)
    if res is None:
        logger.error("Failed to load stop-loss JSON after retries")
        return {}
    return res


def update_stop_loss_json(tradingsymbol, stop_loss):
    try:
        stop_loss = round(float(stop_loss), 2)
        if not np.isfinite(stop_loss):
            raise ValueError
    except Exception:
        logger.error("Invalid stop_loss for %s: %r", tradingsymbol, stop_loss)
        return

    def _do():
        data = _read_json_unlocked(HOLDINGS_FILE_PATH)
        data[tradingsymbol] = stop_loss
        _atomic_write(data, HOLDINGS_FILE_PATH)

    _with_lock(CONFIG["lock_timeout_s"], _do)


def handle_sell(tradingsymbol):
    """Remove symbol from JSON under a single lock; return SELL."""

    def _do():
        data = _read_json_unlocked(HOLDINGS_FILE_PATH)
        if tradingsymbol in data:
            data.pop(tradingsymbol, None)
            _atomic_write(data, HOLDINGS_FILE_PATH)

    _with_lock(CONFIG["lock_timeout_s"], _do)
    logger.info("Removed %s from stop-loss JSON after selling", tradingsymbol)
    return "SELL"


# ---------- Main strategy ----------
def buy_or_sell(df, row, holdings):
    # ---- Validate & extract base fields ----
    try:
        instrument_token = int(row["instrument_token"])
        holdings = holdings.assign(
            instrument_token=holdings["instrument_token"].astype(int)
        )
    except Exception:
        logger.exception("Row/holdings missing instrument_token")
        return "HOLD"

    try:
        h = holdings[holdings["instrument_token"] == instrument_token]
    except Exception:
        logger.exception("Error filtering holdings by instrument_token")
        return "HOLD"

    if h.empty:
        logger.debug("No holdings for instrument_token %s. HOLD", instrument_token)
        return "HOLD"

    try:
        tradingsymbol = h["tradingsymbol"].iloc[0]
        average_price = float(h["average_price"].iloc[0])
    except Exception:
        logger.exception("Error extracting tradingsymbol/average_price")
        return "HOLD"

    try:
        last_row = df.iloc[-1]
        last_price = float(last_row["Close"])
        day_low = _get_float(last_row, "Low", np.nan)
    except Exception:
        logger.exception("Error extracting OHLC from df")
        return "HOLD"

    # ---- Stop-loss state ----
    sl_map = load_stop_loss_json()
    stop_loss = sl_map.get(tradingsymbol, None)
    stop_loss = stop_loss if (stop_loss is None or np.isfinite(stop_loss)) else None

    # ---- Indicators (guarded) ----
    last_atr = _get_float(last_row, "ATR", np.nan)
    if not _is_finite_pos(last_atr):
        last_atr = max(0.01, CONFIG["min_atr_fallback_frac"] * last_price)

    last_rsi = _get_float(last_row, "RSI", np.nan)
    have_rsi = np.isfinite(last_rsi)

    ema10 = _get_float(last_row, "EMA10", np.nan)
    have_ema10 = np.isfinite(ema10)

    ema50 = _get_float(last_row, "EMA50", np.nan)
    have_ema50 = np.isfinite(ema50)

    # MACD histogram
    macd_hist = _get_float(last_row, "MACD_Hist", np.nan)
    have_hist = np.isfinite(macd_hist)

    # Relative volume
    try:
        if "Volume" in df and len(df) >= 20:
            rv = float(last_row["Volume"]) / max(
                1e-12, float(df["Volume"].rolling(20).mean().iloc[-1])
            )
        else:
            rv = 1.0
    except Exception:
        rv = 1.0

    # Bollinger Bands (%b failure pattern)
    ub = mb = lb = np.nan
    have_bb = False
    if "Close" in df and len(df) >= CONFIG["bb_period"]:
        try:
            UB, MB, LB = talib.BBANDS(
                df["Close"].astype(float),
                timeperiod=CONFIG["bb_period"],
                nbdevup=2,
                nbdevdn=2,
            )
            ub = float(UB.iloc[-1])
            mb = float(MB.iloc[-1])
            lb = float(LB.iloc[-1])
            have_bb = all(np.isfinite([ub, mb, lb]))
        except Exception:
            pass

    # %b current/prev
    curr_b = prev_b = np.nan
    if have_bb and len(df) >= CONFIG["bb_period"] + 1:
        try:
            prev_UB, prev_MB, prev_LB = talib.BBANDS(
                df["Close"].astype(float),
                timeperiod=CONFIG["bb_period"],
                nbdevup=2,
                nbdevdn=2,
            )
            prev_u = float(prev_UB.iloc[-2])
            prev_l = float(prev_LB.iloc[-2])
            prev_c = float(df["Close"].iloc[-2])
            prev_b = (prev_c - prev_l) / max(1e-9, (prev_u - prev_l))
            curr_b = (last_price - lb) / max(1e-9, (ub - lb))
        except Exception:
            pass

    # ADX regime
    if (
        all(c in df for c in ("High", "Low", "Close"))
        and len(df) >= CONFIG["adx_period"] + 5
    ):
        try:
            adx = talib.ADX(
                df["High"].astype(float),
                df["Low"].astype(float),
                df["Close"].astype(float),
                timeperiod=CONFIG["adx_period"],
            ).iloc[-1]
            _ = np.isfinite(adx) and adx >= CONFIG["trend_adx_min"]
        except Exception:
            pass

    # Donchian structure
    donch_low = np.nan
    have_donch = len(df) >= CONFIG["donch_period"]
    if have_donch:
        try:
            donch_low = float(
                df["Low"].rolling(CONFIG["donch_period"]).min().iloc[-2]
            )  # prior window min
        except Exception:
            have_donch = False

    # Profit %
    try:
        profit_pct = ((last_price - average_price) / average_price) * 100.0
    except Exception:
        logger.exception("Error calculating profit_percent")
        return "HOLD"

    # ---- Start with a baseline trailing SL ----
    new_sl = stop_loss if stop_loss is not None else (last_price - 2.0 * last_atr)
    if profit_pct >= CONFIG["breakeven_trigger_pct"]:
        be_floor = average_price * (1.0 + CONFIG["breakeven_buffer_pct"] / 100.0)
        new_sl = max(new_sl, be_floor)

    # ---- HARD BREACH first (handles gaps/intrabar) ----
    if np.isfinite(day_low) and np.isfinite(new_sl) and day_low <= new_sl:
        return handle_sell(tradingsymbol)

    # ---- Ordered decision tree ----
    try:
        # 1) LOSS SCENARIO: prefer structure & hard signals over oscillators
        if last_price < average_price:
            # Donchian structure break → exit
            if have_donch and last_price < donch_low:
                return handle_sell(tradingsymbol)

            # EMA10 2-bar + ATR-scaled break
            if have_ema10:
                # Need last 2 closes below EMA10 and depth > k*ATR
                if len(df) >= 2:
                    close_1 = float(df["Close"].iloc[-1])
                    close_2 = float(df["Close"].iloc[-2])
                    ema10_2 = (
                        _finite(df["EMA10"].iloc[-2], np.nan)
                        if "EMA10" in df
                        else np.nan
                    )
                    both_below = (
                        np.isfinite(ema10_2)
                        and (close_1 < ema10)
                        and (close_2 < ema10_2)
                    )
                    deep_break = (ema10 - close_1) > (
                        CONFIG["ema_break_atr_mult"] * last_atr
                    )
                    if both_below and deep_break:
                        return handle_sell(tradingsymbol)

            # MACD histogram bearish (optional in chop; stronger in trend)
            if have_hist and macd_hist < CONFIG["hist_bearish_threshold"]:
                new_sl = max(new_sl, last_price - 1.0 * last_atr)

            # RSI capitulation (only as a tie-breaker; avoid overusing oscillators)
            if have_rsi and last_rsi < 40:
                return handle_sell(tradingsymbol)

        # 2) PROFIT SCENARIO: laddered trailing + regime-aware filters
        else:
            # Profit ladder: widen a bit to reduce whipsaws, also lock level gains vs entry
            for thresh, rule in CONFIG["profit_ladder"]:
                if profit_pct >= thresh:
                    k = float(rule["k"])
                    floor_mult = float(rule["floor_mult"])
                    lvl = max(last_price - k * last_atr, average_price * floor_mult)
                    new_sl = max(new_sl, lvl)
                    break  # apply the highest matching tier only

            # MACD histogram < 0 → tighten (trend loss)
            if have_hist and macd_hist < CONFIG["hist_bearish_threshold"]:
                new_sl = max(new_sl, last_price - 0.9 * last_atr)

            # Momentum failure in profit: cut losers sooner once trend weakens.
            if (
                have_rsi
                and have_hist
                and (last_rsi < CONFIG["momentum_exit_rsi"])
                and (macd_hist < CONFIG["hist_bearish_threshold"])
            ):
                return handle_sell(tradingsymbol)

            # Upper-band failure: tag then lose strength (only tighten, don't insta-exit)
            if np.isfinite(prev_b) and np.isfinite(curr_b) and have_bb:
                if prev_b >= 1.0 and curr_b < 0.5 and last_price < mb:
                    new_sl = max(new_sl, last_price - 0.9 * last_atr)

            # EMA10 break with confirmation & ATR depth (optional SELL)
            if have_ema10 and len(df) >= 2 and have_rsi:
                close_1 = float(df["Close"].iloc[-1])
                close_2 = float(df["Close"].iloc[-2])
                ema10_2 = (
                    _finite(df["EMA10"].iloc[-2], np.nan) if "EMA10" in df else np.nan
                )
                both_below = (
                    np.isfinite(ema10_2) and (close_1 < ema10) and (close_2 < ema10_2)
                )
                deep_break = (ema10 - close_1) > (
                    CONFIG["ema_break_atr_mult"] * last_atr
                )
                if both_below and deep_break and (last_rsi < 50):
                    return handle_sell(tradingsymbol)

            # Donchian structure break in profit → exit
            if have_donch and last_price < donch_low:
                return handle_sell(tradingsymbol)

            # RVOL-confirmed weakness: price < EMA50 with heavy volume
            if (
                have_ema50
                and rv > CONFIG["relative_volume_exit"]
                and last_price < ema50
            ):
                return handle_sell(tradingsymbol)

        # 3) TIME STOP (mostly for chop)
        try:
            if "bars_in_trade" in h.columns:
                bars_in_trade = _finite(h["bars_in_trade"].iloc[0], np.nan)
            else:
                bars_in_trade = np.nan
        except Exception:
            bars_in_trade = np.nan
        if np.isfinite(bars_in_trade) and bars_in_trade >= CONFIG["time_stop_bars"]:
            if profit_pct < CONFIG["time_stop_min_profit_pct"]:
                return handle_sell(tradingsymbol)

    except Exception:
        logger.exception("Error applying trading logic")
        return "HOLD"

    # ---- Finalize SL & breach check ----
    try:
        if new_sl is not None and np.isfinite(new_sl):
            if new_sl > last_price:
                logger.warning(
                    "New SL (%.2f) > last_price (%.2f) for %s. Clamp.",
                    new_sl,
                    last_price,
                    tradingsymbol,
                )
                new_sl = last_price

            if (stop_loss is None) or (new_sl > stop_loss):
                update_stop_loss_json(tradingsymbol, new_sl)

            # Re-check with updated SL (covers post-adjust breach)
            if np.isfinite(day_low) and day_low <= new_sl:
                return handle_sell(tradingsymbol)
            if last_price <= new_sl:
                return handle_sell(tradingsymbol)

        return "HOLD"
    except Exception:
        logger.exception("Error finalizing stop-loss and breach check")
        return "HOLD"
