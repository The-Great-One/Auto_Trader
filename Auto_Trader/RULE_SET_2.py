import json
import logging
import os
import random
import time
from datetime import date

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
    "ema_break_atr_mult": float(os.getenv("AT_SELL_EMA_BREAK_ATR_MULT", "0.5")),  # close below EMA10 by > 0.5*ATR
    "ema_confirm_bars": 2,  # need 2 closes below EMA10
    "hist_bearish_threshold": 0.0,  # MACD histogram < 0 is bearish
    "relative_volume_exit": float(os.getenv("AT_SELL_RELATIVE_VOLUME_EXIT", "1.3")),  # exits that need RVOL
    "time_stop_bars": 20,  # legacy fallback if asset-specific settings are unavailable
    "time_stop_min_profit_pct": 3.0,
    "equity_time_stop_bars": int(os.getenv("AT_EQUITY_TIME_STOP_BARS", "8")),
    "equity_time_stop_min_profit_pct": float(os.getenv("AT_EQUITY_TIME_STOP_MIN_PROFIT_PCT", "1.5")),
    "fund_time_stop_bars": int(os.getenv("AT_FUND_TIME_STOP_BARS", "14")),
    "fund_time_stop_min_profit_pct": float(os.getenv("AT_FUND_TIME_STOP_MIN_PROFIT_PCT", "1.0")),
    "equity_review_start_bars": int(os.getenv("AT_EQUITY_REVIEW_START_BARS", "5")),
    "equity_review_end_bars": int(os.getenv("AT_EQUITY_REVIEW_END_BARS", "10")),
    "equity_review_max_profit_pct": float(os.getenv("AT_EQUITY_REVIEW_MAX_PROFIT_PCT", "2.0")),
    "equity_review_rsi": float(os.getenv("AT_EQUITY_REVIEW_RSI", "50.0")),
    "equity_review_macd_hist": float(os.getenv("AT_EQUITY_REVIEW_MACD_HIST", "0.0")),
    "min_atr_fallback_frac": 0.02,  # if ATR missing, fallback = max(0.01, frac * price)
    "breakeven_trigger_pct": float(os.getenv("AT_SELL_BREAKEVEN_TRIGGER_PCT", "2.5")),  # once crossed, SL should protect principal
    "breakeven_buffer_pct": 0.2,  # lock at least +0.2% above avg after trigger
    "momentum_exit_rsi": float(os.getenv("AT_SELL_MOMENTUM_EXIT_RSI", "42.0")),
    "profit_ladder": [  # (profit% threshold, trail = max(last - k*ATR, entry*floor_mult))
        (30, {"k": 0.25, "floor_mult": 1.18}),
        (20, {"k": 0.40, "floor_mult": 1.12}),
        (15, {"k": 0.70, "floor_mult": 1.10}),
        (10, {"k": 1.00, "floor_mult": 1.07}),
        (5, {"k": 1.40, "floor_mult": 1.05}),
        (0, {"k": 1.70, "floor_mult": 0.98}),
    ],
}

# Dip-aware sell guard (for long-horizon accumulation symbols like NIFTYETF)
_DIP_HOLD_SYMBOLS = {
    x.strip().upper()
    for x in os.getenv("AT_DIP_HOLD_SYMBOLS", "NIFTYETF").split(",")
    if x.strip()
}
_DIP_MAX_DRAWDOWN_PCT = max(0.0, float(os.getenv("AT_DIP_MAX_DRAWDOWN_PCT", "12")))
_DIP_STRONG_BREAK_RSI = float(os.getenv("AT_DIP_STRONG_BREAK_RSI", "35"))
_DIP_STRONG_BREAK_MACD = float(os.getenv("AT_DIP_STRONG_BREAK_MACD", "-0.20"))
# Optional kill-switch date: while today <= this date, dip symbols never SELL
_DIP_NO_SELL_UNTIL = os.getenv("AT_DIP_NO_SELL_UNTIL", "").strip()

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
def _normalize_position_state(value) -> dict:
    if isinstance(value, dict):
        stop_loss = _finite(value.get("stop_loss"), np.nan)
        first_seen_date = str(value.get("first_seen_date") or "").strip() or None
        return {
            "stop_loss": None if np.isnan(stop_loss) else float(stop_loss),
            "first_seen_date": first_seen_date,
        }

    stop_loss = _finite(value, np.nan)
    return {
        "stop_loss": None if np.isnan(stop_loss) else float(stop_loss),
        "first_seen_date": None,
    }


def load_position_state_json():
    def _do():
        data = _read_json_unlocked(HOLDINGS_FILE_PATH)
        return {str(k): _normalize_position_state(v) for k, v in data.items()}

    res = _with_lock(CONFIG["lock_timeout_s"], _do)
    if res is None:
        logger.error("Failed to load position-state JSON after retries")
        return {}
    return res


def load_stop_loss_json():
    return {
        symbol: state.get("stop_loss")
        for symbol, state in load_position_state_json().items()
    }


def upsert_position_state_json(tradingsymbol, stop_loss=None, first_seen_date=None):
    symbol = str(tradingsymbol or "").strip()
    if not symbol:
        return

    stop_loss_value = None
    if stop_loss is not None:
        try:
            stop_loss_value = round(float(stop_loss), 2)
            if not np.isfinite(stop_loss_value):
                raise ValueError
        except Exception:
            logger.error("Invalid stop_loss for %s: %r", tradingsymbol, stop_loss)
            return

    def _do():
        data = _read_json_unlocked(HOLDINGS_FILE_PATH)
        current = _normalize_position_state(data.get(symbol))
        if stop_loss_value is not None:
            current["stop_loss"] = stop_loss_value
        if first_seen_date is not None:
            current["first_seen_date"] = str(first_seen_date)
        elif not current.get("first_seen_date"):
            current["first_seen_date"] = date.today().isoformat()
        data[symbol] = current
        _atomic_write(data, HOLDINGS_FILE_PATH)

    _with_lock(CONFIG["lock_timeout_s"], _do)


def update_stop_loss_json(tradingsymbol, stop_loss):
    upsert_position_state_json(tradingsymbol, stop_loss=stop_loss)


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


def _dip_guard_blocks_sell(tradingsymbol: str, profit_pct: float, rsi: float, macd_hist: float) -> bool:
    symbol_u = (tradingsymbol or "").upper()
    if symbol_u not in _DIP_HOLD_SYMBOLS:
        return False

    # Hard no-sell window (useful when intentionally buying panic dips)
    if _DIP_NO_SELL_UNTIL:
        try:
            today = np.datetime64("today")
            deadline = np.datetime64(_DIP_NO_SELL_UNTIL)
            if today <= deadline:
                logger.info("Dip guard hold for %s until %s", symbol_u, _DIP_NO_SELL_UNTIL)
                return True
        except Exception:
            pass

    # During moderate drawdown, prefer HOLD unless breakdown is very strong
    if np.isfinite(profit_pct) and (-_DIP_MAX_DRAWDOWN_PCT <= profit_pct < 0):
        rsi_bad = np.isfinite(rsi) and (rsi <= _DIP_STRONG_BREAK_RSI)
        macd_bad = np.isfinite(macd_hist) and (macd_hist <= _DIP_STRONG_BREAK_MACD)
        if not (rsi_bad and macd_bad):
            logger.info(
                "Dip guard HOLD %s at %.2f%% (RSI=%s, MACD_Hist=%s)",
                symbol_u,
                profit_pct,
                rsi,
                macd_hist,
            )
            return True

    return False


def _is_etf_like_symbol(tradingsymbol: str) -> bool:
    symbol_u = (tradingsymbol or "").upper()
    etf_tokens = (
        "ETF",
        "IETF",
        "BEES",
        "JUNIORBEES",
        "NIFTY",
        "SENSEX",
        "MID100",
        "NIF100",
        "NIFTY1",
        "GOLD",
        "SILVER",
        "PSUBANK",
        "BANKPSU",
        "BANKETF",
        "BANKIETF",
        "AUTOBEES",
        "INFRABEES",
        "MOMENTUM",
        "LIQUID",
        "NV20",
        "BSE500",
        "SETFNIF",
    )
    return any(token in symbol_u for token in etf_tokens)


def _estimate_bars_in_trade(first_seen_date: str | None) -> float:
    try:
        if not first_seen_date:
            return np.nan
        started = date.fromisoformat(str(first_seen_date))
        return float(max(0, (date.today() - started).days))
    except Exception:
        return np.nan


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

    # ---- Stop-loss / position state ----
    position_state_map = load_position_state_json()
    position_state = position_state_map.get(
        tradingsymbol,
        {"stop_loss": None, "first_seen_date": None},
    )
    stop_loss = position_state.get("stop_loss")
    stop_loss = stop_loss if (stop_loss is None or np.isfinite(stop_loss)) else None
    if not position_state.get("first_seen_date"):
        first_seen_date = date.today().isoformat()
        upsert_position_state_json(tradingsymbol, stop_loss=stop_loss, first_seen_date=first_seen_date)
        position_state["first_seen_date"] = first_seen_date

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

    def _maybe_sell():
        if _dip_guard_blocks_sell(tradingsymbol, profit_pct, last_rsi, macd_hist):
            return "HOLD"
        return handle_sell(tradingsymbol)

    is_etf_like = _is_etf_like_symbol(tradingsymbol)

    try:
        if "bars_in_trade" in h.columns:
            bars_in_trade = _finite(h["bars_in_trade"].iloc[0], np.nan)
        else:
            bars_in_trade = np.nan
    except Exception:
        bars_in_trade = np.nan

    if not np.isfinite(bars_in_trade):
        bars_in_trade = _estimate_bars_in_trade(position_state.get("first_seen_date"))

    review_window_hit = (
        (not is_etf_like)
        and np.isfinite(bars_in_trade)
        and CONFIG["equity_review_start_bars"] <= bars_in_trade <= CONFIG["equity_review_end_bars"]
    )

    # ---- Start with a baseline trailing SL ----
    new_sl = stop_loss if stop_loss is not None else (last_price - 2.0 * last_atr)
    if profit_pct >= CONFIG["breakeven_trigger_pct"]:
        be_floor = average_price * (1.0 + CONFIG["breakeven_buffer_pct"] / 100.0)
        new_sl = max(new_sl, be_floor)

    # ---- HARD BREACH first (handles gaps/intrabar) ----
    if np.isfinite(day_low) and np.isfinite(new_sl) and day_low <= new_sl:
        return _maybe_sell()

    # ---- Ordered decision tree ----
    try:
        # 1) LOSS SCENARIO: prefer structure & hard signals over oscillators
        if last_price < average_price:
            # Donchian structure break → exit
            if have_donch and last_price < donch_low:
                return _maybe_sell()

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
                        return _maybe_sell()

            # MACD histogram bearish (optional in chop; stronger in trend)
            if have_hist and macd_hist < CONFIG["hist_bearish_threshold"]:
                new_sl = max(new_sl, last_price - 1.0 * last_atr)

            # RSI capitulation (only as a tie-breaker; avoid overusing oscillators)
            if have_rsi and last_rsi < 40:
                return _maybe_sell()

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
                return _maybe_sell()

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
                    return _maybe_sell()

            # Donchian structure break in profit → exit
            if have_donch and last_price < donch_low:
                return _maybe_sell()

            # RVOL-confirmed weakness: price < EMA50 with heavy volume
            if (
                have_ema50
                and rv > CONFIG["relative_volume_exit"]
                and last_price < ema50
            ):
                return _maybe_sell()

        # 3) TRADEBOOK-DRIVEN MID-HOLD REVIEW
        if review_window_hit and profit_pct < CONFIG["equity_review_max_profit_pct"]:
            weak_rsi = have_rsi and last_rsi < CONFIG["equity_review_rsi"]
            weak_macd = have_hist and macd_hist < CONFIG["equity_review_macd_hist"]
            below_ema10 = have_ema10 and last_price < ema10
            if (weak_rsi and weak_macd) or (below_ema10 and weak_macd):
                return _maybe_sell()
            if below_ema10 and weak_rsi:
                new_sl = max(new_sl, last_price - 0.75 * last_atr)

        # 4) TIME STOP (mostly for chop)
        if is_etf_like:
            time_stop_bars = CONFIG["fund_time_stop_bars"]
            time_stop_min_profit_pct = CONFIG["fund_time_stop_min_profit_pct"]
        else:
            time_stop_bars = CONFIG["equity_time_stop_bars"]
            time_stop_min_profit_pct = CONFIG["equity_time_stop_min_profit_pct"]

        if not np.isfinite(bars_in_trade):
            time_stop_bars = CONFIG["time_stop_bars"]
            time_stop_min_profit_pct = CONFIG["time_stop_min_profit_pct"]

        if np.isfinite(bars_in_trade) and bars_in_trade >= time_stop_bars:
            if profit_pct < time_stop_min_profit_pct:
                return _maybe_sell()

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
                return _maybe_sell()
            if last_price <= new_sl:
                return _maybe_sell()

        return "HOLD"
    except Exception:
        logger.exception("Error finalizing stop-loss and breach check")
        return "HOLD"
