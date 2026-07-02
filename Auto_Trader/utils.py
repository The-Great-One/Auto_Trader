import json
import multiprocessing
import os
import shutil
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from functools import lru_cache
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import pandas_market_calendars as mcal
import requests
import talib
from filelock import FileLock, Timeout
from kiteconnect import KiteConnect
from retry import retry
from requests.exceptions import RequestException
from sqlalchemy import create_engine

# Import rule set modules
from . import RULE_SET_2, RULE_SET_7
from .news_sentiment import apply_news_overlay
from .tickertape_data import get_mmi_indicator, is_market_open_via_tickertape
from .my_secrets import (
    API_KEY,
    API_SECRET,
    DATABASE,
    DB_PASSWORD,
    HOST,
    USER,
    DEBUG_MODE,
)
from .Request_Token import get_request_token
import logging

logger = logging.getLogger("Auto_Trade_Logger")

TOKEN_PATH = "intermediary_files/access_token.json"
TOKEN_LOCK_PATH = "intermediary_files/access_token.lock"
KITE_MANUAL_LOGIN_FLAG = "intermediary_files/kite_manual_login_required.json"


# Default rule set values
DEFAULT_RULE_SETS = {
    "RULE_SET_2": RULE_SET_2,
    "RULE_SET_7": RULE_SET_7,
}

# Check if any RULE_SET environment variables are set
env_rules_present = any(os.getenv(key) is not None for key in DEFAULT_RULE_SETS)

if env_rules_present:
    # If at least one environment variable is set, use only the ones that are set
    RULE_SETS = {
        key: DEFAULT_RULE_SETS[key]
        for key in DEFAULT_RULE_SETS
        if os.getenv(key) is not None
    }
else:
    # If no environment variables are set, use the default rule sets
    RULE_SETS = DEFAULT_RULE_SETS

# Initialize the NSE market calendar
nse_calendar = mcal.get_calendar("NSE")


def build_access_token():
    """
    Generate a new access token and save it to a JSON file.

    Returns:
        str: The new access token.

    Raises:
        Exception: Re-raises on failure so callers can decide retry/fallback.
        Does NOT call sys.exit() — callers own that decision.
    """
    try:
        kite = KiteConnect(api_key=API_KEY)
        data = kite.generate_session(
            request_token=get_request_token(), api_secret=API_SECRET
        )
        session_data = {
            "access_token": data["access_token"],
            "date": datetime.now().strftime("%Y-%m-%d"),
        }

        # Token refresh must not wipe cached market data, holdings, or other
        # intermediary state. Just ensure the directory exists and replace the
        # token file atomically.
        os.makedirs("intermediary_files", exist_ok=True)
        temp_token_path = f"{TOKEN_PATH}.tmp"

        with open(temp_token_path, "w") as json_file:
            json.dump(session_data, json_file, indent=4)
        os.replace(temp_token_path, TOKEN_PATH)
        try:
            os.remove(KITE_MANUAL_LOGIN_FLAG)
        except FileNotFoundError:
            pass
        logger.info("New access token saved successfully.")
        return data["access_token"]
    except Exception as e:
        logger.error(
            f"Error in generating session: {e}, Traceback: {traceback.format_exc()}"
        )
        raise


def _manual_login_cooldown_seconds() -> int:
    return int(os.getenv("AT_KITE_MANUAL_LOGIN_COOLDOWN_SECONDS", "1800"))


def _raise_if_manual_login_cooldown_active() -> None:
    try:
        with open(KITE_MANUAL_LOGIN_FLAG, "r") as fh:
            payload = json.load(fh)
        created_at = payload.get("created_at")
        created_ts = datetime.fromisoformat(created_at).timestamp() if created_at else 0
    except (FileNotFoundError, json.JSONDecodeError, ValueError, TypeError):
        return

    age = time.time() - created_ts
    base_cooldown = _manual_login_cooldown_seconds()
    error_text = str(payload.get("error") or "").lower()
    # Credential/TOTP failures can lock the Kite account if retried repeatedly.
    # Keep those in cooldown for at least a day unless explicitly overridden.
    if any(
        marker in error_text
        for marker in ("totp", "invalid username", "invalid password", "invalid credentials")
    ):
        cooldown = max(
            base_cooldown,
            int(os.getenv("AT_KITE_CREDENTIAL_FAILURE_COOLDOWN_SECONDS", "86400")),
        )
    else:
        cooldown = base_cooldown
    if age < cooldown:
        raise RuntimeError(
            "Kite login is in manual-intervention cooldown after CAPTCHA/TOTP failure; "
            f"retry_after_seconds={int(cooldown - age)}. Run a manual Kite refresh or wait."
        )


def _mark_manual_login_required(reason: str, exc: Exception) -> None:
    try:
        with open(KITE_MANUAL_LOGIN_FLAG, "w") as fh:
            json.dump(
                {
                    "created_at": datetime.now().isoformat(),
                    "reason": reason,
                    "error": str(exc)[:500],
                },
                fh,
                indent=2,
            )
    except OSError:
        logger.exception("Could not write Kite manual-login cooldown flag")


def _build_access_token_locked(reason: str) -> str:
    """Serialize token creation so concurrent callers do not trigger CAPTCHA storms."""
    os.makedirs("intermediary_files", exist_ok=True)
    _raise_if_manual_login_cooldown_active()
    for lock_attempt in range(2):
        try:
            with FileLock(TOKEN_LOCK_PATH, timeout=180):
                # Another process may have refreshed the token while we waited.
                try:
                    with open(TOKEN_PATH, "r") as json_file:
                        session_data = json.load(json_file)
                    access_token = session_data.get("access_token")
                    session_date = session_data.get("date")
                    if str(datetime.now().date()) == session_date and access_token:
                        logger.info(
                            "Using access token refreshed by another process; reason=%s",
                            reason,
                        )
                        return access_token
                except (FileNotFoundError, json.JSONDecodeError):
                    pass

                logger.warning("Creating a new Kite session; reason=%s", reason)
                try:
                    return build_access_token()
                except Exception as exc:
                    if any(
                        marker in str(exc).lower()
                        for marker in (
                            "captcha",
                            "totp",
                            "manual intervention",
                            "invalid username",
                            "invalid password",
                            "invalid credentials",
                        )
                    ):
                        _mark_manual_login_required(reason, exc)
                    raise
        except PermissionError as exc:
            if lock_attempt == 0:
                try:
                    os.remove(TOKEN_LOCK_PATH)
                    logger.warning(
                        "Removed inaccessible Kite token lock and retrying; path=%s reason=%s",
                        TOKEN_LOCK_PATH,
                        reason,
                    )
                    continue
                except OSError:
                    pass
            raise RuntimeError(
                f"Cannot acquire Kite access-token lock {TOKEN_LOCK_PATH}: {exc}. "
                "Fix file ownership/permissions instead of creating a new token."
            ) from exc
        except Timeout:
            raise RuntimeError("Timed out waiting for Kite access-token refresh lock")
    raise RuntimeError("Unable to acquire Kite access-token refresh lock")


def _is_invalid_token_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return any(
        marker in text
        for marker in (
            "invalid token",
            "token is invalid",
            "api_token",
            "access_token",
            "403",
            "api_key",
        )
    )


def read_session_data():
    """Read today's cached access token; create one only on true cache miss/stale.

    This intentionally does not validate with a broker API call. Validation belongs
    in initialize_kite(), after the token has been loaded, so transient network/API
    errors do not delete a usable token and trigger automated login/CAPTCHA.
    """
    try:
        with open(TOKEN_PATH, "r") as json_file:
            session_data = json.load(json_file)
        access_token = session_data.get("access_token")
        session_date = session_data.get("date")

        if str(datetime.now().date()) == session_date and access_token:
            return access_token
        return _build_access_token_locked("missing_or_stale_token")

    except FileNotFoundError:
        return _build_access_token_locked("token_file_missing")
    except json.JSONDecodeError:
        return _build_access_token_locked("token_file_corrupt")
    except PermissionError as exc:
        # Do not attempt login when the real issue is local filesystem state.
        raise RuntimeError(
            f"Cannot read Kite access token file {TOKEN_PATH}: {exc}. "
            "Fix file ownership/permissions instead of creating a new token."
        ) from exc


def initialize_kite():
    """
    Initialize the KiteConnect object with a valid session.

    Returns:
        KiteConnect: An instance of KiteConnect with a valid session.

    Raises:
        Exception: After exhausting all retries, including token rebuild
            attempts. Recoverable failures (bad token, CAPTCHA) trigger
            cache-clear + rebuild; only hard-fail after max_retries.
    """
    max_retries = 3
    for attempt in range(1, max_retries + 1):
        try:
            kite = KiteConnect(api_key=API_KEY)
            access_token = read_session_data()
            kite.set_access_token(access_token)
            # Validate immediately so we don't return a broken client
            kite.margins()
            return kite
        except Exception:
            logger.exception(
                "Failed to initialize Kite session (attempt %s/%s).",
                attempt,
                max_retries,
            )
            if attempt >= max_retries:
                raise
            if not _is_invalid_token_error(sys.exc_info()[1]):
                # Network/API/permission/transient failures should not erase a
                # possibly valid token or trigger browser-login/CAPTCHA.
                time.sleep(attempt * 2)
                continue
            try:
                os.remove(TOKEN_PATH)
            except FileNotFoundError:
                pass
            try:
                _build_access_token_locked("invalid_token_validation")
            except Exception:
                logger.exception(
                    "Token rebuild also failed (attempt %s/%s) — will retry.",
                    attempt,
                    max_retries,
                )
            time.sleep(attempt * 2)


def compute_supertrend(
    df: pd.DataFrame,
    atr: np.ndarray,
    *,
    multiplier: float = 2.0,
    sup_col: str = "Supertrend",
    sup_dir: str = "Supertrend_Direction",
) -> None:
    """
    Vectorized Supertrend: appends `sup_col` & `sup_dir` in-place.

    Be defensive around empty/short ATR inputs so downstream shadow/lab
    workflows do not crash on sparse histories or partially-formed arrays.
    """
    n = len(df)
    if n == 0:
        df[sup_col] = np.nan
        df[sup_dir] = True
        return

    atr_arr = np.asarray(atr, dtype="float64").reshape(-1)
    if atr_arr.size == 0:
        atr_arr = np.full(n, np.nan, dtype="float64")
    elif atr_arr.size == 1 and n > 1:
        atr_arr = np.full(n, float(atr_arr[0]), dtype="float64")
    elif atr_arr.size != n:
        padded = np.full(n, np.nan, dtype="float64")
        copy_n = min(n, atr_arr.size)
        padded[:copy_n] = atr_arr[:copy_n]
        atr_arr = padded

    hl2 = (df["High"].values + df["Low"].values) * 0.5
    up = hl2 + multiplier * atr_arr
    dn = hl2 - multiplier * atr_arr

    up_shift = np.roll(up, 1)
    dn_shift = np.roll(dn, 1)
    up_shift[0] = dn_shift[0] = np.nan

    raw = np.where(
        df["Close"].values > up_shift,
        dn,
        np.where(df["Close"].values < dn_shift, up, np.nan),
    )
    st = pd.Series(raw, index=df.index, dtype="float64").ffill().values
    direction = df["Close"].values > st

    # Assign both columns in one operation to avoid fragmenting large lab frames.
    df[[sup_col, sup_dir]] = pd.DataFrame(
        {sup_col: st, sup_dir: direction},
        index=df.index,
    )


def compute_fibonacci(
    high: pd.Series,
    low: pd.Series,
) -> dict[str, float]:
    """
    Classic Fibonacci retracement.
    """
    top, bot = float(high.max()), float(low.min())
    span = top - bot
    return {
        "Fibonacci_0": bot,
        "Fibonacci_23_6": top - 0.236 * span,
        "Fibonacci_38_2": top - 0.382 * span,
        "Fibonacci_50": top - 0.5 * span,
        "Fibonacci_61_8": top - 0.618 * span,
        "Fibonacci_100": top,
    }


def _round_number_step(close: pd.Series) -> np.ndarray:
    """Price-aware psychological round-number step for Indian equities."""
    price = pd.to_numeric(close, errors="coerce").astype("float64").to_numpy()
    step = np.select(
        [price < 100, price < 500, price < 1000, price < 5000],
        [5.0, 10.0, 25.0, 50.0],
        default=100.0,
    )
    return np.where(np.isfinite(price) & (price > 0), step, np.nan)


def compute_market_structure(
    df: pd.DataFrame,
    *,
    swing_window: int = 5,
    level_window: int = 20,
    vpoc_window: int = 60,
) -> dict[str, pd.Series | np.ndarray]:
    """
    Add deterministic chart-reading features without look-ahead bias.

    Features:
    - confirmed swing high/low levels (fractal pivots confirmed after `swing_window` bars)
    - rolling prior support/resistance from the last `level_window` bars
    - previous-bar pivot levels (PP/R1/R2/S1/S2)
    - psychological round-number support/resistance
    - approximate rolling volume-profile POC using daily OHLCV bars
    """
    high_s = pd.to_numeric(df["High"], errors="coerce").astype("float64")
    low_s = pd.to_numeric(df["Low"], errors="coerce").astype("float64")
    close_s = pd.to_numeric(df["Close"], errors="coerce").astype("float64")
    vol_s = pd.to_numeric(df["Volume"], errors="coerce").astype("float64")

    # Prior rolling S/R — only previous bars are visible to the current bar.
    rolling_resistance = high_s.rolling(level_window, min_periods=max(5, level_window // 2)).max().shift(1)
    rolling_support = low_s.rolling(level_window, min_periods=max(5, level_window // 2)).min().shift(1)

    # Confirmed fractal swings. The shift ensures a pivot only becomes usable
    # after the required right-side confirmation bars have completed.
    pivot_window = swing_window * 2 + 1
    centered_high = high_s.rolling(pivot_window, center=True, min_periods=pivot_window).max()
    centered_low = low_s.rolling(pivot_window, center=True, min_periods=pivot_window).min()
    swing_high_raw = high_s.where(
        high_s.eq(centered_high) & high_s.gt(high_s.shift(1)) & high_s.gt(high_s.shift(-1))
    )
    swing_low_raw = low_s.where(
        low_s.eq(centered_low) & low_s.lt(low_s.shift(1)) & low_s.lt(low_s.shift(-1))
    )
    last_swing_high = swing_high_raw.shift(swing_window + 1).ffill()
    last_swing_low = swing_low_raw.shift(swing_window + 1).ffill()

    sr_resistance = last_swing_high.combine_first(rolling_resistance)
    sr_support = last_swing_low.combine_first(rolling_support)
    sr_dist_support_pct = (close_s - sr_support) / close_s.replace(0, np.nan)
    sr_dist_resistance_pct = (sr_resistance - close_s) / close_s.replace(0, np.nan)

    # Classic previous-bar pivots. For daily bars this is previous-day PP/R/S.
    prev_high = high_s.shift(1)
    prev_low = low_s.shift(1)
    prev_close = close_s.shift(1)
    pivot_pp = (prev_high + prev_low + prev_close) / 3.0
    pivot_r1 = (2.0 * pivot_pp) - prev_low
    pivot_s1 = (2.0 * pivot_pp) - prev_high
    pivot_r2 = pivot_pp + (prev_high - prev_low)
    pivot_s2 = pivot_pp - (prev_high - prev_low)

    # Weekly-ish prior high/low from the last 5 daily bars.
    prev_5d_high = high_s.rolling(5, min_periods=5).max().shift(1)
    prev_5d_low = low_s.rolling(5, min_periods=5).min().shift(1)

    # Psychological round-number support/resistance.
    round_step = _round_number_step(close_s)
    close_arr = close_s.to_numpy()
    round_support = np.floor(close_arr / round_step) * round_step
    round_resistance = np.ceil(close_arr / round_step) * round_step
    round_nearest = np.round(close_arr / round_step) * round_step
    round_resistance_dist_pct = (round_resistance - close_arr) / np.where(close_arr > 0, close_arr, np.nan)
    round_support_dist_pct = (close_arr - round_support) / np.where(close_arr > 0, close_arr, np.nan)

    # Rolling volume profile POC approximation using prior bars only.
    typical = ((high_s + low_s + close_s) / 3.0).to_numpy()
    volume = vol_s.to_numpy()
    vpoc = np.full(len(df), np.nan, dtype="float64")
    for i in range(len(df)):
        start = max(0, i - vpoc_window)
        if i - start < max(10, vpoc_window // 3):
            continue
        prices = typical[start:i]
        vols = volume[start:i]
        mask = np.isfinite(prices) & np.isfinite(vols) & (vols > 0)
        if not mask.any():
            continue
        ref_price = close_arr[i] if np.isfinite(close_arr[i]) and close_arr[i] > 0 else np.nanmedian(prices[mask])
        bucket = max(ref_price * 0.005, 0.05)
        price_bins = np.round(prices[mask] / bucket) * bucket
        volume_by_bin: dict[float, float] = {}
        for price_bin, v in zip(price_bins, vols[mask]):
            volume_by_bin[float(price_bin)] = volume_by_bin.get(float(price_bin), 0.0) + float(v)
        if volume_by_bin:
            vpoc[i] = max(volume_by_bin.items(), key=lambda item: item[1])[0]
    vpoc_s = pd.Series(vpoc, index=df.index)

    return {
        "SR_Rolling_Resistance_20": rolling_resistance,
        "SR_Rolling_Support_20": rolling_support,
        "SR_Last_Swing_High": last_swing_high,
        "SR_Last_Swing_Low": last_swing_low,
        "SR_Resistance": sr_resistance,
        "SR_Support": sr_support,
        "SR_Dist_Support_Pct": sr_dist_support_pct,
        "SR_Dist_Resistance_Pct": sr_dist_resistance_pct,
        "Pivot_PP": pivot_pp,
        "Pivot_R1": pivot_r1,
        "Pivot_R2": pivot_r2,
        "Pivot_S1": pivot_s1,
        "Pivot_S2": pivot_s2,
        "Prev_5D_High": prev_5d_high,
        "Prev_5D_Low": prev_5d_low,
        "Round_Step": round_step,
        "Round_Nearest": round_nearest,
        "Round_Support": round_support,
        "Round_Resistance": round_resistance,
        "Round_Support_Dist_Pct": round_support_dist_pct,
        "Round_Resistance_Dist_Pct": round_resistance_dist_pct,
        "Volume_Profile_POC": vpoc_s,
    }


def compute_cmf(high, low, close, volume, period=20):
    idx = getattr(close, "index", None)

    high = pd.Series(high, index=idx, dtype="float64")
    low = pd.Series(low, index=idx, dtype="float64")
    close = pd.Series(close, index=idx, dtype="float64")
    volume = pd.Series(volume, index=idx, dtype="float64")

    # Money Flow Multiplier; avoid 0-division by turning 0 spans into NaN
    span = (high - low).replace(0, np.nan)
    mfm = ((close - low) - (high - close)) / span

    mfv = mfm * volume

    cmf = (
        mfv.rolling(window=period, min_periods=period).sum()
        / volume.rolling(window=period, min_periods=period).sum()
    )

    return cmf


def Indicators(
    df: pd.DataFrame,
    *,
    rsi_period: int = 14,
    macd_fast: int = 12,
    macd_slow: int = 26,
    macd_signal: int = 9,
    atr_period: int = 14,
) -> pd.DataFrame:
    """
    Append core + advanced indicators to `df` using uppercase column names.

    Usage:
        df = Indicators(df)
    """
    # Required fields
    required = {"High", "Low", "Close", "Volume"}
    if not required.issubset(df.columns):
        raise KeyError(f"DataFrame missing: {', '.join(required - set(df.columns))}")

    if len(df) < 2:
        # Not enough data for indicators; return with minimal columns
        df["RSI"] = np.nan
        df["ADX"] = np.nan
        df["ATR"] = np.nan
        df["Supertrend"] = np.nan
        df["Supertrend_Direction"] = True
        df["Supertrend_Rule_8_Exit"] = np.nan
        df["Supertrend_Direction_Rule_8_Exit"] = True
        return df

    # Coerce numeric dtypes once
    df[["High", "Low", "Close", "Volume"]] = (
        df[["High", "Low", "Close", "Volume"]]
        .apply(pd.to_numeric, errors="coerce")
        .astype("float64")
    )

    high = df["High"].values
    low = df["Low"].values
    close = df["Close"].values
    vol = df["Volume"].values

    # TA‑Lib outputs
    RSI = talib.RSI(close, timeperiod=rsi_period)
    MACD, MACD_Signal, MACD_Hist = talib.MACD(
        close, fastperiod=macd_fast, slowperiod=macd_slow, signalperiod=macd_signal
    )
    MACD_Rule_8, MACD_Rule_8_Signal, MACD_Rule_8_Hist = talib.MACD(
        close, fastperiod=23, slowperiod=9, signalperiod=9
    )
    EMA_periods = (5, 9, 10, 12, 13, 20, 21, 26, 50, 100, 200)
    EMA_values = {f"EMA{p}": talib.EMA(close, timeperiod=p) for p in EMA_periods}
    ATR = talib.ATR(high, low, close, timeperiod=atr_period)
    UpperBand, MiddleBand, LowerBand = talib.BBANDS(
        close, timeperiod=20, nbdevup=3, nbdevdn=2
    )
    ADX = talib.ADX(high, low, close, timeperiod=14)
    OBV = talib.OBV(close, vol)
    Stochastic_K, Stochastic_D = talib.STOCH(
        high,
        low,
        close,
        fastk_period=14,
        slowk_period=3,
        slowk_matype=0,
        slowd_period=3,
        slowd_matype=0,
    )
    # --- OBV-derived features for adaptive decisioning ---
    OBV_EMA20 = talib.EMA(OBV, timeperiod=20)
    OBV_MA20 = talib.SMA(OBV, timeperiod=20)
    # Rolling std with ddof=1 (sample std). Use Series to keep index.
    _OBV_S = pd.Series(OBV, index=df.index)
    OBV_STD20 = _OBV_S.rolling(20).std(ddof=1)

    # To avoid look-ahead bias, compute z-score vs *prior* window stats:
    OBV_MA20_S1 = pd.Series(OBV_MA20, index=df.index).shift(1)
    OBV_STD20_S1 = pd.Series(OBV_STD20, index=df.index).shift(1)
    OBV_ZScore20 = (_OBV_S - OBV_MA20_S1) / OBV_STD20_S1

    # Rolling SMAs & Volume MA20
    SMA_10_Close = df["Close"].rolling(10).mean()
    SMA_20_Close = df["Close"].rolling(20).mean()
    SMA_20_Low = df["Low"].rolling(20).mean()
    SMA_20_High = df["High"].rolling(20).mean()
    HHV_20 = df["High"].rolling(20).max().shift(1)
    LLV_20 = df["Low"].rolling(20).min().shift(1)
    SMA_200_Close = df["Close"].rolling(200).mean()
    SMA_20_Volume = df["Volume"].rolling(20).mean()
    SMA_200_Volume = df["Volume"].rolling(200).mean()

    Weekly_SMA_20 = talib.SMA(close, timeperiod=100)  # 20*5
    Weekly_SMA_200 = talib.SMA(close, timeperiod=1000)  # 200*5
    ws = pd.Series(Weekly_SMA_200, index=df.index)
    Weekly_SMA_200_1w = ws.shift(5)
    Weekly_SMA_200_2w = ws.shift(10)
    Weekly_SMA_200_3w = ws.shift(15)
    Weekly_SMA_200_4w = ws.shift(20)

    Volume_MA20 = SMA_20_Volume
    VolumeConfirmed = vol > (1.2 * SMA_20_Volume.values)

    # Fibonacci static levels
    fib = compute_fibonacci(df["High"], df["Low"])

    CMF = compute_cmf(df["High"], df["Low"], df["Close"], df["Volume"], period=5)

    # --- NEW: Additional indicators for broader lab coverage ---
    # VWAP (Volume-Weighted Average Price) — intraday benchmark
    typical_price = (high + low + close) / 3.0
    cum_vol = np.cumsum(vol)
    cum_tp_vol = np.cumsum(typical_price * vol)
    VWAP = np.divide(
        cum_tp_vol,
        cum_vol,
        out=np.asarray(typical_price, dtype="float64").copy(),
        where=cum_vol > 0,
    )

    # CCI (Commodity Channel Index)
    CCI = talib.CCI(high, low, close, timeperiod=20)

    # Williams %R
    WILLR = talib.WILLR(high, low, close, timeperiod=14)

    # Parabolic SAR
    SAR = talib.SAR(high, low, acceleration=0.02, maximum=0.2)

    # DMI+ and DMI- (Directional Movement)
    PLUS_DI = talib.PLUS_DI(high, low, close, timeperiod=14)
    MINUS_DI = talib.MINUS_DI(high, low, close, timeperiod=14)
    DX = talib.DX(high, low, close, timeperiod=14)

    # Ichimoku Cloud components
    # Tenkan-sen (Conversion Line): (9-period high + 9-period low) / 2
    period9_high = df["High"].rolling(window=9, min_periods=9).max()
    period9_low = df["Low"].rolling(window=9, min_periods=9).min()
    ICH_TENKAN = (period9_high + period9_low) / 2.0

    # Kijun-sen (Base Line): (26-period high + 26-period low) / 2
    period26_high = df["High"].rolling(window=26, min_periods=26).max()
    period26_low = df["Low"].rolling(window=26, min_periods=26).min()
    ICH_KIJUN = (period26_high + period26_low) / 2.0

    # Senkou Span A (Leading Span A): (Tenkan + Kijun) / 2, shifted forward 26
    ICH_SPAN_A = ((ICH_TENKAN + ICH_KIJUN) / 2.0).shift(26)

    # Senkou Span B (Leading Span B): (52-period high + low) / 2, shifted forward 26
    period52_high = df["High"].rolling(window=52, min_periods=52).max()
    period52_low = df["Low"].rolling(window=52, min_periods=52).min()
    ICH_SPAN_B = ((period52_high + period52_low) / 2.0).shift(26)

    # Cloud color: green (bullish) when Span A > Span B
    ICH_CLOUD_BULL = ICH_SPAN_A > ICH_SPAN_B

    # Chikou Span (Lagging Span): close shifted back 26
    ICH_CHIKOU = pd.Series(close, index=df.index).shift(-26)

    # Bollinger Band %B (where price is within the bands: 0=lower, 1=upper)
    BB_PercentB = np.where(
        (UpperBand - LowerBand) > 0,
        (close - LowerBand) / (UpperBand - LowerBand),
        np.nan,
    )

    # Bollinger Band Width (normalised volatility squeeze detector)
    BB_Width = np.where(
        MiddleBand > 0, (UpperBand - LowerBand) / MiddleBand, np.nan
    )

    # EMA crossover signals (short-term)
    EMA_CROSS_5_20 = np.where(
        np.isnan(EMA_values["EMA5"]) | np.isnan(EMA_values["EMA20"]),
        np.nan,
        np.where(EMA_values["EMA5"] > EMA_values["EMA20"], 1.0, -1.0),
    )
    EMA_CROSS_9_21 = np.where(
        np.isnan(EMA_values["EMA9"]) | np.isnan(EMA_values["EMA21"]),
        np.nan,
        np.where(EMA_values["EMA9"] > EMA_values["EMA21"], 1.0, -1.0),
    )

    # ATR as percentage of close (volatility normaliser)
    ATR_Pct = np.where(close > 0, ATR / close, np.nan)

    # MACD histogram slope (rising = bullish momentum)
    MACD_Hist_Prev = np.roll(MACD_Hist, 1)
    MACD_Hist_Prev[0] = np.nan
    MACD_Hist_Rising = MACD_Hist > MACD_Hist_Prev

    # --- TradingView-popular indicators ---
    # MFI (Money Flow Index) — volume-weighted RSI analogue
    MFI = talib.MFI(high, low, close, vol, timeperiod=14)

    # Stochastic RSI — RSI of RSI, popular on TradingView
    StochRSI_K, StochRSI_D = talib.STOCHRSI(
        close, timeperiod=14, fastk_period=3, fastd_period=3, fastd_matype=0
    )

    # Aroon Up/Down/Oscillator — trend age and direction
    AROON_DOWN, AROON_UP = talib.AROON(high, low, timeperiod=25)
    AROONOSC = talib.AROONOSC(high, low, timeperiod=25)

    # TRIX — triple-smoothed EMA rate of change (trend filter)
    TRIX = talib.TRIX(close, timeperiod=30)

    # PPO — Percentage Price Oscillator (MACD alternative, normalised)
    PPO_val = talib.PPO(close, fastperiod=12, slowperiod=26, matype=0)
    PPO_signal = talib.EMA(PPO_val, timeperiod=9)
    PPO_hist = np.where(np.isfinite(PPO_val) & np.isfinite(PPO_signal), PPO_val - PPO_signal, np.nan)

    # ROC — Rate of Change (momentum)
    ROC = talib.ROC(close, timeperiod=10)

    # Vortex Indicator (+VI/-VI) — trend confirmation
    #   +VI > -VI = bullish, -VI > +VI = bearish
    # Need pandas Series for .shift()
    _high_s = pd.Series(high, index=df.index)
    _low_s = pd.Series(low, index=df.index)
    _close_s = pd.Series(close, index=df.index)
    vm_plus = np.abs(_high_s - _low_s.shift(1)).values
    vm_minus = np.abs(_low_s - _high_s.shift(1)).values
    tr_range_v = np.maximum(
        high - low,
        np.maximum(np.abs(high - _close_s.shift(1).values), np.abs(low - _close_s.shift(1).values)),
    )
    vortex_period = 14
    vm_plus_sum = pd.Series(vm_plus).rolling(vortex_period).sum().values
    vm_minus_sum = pd.Series(vm_minus).rolling(vortex_period).sum().values
    tr_sum = pd.Series(tr_range_v).rolling(vortex_period).sum().values
    VORTEX_PLUS = np.where(tr_sum > 0, vm_plus_sum / tr_sum, np.nan)
    VORTEX_MINUS = np.where(tr_sum > 0, vm_minus_sum / tr_sum, np.nan)
    VORTEX_BULL = np.where(
        np.isnan(VORTEX_PLUS) | np.isnan(VORTEX_MINUS),
        np.nan,
        np.where(VORTEX_PLUS > VORTEX_MINUS, 1.0, -1.0),
    )

    # Chart-structure / support-resistance features.
    market_structure = compute_market_structure(df)

    # Collect into single dict for assign
    assign_kwargs = {
        # momentum
        "RSI": RSI,
        "MACD": MACD,
        "CMF": CMF,
        "MACD_Signal": MACD_Signal,
        "MACD_Hist": MACD_Hist,
        "MACD_Rule_8": MACD_Rule_8,
        "MACD_Rule_8_Signal": MACD_Rule_8_Signal,
        "MACD_Rule_8_Hist": MACD_Rule_8_Hist,
        # volatility
        "ATR": ATR,
        "UpperBand": UpperBand,
        "MiddleBand": MiddleBand,
        "LowerBand": LowerBand,
        "ADX": ADX,
        # volume
        "OBV": OBV,
        "OBV_EMA20": OBV_EMA20,
        "OBV_MA20": OBV_MA20,
        "OBV_STD20": OBV_STD20,
        "OBV_ZScore20": OBV_ZScore20,
        "Volume_MA20": Volume_MA20,
        "VolumeConfirmed": VolumeConfirmed,
        # stochastic
        "Stochastic_%K": Stochastic_K,
        "Stochastic_%D": Stochastic_D,
        # SMAs
        "SMA_10_Close": SMA_10_Close,
        "SMA_20_Close": SMA_20_Close,
        "SMA_20_Low": SMA_20_Low,
        "SMA_20_High": SMA_20_High,
        "HHV_20": HHV_20,
        "LLV_20": LLV_20,
        "SMA_200_Close": SMA_200_Close,
        "SMA_20_Volume": SMA_20_Volume,
        "SMA_200_Volume": SMA_200_Volume,
        # weekly SMAs
        "Weekly_SMA_20": Weekly_SMA_20,
        "Weekly_SMA_200": Weekly_SMA_200,
        "Weekly_SMA_200_1w": Weekly_SMA_200_1w,
        "Weekly_SMA_200_2w": Weekly_SMA_200_2w,
        "Weekly_SMA_200_3w": Weekly_SMA_200_3w,
        "Weekly_SMA_200_4w": Weekly_SMA_200_4w,
        # EMAs
        **EMA_values,
        # Fibonacci
        **fib,
        # --- NEW indicators ---
        "VWAP": VWAP,
        "CCI": CCI,
        "Williams_R": WILLR,
        "SAR": SAR,
        "PLUS_DI": PLUS_DI,
        "MINUS_DI": MINUS_DI,
        "DX": DX,
        "ICH_TENKAN": ICH_TENKAN,
        "ICH_KIJUN": ICH_KIJUN,
        "ICH_SPAN_A": ICH_SPAN_A,
        "ICH_SPAN_B": ICH_SPAN_B,
        "ICH_CLOUD_BULL": ICH_CLOUD_BULL,
        "ICH_CHIKOU": ICH_CHIKOU,
        "BB_PercentB": BB_PercentB,
        "BB_Width": BB_Width,
        "EMA_CROSS_5_20": EMA_CROSS_5_20,
        "EMA_CROSS_9_21": EMA_CROSS_9_21,
        "ATR_Pct": ATR_Pct,
        "MACD_Hist_Rising": MACD_Hist_Rising,
        # --- TradingView-popular indicators ---
        "MFI": MFI,
        "StochRSI_K": StochRSI_K,
        "StochRSI_D": StochRSI_D,
        "AROON_UP": AROON_UP,
        "AROON_DOWN": AROON_DOWN,
        "AROONOSC": AROONOSC,
        "TRIX": TRIX,
        "PPO": PPO_val,
        "PPO_Signal": PPO_signal,
        "PPO_Hist": PPO_hist,
        "ROC": ROC,
        "VORTEX_PLUS": VORTEX_PLUS,
        "VORTEX_MINUS": VORTEX_MINUS,
        "VORTEX_BULL": VORTEX_BULL,
        # --- Chart-structure / support-resistance indicators ---
        **market_structure,
    }

    # Bulk assign via concat to avoid pandas fragmentation from inserting many columns.
    existing = [col for col in assign_kwargs if col in df.columns]
    if existing:
        df = df.drop(columns=existing)
    df = pd.concat([df, pd.DataFrame(assign_kwargs, index=df.index)], axis=1)

    # Supertrend variants
    compute_supertrend(df, ATR, multiplier=2.0)
    compute_supertrend(
        df,
        ATR,
        multiplier=3.0,
        sup_col="Supertrend_Rule_8_Exit",
        sup_dir="Supertrend_Direction_Rule_8_Exit",
    )

    return df


def load_historical_data(symbol):
    try:
        df = pd.read_feather(f"intermediary_files/Hist_Data/{symbol}.feather")
        return df
    except Exception as e:
        logger.error(f"Error loading {symbol}.feather: {e}")
        return None


def preprocess_data(row_df, symbol):
    """
    Preprocess the stock data by appending new row data to the historical data.

    Parameters:
        row_df (pd.DataFrame): The new row data.
        symbol (str): The stock symbol.

    Returns:
        pd.DataFrame or None: The combined DataFrame, or None if preprocessing fails.
    """
    append_df = row_df[["Date", "Close", "Volume", "High", "Low"]].copy()

    df = load_historical_data(symbol)
    if df is None:
        return None

    required_columns = {"Date", "Close", "Volume", "High", "Low"}
    if not required_columns.issubset(df.columns):
        logger.error(f"{symbol}.feather is missing required columns.")
        logger.error(f"{symbol}.feather has {df.columns}")
        return None

    # Convert 'Date' to datetime and set as index
    for dataframe in [df, append_df]:
        dataframe["Date"] = pd.to_datetime(dataframe["Date"], errors="coerce")
        dataframe.dropna(subset=["Date"], inplace=True)
        dataframe.set_index("Date", inplace=True)

    # Concatenate and remove duplicates
    df = pd.concat([df, append_df])
    df = df[~df.index.duplicated(keep="last")]  # Keep the last duplicate
    df.sort_index(inplace=True)
    df = Indicators(df)

    if df.empty:
        logger.error(f"No data available for {symbol} after preprocessing.")
        return None

    return df


def process_single_stock(row):
    """
    Processes a single stock and returns the preprocessed DataFrame.

    Parameters:
        row (dict): A dictionary containing stock data.

    Returns:
        pd.DataFrame or None: The preprocessed DataFrame, or None if processing fails.
    """
    # Prepare row DataFrame
    row_df = pd.DataFrame(
        [
            {
                "Date": row["Date"],
                "Close": row["last_price"],
                "Volume": row["volume_traded"],
                "High": row["ohlc"]["high"],
                "Low": row["ohlc"]["low"],
            }
        ]
    )

    df = preprocess_data(row_df, row["Symbol"])
    return df


def apply_trading_rules(df, row, holdings=None):
    """
    Apply all trading rules from the RULE_SETS dictionary to the stock data
    and return the strongest trading signal (e.g., SELL > BUY > HOLD) along with contributing rules.

    Parameters:
        df (pd.DataFrame): The preprocessed stock data.
        row (dict): The current stock data row.

    Returns:
        tuple: (str, dict) where str is the strongest trading decision,
               and dict contains rules contributing to each decision.
    """
    # Initialize a dictionary to track the decisions and their contributing rules
    decisions = {"SELL": [], "BUY": [], "HOLD": []}

    if holdings is None:
        try:
            holdings = pd.read_feather("intermediary_files/Holdings.feather")
        except Exception as e:
            logger.error(
                f"Error loading holdings for {row['Symbol']}: {e}, Traceback: {traceback.format_exc()}"
            )
            holdings = pd.DataFrame()

    def apply_rule(rule_set_name, rule_set_module):
        try:
            # Apply the trading rule from the current rule set
            decision = rule_set_module.buy_or_sell(df, row, holdings)
            logger.debug(
                f"Rule {rule_set_name} made a {decision} decision for {row['Symbol']}"
            )
            return rule_set_name, decision
        except Exception as e:
            logger.error(
                f"Error applying trading rule {rule_set_name} for {row['Symbol']}: {e}, Traceback: {traceback.format_exc()}"
            )
            return rule_set_name, "HOLD"

    num_cores = max(1, min(len(RULE_SETS), multiprocessing.cpu_count()))
    # Use ThreadPoolExecutor to parallelize rule application
    with ThreadPoolExecutor(max_workers=num_cores) as executor:
        # Submit all rules to the executor and process them concurrently
        futures = {
            executor.submit(apply_rule, rule_set_name, rule_set_module): rule_set_name
            for rule_set_name, rule_set_module in RULE_SETS.items()
        }

        # Collect results as they complete
        for future in as_completed(futures):
            rule_set_name, decision = future.result()
            if decision in decisions:
                decisions[decision].append(rule_set_name)
            else:
                # Log the specific rule set that returned an unknown decision
                logger.error(
                    f"Rule {rule_set_name} returned an unknown decision: {decision}"
                )
                pass

    # Print decisions for each stock (for debugging)
    logger.info(f"Decisions for {row['Symbol']}: {decisions}")

    # Prioritize decisions: SELL > BUY > HOLD
    if decisions["SELL"]:
        return "SELL", {"SELL": decisions["SELL"]}
    elif decisions["BUY"]:
        return "BUY", {"BUY": decisions["BUY"]}
    else:
        return "HOLD", {"HOLD": decisions["HOLD"]}


def process_stock_and_decide(row):
    """
    Processes a single stock and returns a decision dict with contributing rules if any.

    Parameters:
        row (dict): A dictionary containing stock information.

    Returns:
        dict or None: A decision dictionary if a buy/sell decision is made, else None.
    """
    try:
        # Process the stock data
        df = process_single_stock(row)
        if df is not None:
            holdings = pd.DataFrame()
            try:
                holdings = pd.read_feather("intermediary_files/Holdings.feather")
            except Exception:
                holdings = pd.DataFrame()

            # Apply the trading rules
            decision, contributing_rules = apply_trading_rules(df, row, holdings=holdings)

            final_decision = decision
            sentiment_overlays = []

            final_decision, news_overlay = apply_news_overlay(
                final_decision,
                row.get("Symbol"),
                holdings=holdings,
            )
            if news_overlay:
                sentiment_overlays.append(news_overlay)

            if final_decision != "HOLD":
                payload = {
                    "Symbol": row["Symbol"],
                    "Decision": final_decision,
                    "ContributingRules": contributing_rules,
                    "Exchange": row["exchange"],
                    "Close": row["last_price"],
                    "AssetClass": row.get("AssetClass", "EQUITY"),
                    "ETFTheme": row.get("ETFTheme", ""),
                }
                if sentiment_overlays:
                    payload["SentimentOverlay"] = sentiment_overlays[-1]
                    payload["SentimentOverlays"] = sentiment_overlays
                    payload["ContributingRules"] = dict(contributing_rules or {})
                    payload["ContributingRules"].setdefault(final_decision, [])
                    for overlay in sentiment_overlays:
                        source = str(overlay.get("source") or "").upper()
                        if source and source not in payload["ContributingRules"][final_decision]:
                            payload["ContributingRules"][final_decision].append(source)
                return payload
    except Exception as e:
        # Log exceptions with stock symbol for easier debugging
        logger.error(
            f"Error processing stock {row.get('Symbol', 'Unknown')}: {e}, Traceback: {traceback.format_exc()}"
        )
    return None


# Lazily initialize Kite so research / backtest imports do not hit the broker
# API or write intermediary files at module import time.
kite = None


def get_kite_client():
    global kite
    if kite is None:
        kite = initialize_kite()
    return kite


# Retry decorator, with exponential backoff and jitter
@retry(tries=3, delay=2, backoff=2, jitter=(0, 1), exceptions=(Exception,))
def fetch_holdings(kite=None):
    """
    Fetch the list of instruments and holdings from the Kite API,
    and save the holdings to a CSV file.

    Args:
        kite (KiteConnect): An instance of KiteConnect with a valid session.

    Returns:
        pd.DataFrame: DataFrame containing NSE stocks with instrument tokens.
    """
    try:
        kite = kite or get_kite_client()

        # Fetch holdings
        holdings = kite.holdings()
        if holdings:
            holdings = pd.DataFrame(holdings)[
                [
                    "tradingsymbol",
                    "instrument_token",
                    "exchange",
                    "average_price",
                    "quantity",
                    "t1_quantity",
                ]
            ]

            # Merge Holdings and t1_quantity
            holdings["quantity"] = holdings["quantity"] + holdings["t1_quantity"]

            # Filter out holdings with quantity greater than 0
            holdings = holdings[holdings["quantity"] > 0]

            # Save holdings to CSV
            holdings.to_feather("intermediary_files/Holdings.feather")

            logger.info(f"Number of Holdings: {len(holdings)}")
            return holdings
        else:
            # Initialize an empty DataFrame with the expected columns
            holdings = pd.DataFrame(
                columns=[
                    "tradingsymbol",
                    "instrument_token",
                    "exchange",
                    "average_price",
                    "quantity",
                    "t1_quantity",
                ]
            )
            holdings.to_feather("intermediary_files/Holdings.feather")
            logger.debug("No holdings found, returning an empty DataFrame.")
            return holdings

    except Exception as e:
        logger.error(
            f"Error in fetching holdings: {e}, Traceback: {traceback.format_exc()}"
        )
        raise  # Re-raise to trigger the retry decorator


# Retry decorator, with exponential backoff and jitter
@retry(tries=3, delay=2, backoff=2, jitter=(0, 1), exceptions=(Exception,))
def fetch_instruments_list(kite=None):
    """
    Fetch the list of instruments and holdings from the Kite API,
    and save the holdings to a CSV file.

    Args:
        kite (KiteConnect): An instance of KiteConnect with a valid session.

    Returns:
        pd.DataFrame: DataFrame containing NSE stocks with instrument tokens.
    """
    try:
        kite = kite or get_kite_client()

        # Fetch instruments
        instruments = kite.instruments()

        # Filter for NSE stocks only
        nse_stocks = [
            instrument
            for instrument in instruments
            if instrument["instrument_type"] == "EQ"
        ]
        df = pd.DataFrame(nse_stocks)[["instrument_token", "tradingsymbol", "exchange"]]
        return df

    except Exception as e:
        logger.error(
            f"Error in fetching instruments: {e}, Traceback: {traceback.format_exc()}"
        )
        raise  # Re-raise to trigger the retry decorator


def get_market_schedule():
    """
    Get the NSE market schedule for the current day.

    Returns:
    pd.DataFrame or None: Market schedule for the day, or None if market is closed.
    """
    now = datetime.now(ZoneInfo("Asia/Kolkata"))
    schedule = nse_calendar.schedule(start_date=now.date(), end_date=now.date())
    return schedule if not schedule.empty else None


def is_Market_Open(schedule=get_market_schedule()):
    """
    Check if the NSE market is currently open.
    Returns True if DEBUG_MODE is True.

    Args:
    schedule (pd.DataFrame): Market schedule for the day.

    Returns:
    bool: True if the market is open, False otherwise.
    """
    if DEBUG_MODE:
        return True

    # Prefer Tickertape's public exchange-status endpoint as the live market
    # calendar signal, then fall back to pandas-market-calendars if the public
    # web endpoint or optional package is unavailable. Kite remains the source
    # of truth for execution/prices; this is only an open/closed guard.
    if os.getenv("AT_TICKERTAPE_MARKET_STATUS", "1").strip().lower() not in {
        "0",
        "false",
        "no",
        "off",
    }:
        tickertape_open = is_market_open_via_tickertape("IN")
        if tickertape_open is not None:
            return tickertape_open

    if schedule is None:
        logger.info("Market is closed today.")
        return False

    now = datetime.now(ZoneInfo("Asia/Kolkata"))
    market_open = schedule.iloc[0]["market_open"].astimezone(ZoneInfo("Asia/Kolkata"))
    market_close = schedule.iloc[0]["market_close"].astimezone(ZoneInfo("Asia/Kolkata"))

    return market_open <= now <= market_close


def is_PreMarket_Open(schedule=get_market_schedule()):
    """
    Check if the NSE premarket is currently open.

    Args:
    schedule (pd.DataFrame): Market schedule for the day.

    Returns:
    bool: True if the premarket is open, False otherwise.
    """
    if schedule is None:
        logger.info("Market is closed today.")
        return False

    now = datetime.now(ZoneInfo("Asia/Kolkata"))
    market_open = schedule.iloc[0]["market_open"].astimezone(ZoneInfo("Asia/Kolkata"))
    premarket_open = market_open - timedelta(minutes=15)

    return premarket_open <= now < market_open


def get_instrument_token(good_stock_list_df, instruments_df):
    """
    Merge a list of good stocks with instruments data to obtain instrument tokens,
    prioritizing NSE exchange.

    Args:
        good_stock_list_df (pd.DataFrame): DataFrame with a list of good stocks.
        instruments_df (pd.DataFrame): DataFrame containing instrument data.

    Returns:
        pd.DataFrame: DataFrame with symbols, instrument tokens, and exchange info.
    """
    # Perform an inner join on the 'Symbol' and 'tradingsymbol' columns
    merged_df = pd.merge(
        good_stock_list_df,
        instruments_df,
        left_on="Symbol",
        right_on="tradingsymbol",
        how="inner",
    )

    # Sort the DataFrame so that records with 'NSE' are prioritized
    sorted_df = merged_df.sort_values(by="exchange", ascending=False)

    # Drop duplicates by 'Symbol', keeping the first occurrence, which prioritizes 'NSE'
    deduplicated_df = sorted_df.drop_duplicates(subset=["Symbol"], keep="first")

    # Keep metadata columns from screener list (e.g., AssetClass/ETFTheme/Sector).
    metadata_cols = [
        col
        for col in good_stock_list_df.columns
        if col not in {"Symbol", "instrument_token", "exchange"}
    ]
    selected_cols = ["Symbol", "instrument_token", "exchange", *metadata_cols]
    final_nse_prioritized_df = deduplicated_df[selected_cols]
    if "AssetClass" not in final_nse_prioritized_df.columns:
        final_nse_prioritized_df["AssetClass"] = "EQUITY"
    if "ETFTheme" not in final_nse_prioritized_df.columns:
        final_nse_prioritized_df["ETFTheme"] = ""
    else:
        final_nse_prioritized_df["ETFTheme"] = final_nse_prioritized_df[
            "ETFTheme"
        ].fillna("")

    return final_nse_prioritized_df


# Use LRU Cache for loading instruments data
@lru_cache(maxsize=None)
def load_instruments_data():
    """
    Load instrument data from CSV file with LRU caching to avoid re-reading the file.
    """
    try:
        instruments_df = pd.read_feather("intermediary_files/Instruments.feather")
        return instruments_df.set_index("instrument_token").to_dict(orient="index")
    except Exception as e:
        logger.error(
            f"Failed to read Instruments.csv: {e}, Traceback: {traceback.format_exc()}"
        )
        sys.exit(1)  # Exit if we cannot load instruments data


def cleanup_stop_loss_json(holdings=None):
    """
    Cleans up the stop-loss JSON file by removing any entries that
    do not correspond to currently held tradingsymbols.

    Parameters:
    holdings (pd.DataFrame): A DataFrame with a 'tradingsymbol' column
                             representing currently held instruments.
    """

    HOLDINGS_FILE_PATH = "intermediary_files/Holdings.json"
    LOCK_FILE_PATH = "intermediary_files/Holdings.lock"

    def safe_float(value, default=None):
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    # Load current stop-loss data
    lock = FileLock(LOCK_FILE_PATH)
    stop_loss_data = {}
    try:
        with lock.acquire(timeout=10):
            if os.path.exists(HOLDINGS_FILE_PATH):
                with open(HOLDINGS_FILE_PATH, "r") as json_file:
                    try:
                        data = json.load(json_file)
                        # Ensure all values are floats
                        for k, v in data.items():
                            data[k] = safe_float(v, default=None)
                        stop_loss_data = data
                    except json.JSONDecodeError:
                        # File corrupted, start fresh
                        stop_loss_data = {}
            else:
                # No file exists yet, nothing to do
                return
    except Timeout:
        # Could not acquire lock, log and return
        logger.error(f"Timeout acquiring lock for {HOLDINGS_FILE_PATH}")
        return
    except Exception as e:
        logger.error(f"Error loading stop-loss from JSON: {str(e)}")
        return

    if holdings is None:
        holdings = fetch_holdings()

    # Verify holdings DataFrame has the required column
    if "tradingsymbol" not in holdings.columns:
        logger.error("Holdings DataFrame does not have a 'tradingsymbol' column.")
        return

    current_symbols = set(holdings["tradingsymbol"].unique())
    keys_to_remove = [
        symbol for symbol in stop_loss_data.keys() if symbol not in current_symbols
    ]

    if not keys_to_remove:
        logger.info("No outdated stop-loss entries to remove. JSON is up-to-date.")
        return

    # Acquire lock again to write updated data
    try:
        with lock.acquire(timeout=10):
            for key in keys_to_remove:
                del stop_loss_data[key]

            with open(HOLDINGS_FILE_PATH, "w") as json_file:
                json.dump(stop_loss_data, json_file, indent=4)
            logger.info(
                f"Removed {len(keys_to_remove)} outdated stop-loss entries from JSON."
            )
    except Timeout:
        logger.error(
            f"Timeout while trying to acquire the lock for {HOLDINGS_FILE_PATH}."
        )
        return
    except Exception as e:
        logger.error(f"Error during cleanup of stop-loss JSON: {str(e)}")
        return


@lru_cache(maxsize=None)
def get_params_grid():
    """
    Connect to a MySQL database using SQLAlchemy, read the tables,
    and convert the Trade_Params table into a nested dictionary.

    Args:
        host (str): Host address for the MySQL database.
        user (str): Username for the MySQL database.
        password (str): Password for the MySQL database.
        database (str): Name of the database.

    Returns:
        dict: A nested dictionary with the ticker as the key and parameter key-value pairs as the value.
    """
    try:
        # Create the SQLAlchemy engine
        engine = create_engine(
            f"mysql+mysqlconnector://{USER}:{DB_PASSWORD}@{HOST}/{DATABASE}"
        )

        # Query the Trade_Params table
        query = "SELECT * FROM Trade_Params"
        df_trade_params = pd.read_sql(query, engine)

        # Convert the DataFrame to a nested dictionary
        nested_dict = df_trade_params.set_index("ticker").T.to_dict()
        return nested_dict

    except Exception as e:
        print(f"Error: {e}")
        return {}


_last_data = None
_last_fetch = 0
TTL = 1800  # 30 minutes in seconds


def get_mmi_now(force_refresh: bool = False):
    """Fetch Tickertape MMI data, cached for 30 minutes to avoid over-hitting."""
    global _last_data, _last_fetch

    now = time.time()
    if force_refresh or (now - _last_fetch > TTL) or _last_data is None:
        try:
            _last_data = get_mmi_indicator(force_refresh=force_refresh)
            if _last_data is not None:
                _last_fetch = now
        except ValueError:
            return None

    return _last_data
