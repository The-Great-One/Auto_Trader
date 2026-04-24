import os
import time
import json
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
from kiteconnect import KiteConnect
from kiteconnect.exceptions import NetworkException
import pandas as pd
import ray
from tqdm import tqdm
from retry import retry
from Auto_Trader.utils import (
    is_Market_Open,
    is_PreMarket_Open,
    fetch_instruments_list,
    read_session_data,
)
from filelock import FileLock
from Auto_Trader.my_secrets import API_KEY

# Constants for fetched-data tracking and storage
HIST_DIR = "intermediary_files/Hist_Data"
CACHE_INSTRUMENTS_FILE = "intermediary_files/instruments_cache.json"

# Kite API interval limits and batch settings
INTERVAL_LIMITS = {
    "day": 2000,
    "60minute": 400,
    "30minute": 200,
    "15minute": 200,
    "10minute": 120,
    "5minute": 100,
    "3minute": 90,
    "minute": 60,
}
KITE_INTERVAL = os.getenv("AT_KITE_INTERVAL", "day").strip().lower()
if KITE_INTERVAL not in INTERVAL_LIMITS:
    KITE_INTERVAL = "day"
_INTERVAL_SUFFIX = KITE_INTERVAL.replace("minute", "m")
FETCHED_DATA_FILE = f"intermediary_files/fetched_data_{_INTERVAL_SUFFIX}.json"
LOCK_FILE = f"intermediary_files/fetched_data_{_INTERVAL_SUFFIX}.lock"
BATCH_SIZE = 20
PAUSE_BETWEEN_BATCHES = 0.5
RETRY_ON_RATE_LIMIT = 3  # number of retries for rate limit per chunk
RATE_LIMIT_SLEEP = 2.0  # seconds to wait on rate limit


def _chunk_date_range(start_dt, end_dt, max_days):
    """Yield chunks no larger than max_days."""
    cursor = start_dt
    while cursor < end_dt:
        chunk_end = min(cursor + relativedelta(days=max_days), end_dt)
        yield cursor, chunk_end
        cursor = chunk_end


def _is_intraday_interval() -> bool:
    return KITE_INTERVAL != "day"


def _interval_to_timedelta(interval: str) -> timedelta:
    if interval == "day":
        return timedelta(days=1)
    if interval == "minute":
        return timedelta(minutes=1)
    if interval.endswith("minute"):
        return timedelta(minutes=int(interval.replace("minute", "")))
    return timedelta(days=1)


@ray.remote
class FetchedDataManager:
    def __init__(self):
        self.fetched_data = self._load()

    def _load(self):
        try:
            if os.path.exists(FETCHED_DATA_FILE):
                with open(FETCHED_DATA_FILE) as f:
                    return json.load(f)
        except Exception as e:
            print(f"[Error] Loading fetched data JSON: {e}")
        return {}

    def _save(self):
        try:
            os.makedirs(os.path.dirname(FETCHED_DATA_FILE), exist_ok=True)
            with FileLock(LOCK_FILE):
                with open(FETCHED_DATA_FILE, "w") as f:
                    json.dump(self.fetched_data, f)
        except Exception as e:
            print(f"[Error] Saving fetched data JSON: {e}")

    def is_fetched(self, symbol):
        return self.fetched_data.get(symbol) == str(date.today())

    def mark_fetched(self, symbol):
        try:
            self.fetched_data[symbol] = str(date.today())
            self._save()
        except Exception as e:
            print(f"[Error] Marking symbol '{symbol}' fetched: {e}")


@ray.remote
@retry(tries=2, delay=2)
def download_symbol_data(symbol, fetched_mgr, api_key, access_token, token_map):
    try:
        if ray.get(fetched_mgr.is_fetched.remote(symbol)):
            return symbol, True, 0
    except Exception as e:
        print(f"[Error] Checking fetched status for '{symbol}': {e}")
        return symbol, False, 0

    try:
        feather_path = os.path.join(HIST_DIR, f"{symbol}.feather")
    except Exception as e:
        print(f"[Error] Constructing feather path for '{symbol}': {e}")
        return symbol, False, 0

    existing_df = None
    try:
        if os.path.exists(feather_path):
            existing_df = pd.read_feather(feather_path)
            last_ts = pd.to_datetime(existing_df["Date"], errors="coerce").max()
            if pd.isna(last_ts):
                raise ValueError("No valid timestamp in historical data")
            if _is_intraday_interval():
                start_date = last_ts.to_pydatetime() + _interval_to_timedelta(
                    KITE_INTERVAL
                )
            else:
                start_date = last_ts.date() + timedelta(days=1)
        else:
            if _is_intraday_interval():
                lookback_days = int(os.getenv("AT_INTRADAY_LOOKBACK_DAYS", "60"))
                start_date = datetime.now() - timedelta(days=lookback_days)
            else:
                start_date = date.today() - relativedelta(years=5)
    except Exception as e:
        print(f"[Error] Determining start date for '{symbol}': {e}")
        return symbol, False, 0

    today = datetime.now() if _is_intraday_interval() else date.today()
    if start_date >= today:
        try:
            ray.get(fetched_mgr.mark_fetched.remote(symbol))
        except Exception as e:
            print(f"[Error] Marking '{symbol}' as fetched when up-to-date: {e}")
        return symbol, True, 0

    token = token_map.get(symbol)
    if not token:
        print(f"[Warning] No instrument token for symbol '{symbol}'")
        return symbol, False, 0

    try:
        kite = KiteConnect(api_key=api_key)
        kite.set_access_token(access_token)
    except Exception as e:
        print(f"[Error] Initializing Kite client for '{symbol}': {e}")
        return symbol, False, 0

    frames = []
    for sdt, edt in _chunk_date_range(
        start_date, today, INTERVAL_LIMITS[KITE_INTERVAL]
    ):
        success = False
        for attempt in range(RETRY_ON_RATE_LIMIT):
            try:
                data = kite.historical_data(
                    token,
                    from_date=sdt,
                    to_date=edt,
                    interval=KITE_INTERVAL,
                    oi=False,
                )
                success = True
                break
            except NetworkException as ne:
                print(
                    f"[Rate Limit] '{symbol}' chunk {sdt} to {edt}: {ne} (attempt {attempt + 1})"
                )
                time.sleep(RATE_LIMIT_SLEEP)
            except Exception as e:
                print(f"[Error] Fetching data for '{symbol}' from {sdt} to {edt}: {e}")
                return symbol, False, 0
        if not success:
            print(f"[Error] Failed after retries for '{symbol}' chunk {sdt} to {edt}")
            return symbol, False, 0
        try:
            frames.append(pd.DataFrame(data))
        except Exception as e:
            print(f"[Error] Converting data to DataFrame for '{symbol}': {e}")
            return symbol, False, 0
        time.sleep(0.35)

    if not frames:
        print(f"[Warning] No data frames collected for '{symbol}'")
        return symbol, False, 0

    try:
        df = pd.concat(frames, ignore_index=True)
        df.rename(
            columns={
                "date": "Date",
                "open": "Open",
                "high": "High",
                "low": "Low",
                "close": "Close",
                "volume": "Volume",
            },
            inplace=True,
        )
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        if not _is_intraday_interval():
            df["Date"] = df["Date"].dt.date
        df.dropna(subset=["Date"], inplace=True)
        df.drop_duplicates(subset=["Date"], inplace=True)
        df.sort_values("Date", inplace=True)
    except Exception as e:
        print(f"[Error] Processing DataFrame for '{symbol}': {e}")
        return symbol, False, 0

    try:
        if (
            not _is_intraday_interval()
            and (is_Market_Open() or is_PreMarket_Open())
            and df["Date"].iloc[-1] == today
        ):
            df = df.iloc[:-1]
    except Exception as e:
        print(f"[Error] Dropping today's partial bar for '{symbol}': {e}")

    # Merge with existing history if available
    if existing_df is not None and len(existing_df) > 0:
        try:
            existing_df["Date"] = pd.to_datetime(existing_df["Date"], errors="coerce")
            if not _is_intraday_interval():
                existing_df["Date"] = existing_df["Date"].dt.date
            df = pd.concat([existing_df, df], ignore_index=True)
            df.drop_duplicates(subset=["Date"], keep="last", inplace=True)
            df.sort_values("Date", inplace=True)
            df.reset_index(drop=True, inplace=True)
        except Exception as e:
            print(f"[Warning] Could not merge existing data for {symbol}: {e}")

    try:
        os.makedirs(HIST_DIR, exist_ok=True)
        df.to_feather(feather_path)
    except Exception as e:
        print(f"[Error] Saving feather for '{symbol}': {e}")
        return symbol, False, 0

    try:
        ray.get(fetched_mgr.mark_fetched.remote(symbol))
    except Exception as e:
        print(f"[Error] Marking '{symbol}' fetched after save: {e}")

    return symbol, True, len(df)


def download_historical_quotes(df):
    try:
        if "Symbol" not in df.columns:
            raise ValueError("Missing 'Symbol' column")
    except Exception as e:
        print(f"[Error] Input DataFrame validation: {e}")
        return []

    try:
        os.makedirs(HIST_DIR, exist_ok=True)
    except Exception as e:
        print(f"[Error] Creating HIST_DIR '{HIST_DIR}': {e}")

    try:
        ray.init(ignore_reinit_error=True)
        fetched_mgr = FetchedDataManager.remote()
    except Exception as e:
        print(f"[Error] Initializing Ray or FetchedDataManager: {e}")
        return []

    try:
        if os.path.exists(CACHE_INSTRUMENTS_FILE):
            with open(CACHE_INSTRUMENTS_FILE) as f:
                token_map = json.load(f)
        else:
            instruments_df = fetch_instruments_list()
            token_map = dict(
                zip(instruments_df["tradingsymbol"], instruments_df["instrument_token"])
            )
            os.makedirs(os.path.dirname(CACHE_INSTRUMENTS_FILE), exist_ok=True)
            with open(CACHE_INSTRUMENTS_FILE, "w") as f:
                json.dump(token_map, f)
    except Exception as e:
        print(f"[Error] Loading or caching instrument tokens: {e}")
        ray.shutdown()
        return []

    try:
        api_key = API_KEY
        access_token = read_session_data()
    except Exception as e:
        print(f"[Error] Building access token: {e}")
        ray.shutdown()
        return []

    tickers = df["Symbol"].tolist()
    fetched_symbols = []
    try:
        with tqdm(total=len(tickers), desc="Downloading tickers") as pbar:
            for i in range(0, len(tickers), BATCH_SIZE):
                batch = tickers[i : i + BATCH_SIZE]
                futures = []
                for t in batch:
                    futures.append(
                        download_symbol_data.remote(
                            t, fetched_mgr, api_key, access_token, token_map
                        )
                    )
                results = ray.get(futures)
                for symbol, success, _ in results:
                    if success:
                        fetched_symbols.append(symbol)
                        pbar.update(1)
                time.sleep(PAUSE_BETWEEN_BATCHES)
    except Exception as e:
        print(f"[Error] Fetching symbols in batches: {e}")
    finally:
        ray.shutdown()

    try:
        if os.path.exists(FETCHED_DATA_FILE):
            with open(FETCHED_DATA_FILE) as f:
                fetched = json.load(f)
            return list(fetched.keys())
    except Exception as e:
        print(f"[Error] Reading FETCHED_DATA_FILE: {e}")
    return tickers
