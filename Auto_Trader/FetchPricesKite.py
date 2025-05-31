import os
import time
import json
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
from kiteconnect import KiteConnect
from kiteconnect.exceptions import NetworkException
import pandas as pd
import ray
from tqdm import tqdm
from retry import retry
from Auto_Trader.utils import is_Market_Open, is_PreMarket_Open, fetch_instruments_list, read_session_data
from filelock import FileLock
from Auto_Trader.my_secrets import API_KEY

# Constants for fetched-data tracking and storage
FETCHED_DATA_FILE = "intermediary_files/fetched_data.json"
LOCK_FILE = "intermediary_files/fetched_data.lock"
HIST_DIR = "intermediary_files/Hist_Data"
CACHE_INSTRUMENTS_FILE = "intermediary_files/instruments_cache.json"

# Kite API interval limits and batch settings
INTERVAL_LIMITS = {"day": 2000}
BATCH_SIZE = 20
PAUSE_BETWEEN_BATCHES = 0.5
RETRY_ON_RATE_LIMIT = 3  # number of retries for rate limit per chunk
RATE_LIMIT_SLEEP = 2.0   # seconds to wait on rate limit


def _chunk_date_range(start_dt, end_dt, max_days):
    """Yield chunks no larger than max_days."""
    cursor = start_dt
    while cursor < end_dt:
        chunk_end = min(cursor + relativedelta(days=max_days), end_dt)
        yield cursor, chunk_end
        cursor = chunk_end


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
                with open(FETCHED_DATA_FILE, 'w') as f:
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

    try:
        if os.path.exists(feather_path):
            existing = pd.read_feather(feather_path)
            last_date = pd.to_datetime(existing['Date']).max().date()
            start_date = last_date + timedelta(days=1)
        else:
            start_date = date.today() - relativedelta(years=5)
    except Exception as e:
        print(f"[Error] Determining start date for '{symbol}': {e}")
        return symbol, False, 0

    today = date.today()
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

    frames = []
    for sdt, edt in _chunk_date_range(start_date, today, INTERVAL_LIMITS['day']):
        success = False
        for attempt in range(RETRY_ON_RATE_LIMIT):
            try:
                kite = KiteConnect(api_key=api_key)
                kite.set_access_token(access_token)
                data = kite.historical_data(
                    token, from_date=sdt, to_date=edt, interval='day', oi=False
                )
                success = True
                break
            except NetworkException as ne:
                print(f"[Rate Limit] '{symbol}' chunk {sdt} to {edt}: {ne} (attempt {attempt+1})")
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
        df.rename(columns={'date': 'Date', 'open': 'Open', 'high': 'High',
                           'low': 'Low', 'close': 'Close', 'volume': 'Volume'}, inplace=True)
        df['Date'] = pd.to_datetime(df['Date']).dt.date
        df.drop_duplicates(subset=['Date'], inplace=True)
        df.sort_values('Date', inplace=True)
    except Exception as e:
        print(f"[Error] Processing DataFrame for '{symbol}': {e}")
        return symbol, False, 0

    try:
        if (is_Market_Open() or is_PreMarket_Open()) and df['Date'].iloc[-1] == today:
            df = df.iloc[:-1]
    except Exception as e:
        print(f"[Error] Dropping today's partial bar for '{symbol}': {e}")

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
        if 'Symbol' not in df.columns:
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
            token_map = dict(zip(instruments_df['tradingsymbol'], instruments_df['instrument_token']))
            os.makedirs(os.path.dirname(CACHE_INSTRUMENTS_FILE), exist_ok=True)
            with open(CACHE_INSTRUMENTS_FILE, 'w') as f:
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

    tickers = df['Symbol'].tolist()
    fetched_symbols = []
    try:
        with tqdm(total=len(tickers), desc='Downloading tickers') as pbar:
            for i in range(0, len(tickers), BATCH_SIZE):
                batch = tickers[i:i+BATCH_SIZE]
                futures = []
                for t in batch:
                    futures.append(download_symbol_data.remote(t, fetched_mgr, api_key, access_token, token_map))
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