import os
import pandas as pd
import yfinance as yf
import ray
from tqdm import tqdm
from retry import retry
from Auto_Trader.utils import is_Market_Open, is_PreMarket_Open
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import json
from filelock import FileLock

# JSON file to store fetched symbols and dates
FETCHED_DATA_FILE = "intermediary_files/fetched_data.json"
LOCK_FILE = "intermediary_files/fetched_data.lock"
HIST_DIR = "intermediary_files/Hist_Data"

# Ray actor to manage shared state
@ray.remote
class FetchedDataManager:
    def __init__(self):
        self.fetched_data = self.load_fetched_data()

    def load_fetched_data(self):
        if os.path.exists(FETCHED_DATA_FILE):
            with open(FETCHED_DATA_FILE, 'r') as file:
                return json.load(file)
        return {}

    def save_fetched_data(self):
        with FileLock(LOCK_FILE):
            with open(FETCHED_DATA_FILE, 'w') as file:
                json.dump(self.fetched_data, file)

    def is_fetched(self, ticker):
        today = str(datetime.now().date())
        return self.fetched_data.get(ticker) == today

    def mark_fetched(self, ticker):
        today = str(datetime.now().date())
        self.fetched_data[ticker] = today
        self.save_fetched_data()

@ray.remote
@retry(tries=2, delay=2)
def download_ticker_data(ticker, fetched_data_manager):
    # Skip if we've already fetched today
    if ray.get(fetched_data_manager.is_fetched.remote(ticker)):
        return True

    # Determine start date for incremental update
    feather_path = os.path.join(HIST_DIR, f"{ticker}.feather")
    if os.path.exists(feather_path):
        existing = pd.read_feather(feather_path)
        last_date = pd.to_datetime(existing["Date"]).max().date()
        start_date = last_date + timedelta(days=1)
    else:
        # No existing data → fetch the last 3 months
        start_date = datetime.now().date() - relativedelta(months=3)

    today_str = str(datetime.now().date())
    start_str = str(start_date)

    # If start is after today, nothing to do
    if start_date >= datetime.now().date():
        ray.get(fetched_data_manager.mark_fetched.remote(ticker))
        return True

    # Try NSE (.NS) then BSE (.BO)
    for suffix in [".NS", ".BO"]:
        data = yf.download(
            ticker + suffix,
            threads=20,
            progress=False,
            start=start_str,
            end=today_str
        )

        # drop extra level if present
        if getattr(data.columns, "nlevels", 1) > 1:
            data.columns = data.columns.droplevel(1)

        if not data.empty:
            break

    # If still empty, bail
    if data.empty:
        return False

    # Prepare and write
    data = data.reset_index()[["Date", "Open", "High", "Low", "Close", "Volume"]]
    data = data.sort_values("Date", ascending=True)

    # Exclude today’s partial bars if market is open
    if (is_Market_Open() or is_PreMarket_Open()) and data["Date"].iloc[-1] == datetime.today().date():
        data = data.iloc[:-1]

    os.makedirs(HIST_DIR, exist_ok=True)
    data.to_feather(feather_path)

    # Mark as done
    ray.get(fetched_data_manager.mark_fetched.remote(ticker))
    return True

def download_historical_quotes(df):
    if 'Symbol' not in df.columns:
        raise ValueError("Missing 'Symbol' Column")

    os.makedirs(HIST_DIR, exist_ok=True)
    ray.init(ignore_reinit_error=True)
    fetched_data_manager = FetchedDataManager.remote()

    tickers = df['Symbol'].tolist()
    with tqdm(total=len(tickers), desc="Downloading tickers") as pbar:
        batch_size = 10
        for i in range(0, len(tickers), batch_size):
            batch = tickers[i:i + batch_size]
            result_ids = [
                download_ticker_data.remote(t, fetched_data_manager)
                for t in batch
            ]
            for success in ray.get(result_ids):
                if success:
                    pbar.update(1)

    ray.shutdown()