import os
import pandas as pd
import yfinance as yf
import glob
import ray
from tqdm import tqdm
from retry import retry
from Auto_Trader.utils import Indicators, is_Market_Open, is_PreMarket_Open
from NSEDownload import stocks
from datetime import datetime
from dateutil.relativedelta import relativedelta
from jugaad_data.nse import stock_df
import json
from filelock import FileLock

# JSON file to store fetched symbols and dates
FETCHED_DATA_FILE = "intermediary_files/fetched_data.json"
LOCK_FILE = "intermediary_files/fetched_data.lock"

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
        with FileLock(LOCK_FILE):  # Lock the file before writing
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
@retry(tries=3, delay=2)
def download_ticker_data(ticker, fetched_data_manager):
    # Check if already fetched today
    if ray.get(fetched_data_manager.is_fetched.remote(ticker)):
        print(f"Skipping {ticker}, already fetched today.")
        return

    print(f"Processing {ticker}...")
    ticker_ns = ticker + ".NS"
    ticker_bs = ticker + ".BO"

    try:
        data = yf.download(ticker_ns, threads=20, progress=False, period="max")

        if data.empty:
            data = yf.download(ticker_bs, threads=20, progress=False, period="max")

        if data.empty:
            print(f"No data found for {ticker_bs}, trying NSEDownload")
            data = stocks.get_data(stock_symbol=ticker, start_date=str(datetime.now().date() - relativedelta(months=3)), end_date=str(datetime.now().date()))

            if not data.empty:
                print(f"Data fetched from NSEDownload for {ticker}")
                rename_dict = {
                    'Date': 'Date',
                    'Open Price': 'Open',
                    'High Price': 'High',
                    'Low Price': 'Low',
                    'Close Price': 'Close',
                    'Last Price': 'Adj Close',
                    'Total Traded Quantity': 'Volume',
                }
                data = data.rename(columns=rename_dict)

        if data.empty:
            print(f"No data found for {ticker}, trying jugaad_data")
            data = stock_df(symbol=ticker, from_date=datetime.now().date() - relativedelta(months=3), to_date=datetime.now().date(), series="EQ")

            if not data.empty:
                print(f"Data fetched from jugaad_data for {ticker}")
                rename_dict = {
                    'DATE': 'Date',
                    'OPEN': 'Open',
                    'HIGH': 'High',
                    'LOW': 'Low',
                    'CLOSE': 'Close',
                    'LTP': 'Adj Close',
                    'VOLUME': 'Volume',
                }
                data = data.rename(columns=rename_dict)

        if not data.empty:
            data = data.reset_index()[["Date", "Close", "Volume"]]
            if is_Market_Open() or is_PreMarket_Open():
                today = datetime.today().date()
                data = data[data['Date'] != str(today)]
                data.to_csv(f"intermediary_files/Hist_Data/{ticker}.csv", index=False)
            else:
                data.to_csv(f"intermediary_files/Hist_Data/{ticker}.csv", index=False)

            # Mark as fetched today
            ray.get(fetched_data_manager.mark_fetched.remote(ticker))

        else:
            print(f"No data found for {ticker} using either symbol.")

    except Exception as e:
        pass


def download_historical_quotes(df):
    fetched_data_manager = FetchedDataManager.remote()

    today = str(datetime.now().date())

    if 'Symbol' not in df.columns:
        raise ValueError("Missing 'Symbol' Column")

    os.makedirs('intermediary_files/Hist_Data', exist_ok=True)

    ray.init(ignore_reinit_error=True)

    tickers = df['Symbol'].tolist()
    batch_size = 10
    batched_tickers = [tickers[i:i + batch_size] for i in range(0, len(tickers), batch_size)]

    for batch in batched_tickers:
        result_ids = [download_ticker_data.remote(ticker, fetched_data_manager) for ticker in batch]
        for _ in tqdm(ray.get(result_ids), total=len(result_ids), desc="Processing batch"):
            pass

    ray.shutdown()
