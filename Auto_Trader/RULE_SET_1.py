import pandas as pd
import ta
from functools import lru_cache
from multiprocessing import Pool, cpu_count
import json
import os

PROFIT_TRACKER_FILE = 'intermediary_files/profit_tracker.json'

def load_profit_tracker():
    """
    Load the profit tracker JSON file. If the file is corrupted or does not exist,
    start with an empty tracker.
    
    Returns:
    dict: Dictionary with symbols as keys and their maximum profit_percent.
    """
    if os.path.exists(PROFIT_TRACKER_FILE):
        try:
            with open(PROFIT_TRACKER_FILE, 'r') as file:
                return json.load(file)
        except json.JSONDecodeError as e:
            print(f"Error reading {PROFIT_TRACKER_FILE}: {e}")
            print("Starting with an empty profit tracker.")
            return {}
    return {}

def save_profit_tracker(tracker):
    """
    Append to the profit tracker JSON file safely by merging existing and new data.
    First, load the existing data, merge with the new data, then save it.
    
    Parameters:
    tracker (dict): Dictionary with symbols as keys and their maximum profit_percent.
    """
    # Load existing data
    existing_data = load_profit_tracker()
    
    # Update existing data with new tracker entries
    existing_data.update(tracker)
    
    # Write to a temporary file first
    temp_file = PROFIT_TRACKER_FILE + '.tmp'
    
    try:
        with open(temp_file, 'w') as file:
            json.dump(existing_data, file)
        
        # Once the temp file is written, rename it to the actual file
        os.replace(temp_file, PROFIT_TRACKER_FILE)  # Atomic rename
        print(f"Profit tracker updated successfully to {PROFIT_TRACKER_FILE}.")
        
    except Exception as e:
        print(f"Error saving profit tracker: {e}")
        
        # Clean up the temp file if something went wrong
        if os.path.exists(temp_file):
            os.remove(temp_file)

# Initialize or load the existing profit tracker
profit_tracker = load_profit_tracker()

@lru_cache(maxsize=None)
def load_historical_data(symbol):
    """
    Load historical data for a given symbol and cache the result.

    Parameters:
    symbol (str): The stock symbol for which historical data is to be loaded.

    Returns:
    pd.DataFrame: DataFrame containing the historical data, or None if loading fails.
    """
    try:
        df = pd.read_csv(f"intermediary_files/Hist_Data/{symbol}.csv")
        return df
    except Exception as e:
        print(f"Error loading {symbol}.csv: {e}")
        return None


def preprocess_data(row_df, symbol):
    """
    Preprocess the stock data by appending new row data to the historical data.

    Parameters:
    row_df (pd.DataFrame): DataFrame containing the new row of data.
    symbol (str): The stock symbol for which data is being processed.

    Returns:
    pd.DataFrame: Combined DataFrame with the historical and new row data, or None if an error occurs.
    """
    append_df = row_df[["Date", "Close", "Volume"]]

    df = load_historical_data(symbol)
    if df is None:
        return None

    # Check for required columns
    if not all(col in df.columns for col in ["Date", "Close", "Volume"]):
        print(f"{symbol}.csv Columns Missing")
        return None

    # Preprocess date and remove duplicates
    df["Date"] = pd.to_datetime(df["Date"], errors='coerce')
    df.dropna(subset=["Date"], inplace=True)
    df.set_index("Date", inplace=True)
    df = df[~df.index.duplicated(keep='first')]
    df.sort_index(inplace=True)

    # Append new row data using pd.concat
    append_df.set_index("Date", inplace=True)
    df = pd.concat([df, append_df])

    return df


def update_profit_tracker(symbol, current_profit_percent):
    """
    Update the profit tracker JSON with the new profit percentage if it's greater than the existing one.
    
    Parameters:
    symbol (str): The stock symbol.
    current_profit_percent (float): The current profit percentage.
    """
    if symbol not in profit_tracker or profit_tracker[symbol] < current_profit_percent:
        profit_tracker[symbol] = current_profit_percent
        save_profit_tracker(profit_tracker)


def buy_or_sell(df, average_price, symbol):
    """
    Determine whether to buy, sell, or hold based on technical indicators and trailing stop loss.

    Parameters:
    df (pd.DataFrame): DataFrame containing stock data with at least 'Close' and 'Volume' columns.
    average_price (float): The average purchase price for the stock.
    symbol (str): The stock symbol.

    Returns:    
    str: "BUY", "SELL", or "HOLD" based on the computed indicators and trailing stop loss.
    """
    if "Close" not in df.columns or "Volume" not in df.columns:
        raise KeyError("The DataFrame does not have the required columns: 'Close' or 'Volume'.")

    # Calculate key indicators
    df['RSI'] = ta.momentum.RSIIndicator(df['Close'], window=14).rsi()
    macd_indicator = ta.trend.MACD(close=df['Close'], window_fast=9, window_slow=23, window_sign=9)
    df['MACD'] = macd_indicator.macd()
    df['MACD_Signal'] = macd_indicator.macd_signal()
    df['MACD_Hist'] = macd_indicator.macd_diff()
    df["EMA10"] = ta.trend.EMAIndicator(close=df["Close"], window=10).ema_indicator() 
    df["EMA20"] = ta.trend.EMAIndicator(close=df["Close"], window=20).ema_indicator() 
    df["EMA50"] = ta.trend.EMAIndicator(close=df["Close"], window=50).ema_indicator() 
    df["EMA100"] = ta.trend.EMAIndicator(close=df["Close"], window=100).ema_indicator() 
    df["EMA200"] = ta.trend.EMAIndicator(close=df["Close"], window=200).ema_indicator() 
    df['Volume_MA20'] = ta.trend.SMAIndicator(df['Volume'], window=20).sma_indicator()

    # Calculate profit_percent
    df['profit_percent'] = ((df['Close'] - average_price) / average_price) * 100
    
    # Handle null profit_percent
    df['profit_percent'].fillna(0, inplace=True)

    # Update profit tracker
    current_profit_percent = df['profit_percent'].iloc[-1]
    update_profit_tracker(symbol, current_profit_percent)

    # Trailing stop-loss condition: Trigger sell if profit drops by 3% from peak
    if symbol in profit_tracker:
        peak_profit_percent = profit_tracker[symbol]
        trailing_stop_threshold = peak_profit_percent - 2  # Allow a 3% drop from the peak
        if current_profit_percent <= trailing_stop_threshold:
            return "SELL"

    # Buy signal
    df['Buy'] = (
        (df['EMA10'] > df['EMA20']) & 
        (df['RSI'] > 60) & (df['RSI'] <= 70) &
        (df['MACD_Hist'] > 0) &
        (df['Volume'] > 1.0 * df['Volume_MA20'])
    )

    # Sell signal
    df['Sell'] = (
        (df['EMA10'] < df['EMA20']) &
        (df['RSI'] < 55) &
        (df['MACD_Hist'] < -1) &  # Loosen MACD_Hist threshold for quicker reaction
        (df['Volume'] > 1.3 * df['Volume_MA20'])  # Loosen volume condition for more exits
    )

    last_row = df.tail(1)

    if last_row['Buy'].iloc[0]:
        return "BUY"
    elif last_row['Sell'].iloc[0]:
        return "SELL"
    else:
        return "HOLD"


def process_single_stock(row):
    """
    Processes a single stock and returns the decision.

    Parameters:
    row (pd.Series): Series containing the stock data.

    Returns:
    dict or None: A dictionary with the stock's decision, or None if no decision is made.
    """
    row_df = pd.DataFrame([row], columns=row.index)
    row_df = row_df.rename(columns={
        'last_price': 'Close',
        'volume_traded': 'Volume',
    })
    
    df = preprocess_data(row_df, row["Symbol"])
    if df is not None:
        decision = buy_or_sell(df, row["average_price"], row["Symbol"])
        if decision != "HOLD":
            return {"Symbol": row["Symbol"], "Decision": decision, "Exchange": row["exchange"], "Close": row["last_price"]}
    return None


def Rule_1(data):
    """
    Apply trading rules to determine buy or sell decisions for each stock.

    Parameters:
    data (pd.DataFrame): DataFrame containing the latest stock prices, traded volumes, and instrument tokens.

    Returns:
    list: A list of dictionaries containing the buy/sell decisions for each stock.
    """
    # Load the instruments DataFrame once
    instruments_df = pd.read_csv("intermediary_files/Instruments.csv")
    data = pd.DataFrame(data=data)[["last_price", "volume_traded", "instrument_token"]]
    data = pd.merge(
        data,
        instruments_df,
        left_on="instrument_token",
        right_on="instrument_token",
        how="inner",
    )
    data['Date'] = pd.Timestamp.today().strftime('%Y-%m-%d')

    with Pool(cpu_count()) as pool:
        results = pool.map(process_single_stock, [row for _, row in data.iterrows()])
    
    decisions = [result for result in results if result is not None]
    return decisions