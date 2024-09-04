import pandas as pd
import ta
import numpy as np
from functools import lru_cache
from multiprocessing import Pool, cpu_count

# Load the instruments DataFrame once
instruments_df = pd.read_csv("Instruments.csv")


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
        df = pd.read_csv(f"Hist_Data/{symbol}.csv")
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


def buy_or_sell(df):
    """
    Determine whether to buy, sell, or hold based on technical indicators.

    Parameters:
    df (pd.DataFrame): DataFrame containing stock data with at least 'Close' and 'Volume' columns.

    Returns:
    str: "BUY", "SELL", or "HOLD" based on the computed indicators.
    """
    if "Close" not in df.columns or "Volume" not in df.columns:
        raise KeyError("The DataFrame does not have the required columns: 'Close' or 'Volume'.")

    # Calculate key indicators
    df['RSI'] = np.floor(ta.momentum.RSIIndicator(df['Close'], window=14).rsi())
    macd_indicator = ta.trend.MACD(close=df['Close'], window_fast=9, window_slow=23, window_sign=9)
    df['MACD'] = np.floor(macd_indicator.macd())
    df['MACD_Signal'] = np.floor(macd_indicator.macd_signal())
    df['MACD_Hist'] = np.floor(macd_indicator.macd_diff())
    df['EMA10'] = np.floor(ta.trend.EMAIndicator(close=df['Close'], window=10).ema_indicator())
    df['EMA20'] = np.floor(ta.trend.EMAIndicator(close=df['Close'], window=20).ema_indicator())
    df['EMA50'] = np.floor(ta.trend.EMAIndicator(close=df['Close'], window=50).ema_indicator())
    df['EMA100'] = np.floor(ta.trend.EMAIndicator(close=df['Close'], window=100).ema_indicator())
    df['EMA200'] = np.floor(ta.trend.EMAIndicator(close=df['Close'], window=200).ema_indicator())

    df['Volume_MA20'] = df['Volume'].rolling(window=20).mean()

    # Ensure NaNs are filled forward/backward
    df.ffill(inplace=True)
    df.bfill(inplace=True)

   # Buy Signal
    df['Buy'] = (
        (df['RSI'] >= 60) & (df['RSI'] <= 70) &
        (df['MACD'] >= df['MACD_Signal']) &
        (df['MACD_Hist'] >= 0) &
        (df['Close'] >= df['EMA20']) &
        (df['Close'] >= df['EMA50']) &
        (df['Close'] >= df['EMA100']) &
        (df['Close'] >= df['EMA200']) &
        (df['RSI'] > df['RSI'].shift(1)) &
        (df['RSI'].shift(1) > df['RSI'].shift(2)) &
        (df['RSI'].shift(2) > df['RSI'].shift(3))
    )

    # Sell Signal
    df['Sell'] = (
        (df['RSI'] > 75) |
        (df['RSI'] < 50) |
        (df['MACD'] < df['MACD_Signal']) |
        (df['MACD_Hist'] < 0) |
        (df['Close'] < df['EMA10'])
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
        decision = buy_or_sell(df)
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