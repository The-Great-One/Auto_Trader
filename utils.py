from datetime import datetime, time
from kiteconnect import KiteConnect
import pandas as pd
import json
import numpy as np
from sympy import true
import ta
from my_secrets import API_KEY, API_SECRET
from Request_Token import get_request_token
from functools import lru_cache


@lru_cache(maxsize=None)
def initialize_kite():
    """
    Initialize the KiteConnect object with a valid session.
    
    Returns:
        KiteConnect: An instance of KiteConnect with a valid session.
    """
    kite = KiteConnect(api_key=API_KEY)
    access_token = read_session_data()
    kite.set_access_token(access_token)
    return kite


def is_Market_Open():
    """
    Check if the NSE market is currently open.
    
    Returns:
        bool: True if the market is open, False otherwise.
    """
    now = datetime.now()
    current_time = now.time()
    current_day = now.weekday()

    # NSE is open Monday to Friday, from 9:15 AM to 3:30 PM IST
    market_open = time(9, 15)
    market_close = time(15, 30)

    return current_day < 5 and market_open <= current_time <= market_close

def is_PreMarket_Open():
    """
    Check if the NSE Premarket is currently open.
    
    Returns:
        bool: True if the market is open, False otherwise.
    """
    now = datetime.now()
    current_time = now.time()
    current_day = now.weekday()

    # NSE Premarket is open Monday to Friday, from 9:00 AM to 9:15 AM IST
    market_open = time(9, 00)
    market_close = time(9, 15)

    return current_day < 5 and market_open <= current_time <= market_close

@lru_cache(maxsize=None)
def build_access_token():
    """
    Generate a new access token and save it to a JSON file.
    
    Returns:
        str: The new access token.
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
        with open("access_token.json", "w") as json_file:
            json.dump(session_data, json_file, indent=4)
        print("Session Expired..Creating New.")
        return data["access_token"]
    except Exception as e:
        print(f"Error in generating session: {e}")
        return None

@lru_cache(maxsize=None)
def read_session_data():
    """
    Read the access token from a JSON file and validate its date.
    
    Returns:
        str: The valid access token, or None if a new one needs to be created.
    """
    try:
        with open("access_token.json", "r") as json_file:
            session_data = json.load(json_file)
        access_token = session_data.get("access_token")
        session_date = session_data.get("date")

        if str(datetime.now().date()) == session_date:
            return access_token
        else:
            return build_access_token()

    except (FileNotFoundError, json.JSONDecodeError):
        print("Session data file not found or invalid. Creating a new session.")
        return build_access_token()

# Initialize Kite
kite = initialize_kite()

def fetch_instruments_list(kite=kite):
    """
    Fetch the list of instruments and holdings from the Kite API, 
    and save the holdings to a CSV file.
    
    Args:
        kite (KiteConnect): An instance of KiteConnect with a valid session.
    
    Returns:
        pd.DataFrame: DataFrame containing NSE stocks with instrument tokens.
    """
    try:
        # Fetch instruments and holdings
        instruments = kite.instruments()
        holdings = kite.holdings()
        holdings = pd.DataFrame(holdings)[["tradingsymbol", "instrument_token", "exchange", "quantity"]]
        
        # Filter out holdings with quantity greater than 0
        holdings = holdings[holdings["quantity"] > 0]
        
        holdings.to_csv("Holdings.csv", index=False)
        
        # Filter for NSE stocks only
        nse_stocks = [
            instrument for instrument in instruments if instrument["instrument_type"] == "EQ"
        ]
        df = pd.DataFrame(nse_stocks)[["instrument_token", "tradingsymbol", "exchange"]]
        print("Instruments and Holdings Fetched and Saved!")
        return df

    except Exception as e:
        print(f"Error in fetching instruments or holdings: {e}")
        return pd.DataFrame()


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

    # Select relevant columns
    final_nse_prioritized_df = deduplicated_df[
        ["Symbol", "instrument_token", "exchange"]
    ]

    return final_nse_prioritized_df


def Indicators(df):
    """
    Calculate key financial indicators such as RSI, MACD, and EMA for a DataFrame of stock prices.
    
    Args:
        df (pd.DataFrame): DataFrame containing stock data with 'Close' and 'Volume' columns.
    
    Returns:
        pd.DataFrame: DataFrame with additional indicator columns.
    
    Raises:
        KeyError: If 'Close' or 'Volume' columns are not present in the DataFrame.
    """
    if "Close" not in df.columns or "Volume" not in df.columns:
        raise KeyError("The DataFrame does not have the required columns: 'Close' or 'Volume'.")

    # Calculate key indicators
    df["RSI"] = np.floor(ta.momentum.RSIIndicator(df["Close"], window=14).rsi())
    macd_indicator = ta.trend.MACD(close=df["Close"], window_fast=9, window_slow=23, window_sign=9)
    df["MACD"] = np.floor(macd_indicator.macd())
    df["MACD_Signal"] = np.floor(macd_indicator.macd_signal())
    df["MACD_Hist"] = np.floor(macd_indicator.macd_diff())
    df["EMA10"] = np.floor(ta.trend.EMAIndicator(close=df["Close"], window=10).ema_indicator())
    df["EMA20"] = np.floor(ta.trend.EMAIndicator(close=df["Close"], window=20).ema_indicator())
    df["EMA50"] = np.floor(ta.trend.EMAIndicator(close=df["Close"], window=50).ema_indicator())

    # Calculate average volume over the past 20 days
    df["Volume_MA20"] = df["Volume"].rolling(window=20).mean()

    # Ensure NaNs are filled forward/backward
    df.ffill(inplace=True)
    df.bfill(inplace=True)

    # Reset the index to access 'Date' as a column
    df = df.reset_index()

    # Return the last non-empty row with the relevant columns
    return df