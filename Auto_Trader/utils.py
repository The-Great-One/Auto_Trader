import json
import multiprocessing
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache

import numpy as np
import pandas as pd
import talib
from filelock import FileLock, Timeout
from sqlalchemy import create_engine

# Import rule set modules
from Auto_Trader import (RULE_SET_2,
                         RULE_SET_7, KiteConnect,
                         ZoneInfo, datetime, json, logging, mcal, np, os, pd,
                         retry, shutil, sys, talib, timedelta, traceback)
from Auto_Trader.my_secrets import (API_KEY, API_SECRET, DATABASE, DB_PASSWORD,
                                    HOST, USER, DEBUG_MODE)
from Auto_Trader.Request_Token import get_request_token

logger = logging.getLogger("Auto_Trade_Logger")

# Default rule set values
DEFAULT_RULE_SETS = {
    'RULE_SET_2': RULE_SET_2,
    'RULE_SET_7': RULE_SET_7,
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
nse_calendar = mcal.get_calendar('NSE')

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
        # Check if the directory exists
        if os.path.isdir("intermediary_files"):
            # Loop through each item in intermediary_files
            for item in os.listdir("intermediary_files"):
                item_path = os.path.join("intermediary_files", item)
                # Skip the file you want to keep
                if item != "Holdings.json":
                    # Remove files or directories
                    if os.path.isfile(item_path):
                        os.remove(item_path)
                    elif os.path.isdir(item_path):
                        shutil.rmtree(item_path)
            
        os.makedirs("intermediary_files", exist_ok=True)
        
        with open("intermediary_files/access_token.json", "w") as json_file:
            json.dump(session_data, json_file, indent=4)
        return data["access_token"]
    except Exception as e:
        logger.error(f"Error in generating session: {e}, Traceback: {traceback.format_exc()}")
        sys.exit()
        return None

def read_session_data():
    """
    Read the access token from a JSON file and validate its date.
    
    Returns:
        str: The valid access token, or None if a new one needs to be created.
    """
    try:
        with open("intermediary_files/access_token.json", "r") as json_file:
            session_data = json.load(json_file)
        access_token = session_data.get("access_token")
        session_date = session_data.get("date")

        if str(datetime.now().date()) == session_date:
            return access_token
        else:
            return build_access_token()

    except (FileNotFoundError, json.JSONDecodeError):
        logger.warning("Creating a new session.")
        return build_access_token()

def initialize_kite():
    """
    Initialize the KiteConnect object with a valid session.
    
    Returns:
        KiteConnect: An instance of KiteConnect with a valid session.
    """
    try:
        kite = KiteConnect(api_key=API_KEY)
        access_token = read_session_data()
        kite.set_access_token(access_token)
        return kite
    except:
        build_access_token()
        return initialize_kite()

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
    """
    hl2 = (df["High"].values + df["Low"].values) * 0.5
    up = hl2 + multiplier * atr
    dn = hl2 - multiplier * atr

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

    df[sup_col] = st
    df[sup_dir] = direction


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

def compute_cmf(high, low, close, volume, period=20):
    high = np.asarray(high)
    low = np.asarray(low)
    close = np.asarray(close)
    volume = np.asarray(volume)

    mf_multiplier = ((close - low) - (high - close)) / (high - low + 1e-10)  # Avoid div by zero
    mf_volume = mf_multiplier * volume

    mfv_sum = pd.Series(mf_volume).rolling(window=period).sum()
    vol_sum = pd.Series(volume).rolling(window=period).sum()

    cmf = mfv_sum / vol_sum
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

    # Coerce numeric dtypes once
    df[["High","Low","Close","Volume"]] = (
        df[["High","Low","Close","Volume"]]
        .apply(pd.to_numeric, errors="coerce").astype("float64")
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
    EMA_periods = (5,9,10,12,13,20,21,26,50,100,200)
    EMA_values = {f"EMA{p}": talib.EMA(close, timeperiod=p) for p in EMA_periods}
    ATR = talib.ATR(high, low, close, timeperiod=atr_period)
    UpperBand, MiddleBand, LowerBand = talib.BBANDS(
        close, timeperiod=20, nbdevup=3, nbdevdn=2
    )
    ADX = talib.ADX(high, low, close, timeperiod=14)
    OBV = talib.OBV(close, vol)
    Stochastic_K, Stochastic_D = talib.STOCH(
        high, low, close,
        fastk_period=14, slowk_period=3, slowk_matype=0,
        slowd_period=3, slowd_matype=0
    )

    # Rolling SMAs & Volume MA20
    SMA_10_Close = df["Close"].rolling(10).mean()
    SMA_20_Close = df["Close"].rolling(20).mean()
    SMA_20_Low   = df["Low"].rolling(20).mean()
    SMA_20_High  = df["High"].rolling(20).mean()
    SMA_200_Close= df["Close"].rolling(200).mean()
    SMA_20_Volume= df["Volume"].rolling(20).mean()
    SMA_200_Volume= df["Volume"].rolling(200).mean()

    Weekly_SMA_20 = talib.SMA(close, timeperiod=100)  # 20*5
    Weekly_SMA_200= talib.SMA(close, timeperiod=1000) # 200*5
    ws = pd.Series(Weekly_SMA_200, index=df.index)
    Weekly_SMA_200_1w = ws.shift(5)
    Weekly_SMA_200_2w = ws.shift(10)
    Weekly_SMA_200_3w = ws.shift(15)
    Weekly_SMA_200_4w = ws.shift(20)

    Volume_MA20 = SMA_20_Volume
    VolumeConfirmed = vol > (1.2 * SMA_20_Volume.values)

    # Fibonacci static levels
    fib = compute_fibonacci(df["High"], df["Low"])

    CMF = compute_cmf(df['High'], df['Low'], df['Close'], df['Volume'], period=5)

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
        "Volume_MA20": Volume_MA20,
        "VolumeConfirmed": VolumeConfirmed,
        # stochastic
        "Stochastic_%K": Stochastic_K,
        "Stochastic_%D": Stochastic_D,
        # SMAs
        "SMA_10_Close": SMA_10_Close,
        "SMA_20_Close": SMA_20_Close,
        "SMA_20_Low":   SMA_20_Low,
        "SMA_20_High":  SMA_20_High,
        "SMA_200_Close":SMA_200_Close,
        "SMA_20_Volume":SMA_20_Volume,
        "SMA_200_Volume":SMA_200_Volume,
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
    }

    # Bulk assign
    df = df.assign(**assign_kwargs)

    # Supertrend variants
    compute_supertrend(df, ATR, multiplier=2.0)
    compute_supertrend(
        df, ATR,
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
        dataframe['Date'] = pd.to_datetime(dataframe['Date'], errors='coerce')
        dataframe.dropna(subset=['Date'], inplace=True)
        dataframe.set_index('Date', inplace=True)

    # Concatenate and remove duplicates
    df = pd.concat([df, append_df])
    df = df[~df.index.duplicated(keep='last')]  # Keep the last duplicate
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
    row_df = pd.DataFrame([{
        'Date': row['Date'],
        'Close': row['last_price'],
        'Volume': row['volume_traded'],
        'High': row['ohlc']['high'],
        "Low": row['ohlc']['low'],
    }])

    df = preprocess_data(row_df, row['Symbol'])
    return df

def apply_trading_rules(df, row):
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

    def apply_rule(rule_set_name, rule_set_module):
        try:
            holdings = pd.read_feather("intermediary_files/Holdings.feather")
            # Apply the trading rule from the current rule set
            decision = rule_set_module.buy_or_sell(df, row, holdings)
            logger.debug(f"Rule {rule_set_name} made a {decision} decision for {row['Symbol']}")
            return rule_set_name, decision
        except Exception as e:
            logger.error(f"Error applying trading rule {rule_set_name} for {row['Symbol']}: {e}, Traceback: {traceback.format_exc()}")
            return rule_set_name, "HOLD"

    num_cores = multiprocessing.cpu_count()
    # Use ThreadPoolExecutor to parallelize rule application
    with ThreadPoolExecutor(max_workers=num_cores) as executor:
        # Submit all rules to the executor and process them concurrently
        futures = {executor.submit(apply_rule, rule_set_name, rule_set_module): rule_set_name
                   for rule_set_name, rule_set_module in RULE_SETS.items()}

        # Collect results as they complete
        for future in as_completed(futures):
            rule_set_name, decision = future.result()
            if decision in decisions:
                decisions[decision].append(rule_set_name)
            else:
                # Log the specific rule set that returned an unknown decision
                logger.error(f"Rule {rule_set_name} returned an unknown decision: {decision}")
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
            # Apply the trading rules
            decision, contributing_rules = apply_trading_rules(df, row)
            if decision != "HOLD":
                return {
                    "Symbol": row['Symbol'],
                    "Decision": decision,
                    "ContributingRules": contributing_rules,
                    "Exchange": row['exchange'],
                    "Close": row['last_price']
                }
    except Exception as e:
        # Log exceptions with stock symbol for easier debugging
        logger.error(f"Error processing stock {row.get('Symbol', 'Unknown')}: {e}, Traceback: {traceback.format_exc()}")
    return None


# Initialize Kite
kite = initialize_kite()

# Retry decorator, with exponential backoff and jitter
@retry(tries=3, delay=2, backoff=2, jitter=(0, 1), exceptions=(Exception,))
def fetch_holdings(kite=kite):
    """
    Fetch the list of instruments and holdings from the Kite API, 
    and save the holdings to a CSV file.
    
    Args:
        kite (KiteConnect): An instance of KiteConnect with a valid session.
    
    Returns:
        pd.DataFrame: DataFrame containing NSE stocks with instrument tokens.
    """
    try:
        # Fetch holdings
        holdings = kite.holdings()
        if holdings:
            holdings = pd.DataFrame(holdings)[["tradingsymbol", "instrument_token", "exchange", "average_price", "quantity", "t1_quantity"]]
            
            # Merge Holdings and t1_quantity
            holdings['quantity'] = holdings['quantity'] + holdings['t1_quantity']
                        
            # Filter out holdings with quantity greater than 0
            holdings = holdings[holdings["quantity"] > 0]
            
            # Save holdings to CSV
            holdings.to_feather("intermediary_files/Holdings.feather")
            
            logger.info(f"Number of Holdings: {len(holdings)}")
            return holdings
        else:
            # Initialize an empty DataFrame with the expected columns
            holdings = pd.DataFrame(columns=["tradingsymbol", "instrument_token", "exchange", "average_price", "quantity", "t1_quantity"])
            holdings.to_feather("intermediary_files/Holdings.feather")
            logger.debug("No holdings found, returning an empty DataFrame.")
            return holdings

    except Exception as e:
        logger.error(f"Error in fetching holdings: {e}, Traceback: {traceback.format_exc()}")
        raise  # Re-raise to trigger the retry decorator
    
# Retry decorator, with exponential backoff and jitter
@retry(tries=3, delay=2, backoff=2, jitter=(0, 1), exceptions=(Exception,))
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
        # Fetch instruments
        instruments = kite.instruments()
        
        # Filter for NSE stocks only
        nse_stocks = [
            instrument for instrument in instruments if instrument["instrument_type"] == "EQ"
        ]
        df = pd.DataFrame(nse_stocks)[["instrument_token", "tradingsymbol", "exchange"]]
        return df

    except Exception as e:
        logger.error(f"Error in fetching instruments: {e}, Traceback: {traceback.format_exc()}")
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

    if schedule is None:
        logger.info("Market is closed today.")
        return False
    
    now = datetime.now(ZoneInfo("Asia/Kolkata"))
    market_open = schedule.iloc[0]['market_open'].astimezone(ZoneInfo("Asia/Kolkata"))
    market_close = schedule.iloc[0]['market_close'].astimezone(ZoneInfo("Asia/Kolkata"))
    
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
    market_open = schedule.iloc[0]['market_open'].astimezone(ZoneInfo("Asia/Kolkata"))
    premarket_open = (market_open - timedelta(minutes=15))
    
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

    # Select relevant columns
    final_nse_prioritized_df = deduplicated_df[
        ["Symbol", "instrument_token", "exchange"]
    ]

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
        logger.error(f"Failed to read Instruments.csv: {e}, Traceback: {traceback.format_exc()}")
        sys.exit(1)  # Exit if we cannot load instruments data

def cleanup_stop_loss_json(holdings = fetch_holdings()):
    """
    Cleans up the stop-loss JSON file by removing any entries that 
    do not correspond to currently held tradingsymbols.
    
    Parameters:
    holdings (pd.DataFrame): A DataFrame with a 'tradingsymbol' column
                             representing currently held instruments.
    """
    
    HOLDINGS_FILE_PATH = 'intermediary_files/Holdings.json'
    LOCK_FILE_PATH = 'intermediary_files/Holdings.lock'

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
                with open(HOLDINGS_FILE_PATH, 'r') as json_file:
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

    # Verify holdings DataFrame has the required column
    if 'tradingsymbol' not in holdings.columns:
        logger.error("Holdings DataFrame does not have a 'tradingsymbol' column.")
        return

    current_symbols = set(holdings['tradingsymbol'].unique())
    keys_to_remove = [symbol for symbol in stop_loss_data.keys() if symbol not in current_symbols]

    if not keys_to_remove:
        logger.info("No outdated stop-loss entries to remove. JSON is up-to-date.")
        return

    # Acquire lock again to write updated data
    try:
        with lock.acquire(timeout=10):
            for key in keys_to_remove:
                del stop_loss_data[key]

            with open(HOLDINGS_FILE_PATH, 'w') as json_file:
                json.dump(stop_loss_data, json_file, indent=4)
            logger.info(f"Removed {len(keys_to_remove)} outdated stop-loss entries from JSON.")
    except Timeout:
        logger.error(f"Timeout while trying to acquire the lock for {HOLDINGS_FILE_PATH}.")
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
        engine = create_engine(f"mysql+mysqlconnector://{USER}:{DB_PASSWORD}@{HOST}/{DATABASE}")
        
        # Query the Trade_Params table
        query = "SELECT * FROM Trade_Params"
        df_trade_params = pd.read_sql(query, engine)

        # Convert the DataFrame to a nested dictionary
        nested_dict = df_trade_params.set_index("ticker").T.to_dict()
        return nested_dict

    except Exception as e:
        print(f"Error: {e}")
        return {}