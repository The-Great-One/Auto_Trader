from Auto_Trader import mcal, KiteConnect, json, datetime, pd, retry, ZoneInfo, timedelta, logging, sys, shutil, os, np, talib
from Auto_Trader.my_secrets import API_KEY, API_SECRET
from Auto_Trader.Request_Token import get_request_token
from concurrent.futures import ThreadPoolExecutor, as_completed

# Import rule set modules
from Auto_Trader import RULE_SET_1, RULE_SET_2, RULE_SET_3, RULE_SET_4, RULE_SET_5, RULE_SET_6, RULE_SET_7, RULE_SET_8

logger = logging.getLogger("Auto_Trade_Logger")

# Map rule set names to their modules
RULE_SETS = {
    'RULE_SET_1': RULE_SET_1,
    'RULE_SET_2': RULE_SET_2,
    'RULE_SET_3': RULE_SET_3,
    'RULE_SET_4': RULE_SET_4,
    'RULE_SET_5': RULE_SET_5,
    'RULE_SET_6': RULE_SET_6,
    'RULE_SET_7': RULE_SET_7,
    'RULE_SET_8': RULE_SET_8,
    # Add new rule sets here
}

# Initialize the NSE market calendar
nse_calendar = mcal.get_calendar('NSE')

def build_access_token():
    """
    Generate a new access token and save it to a JSON file.
    
    Returns:
        str: The new access token.
    """
    try:
        logger.warning("Session Expired..Creating New.")
        kite = KiteConnect(api_key=API_KEY)
        data = kite.generate_session(
            request_token=get_request_token(), api_secret=API_SECRET
        )
        session_data = {
            "access_token": data["access_token"],
            "date": datetime.now().strftime("%Y-%m-%d"),
        }
        if os.path.isdir('intermediary_files'):
            shutil.rmtree("intermediary_files")
            
        os.makedirs("intermediary_files", exist_ok=True)
        
        with open("intermediary_files/access_token.json", "w") as json_file:
            json.dump(session_data, json_file, indent=4)
        return data["access_token"]
    except Exception as e:
        logger.error(f"Error in generating session: {e}")
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
        logger.warning("Session data file not found or invalid. Creating a new session.")
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

def calculate_supertrend_talib_optimized(df, atr, period=10, multiplier=2):
    """
    Vectorized Supertrend calculation using TA-Lib for ATR.

    Parameters:
    -----------
    df : pd.DataFrame
        The DataFrame containing historical stock data with 'High', 'Low', and 'Close' columns.
    atr : pd.Series
        The Average True Range values already calculated.
    period : int
        The look-back period for calculating the ATR (default is 10).
    multiplier : float
        The multiplier for the ATR to create the Supertrend bands (default is 2).

    Returns:
    --------
    pd.DataFrame
        The DataFrame with additional columns for Supertrend values and direction.
    """
    hl2 = (df['High'] + df['Low']) / 2
    upper_band = hl2 + (multiplier * atr)
    lower_band = hl2 - (multiplier * atr)

    # Vectorized calculation of Supertrend
    supertrend = np.where(df['Close'] > upper_band.shift(1), lower_band,
                          np.where(df['Close'] < lower_band.shift(1), upper_band, np.nan))

    # Forward fill to maintain trend direction
    supertrend = pd.Series(supertrend).fillna(method='ffill').values

    # Determine the trend direction
    direction = np.where(df['Close'] > supertrend, True, False)

    # Return the DataFrame with the relevant columns using .assign()
    return df.assign(Supertrend=supertrend, Supertrend_Direction=direction)

def Indicators(df, rsi_period=14, macd_fast=12, macd_slow=26, macd_signal=9, atr_period=14):
    """
        Calculate key financial indicators using TA-Lib for a DataFrame of stock prices, optimized for performance.

        Args:
            df (pd.DataFrame): DataFrame containing stock data with 'Close', 'High', 'Low', and 'Volume' columns.
            rsi_period (int): Time period for RSI calculation.
            macd_fast (int): Fast period for MACD calculation.
            macd_slow (int): Slow period for MACD calculation.
            macd_signal (int): Signal period for MACD calculation.
            atr_period (int): Time period for ATR calculation.

        Returns:
            pd.DataFrame: DataFrame with additional indicator columns.
        """
    if "Close" not in df.columns or "Volume" not in df.columns:
        raise KeyError("The DataFrame does not have the required columns: 'Close' or 'Volume'.")

    # Calculate RSI
    rsi = talib.RSI(df["Close"], timeperiod=rsi_period)

    # Calculate MACD
    macd, macd_signal, macd_hist = talib.MACD(df["Close"], fastperiod=macd_fast, slowperiod=macd_slow, signalperiod=macd_signal)

    # Calculate EMAs for different periods
    ema_values = {f"EMA{period}": talib.EMA(df["Close"], timeperiod=period) for period in [5, 9, 10, 13, 20, 21, 50, 100, 200, 12, 26]}
    ema20_low = talib.EMA(df["Low"], timeperiod=20)  # EMA based on the Low prices

    # Calculate ATR
    atr = talib.ATR(df['High'], df['Low'], df['Close'], timeperiod=atr_period)

    # Calculate Supertrend using the optimized function
    df = calculate_supertrend_talib_optimized(df, atr, period=10, multiplier=2)

    # Calculate SMAs
    sma_10_close = talib.SMA(df['Close'], timeperiod=10)
    sma_20_close = talib.SMA(df['Close'], timeperiod=20)
    sma_200_close = talib.SMA(df['Close'], timeperiod=200)
    sma_20_high = talib.SMA(df['High'], timeperiod=20)
    sma_20_volume = talib.SMA(df['Volume'], timeperiod=20)
    sma_200_volume = talib.SMA(df['Volume'], timeperiod=200)

    # Weekly SMAs
    weekly_sma_20 = talib.SMA(df['Close'], timeperiod=20 * 5)
    weekly_sma_200 = talib.SMA(df['Close'], timeperiod=200 * 5)

    # Shifting Weekly SMA for the last 4 weeks
    weekly_sma_200_1w = weekly_sma_200.shift(5)
    weekly_sma_200_2w = weekly_sma_200.shift(10)
    weekly_sma_200_3w = weekly_sma_200.shift(15)
    weekly_sma_200_4w = weekly_sma_200.shift(20)

    # Volume confirmation
    volume_confirmed = df['Volume'] > (1.2 * sma_20_volume)

    # Assign calculated indicators to the DataFrame using .assign()
    df = df.assign(
        RSI=rsi,
        MACD=macd,
        MACD_Signal=macd_signal,
        MACD_Hist=macd_hist,
        ATR=atr,
        SMA_10_Close=sma_10_close,
        SMA_20_Close=sma_20_close,
        SMA_200_Close=sma_200_close,
        SMA_20_High=sma_20_high,
        SMA_20_Volume=sma_20_volume,
        SMA_200_Volume=sma_200_volume,
        Weekly_SMA_20=weekly_sma_20,
        Weekly_SMA_200=weekly_sma_200,
        Weekly_SMA_200_1w=weekly_sma_200_1w,
        Weekly_SMA_200_2w=weekly_sma_200_2w,
        Weekly_SMA_200_3w=weekly_sma_200_3w,
        Weekly_SMA_200_4w=weekly_sma_200_4w,
        EMA20_LOW=ema20_low,
        VolumeConfirmed=volume_confirmed,
        **ema_values  # Spread the EMA dictionary to add each EMA column
    )

    # Return the DataFrame with the relevant columns
    return df


def load_historical_data(symbol):
    """
    Load historical data for a given symbol and cache the result.

    Parameters:
        symbol (str): The stock symbol.

    Returns:
        pd.DataFrame or None: The historical data DataFrame, or None if loading fails.
    """
    try:
        # Specify dtypes for more efficient memory usage
        df = pd.read_csv(
    f"intermediary_files/Hist_Data/{symbol}.csv"
    )
        return df
    except Exception as e:
        logger.error(f"Error loading {symbol}.csv: {e}")
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
    append_df = row_df[["Date", "Close", "Volume"]].copy()

    df = load_historical_data(symbol)
    if df is None:
        return None

    required_columns = {"Date", "Close", "Volume"}
    if not required_columns.issubset(df.columns):
        logger.error(f"{symbol}.csv is missing required columns.")
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
        'Volume': row['volume_traded']
    }])

    df = preprocess_data(row_df, row['Symbol'])
    return df

def apply_trading_rules(df, row):
    """
    Apply all trading rules from the RULE_SETS dictionary to the stock data
    and return the strongest trading signal (e.g., SELL > BUY > HOLD).
    
    Parameters:
        df (pd.DataFrame): The preprocessed stock data.
        row (dict): The current stock data row.

    Returns:
        str: The strongest trading decision from the rule sets.
    """
    # Initialize a dictionary to track the decisions
    decisions = {"SELL": 0, "BUY": 0, "HOLD": 0}

    def apply_rule(rule_set_name, rule_set_module):
        try:
            holdings = pd.read_csv("intermediary_files/Holdings.csv")
            # Apply the trading rule from the current rule set
            decision = rule_set_module.buy_or_sell(df, row, holdings)
            logger.info(f"Rule {rule_set_name} made a {decision} decision for {row['Symbol']}")
            return decision
        except Exception as e:
            logger.error(f"Error applying trading rule {rule_set_name} for {row['Symbol']}: {e}")
            return "HOLD"

    # Use ThreadPoolExecutor to parallelize rule application
    with ThreadPoolExecutor() as executor:
        # Submit all rules to the executor and process them concurrently
        futures = {executor.submit(apply_rule, rule_set_name, rule_set_module): rule_set_name
                   for rule_set_name, rule_set_module in RULE_SETS.items()}

        # Collect results as they complete
        for future in as_completed(futures):
            decision = future.result()
            rule_set_name = futures[future]  # Get the corresponding rule set name
            if decision in decisions:
                decisions[decision] += 1
            else:
                # Log the specific rule set that returned an unknown decision
                logger.error(f"Rule {rule_set_name} returned an unknown decision: {decision}")
                pass

    # Print decisions for each stock (for debugging)
    logger.debug(f"Decisions for {row['Symbol']}: {decisions}")

    # Prioritize decisions: SELL > BUY > HOLD
    if decisions["SELL"] > 0:
        logger.info(f"Final decision for {row['Symbol']}: SELL")
        return "SELL"
    elif decisions["BUY"] > 0:
        logger.info(f"Final decision for {row['Symbol']}: BUY")
        return "BUY"
    else:
        logger.info(f"Final decision for {row['Symbol']}: HOLD")
        return "HOLD"


def process_stock_and_decide(row):
    """
    Processes a single stock and returns a decision dict if any.

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
            decision = apply_trading_rules(df, row)
            if decision != "HOLD":
                return {
                    "Symbol": row['Symbol'],
                    "Decision": decision,
                    "Exchange": row['exchange'],
                    "Close": row['last_price']
                }
    except Exception as e:
        # Log exceptions with stock symbol for easier debugging
        logger.error(f"Error processing stock {row.get('Symbol', 'Unknown')}: {e}")
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
        holdings = pd.DataFrame(holdings)[["tradingsymbol", "instrument_token", "exchange", "average_price", "quantity", "t1_quantity"]]
        
        #Merge Holdings and t1_quantity
        holdings['quantity'] = holdings['quantity'] + holdings['t1_quantity']
        
        # Filter out holdings with quantity greater than 0
        holdings = holdings[holdings["quantity"] > 0]
        
        holdings.to_csv("intermediary_files/Holdings.csv", index=False)
        
        logger.debug("Holdings Fetched and Saved!")
        return holdings

    except Exception as e:
        logger.error(f"Error in fetching holdings: {e}")
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
        logger.error(f"Error in fetching instruments: {e}")
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
    
    Args:
    schedule (pd.DataFrame): Market schedule for the day.
    
    Returns:
    bool: True if the market is open, False otherwise.
    """
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