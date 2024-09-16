from Auto_Trader import RULE_SET_3, mcal, lru_cache, KiteConnect, json, datetime, ta, pd, retry, ZoneInfo, timedelta
from Auto_Trader.my_secrets import API_KEY, API_SECRET
from Auto_Trader.Request_Token import get_request_token
from concurrent.futures import ThreadPoolExecutor, as_completed

# Import rule set modules
from Auto_Trader import RULE_SET_1, RULE_SET_2, RULE_SET_3

# Map rule set names to their modules
RULE_SETS = {
    'RULE_SET_1': RULE_SET_1,
    'RULE_SET_2': RULE_SET_2,
    'RULE_SET_3': RULE_SET_3,
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
        kite = KiteConnect(api_key=API_KEY)
        data = kite.generate_session(
            request_token=get_request_token(), api_secret=API_SECRET
        )
        session_data = {
            "access_token": data["access_token"],
            "date": datetime.now().strftime("%Y-%m-%d"),
        }
        with open("intermediary_files/access_token.json", "w") as json_file:
            json.dump(session_data, json_file, indent=4)
        print("Session Expired..Creating New.")
        return data["access_token"]
    except Exception as e:
        print(f"Error in generating session: {e}")
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
        print("Session data file not found or invalid. Creating a new session.")
        return build_access_token()

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
    df["RSI"] = round(ta.momentum.RSIIndicator(df["Close"], window=14).rsi(), 2)
    macd_indicator = ta.trend.MACD(close=df["Close"], window_fast=9, window_slow=23, window_sign=9)
    df["MACD"] = macd_indicator.macd()
    df["MACD_Signal"] = macd_indicator.macd_signal()
    df["MACD_Hist"] = macd_indicator.macd_diff()
    df["EMA10"] = ta.trend.EMAIndicator(close=df["Close"], window=10).ema_indicator()
    df["EMA20"] = ta.trend.EMAIndicator(close=df["Close"], window=20).ema_indicator()
    df["EMA50"] = ta.trend.EMAIndicator(close=df["Close"], window=50).ema_indicator()
    df["EMA12"] = ta.trend.EMAIndicator(close=df["Close"], window=12).ema_indicator()
    df["EMA26"] = ta.trend.EMAIndicator(close=df["Close"], window=26).ema_indicator()

    # Calculate average volume over the past 20 days
    df['Volume_MA'] = ta.trend.SMAIndicator(df['Volume'], window=20).sma_indicator()
    
    # Ensure NaNs are filled forward/backward
    df.ffill(inplace=True)
    df.bfill(inplace=True)

    # Reset the index to access 'Date' as a column
    df = df.reset_index()

    # Return the last non-empty row with the relevant columns
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
        print(f"Error loading {symbol}.csv: {e}")
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
        print(f"{symbol}.csv is missing required columns.")
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
        print(f"No data available for {symbol} after preprocessing.")
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
            # print(f"Rule {rule_set_name} made a {decision} decision for {row['Symbol']}")
            return decision
        except Exception as e:
            print(f"Error applying trading rule {rule_set_name} for {row['Symbol']}: {e}")
            return "HOLD"

    # Use ThreadPoolExecutor to parallelize rule application
    with ThreadPoolExecutor() as executor:
        # Submit all rules to the executor and process them concurrently
        futures = {executor.submit(apply_rule, rule_set_name, rule_set_module): rule_set_name
                   for rule_set_name, rule_set_module in RULE_SETS.items()}

        # Collect results as they complete
        for future in as_completed(futures):
            decision = future.result()
            if decision in decisions:
                decisions[decision] += 1
            else:
                pass
                # print(f"Unknown decision {decision} encountered.")

    # Print decisions for each stock (for debugging)
    # print(f"Decisions for {row['Symbol']}: {decisions}")

    # Prioritize decisions: SELL > BUY > HOLD
    if decisions["SELL"] > 0:
        # print(f"Final decision for {row['Symbol']}: SELL")
        return "SELL"
    elif decisions["BUY"] > 0:
        # print(f"Final decision for {row['Symbol']}: BUY")
        return "BUY"
    else:
        # print(f"Final decision for {row['Symbol']}: HOLD")
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
        print(f"Error processing stock {row.get('Symbol', 'Unknown')}: {e}")
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
        holdings = pd.DataFrame(holdings)[["tradingsymbol", "instrument_token", "exchange", "average_price", "quantity"]]
        
        # Filter out holdings with quantity greater than 0
        holdings = holdings[holdings["quantity"] > 0]
        
        holdings.to_csv("intermediary_files/Holdings.csv", index=False)
        
        # print("Holdings Fetched and Saved!")
        return holdings

    except Exception as e:
        print(f"Error in fetching holdings: {e}")
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
        # print("Instruments Fetched and Saved!")
        return df

    except Exception as e:
        print(f"Error in fetching instruments: {e}")
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

def is_Market_Open(schedule):
    """
    Check if the NSE market is currently open.
    
    Args:
    schedule (pd.DataFrame): Market schedule for the day.
    
    Returns:
    bool: True if the market is open, False otherwise.
    """
    if schedule is None:
        print("Market is closed today.")
        return False
    
    now = datetime.now(ZoneInfo("Asia/Kolkata"))
    market_open = schedule.iloc[0]['market_open'].astimezone(ZoneInfo("Asia/Kolkata"))
    market_close = schedule.iloc[0]['market_close'].astimezone(ZoneInfo("Asia/Kolkata"))
    
    return market_open <= now <= market_close

def is_PreMarket_Open(schedule):
    """
    Check if the NSE premarket is currently open.
    
    Args:
    schedule (pd.DataFrame): Market schedule for the day.
    
    Returns:
    bool: True if the premarket is open, False otherwise.
    """
    if schedule is None:
        print("Market is closed today.")
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