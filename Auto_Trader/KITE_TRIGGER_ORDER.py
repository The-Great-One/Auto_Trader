from kiteconnect import KiteConnect
from Auto_Trader.my_secrets import API_KEY
from Auto_Trader.utils import read_session_data
from math import floor
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from kiteconnect.exceptions import (
    GeneralException,
    TokenException,
    PermissionException,
    OrderException,
    InputException,
    DataException,
    NetworkException,
)
from Auto_Trader.TelegramLink import send_to_channel
import asyncio

# Initialize KiteConnect
kite = KiteConnect(api_key=API_KEY)
kite.set_access_token(read_session_data())

def trigger(symbol, exchange, trans_quantity, order_type, close_price=None):
    """
    Places a market order for the specified symbol and sends a notification.

    Args:
        symbol (str): The trading symbol of the stock.
        exchange (str): The exchange on which the stock is listed ("NSE" or "BSE").
        trans_quantity (int): The number of shares to trade.
        order_type (str): "BUY" or "SELL" to indicate the type of order.
        close_price (float, optional): The closing price of the stock (for buy orders).

    Returns:
        None
    """
    print(f"Triggering {'BUY' if order_type == 'BUY' else 'SELL'} order for {symbol} on {exchange} with quantity {trans_quantity}.")
    exchange = "NSE" if exchange == "NSE" else "BSE"
    transaction_type = kite.TRANSACTION_TYPE_BUY if order_type == "BUY" else kite.TRANSACTION_TYPE_SELL

    try:
        order_id = kite.place_order(
            variety=kite.VARIETY_REGULAR,
            tradingsymbol=symbol,
            exchange=kite.EXCHANGE_NSE,
            transaction_type=transaction_type,
            quantity=trans_quantity,
            order_type=kite.ORDER_TYPE_MARKET,
            product=kite.PRODUCT_CNC,
            validity=kite.VALIDITY_DAY
        )

        # Send message with order details
        message = f"""
        Symbol: {symbol}
        Quantity: {trans_quantity}
        Price: {close_price if close_price else 'N/A'}
        Type: {'BUY' if order_type == 'BUY' else 'SELL'}
        """
        asyncio.run(send_to_channel(message))

        if transaction_type == kite.TRANSACTION_TYPE_BUY:
            print(f"Bought: {symbol} (Order ID: {order_id})")
        else:
            print(f"Sold: {symbol} (Order ID: {order_id})")

    except NetworkException as ne:
        print(f"Network error while placing order for {symbol}: {ne}")
    except TokenException as te:
        print(f"Authentication error while placing order for {symbol}: {te}")
    except OrderException as oe:
        print(f"Order placement failed for {symbol}: {oe}")
    except PermissionException as pe:
        print(f"Permission error for {symbol}: {pe}")
    except InputException as ie:
        print(f"Input error for {symbol}: {ie}")
    except DataException as de:
        print(f"Data error for {symbol}: {de}")
    except GeneralException as ge:
        print(f"General error for {symbol}: {ge}")
    except Exception as e:
        print(f"Unexpected error while placing order for {symbol}: {e}")


def get_positions():
    """
    Retrieves the current positions in the portfolio.

    Returns:
        dict: A dictionary with tradingsymbols as keys and the corresponding position quantity as values.
    """
    try:
        positions = pd.DataFrame(kite.positions())
        net_positions = positions['net']
        position_dict = {pos['tradingsymbol']: pos['quantity'] for pos in net_positions if pos['quantity'] != 0}
        return position_dict
    except Exception as e:
        print(f"Error retrieving positions: {e}")
        return {}

def get_holdings():
    """
    Retrieves the current positions in the portfolio.

    Returns:
        dict: A dictionary with tradingsymbols as keys and the corresponding position quantity as values.
    """
    try:
        holdings = kite.holdings()
        holdings_dict = {pos['tradingsymbol']: pos['quantity'] for pos in holdings if pos['quantity'] != 0}
        return holdings_dict
    except Exception as e:
        print(f"Error retrieving positions: {e}")
        return {}
    
def is_symbol_in_order_book(symbol):
    """
    Checks if the given symbol is already present in the current order book.

    Args:
        symbol (str): The trading symbol to check in the order book.

    Returns:
        bool: True if the symbol is present in the order book, False otherwise.
    """
    try:
        orders = kite.orders()
        for order in orders:
            if order['tradingsymbol'] == symbol and order['status'] in ['TRIGGER_PENDING', 'OPEN']:
                return True
        return False
    except Exception as e:
        print(f"Error checking order book for {symbol}: {e}")
        return False


def should_place_buy_order(symbol):
    """
    Determines whether a buy order should be placed by checking existing positions and the order book.

    Args:
        symbol (str): The trading symbol of the stock.

    Returns:
        bool: True if a buy order should be placed, False otherwise.
    """
    positions = get_positions()
    holdings = get_holdings()
    # Check if the symbol already has a position
    if symbol in positions:
        return False
    
    if symbol in holdings:
        return False
    
    # Check if the symbol is already in the order book
    if is_symbol_in_order_book(symbol):
        return False
    
    return True

def handle_decisions(decisions):
    """
    Processes a list of trading decisions, executing sell orders first to free up funds,
    and then executing buy orders if sufficient funds are available and the symbol is not
    already in the order book or in positions.

    Args:
        decisions (list of dict): A list of trading decisions where each decision contains
                                  the following keys: "Symbol", "Exchange", "Close", "Decision".

    Returns:
        None
    """
    holdings = pd.DataFrame(kite.holdings()).set_index("tradingsymbol")
    
    symbols_held = list(holdings.index)

    # Separate sell and buy decisions
    sell_decisions = [
        decision for decision in decisions if decision["Decision"] == "SELL" and decision["Symbol"] in symbols_held
    ]
    buy_decisions = [
        decision for decision in decisions if decision["Decision"] == "BUY" and decision["Symbol"] not in symbols_held
    ]

    # Use ThreadPoolExecutor for parallel processing
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []

        # First execute sell orders
        for decision in sell_decisions:
            symbol = decision["Symbol"]
            exchange = decision["Exchange"]
            close_price = decision["Close"]
            
            # Safely access the quantity for the symbol
            quantity = holdings.loc[symbol]["quantity"] if symbol in holdings.index else 0
            
            if quantity == 0:
                continue

            futures.append(executor.submit(trigger, symbol, exchange, quantity, "SELL", close_price))

        # Ensure all sell orders are completed before proceeding
        for future in as_completed(futures):
            try:
                future.result()  # Handle any exceptions that might have occurred
            except Exception as e:
                print(f"Error in executing sell order: {e}")

        # Clear the futures list for buy orders
        futures.clear()

        # Then execute buy orders
        for decision in buy_decisions:
            symbol = decision["Symbol"]
            exchange = decision["Exchange"]
            close_price = decision["Close"]

            try:
                funds = kite.margins("equity")["available"]["live_balance"]
                print(f"Available funds: {funds}")
                if funds <= 11000:
                    print("Insufficient funds to place more buy orders. Stopping buy order processing.")
                    break

                # Check if a buy order should be placed
                if not should_place_buy_order(symbol):
                    continue

                quantity = floor(11000 / close_price)
                if quantity <= 0:
                    print(f"Calculated quantity {quantity} for {symbol} is not positive. Skipping buy order.")
                    continue

                futures.append(executor.submit(trigger, symbol, exchange, quantity, "BUY", close_price))

            except NetworkException as ne:
                print(f"Network error while retrieving funds: {ne}")
            except TokenException as te:
                print(f"Authentication error while retrieving funds: {te}")
            except PermissionException as pe:
                print(f"Permission error while retrieving funds: {pe}")
            except DataException as de:
                print(f"Data error while retrieving funds: {de}")
            except GeneralException as ge:
                print(f"General error while retrieving funds: {ge}")
            except Exception as e:
                print(f"Unexpected error while processing buy decision for {symbol}: {e}")

        # Handle the completion of buy orders
        for future in as_completed(futures):
            try:
                future.result()  # Handle any exceptions that might have occurred
            except Exception as e:
                print(f"Error in executing buy order: {e}")

        # Rate limiting: Ensure we don't exceed the API limits
        # print("Sleeping for 0.1 seconds to respect rate limits.")
        time.sleep(0.1)

    # print("Finished handling trading decisions.")