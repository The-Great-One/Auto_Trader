from re import sub
from kiteconnect import KiteTicker
from Auto_Trader.my_secrets import *
from Auto_Trader.utils import read_session_data
from queue import Queue
from Auto_Trader.TelegramLink import send_to_channel
import asyncio
import logging
import traceback

logger = logging.getLogger("Auto_Trade_Logger")

def run_ticker(sub_tokens, q):
    global queue
    queue = q
    kws = KiteTicker(api_key=API_KEY, access_token=read_session_data())
    
    def on_ticks(ws, ticks):
        addtoqueue(queue, ticks)  # Enqueue ticks for processing

    def on_connect(ws, response):
        if sub_tokens:
            logger.info("Starting Ticker")
            ws.subscribe(sub_tokens)
            ws.set_mode(ws.MODE_QUOTE, sub_tokens)
        else:
            logger.error("No subscription tokens provided.")

    def on_close(ws, code, reason):
        logger.warning(f"WebSocket closed with code: {code}, reason: {reason}")
        try:
            ws.stop()
        except Exception as e:
            logger.error(f"Error stopping WebSocket: {e}, Traceback: {traceback.format_exc()}")
        # Optionally reconnect
        logger.warning("Attempting to reconnect...")
        kws.connect()

    kws.on_ticks = on_ticks
    kws.on_connect = on_connect
    kws.on_close = on_close

    kws.connect()

def addtoqueue(q, ticks):
    q.put(ticks)