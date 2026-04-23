from kiteconnect import KiteTicker
from Auto_Trader.my_secrets import API_KEY
from Auto_Trader.utils import read_session_data
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
            from twisted.internet import reactor
            if reactor.running:
                ws.stop()
        except Exception as e:
            logger.error(
                f"Error stopping WebSocket: {e}, Traceback: {traceback.format_exc()}"
            )
        # Reconnect only for abnormal close; skip if market hours ended (code 1000 = normal)
        if code != 1000:
            logger.warning("Attempting to reconnect...")
            try:
                kws.connect()
            except Exception as e:
                logger.error(f"Reconnect failed: {e}")

    kws.on_ticks = on_ticks
    kws.on_connect = on_connect
    kws.on_close = on_close

    kws.connect()


def addtoqueue(q, ticks):
    q.put(ticks)
