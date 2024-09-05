from urllib import response
from kiteconnect import KiteTicker
from Auto_Trader.my_secrets import *
from Auto_Trader.utils import read_session_data
from queue import Queue


def run_ticker(sub_tokens, q):
    
    global queue
    queue = q
    # Initialize KiteTicker
    kws = KiteTicker(api_key=API_KEY, access_token=read_session_data())
    

    def on_ticks(ws, ticks):
        addtoqueue(ticks)  # Enqueue ticks for processing

    def on_connect(ws, response):
        ws.subscribe(sub_tokens)
        ws.set_mode(ws.MODE_QUOTE, sub_tokens)

    def on_close(ws, code, reason):
        ws.stop()


    kws.on_ticks = on_ticks
    kws.on_connect = on_connect
    kws.on_close = on_close

    kws.connect()


    
def addtoqueue(ticks):
    queue.put(ticks)