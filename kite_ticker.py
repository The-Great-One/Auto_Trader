from urllib import response
from kiteconnect import KiteTicker
from my_secrets import *
from utils import read_session_data
from rt_compute import Apply_Rules
from queue import Queue
from threading import Thread

def run_ticker(sub_tokens):
    # Initialize KiteTicker
    kws = KiteTicker(api_key=API_KEY, access_token=read_session_data())
    
    # Create a Queue for passing ticks to the processing thread
    queue = Queue()

    def on_ticks(ws, ticks):
        queue.put(ticks)  # Enqueue ticks for processing

    def on_connect(ws, response):
        ws.subscribe(sub_tokens)
        ws.set_mode(ws.MODE_QUOTE, sub_tokens)

    def on_close(ws, code, reason):
        ws.stop()

    def process_ticks(queue):
        while True:
            ticks = queue.get()
            if ticks is None:  # Graceful shutdown
                break
            Apply_Rules(ticks)  # Process each tick

    # Start a separate thread to handle tick processing
    processor_thread = Thread(target=process_ticks, args=(queue,))
    processor_thread.daemon = True  # Allows the thread to exit when the main program exits
    processor_thread.start()

    kws.on_ticks = on_ticks
    kws.on_connect = on_connect
    kws.on_close = on_close

    try:
        kws.connect()
    finally:
        queue.put(None)  # Signal the processing thread to stop
        processor_thread.join()  # Ensure the thread finishes before exiting
