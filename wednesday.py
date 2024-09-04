import time
import threading
from kite_ticker import run_ticker
from utils import is_Market_Open
from Build_Master import create_master

class MarketMonitor(threading.Thread):
    def __init__(self):
        super().__init__()
        self.ticker_thread = None
        self.ticker_running = False
        self.stop_thread = False

    def run(self):
        while not self.stop_thread:
            if is_Market_Open():
                if not self.ticker_running:
                    print("Market is open. Starting ticker...")
                    self.ticker_thread = threading.Thread(target=self.start_ticker)
                    self.ticker_running = True
                    self.ticker_thread.start()
            else:
                if self.ticker_running:
                    print("Market is closed. Stopping ticker...")
                    self.ticker_running = False
                    self.force_stop_ticker()
            time.sleep(60)  # Check the market status every 60 seconds

    def start_ticker(self):
        instruments = create_master()
        run_ticker(instruments)

    def force_stop_ticker(self):
        if self.ticker_thread:
            # Simply set the flag to stop the ticker thread
            self.stop_thread = True
            self.ticker_thread.join()  # Wait for the ticker thread to finish

    def stop(self):
        self.stop_thread = True
        if self.ticker_running and self.ticker_thread:
            self.ticker_thread.join()

def main():
    market_monitor = MarketMonitor()
    market_monitor.start()

    try:
        while True:
            time.sleep(1)  # Keep the main thread alive
    except KeyboardInterrupt:
        print("Stopping market monitor...")
        market_monitor.stop()
        market_monitor.join()

if __name__ == '__main__':
    main()
