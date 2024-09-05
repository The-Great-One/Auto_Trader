import time
from datetime import datetime, timedelta
from multiprocessing import Process, Queue
from Auto_Trader import *

def monitor_market():
    processes = []
    q = Queue()
    last_check_time = datetime.now() - timedelta(minutes=10)  # Initial check time in the past
    market_status_cache = False  # Cache for market status

    while True:
        current_time = datetime.now()
        
        # Only call is_Market_Open() every 10 minutes or when the cache is outdated
        if (current_time - last_check_time).total_seconds() > 600:  # 600 seconds = 10 minutes
            market_status_cache = is_Market_Open()
            last_check_time = current_time  # Update the last check time

        if market_status_cache:
            if not processes:  # Start processes if the market is open and no processes are running
                print("Market is open. Starting processes.")
                
                p1 = Process(target=run_ticker, args=(create_master(), q))
                p2 = Process(target=Apply_Rules, args=(q,))
                
                p1.start()
                p2.start()
                
                processes = [p1, p2]
        else:
            if processes:  # Stop processes if they are running when the market closes
                print("Market is closed. Stopping processes.")
                
                for p in processes:
                    p.terminate()
                    p.join()  # Wait for processes to terminate
                
                processes = []

        # Adjust sleep time based on market status
        sleep_time = 60 if market_status_cache else 600  # Check every minute when market is open, every 10 minutes otherwise
        time.sleep(sleep_time)

if __name__ == '__main__':
    monitor_market()