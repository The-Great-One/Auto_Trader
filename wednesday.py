import time
from datetime import datetime
from Auto_Trader import is_Market_Open, run_ticker, create_master, Apply_Rules, Updater, Queue, Process
from Auto_Trader.TelegramLink import telegram_main

def monitor_market():
    processes = []
    q = Queue()  # Queue for Orders Placements
    message_queue = Queue() # Queue for Telegram Messages

    while True:
        # Check the market status every 60 seconds
        market_status = is_Market_Open()  # Directly check market status without caching
        
        # If market is open, start processes if they are not already running
        if market_status and not processes:
            print("Market is open. Starting processes.")
            message_queue.put("Market is open. Starting processes.")
            # Start the worker processes
            p1 = Process(target=run_ticker, args=(create_master(), q))
            p2 = Process(target=Apply_Rules, args=(q, message_queue,))
            p3 = Process(target=Updater)
            p4 = Process(target=telegram_main, args=(message_queue,))

            p1.start()
            p2.start()
            p3.start()
            p4.start()

            processes = [p1, p2, p3, p4]
        
        # If the market is closed and processes are running, terminate them
        elif not market_status and processes:
            print("Market is closed. Stopping processes.")

            # Stop and join processes
            for p in processes:
                p.terminate()
                p.join()

            processes = []

        time.sleep(60)  # Sleep for 60 seconds between market status checks

if __name__ == '__main__':
    try:
        monitor_market()
    except KeyboardInterrupt:
        print("Monitor stopped by user.")