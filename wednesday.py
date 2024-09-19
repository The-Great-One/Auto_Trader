# monitor_market.py
import time
from datetime import datetime, timedelta
from multiprocessing import Process, Queue
from Auto_Trader import is_Market_Open, run_ticker, create_master, Apply_Rules, Updater
from Auto_Trader.TelegramLink import telegram_main
from Auto_Trader.queue_manager import addtoqueue

def monitor_market():
    processes = []
    q = Queue()  # Queue for general market-related processes
    message_queue = Queue()  # Queue for Telegram messages
    last_check_time = datetime.now() - timedelta(minutes=1)
    market_status_cache = False

    # Start the Telegram process (p4) and pass the message_queue to it
    p4 = Process(target=telegram_main, args=(message_queue,))
    p4.start()

    while True:
        current_time = datetime.now()

        # Call is_Market_Open() every 10 minutes
        if (current_time - last_check_time).total_seconds() > 60:
            market_status_cache = is_Market_Open()
            last_check_time = current_time

        if market_status_cache:
            if not processes:  # Start processes if the market is open and no processes are running
                print("Market is open. Starting processes.")

                # Start the worker processes
                p1 = Process(target=run_ticker, args=(create_master(), q))
                p2 = Process(target=Apply_Rules, args=(q,))
                p3 = Process(target=Updater)

                p1.start()
                p2.start()
                p3.start()

                processes = [p1, p2, p3, p4]
        else:
            if processes:  # Stop processes if they are running when the market closes
                print("Market is closed. Stopping processes.")

                # Add a message to the Telegram queue
                addtoqueue(message_queue, "Market is closed. Stopping processes...")

                # Stop and join processes
                for p in processes:
                    p.terminate()
                    p.join()

                processes = []

        time.sleep(60)  # Sleep for 60 seconds

if __name__ == '__main__':
    monitor_market()