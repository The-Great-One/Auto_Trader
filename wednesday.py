import time
import sys
from multiprocessing import Queue, Process
from Auto_Trader import is_Market_Open, run_ticker, create_master, Apply_Rules, Updater
from Auto_Trader.TelegramLink import telegram_main

def monitor_market():
    processes = []
    q = Queue()  # Queue for Orders Placements
    message_queue = Queue()  # Queue for Telegram Messages

    def start_processes():
        """Starts all necessary processes."""
        print("Market is open. Starting processes.")
        message_queue.put("Market is open. Starting processes.")

        # Start the worker processes
        p1 = Process(target=run_ticker, args=(create_master(), q))
        p2 = Process(target=Apply_Rules, args=(q, message_queue))
        p3 = Process(target=Updater)
        p4 = Process(target=telegram_main, args=(message_queue,))

        p1.start()
        p2.start()
        p3.start()
        p4.start()

        return [p1, p2, p3, p4]

    def stop_processes(processes):
        """Stops all running processes."""
        print("Market is closed. Stopping processes.")
        message_queue.put("Market is closed. Stopping processes.")

        for p in processes:
            p.terminate()  # Gracefully terminate the process
            p.join()  # Ensure the process has finished
        return []

    while True:
        try:
            market_status = is_Market_Open()  # Check market status
            if market_status and not processes:
                # Start processes if market is open and none are running
                processes = start_processes()

            elif not market_status and processes:
                # Stop processes and exit the program when the market closes
                processes = stop_processes(processes)
                sys.exit(0)  # Exit the script cleanly; systemd will restart it

            time.sleep(60)  # Sleep for 60 seconds before checking again

        except Exception as e:
            print(f"Error occurred: {e}")
            message_queue.put(f"Error occurred: {e}")
            if processes:
                processes = stop_processes(processes)
            sys.exit(1)  # Exit with an error code to indicate failure

if __name__ == '__main__':
    try:
        monitor_market()
    except KeyboardInterrupt:
        print("Monitor stopped by user.")
        sys.exit(0)  # Exit cleanly if interrupted by the user
