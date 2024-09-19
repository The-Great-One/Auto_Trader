# queue_manager.py
from multiprocessing import Queue

# Create the global queue for sending messages to Telegram
message_queue = Queue()

def addtoqueue(q, message):
    """Function to add messages to the message queue."""
    q.put(message)