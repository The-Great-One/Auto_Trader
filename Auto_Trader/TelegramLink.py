import asyncio
import logging
import os
import time
from telegram import Bot
import traceback
from Auto_Trader.my_secrets import TG_TOKEN, CHANNEL

if TG_TOKEN is not None and CHANNEL is not None:
    # Initialize the bot globally
    bot = Bot(token=TG_TOKEN)

    logger = logging.getLogger("Auto_Trade_Logger")
    TEST_CHANNEL = os.getenv("AT_TEST_TRADER_CHANNEL", "").strip()

    # Rate-limit: minimum seconds between messages to avoid Telegram flood control
    _MIN_MSG_INTERVAL = max(1, int(os.getenv("AT_TG_MIN_INTERVAL", "3")))
    _last_send_time = 0.0

    async def send_to_channel(message_queue):
        """Asynchronously sends messages from the queue to the Telegram channel."""
        global _last_send_time
        # Drain any stale messages that queued up before this process started
        drained = 0
        while not message_queue.empty():
            try:
                message_queue.get_nowait()
                drained += 1
            except Exception:
                break
        if drained:
            logger.info(f"TelegramLink: drained {drained} stale queued messages at startup")
        while True:
            message = message_queue.get()  # Wait for the next message from the queue
            if message == "STOP":
                break  # Exit if STOP is received
            # Skip if queue is backed up (>5 pending): keep only the latest
            skip_count = 0
            while message_queue.qsize() > 5:
                try:
                    message_queue.get_nowait()
                    skip_count += 1
                except Exception:
                    break
            if skip_count:
                logger.info(f"TelegramLink: skipped {skip_count} backed-up messages")
            try:
                chat_id = CHANNEL
                text = str(message)
                # Route paper-shadow alerts to test trader channel when configured
                if text.startswith("[PAPER]") and TEST_CHANNEL:
                    chat_id = TEST_CHANNEL
                # Rate-limit: enforce minimum interval between sends
                elapsed = time.monotonic() - _last_send_time
                if elapsed < _MIN_MSG_INTERVAL:
                    await asyncio.sleep(_MIN_MSG_INTERVAL - elapsed)
                await bot.send_message(chat_id=chat_id, text=text)
                _last_send_time = time.monotonic()
            except Exception as e:
                logger.error(
                    f"Error sending message: {e}, Traceback: {traceback.format_exc()}"
                )
                # On flood control, wait before retrying next message
                if "Flood control" in str(e) or "RetryAfter" in str(e) or "Retry in" in str(e):
                    import re
                    match = re.search(r'Retry in (\d+) seconds?', str(e))
                    wait = int(match.group(1)) + 1 if match else 30
                    logger.info(f"Flood control: waiting {wait}s before next send")
                    await asyncio.sleep(wait)
                    _last_send_time = time.monotonic()

    def telegram_main(message_queue):
        """Main function to handle the Telegram message sending process."""
        asyncio.run(send_to_channel(message_queue))

else:

    async def send_to_channel(message_queue):
        pass

    def telegram_main(message_queue):
        """Main function to handle the Telegram message sending process."""
        asyncio.run(send_to_channel(message_queue))