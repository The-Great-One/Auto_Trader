import asyncio
import logging
import os
from telegram import Bot
import traceback
from Auto_Trader.my_secrets import TG_TOKEN, CHANNEL

if TG_TOKEN is not None and CHANNEL is not None:
    # Initialize the bot globally
    bot = Bot(token=TG_TOKEN)

    logger = logging.getLogger("Auto_Trade_Logger")
    TEST_CHANNEL = os.getenv("AT_TEST_TRADER_CHANNEL", "").strip()

    async def send_to_channel(message_queue):
        """Asynchronously sends messages from the queue to the Telegram channel."""
        while True:
            message = message_queue.get()  # Wait for the next message from the queue
            if message == "STOP":
                break  # Exit if STOP is received
            try:
                chat_id = CHANNEL
                text = str(message)
                # Route paper-shadow alerts to test trader channel when configured
                if text.startswith("[PAPER]") and TEST_CHANNEL:
                    chat_id = TEST_CHANNEL
                await bot.send_message(chat_id=chat_id, text=text)
            except Exception as e:
                logger.error(
                    f"Error sending message: {e}, Traceback: {traceback.format_exc()}"
                )

    def telegram_main(message_queue):
        """Main function to handle the Telegram message sending process."""
        asyncio.run(send_to_channel(message_queue))

else:

    async def send_to_channel(message_queue):
        pass

    def telegram_main(message_queue):
        """Main function to handle the Telegram message sending process."""
        asyncio.run(send_to_channel(message_queue))
