import asyncio
import logging
from telegram import Bot
from Auto_Trader.my_secrets import TG_TOKEN, CHANNEL

if TG_TOKEN is not None and CHANNEL is not None:
    # Initialize the bot globally
    bot = Bot(token=TG_TOKEN)

    logger = logging.getLogger("Auto_Trade_Logger")

    async def send_to_channel(message_queue):
        """Asynchronously sends messages from the queue to the Telegram channel."""
        while True:
            message = message_queue.get()  # Wait for the next message from the queue
            if message == "STOP":
                break  # Exit if STOP is received
            try:
                await bot.send_message(chat_id=CHANNEL, text=message)
            except Exception as e:
                logger.error(f"Error sending message: {e}")

    def telegram_main(message_queue):
        """Main function to handle the Telegram message sending process."""
        asyncio.run(send_to_channel(message_queue))

else:
    async def send_to_channel(message_queue):
        pass
    
    def telegram_main(message_queue):
        """Main function to handle the Telegram message sending process."""
        asyncio.run(send_to_channel(message_queue))