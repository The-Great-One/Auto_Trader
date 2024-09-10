# TelegramLink.py
import asyncio
from telegram import Bot
from Auto_Trader.my_secrets import TG_TOKEN, CHANNEL

# Initialize the bot
bot = Bot(token=TG_TOKEN)

async def send_to_channel(message: str) -> None:
    try:
        channel_id = CHANNEL
        
        # Send the message
        await bot.send_message(chat_id=channel_id, text=message)
    except Exception as e:
        print(f"Error sending message: {e}")
