from datetime import datetime
import os
import telebot

from datetime import datetime, timedelta
import time 

from dotenv import load_dotenv
load_dotenv()

class TelegramMonitoring:
    def __init__(self):
        # Create the bot instance
        self.bot = telebot.TeleBot(token=os.getenv('TELEGRAM_TOKEN'))

    def duration_formatter(self, total_seconds):
        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)

        return f"{hours} h {minutes} min {seconds} sec"

    def send_telegram_message(self, message_text):
        '''Telegram send monitoring progress message'''    
        try:
            self.bot.send_message(os.getenv('TELEGRAM_CHAT_ID'), message_text)
            # print("Message sent successfully!")
        except Exception as e:
            print(f"An error occurred: {e}")