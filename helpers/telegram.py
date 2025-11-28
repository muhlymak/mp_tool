import httpx
from loguru import logger
from dotenv import load_dotenv
import os


load_dotenv()
bot_token = os.getenv('telegram_token')
telegram_chat_id = os.getenv('telegram_chat_id')

def send_telegram_message(message: str, bot_token: str = bot_token, chat_id: str = telegram_chat_id, ):
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        # можно также указать "parse_mode": "HTML" или "Markdown"
    }
    resp = httpx.post(url, json=payload)
    try:
        resp.raise_for_status()
    except Exception as e:
        logger.error(f"Ошибка при отправке сообщения в Telegram: {e}, response: {resp.text}")
    else:
        logger.info(f"Отправлено сообщение в Telegram: {message}")
