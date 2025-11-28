import httpx
from dotenv import load_dotenv
import os

load_dotenv()

class WbApiClient:
    def __init__(self, token: str = os.getenv("wb_api_token")):
        self.token = token
        self.headers = {
            "Authorization": f"Bearer {self.token}",
        }

    def post_reprice(self, data):
        with httpx.Client() as client:
            response = client.post(
                "https://discounts-prices-api.wildberries.ru/api/v2/upload/task",
                headers=self.headers, 
                json=data
            )
            return response
        
    def post_connect_cards(self, data):
        with httpx.Client() as client:
            response = client.post(
                "https://content-api.wildberries.ru/content/v2/cards/moveNm",
                headers=self.headers, 
                json=data
            )
            return response