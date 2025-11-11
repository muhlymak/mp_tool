import re
import httpx
from dotenv import load_dotenv
import os

load_dotenv()

class LmApiClient:
    def __init__(self, client_id: str = os.getenv("lm_client_id"), secret_id: str = os.getenv("lm_client_secret")):
        self.client_id = client_id
        self.secret_id = secret_id
        self.token = self.get_token(client_id=self.client_id, secret_id=self.secret_id)
        self.headers = {
            "Authorization": f"Bearer {self.token}"
        }

    def get_token(self, client_id, secret_id):
        params = {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": secret_id
        }
        with httpx.Client() as client:
            response = client.get(
                "https://api-b2b.lamoda.ru/auth/token",
                params=params
            )
            if response.status_code == 200:
                token = response.json()["access_token"]
                return token
            else:
                raise Exception(f"Error: {response.status_code} - {response.text}")
        
    def show_token(self):
        print(self.token)

    def post_reprice(self, data):
        with httpx.Client() as client:
            response = client.post(
                "https://api-b2b.lamoda.ru/api/v1/nomenclature/country/ru/prices",
                headers=self.headers, 
                json=data
            )
            return response
        
