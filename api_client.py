import requests
import logging

class APIClient:
    def __init__(self, base_url, api_key=None):
        self.base_url = base_url
        self.api_key = api_key
        self.headers = {'Authorization': f'Bearer {self.api_key}'} if api_key else {}

    def fetch_data(self):
        """Fetches entire dataset from base URL"""
        try:
            response = requests.get(self.base_url, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logging.error(f"API Error: {e}")
            return {}