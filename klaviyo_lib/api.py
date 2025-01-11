import os
import requests
import json
from urllib.parse import urlparse

class KlaviyoConnection:
    def __init__(self):
        self.base_url = "https://a.klaviyo.com/api/"
        self.creds_file = "/home/snparada/Spacionatural/Libraries/klaviyo_lib/creds/keys.json"
        self.api_key = self.load_api_key()

    def load_api_key(self):
        with open(self.creds_file) as f:
            creds = json.load(f)
            return creds["key"]

    def get_headers(self):
        return {
            'Content-Type': 'application/json',
            'revision': '2024-02-15',
            'Authorization': f'Klaviyo-API-Key {self.api_key}'
        }

    def make_request(self, endpoint, method='GET', data=None, params=None):
        url = f"{self.base_url}{endpoint}"
        headers = self.get_headers()
        if method == 'GET':
            response = requests.get(url, headers=headers, params=params)
        elif method == 'POST' or method == 'PATCH':
            response = requests.request(method, url, headers=headers, json=data)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")
        response.raise_for_status()
        return response.json()

    def extract_endpoint_and_params(self, url):
        parsed_url = urlparse(url)
        
        try:
            endpoint = parsed_url.path.removeprefix('/api/')
        except TypeError:
            path = parsed_url.path.decode('utf-8')
            endpoint = path.removeprefix('/api/')
        
        query_params = parsed_url.query

        result = f"{endpoint}?{query_params}"
        return result