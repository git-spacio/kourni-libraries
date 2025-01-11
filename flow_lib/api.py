import hmac
import hashlib
from decouple import config
import requests
from typing import Dict


class FlowAPI:
    def __init__(self, sandbox: bool = False):
        """
        Initialize Flow API connection
        Args:
            sandbox (bool): If True, uses sandbox environment. Default is False
        """
        self.base_url = "https://sandbox.flow.cl/api" if sandbox else "https://www.flow.cl/api"
        self.api_key = config('API_KEY')
        self.secret_key = config('SECRET_KEY')

    def _sign_params(self, params: Dict) -> str:
        """
        Sign parameters using HMAC SHA256
        Args:
            params (Dict): Parameters to sign
        Returns:
            str: Signature hash
        """
        # Sort parameters alphabetically
        sorted_params = dict(sorted(params.items()))
        
        # Concatenate parameters
        params_string = ''.join(f"{key}{value}" for key, value in sorted_params.items())
        
        # Create signature using HMAC SHA256
        signature = hmac.new(
            self.secret_key.encode(),
            params_string.encode(),
            hashlib.sha256
        ).hexdigest()
        
        return signature

    def _make_request(self, endpoint: str, params: Dict, method: str = 'GET') -> Dict:
        """
        Make HTTP request to Flow API
        Args:
            endpoint (str): API endpoint
            params (Dict): Request parameters
            method (str): HTTP method (GET or POST)
        Returns:
            Dict: API response
        """
        # Add API key to parameters
        params['apiKey'] = self.api_key
        
        # Sign parameters
        params['s'] = self._sign_params(params)
        
        # Build full URL
        url = f"{self.base_url}/{endpoint}"
        
        # Make request
        if method.upper() == 'GET':
            response = requests.get(url, params=params)
        else:
            response = requests.post(url, data=params)
        
        # Raise exception for error status codes
        response.raise_for_status()
        
        return response.json()
