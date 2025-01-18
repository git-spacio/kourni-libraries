from decouple import Config, RepositoryEnv
import requests, json, os
dotenv_path = '/home/snparada/Spacionatural/Libraries/shopify_lib/creds/.env'
config = Config(RepositoryEnv(dotenv_path))

class ShopifyAPI:
    def __init__(self, shop_url=None, api_password=None, api_version=None):
        # Leer las variables de entorno utilizando decouple
        self.shop_url = shop_url if shop_url else config('SHOPIFY_SHOP_URL')
        self.api_password = api_password if api_password else config('SHOPIFY_PASSWORD')
        self.api_version = api_version if api_version else config('SHOPIFY_API_VERSION')
        self.base_url = f"{self.shop_url}/admin/api/{self.api_version}/"
        self.last_response = None

    def get_headers(self):
        return {
            'Content-Type': 'application/json',
            'X-Shopify-Access-Token': self.api_password
        }

    # Método para leer datos de la tienda Shopify via API
    def read(self, resource, params={}):
        url = f"{self.base_url}{resource}?"
        for key, value in params.items():
            url += f"{key}={value}&"
        response = requests.request("GET", url, headers=self.get_headers())
        self.last_response = response
        return json.loads(response.text)