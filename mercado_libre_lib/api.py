import requests
from urllib.parse import urlencode
import webbrowser
from decouple import Config, RepositoryEnv
import logging

logging.basicConfig(level=logging.INFO)

class MeliAPI:
    def __init__(self):
        # Cargar las variables de entorno usando python-decouple
        dotenv_path = '/home/snparada/Spacionatural/Libraries/mercado_libre_lib/creds/.env'
        config = Config(RepositoryEnv(dotenv_path))
        
        # Obtener las credenciales
        self.app_id = config('APP_ID')
        self.client_secret = config('CLIENT_SECRET')
        self.redirect_uri = config('REDIRECT_URI')
        
        # URL base para las API de Mercado Libre
        self.base_url = 'https://api.mercadolibre.com'
        
        # Obtener el token de acceso
        self.access_token = self._get_access_token()

    def get_seller_id(self):
        """
        Obtiene el seller_id del usuario autenticado.
        """
        user_info = self.get_user_info()
        return user_info.get('id')

    def get_user_info(self):
        """
        Obtiene la información del usuario autenticado.
        """
        url = f"{self.base_url}/users/me"
        headers = self.get_headers()
        
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            return response.json()
        else:
            logging.error(f"Error obtaining user info: {response.text}")
            raise Exception(f"Error al obtener la información del usuario: {response.text}")

    def _get_access_token(self):
        auth_url = "https://auth.mercadolibre.cl/authorization"
        params = {
            "response_type": "code",
            "client_id": self.app_id,
            "redirect_uri": self.redirect_uri
        }
        auth_url_with_params = f"{auth_url}?{urlencode(params)}"
        
        print("Por favor, visita la siguiente URL y autoriza la aplicación:")
        print(auth_url_with_params)
        webbrowser.open(auth_url_with_params)
        
        auth_code = input("Ingresa el código de autorización recibido: ")
        
        token_url = "https://api.mercadolibre.com/oauth/token"
        data = {
            "grant_type": "authorization_code",
            "client_id": self.app_id,
            "client_secret": self.client_secret,
            "code": auth_code,
            "redirect_uri": self.redirect_uri
        }
        
        response = requests.post(token_url, data=data)
        if response.status_code == 200:
            access_token = response.json()["access_token"]
            logging.info("Access token obtained successfully")
            return access_token
        else:
            logging.error(f"Error obtaining access token: {response.text}")
            raise Exception(f"Error al obtener el token de acceso: {response.text}")

    def get_headers(self):
        return {'Authorization': f'Bearer {self.access_token}'}
