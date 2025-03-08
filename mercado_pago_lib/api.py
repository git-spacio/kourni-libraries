import mercadopago
import pandas as pd
from decouple import Config, RepositoryEnv

class MercadoPagoAPI:
    def __init__(self, environment='productive'):
        base_path = '/home/snparada/Spacionatural/Libraries/mercado_pago_lib/creds/'
        env_file = '.env' if environment == 'productive' else '.env.test'
        env_path = base_path + env_file
        
        config = Config(RepositoryEnv(env_path))
        
        self.access_token = config('MP_ACCESS_TOKEN')
        self.sdk = self._initialize_sdk()
        
    def _initialize_sdk(self):
        """Initialize the Mercado Pago SDK with the access token."""
        sdk = mercadopago.SDK(self.access_token)
        return sdk
    