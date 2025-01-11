import xmlrpc.client as xc
import pandas as pd
from decouple import Config, RepositoryEnv

class OdooAPI:
    def __init__(self):
        env_path = '/home/snparada/Spacionatural/Libraries/odoo_lib/.env'
        config = Config(RepositoryEnv(env_path))
        
        self.url = config('ODOO_URL')
        self.db = config('ODOO_DB')
        self.username = config('ODOO_USERNAME')
        self.password = config('ODOO_PASSWORD')
        self.uid = self._authenticate()
        self.models = self._create_model()

    def _authenticate(self):
        common = xc.ServerProxy(f'{self.url}/xmlrpc/2/common')
        uid = common.authenticate(self.db, self.username, self.password, {})
        return uid

    def _create_model(self):
        return xc.ServerProxy(f'{self.url}/xmlrpc/2/object')

    def get_fields(self, table):
        fields = self.models.execute_kw(self.db, self.uid, self.password, table, 'fields_get', [])
        df_fields = pd.DataFrame.from_dict(fields, orient='index')
        return df_fields