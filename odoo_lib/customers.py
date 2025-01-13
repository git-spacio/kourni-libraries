from datetime import datetime
from .api import OdooAPI
import pandas as pd

class OdooCustomers(OdooAPI):
    
    def __init__(self, database='productive'):
        super().__init__(database=database)
    
    def read_customer_by_id(self, id):
        """
        Lee un cliente específico por su ID
        
        :param id: ID del cliente
        :return: DataFrame con los datos del cliente o mensaje de error
        """
        try:
            fields = [
                'name',
                'vat',  # RUT o identificación fiscal
                'email',
                'phone',
                'mobile',
                'street',
                'city',
                'state_id',
                'country_id',
                'company_type',  # persona o empresa
                'l10n_latam_identification_type_id',  # tipo de documento
                'create_date',
                'write_date',
                'customer_rank',  # si es cliente
                'supplier_rank',  # si es proveedor
            ]
            
            customer = self.models.execute_kw(
                self.db, self.uid, self.password,
                'res.partner', 'read',
                [id],
                {'fields': fields}
            )
            
            if customer:
                df = pd.DataFrame([customer[0]])
                return df
            else:
                return "Cliente no encontrado"
            
        except Exception as e:
            return f"Error al leer el cliente: {str(e)}"
    
    def read_all_customers_in_df(self, domain=None, limit=None):
        """
        Lee todos los clientes que coincidan con el dominio especificado
        
        :param domain: Lista de condiciones para filtrar los clientes (opcional)
        :param limit: Número máximo de registros a retornar (opcional)
        :return: DataFrame con los clientes o mensaje de error
        """
        try:
            fields = [
                'id',            # Añadimos el ID del cliente
                'name',
                'vat',
                'email',
                'phone',
                'mobile',
                'street',
                'city',
                'state_id',
                'country_id',
                'company_type',
                'l10n_latam_identification_type_id',
                'create_date',
                'write_date',
                'customer_rank',
                'supplier_rank',
            ]
            
            # Si no se especifica un dominio, usar lista vacía
            domain = domain or [('customer_rank', '>', 0)]  # Por defecto, solo clientes
            
            # Buscar IDs de clientes
            customer_ids = self.models.execute_kw(
                self.db, self.uid, self.password,
                'res.partner', 'search',
                [domain],
                {'limit': limit} if limit else {}
            )
            
            if not customer_ids:
                return "No se encontraron clientes"
            
            # Leer datos de los clientes
            customers = self.models.execute_kw(
                self.db, self.uid, self.password,
                'res.partner', 'read',
                [customer_ids],
                {'fields': fields}
            )
            
            # Convertir a DataFrame
            df = pd.DataFrame(customers)
            return df
            
        except Exception as e:
            return f"Error al leer los clientes: {str(e)}"
    
    def search_customer_by_vat(self, vat):
        """
        Busca un cliente por su RUT/VAT
        
        :param vat: RUT o identificación fiscal del cliente
        :return: DataFrame con los datos del cliente o mensaje de error
        """
        try:
            domain = [
                ('vat', '=', vat),
                ('customer_rank', '>', 0)
            ]
            
            customer_ids = self.models.execute_kw(
                self.db, self.uid, self.password,
                'res.partner', 'search',
                [domain]
            )
            
            if customer_ids:
                return self.read_customer_by_id(customer_ids[0])
            else:
                return "Cliente no encontrado"
            
        except Exception as e:
            return f"Error al buscar el cliente: {str(e)}"
