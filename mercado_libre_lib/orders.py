import requests
import pandas as pd  # Se añade la importación de pandas
from mercado_libre_lib.api import MeliAPI
import time
import logging
from pprint import pprint
from tqdm import tqdm

class MeliOrders:
    def __init__(self):
        self.api = MeliAPI()
        self.base_url = self.api.base_url

    def read_order_by_id(self, order_id):
        """
        Lee la información de una orden específica por su ID.
        
        :param order_id: ID de la orden a consultar
        :return: Diccionario con la información de la orden
        """
        url = f"{self.base_url}/orders/{order_id}"
        headers = self.api.get_headers()
        
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            return response.json()
        else:
            response.raise_for_status()

    def read_products_order(self, order_id):
        """
        Lee la información de los productos de una orden específica.
        
        :param order_id: ID de la orden cuyos productos se quieren consultar
        :return: Diccionario con la información de los productos de la orden
        """
        url = f"{self.base_url}/orders/{order_id}/product"
        headers = self.api.get_headers()
        
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            return response.json()
        else:
            response.raise_for_status()

    def read_shipping_by_id(self, shipping_id):
        """
        Lee la información de envío por su ID.
        
        :param shipping_id: ID del envío a consultar
        :return: Diccionario con la información del envío
        """
        url = f"{self.base_url}/shipments/{shipping_id}"
        headers = self.api.get_headers()
        headers['x-format-new'] = 'true'
        
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            return response.json()
        else:
            response.raise_for_status()

    def read_all_orders(self, limit=50, offset=0, max_retries=3, delay=5):
        seller_id = self.api.get_seller_id()
        url = f"{self.base_url}/orders/search"
        headers = self.api.get_headers()
        params = {
            "seller": seller_id,
            "sort": "date_desc",
            "limit": limit,
            "offset": offset
        }

        todas_las_ordenes = []
        retry_count = 0
        
        # Obtener el total de órdenes para inicializar tqdm
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        total_ordenes = response.json().get('paging', {}).get('total', 0)
        
        # Inicializar tqdm
        with tqdm(total=total_ordenes, desc="Obteniendo órdenes") as pbar:
            while True:
                try:
                    response = requests.get(url, headers=headers, params=params)
                    response.raise_for_status()
                    data = response.json()
                    ordenes = data.get('results', [])
                    todas_las_ordenes.extend(ordenes)
                    
                    # Actualizar la barra de progreso
                    pbar.update(len(ordenes))
                    
                    if len(todas_las_ordenes) >= total_ordenes or not ordenes:
                        break
                    else:
                        params['offset'] += limit
                        retry_count = 0  # Reset retry count on successful request
                    time.sleep(delay/10)
                except requests.exceptions.RequestException as e:
                    retry_count += 1
                    if retry_count > max_retries:
                        logging.error(f"Error obteniendo órdenes después de {max_retries} intentos: {str(e)}")
                        break
                    logging.warning(f"Error obteniendo órdenes (intento {retry_count}): {str(e)}. Reintentando en {delay} segundos...")
                    time.sleep(delay)
        
        # Convertir la lista de órdenes a un DataFrame de pandas
        df_ordenes = pd.DataFrame(todas_las_ordenes)
        return df_ordenes
