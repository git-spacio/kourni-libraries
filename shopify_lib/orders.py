import pandas as pd
import sys
sys.path.append('/home/snparada/Spacionatural/Libraries/')
from shopify_lib.api import ShopifyAPI

class ShopifyOrders(ShopifyAPI):

    def __init__(self, shop_url=None, api_password=None, api_version="2024-01"):
        super().__init__(shop_url, api_password, api_version)

    # CRUD
    def read_last_order(self):
        data = self.read('orders.json', params={'limit': 1, 'order': 'created_at desc'})
        return data['orders'][0] if data['orders'] else None

    def read_all_orders_with_since_id(self, since_id=0, order_status=None):
        datas = []
        params = {}
        if order_status == None:
            params = {'limit': 250, 'since_id': since_id}
        else:
            params = {'limit': 250, 'since_id': since_id, 'status': order_status}
        data = self.read('orders.json', params=params)
        for order in data['orders']:
            since_id = order['id']
            datas.append(order)
        return since_id, datas

    def read_all_orders_by_date(self, start_date, end_date, order_status=None):
        """
        Retrieve all orders between two dates using REST API pagination.
        
        Args:
            start_date (str): Start date in ISO format (YYYY-MM-DD)
            end_date (str): End date in ISO format (YYYY-MM-DD)
            order_status (str, optional): Filter by order status
            
        Returns:
            list: List of orders within the date range
        """
        datas = []
        
        base_params = {
            'limit': 250,
            'created_at_min': start_date,
            'created_at_max': end_date,
            'status': order_status if order_status else 'any'
        }

        # Bucle de paginación
        while True:
            data = self.read('orders.json', params=base_params)
            orders = data.get('orders', [])
            datas.extend(orders)

            # Verificar si hay más páginas usando los headers de Link
            links = self.last_response.headers.get('Link', '')
            if 'rel="next"' not in links:
                break

            # Extraer el page_info del header Link para la siguiente página
            next_link = [link for link in links.split(', ') if 'rel="next"' in link][0]
            page_info = next_link.split('page_info=')[1].split('>')[0]
            base_params['page_info'] = page_info

        return datas