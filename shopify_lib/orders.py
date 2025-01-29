import pandas as pd
import sys
import json
import time
from requests.exceptions import HTTPError
sys.path.append('/home/snparada/Spacionatural/Libraries/')
from shopify_lib.api import ShopifyAPI
from shopify_lib.customers import ShopifyCustomers
from pprint import pprint

class ShopifyOrders(ShopifyAPI):

    def __init__(self, shop_url=None, api_password=None, api_version=None):
        super().__init__(shop_url, api_password, api_version)
        self.customers = ShopifyCustomers()
        

#--------------------CRUD FUNCTIONS --------------------
    def create_new_draft_order(self, products, email):
        """
        Create a new draft order for a customer.
        
        Args:
            products (list): List of dictionaries containing product information
                Each product should have: {'variant_id': id, 'quantity': qty}
            email (str): Customer email
        
        Returns:
            tuple: (draft_order_id, payment_url) or (None, None) if error
        """
        # Prepare draft order data
        draft_order_data = {
            'draft_order': {
                'email': email,
                'line_items': products,
                'send_email_invite': False
            }
        }
        
        try:
            # Create the draft order
            response = self.post('draft_orders.json', json=draft_order_data)
            draft_order = response['draft_order']
            
            return draft_order['id'], draft_order.get('invoice_url')
        except Exception as e:
            print(f"Error creating draft order: {e}")
            return None, None

    def read_payment_link_by_email_or_draft_id(self, draft_id=None, email=None):
        """
        Retrieve payment link for a draft order using either draft_id or customer email.
        
        Args:
            draft_id (int, optional): The draft order ID
            email (str, optional): Customer email to search for their latest draft order
            
        Returns:
            str: Payment URL if found
            None: If no draft order found or if it's already completed
        """
        try:
            # If draft_id is provided, use it directly
            if draft_id:
                draft_order = self.read_draft_order_by_id(draft_id)
                if draft_order and draft_order.get('status') != 'completed':
                    return draft_order.get('invoice_url')
                return None
            
            # If email is provided, search for the latest draft order
            elif email:
                # Query draft orders with email filter
                data = self.read('draft_orders.json', params={'email': email})
                draft_orders = data.get('draft_orders', [])
                
                # Find the most recent incomplete draft order
                for draft_order in draft_orders:
                    if draft_order.get('status') != 'completed':
                        return draft_order.get('invoice_url')
                
                return None
            
            else:
                raise ValueError("Either draft_id or email must be provided")
            
        except Exception as e:
            print(f"Error retrieving payment link: {e}")
            return None

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
    
    def read_order_by_number(self, order_number):
        """
        Retrieve an order by its order number (name).
        
        Args:
            order_number (str): The order number/name (can start with '#' or 'SN-')
            
        Returns:
            dict: Order data if found, None if not found
        """
        # Remove '#' if present in the order number
        order_number = order_number.replace('#', '')
        
        # Clean SN- prefix in any variation (case insensitive)
        order_number = order_number.lower().replace('sn', '').replace('-', '').replace(' ', '')

        order_number = str(order_number)
        
        # Query using the name field
        data = self.read('orders.json', params={'name': order_number, 'status': 'any'})
        
        # Return the first matching order or None if no matches
        return data['orders'][0] if data['orders'] else None

    def read_draft_order_by_id(self, draft_id):
        """
        Lee un Draft Order por su ID numérico. 
        """
        data = self.read(f'draft_orders/{draft_id}.json')
        return data.get('draft_order')


# ----------------------AUX FUNCTIONS ----------------------
