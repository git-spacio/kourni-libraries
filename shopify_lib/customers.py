import sys
sys.path.append('/home/snparada/Spacionatural/Libraries/shopify_lib')
from api import ShopifyAPI
import pandas as pd
import requests, json, time
from urllib.parse import urljoin, urlparse

class ShopifyCustomers(ShopifyAPI):
    def __init__(self, shop_url=None, api_password=None, api_version="2024-01"):
        super().__init__(shop_url, api_password, api_version)

    # CRUD

    def read_all_customers(self):
        customers = []
        endpoint = 'customers.json?limit=250'
        while endpoint:
            full_url = urljoin(self.base_url, endpoint)
            response = requests.get(full_url, headers=self.get_headers())

            if response.status_code == 200 and response.content:
                data = json.loads(response.content)
                customers.extend(data['customers'])

                next_link = response.links.get('next')
                if next_link:
                    next_url = next_link['url']
                    parsed_url = urlparse(next_url)
                    endpoint = parsed_url.path + "?" + parsed_url.query
                else:
                    endpoint = None
            else:
                print(f"Error al obtener clientes: {response.status_code}, {response.text}")
                break

        return customers

    def read_all_customers_in_dataframe(self):
        customers = self.read_all_customers()
        customer_rows = []
        for customer in customers:
            row = {
                'id': customer['id'],
                'email': customer['email'],
                'first_name': customer['first_name'],
                'last_name': customer['last_name'],
                'created_at': customer['created_at'],
                'updated_at': customer['updated_at'],
                'orders_count': customer['orders_count'],
                'total_spent': customer['total_spent'],
                'last_order_id': customer['last_order_id'],
                'note': customer['note'],
                'verified_email': customer['verified_email'],
                'phone': customer['phone'],
                'tags': customer['tags'],
                'last_order_name': customer['last_order_name'],
                'currency': customer['currency']
            }
            customer_rows.append(row)
        
        df_customers = pd.DataFrame(customer_rows)
        return df_customers

    def read_customer_metafields(self, customer_id):
        endpoint = f"customers/{customer_id}/metafields.json"
        url = urljoin(self.base_url, endpoint)
        response = requests.get(url, headers=self.get_headers())
        if response.status_code == 200:
            metafields = response.json().get('metafields', [])
            print(f"Metafields for customer {customer_id}: {json.dumps(metafields, indent=2)}")
            return metafields
        else:
            print(f"Failed to retrieve metafields for customer {customer_id}: {response.status_code} - {response.text}")
            return None

    def update_customer(self, customer_id, update_data):
        update_url = urljoin(self.base_url, f"customers/{customer_id}.json")
        data = {
            "customer": update_data
        }
        response = requests.put(update_url, json=data, headers=self.get_headers())
        if response.status_code == 200:
            print(f"Customer {customer_id} was updated successfully.")
        else:
            print(f"Failed to update customer {customer_id}: {response.status_code} - {response.text}")

        time.sleep(3)

    def update_customer_metafield(self, customer_id, metafield_name, metafield_value):
        metafield_data = {
            "metafield": {
                "namespace": "facturacion",
                "key": metafield_name,
                "value": metafield_value,
                "value_type": "string"
            }
        }
        
        response = self.update_customer(customer_id, {"metafields": [metafield_data]})
        
        if response:
            print(f"Metacampo {metafield_name} actualizado con éxito para el cliente {customer_id}.")
        else:
            print(f"Error al actualizar el metacampo {metafield_name} para el cliente {customer_id}.")


    def delete_customer(self, customer_id):
        delete_url = urljoin(self.base_url, f"customers/{customer_id}.json")
        response = requests.delete(delete_url, headers=self.get_headers())
        if response.status_code == 200:
            print(f"Customer {customer_id} was deleted successfully.")
        else:
            print(f"Failed to delete customer {customer_id}: {response.status_code} - {response.text}")

        time.sleep(3)

# AUX Functions

    def export_customers_to_json(self, customers, path):
        with open(path, 'w', encoding='utf-8') as file:
            json.dump(customers, file, ensure_ascii=False, indent=4)
