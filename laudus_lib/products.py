import datetime
import pandas as pd
import json
import requests
import sys
sys.path.append('/home/snparada/Spacionatural/Libraries/')
from laudus_lib.api import LaudusAPI


class LaudusProducts(LaudusAPI):
    def __init__(self):
        super().__init__()
        self.header_authentication()
# CRUD

    def read_all_product_list(self):
        url_lista_productos = 'https://api.laudus.cl/production/products/list'
        parametros_lista = {
            "options": {
                "offset": 0,
                "limit": 0
            },
            "fields": [
                "productId",
                "sku",
                "description",
                "unitCost",
                "discontinued",
                "unitPrice",
                "unitPriceWithTaxes",
                "productCategory.productCategoryId",
                "productCategory.name",
                "productCategory.fullPath"
            ],
            "filterBy": [
                {
                    "field": "discontinued",
                    "operator": "=",
                    "value": False
                }
            ]
        }
        lista_productos = requests.post(
            url_lista_productos,
            headers=self.headers_auth,
            json=parametros_lista
        )
        lista_productos = lista_productos.json()
        df = pd.DataFrame(lista_productos)
        # Creamos un diccionario que mapea los primeros dos caracteres de 'sku' a su correspondiente 'Tipo'
        tipo_dict = {'ME': 'ME', 'MA': 'ME', 'MP': 'MP', 'Ma': 'ME'}

        # Creamos una nueva columna 'sku_prefijo' que contiene los dos primeros caracteres de 'sku'
        df['sku_prefijo'] = df['sku'].str[:2].str.upper()

        # Utilizamos 'map()' para mapear 'sku_prefijo' a 'Tipo' según nuestro diccionario
        df['Tipo'] = df['sku_prefijo'].map(tipo_dict)

        # Para los valores que no estaban en nuestro diccionario, llenamos con 'PT'
        df['Tipo'].fillna('PT', inplace=True)

        # Eliminamos la columna 'sku_prefijo'
        df.drop(columns='sku_prefijo', inplace=True)

        df.to_csv(
            '/home/sam/Spacionatural/Data/Dimensions/products_laudus.csv', index=False)

        return df

    def update_product_stock(self, product_data):
        time = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        tag_to_warehouse_map = {
            '1': '003',  # ME tags go to warehouse 3
            '2': '002'   # MP tags go to warehouse 2
        }
        tag_id = product_data[0].get('product_tag_ids', [])[0]
        warehouse_id = tag_to_warehouse_map.get(str(tag_id), 'default_value')
        product_id = product_data[0]['x_studio_id_laudus']
        body = {
            "date": time,
            "warehouse": {
                "warehouseId": warehouse_id
            },
            "notes": "Inventario actualizado desde Odoo",
            "items": [
                {
                    "product": {
                        "productId": product_id
                    },
                    "quantity": product_data[0]['qty_available']
                }
            ]
        }
        url = "https://api.laudus.cl/production/inventories"
        if product_id:
            response = requests.post(url, headers=self.headers_auth, json=body)
            if response.status_code == 200:
                message = f"Stock actual producto {product_data[0]['x_studio_id_laudus']} actualizado con éxito en Laudus"
            elif response.status_code != 200:
                message = f"No fue posible actualizar el inventario de {product_data[0]['x_studio_id_laudus']} en Laudus. Código de error:{response.status_code}"
            return message
        else:
            message = "El producto no tiene el id de Laudus establecido en Odoo."

    def read_stock_product(self, warehouse_id, path_to_file):
        url = f'https://api.laudus.cl/production/products/stock?warehouseId={warehouse_id}'
        response = requests.get(url, headers=self.headers_auth)
        products_list = response.json()
        # print(products_list)

        # Acceder a la lista de productos y convertir el stock a entero
        if 'products' in products_list:
            for product in products_list['products']:
                product['stock'] = int(product['stock'])
        

        # Escribir los datos en el archivo JSON
        with open(path_to_file, 'w') as file:
            json.dump(products_list, file)