
import sys
sys.path.append('/home/snparada/Spacionatural/Libraries/shopify_lib')
from api import ShopifyAPI
import pandas as pd
import requests, json, time
from urllib.parse import quote,urljoin, urlparse


class ShopifyProducts(ShopifyAPI):
    def __init__(self, shop_url=None, api_password=None, api_version="2024-01"):
        super().__init__(shop_url, api_password, api_version)
        self.sku_to_product_id = self.map_sku_to_product_id()

# CRUD      
    
    def read_all_products(self):       
        products = []
        endpoint = 'products.json?limit=250'
        while endpoint:
            full_url = urljoin(self.base_url, endpoint)
            response = requests.get(full_url, headers=self.get_headers())

            if response.status_code == 200 and response.content:
                data = json.loads(response.content)
                products.extend(data['products'])

                next_link = response.links.get('next')
                if next_link:
                    next_url = next_link['url']
                    parsed_url = urlparse(next_url)
                    endpoint = parsed_url.path + "?" + parsed_url.query
                else:
                    endpoint = None
            else:
                print(f"Error al obtener productos: {response.status_code}, {response.text}")
                break

        return products

    def read_all_products_in_dataframe(self):            
        products = []
        endpoint = 'products.json?limit=250'
        
        # Loop para obtener todos los productos
        while endpoint:
            full_url = urljoin(self.base_url, endpoint)
            response = requests.get(full_url, headers=self.get_headers())

            if response.status_code == 200 and response.content:
                data = json.loads(response.content)
                products.extend(data['products'])

                # Verificar si hay un enlace "next" para continuar
                next_link = response.links.get('next')
                if next_link:
                    next_url = next_link['url']
                    parsed_url = urlparse(next_url)
                    endpoint = parsed_url.path + "?" + parsed_url.query
                else:
                    endpoint = None
            else:
                print(f"Error al obtener productos: {response.status_code}, {response.text}")
                break

        # Expandir las variantes a nivel de filas
        product_rows = []
        for product in products:
            for variant in product['variants']:
                row = {
                    'id': product['id'],
                    'title': product['title'],
                    'vendor': product['vendor'],
                    'body_html': product['body_html'],
                    'product_type': product['product_type'],
                    'created_at': product['created_at'],
                    'handle': product['handle'],
                    'updated_at': product['updated_at'],
                    'published_at': product['published_at'],
                    'tags': product['tags'],
                    'status': product['status'],
                    'variant_id': variant['id'],
                    'variant_title': variant['title'],
                    'variant_compare_at_price': variant.get('compare_at_price'),
                    'variant_price': variant['price'],
                    'variant_sku': variant['sku'],
                    'variant_inventory_quantity': variant['inventory_quantity']
                }
                product_rows.append(row)
        
        # Convertir la lista de filas a un DataFrame
        df_products = pd.DataFrame(product_rows)
        
        return df_products

    def read_all_images(self):
        images = []
        products = self.read_all_products()
        for product in products:
            for image in product['images']:
                image['description'] = product['body_html']
                images.append(image)
        return images

    def read_actual_complementary_products(self, product_id):
        endpoint = f"products/{product_id}/metafields.json"
        url = urljoin(self.base_url, endpoint)
        
        response = requests.get(url, headers=self.get_headers())
        if response.status_code == 200:
            metafields = response.json().get('metafields', [])
            complementary_metafield = next((m for m in metafields if m['namespace'] == "shopify--discovery--product_recommendation" and m['key'] == "complementary_products"), None)
            
            if complementary_metafield:
                return complementary_metafield
            else:
                print(f"No complementary products found for product {product_id}")
                return None
        else:
            print(f"Failed to retrieve metafields for product {product_id}: {response.status_code} - {response.text}")
            return None

    def read_variant_id_by_sku(self, sku):
        # Primero, intentamos obtener el product_id usando el mapa sku_to_product_id
        product_id = self.sku_to_product_id.get(sku)
        
        if product_id:
            # Si encontramos el product_id, buscamos la variante específica
            endpoint = f"products/{product_id}.json"
            url = urljoin(self.base_url, endpoint)
            response = requests.get(url, headers=self.get_headers())
            
            if response.status_code == 200:
                product_data = response.json()['product']
                for variant in product_data['variants']:
                    if variant['sku'] == sku:
                        return variant['id']
        
        # Si no encontramos el producto o la variante, hacemos una búsqueda más amplia
        endpoint = f"variants.json?sku={sku}"
        url = urljoin(self.base_url, endpoint)
        response = requests.get(url, headers=self.get_headers())
        
        if response.status_code == 200:
            variants = response.json()['variants']
            if variants:
                return variants[0]['id']
        
        # Si no encontramos nada, retornamos None
        print(f"No se encontró ninguna variante con el SKU: {sku}")
        return None

    def read_product_metafields(self, product_id):
        endpoint = f"products/{product_id}/metafields.json"
        url = urljoin(self.base_url, endpoint)
        response = requests.get(url, headers=self.get_headers())
        if response.status_code == 200:
            metafields = response.json().get('metafields', [])
            print(f"Metafields for product {product_id}: {json.dumps(metafields, indent=2)}")
            return metafields
        else:
            print(f"Failed to retrieve metafields for product {product_id}: {response.status_code} - {response.text}")
            return None

    def read_location_id(self, inventory_item_id):
        endpoint = f"inventory_levels.json?inventory_item_ids={inventory_item_id}"
        # Construye la URL completa usando self.base_url
        inventory_url = urljoin(self.base_url, endpoint)

        # Realiza la petición GET usando self.get_headers() para incluir las cabeceras correctas
        response = requests.get(inventory_url, headers=self.get_headers())

        if response.status_code == 200:
            inventory_data = response.json()["inventory_levels"]
            if inventory_data:
                return inventory_data[0]["location_id"]
            else:
                print(f"No se encontraron niveles de inventario para el inventory_item_id {inventory_item_id}")
                return None
        else:
            print(f"Error al obtener location_id para inventory_item_id {inventory_item_id}: {response.status_code} - {response.text}")
            return None
    
    def update_complementary_products(self, product_id, complementary_product_id):
        # Primero, obtén los metafields existentes para el producto
        endpoint = f"products/{product_id}/metafields.json"
        url = urljoin(self.base_url, endpoint)
        
        response = requests.get(url, headers=self.get_headers())
        if response.status_code == 200:
            metafields = response.json().get('metafields', [])
            complementary_metafield = next((m for m in metafields if m['namespace'] == "shopify--discovery--product_recommendation" and m['key'] == "complementary_products"), None)
            
            gid_complementary_product_id = f"gid://shopify/Product/{complementary_product_id}"
            
            if complementary_metafield:
                # Si el metafield ya existe, actualiza su valor
                metafield_id = complementary_metafield['id']
                existing_values = json.loads(complementary_metafield['value'])
                if gid_complementary_product_id not in existing_values:
                    existing_values.append(gid_complementary_product_id)
                    data = {
                        "metafield": {
                            "id": metafield_id,
                            "value": json.dumps(existing_values),
                            "type": "list.product_reference"
                        }
                    }
                    update_url = urljoin(self.base_url, f"metafields/{metafield_id}.json")
                    response = requests.put(update_url, json=data, headers=self.get_headers())
            else:
                # Crear un nuevo metafield si no existe
                data = {
                    "metafield": {
                        "namespace": "shopify--discovery--product_recommendation",
                        "key": "complementary_products",
                        "value": json.dumps([gid_complementary_product_id]),
                        "type": "list.product_reference"
                    }
                }
                response = requests.post(url, json=data, headers=self.get_headers())
            
            if response.status_code not in [200, 201]:
                print(f"Failed to update complementary product {complementary_product_id} for product {product_id}: {response.status_code} - {response.text}")
                print(f"Response content: {response.content}")  # Print the response content for debugging
        else:
            print(f"Failed to retrieve metafields for product {product_id}: {response.status_code} - {response.text}")

    def update_image_seo(self, product_id, image_id, new_alt):
        update_url = urljoin(self.base_url, f"products/{product_id}/images/{image_id}.json")
        data = {
            "image": {
                "id": image_id,
                "alt": new_alt
            }
        }
        response = requests.put(update_url, json=data, headers=self.get_headers())
        if response.status_code == 200:
            print(f"{image_id} image was updated")
        else:
            print(f"{image_id} image updating was failed")

    def update_stock(self, inventory_item_id, new_stock, sku):
        location_id = self.read_location_id(inventory_item_id)
        if location_id:
            # Usa self.base_url para construir la URL completa
            update_url = urljoin(self.base_url, "inventory_levels/set.json")
            
            data = {
                "location_id": location_id,
                "inventory_item_id": inventory_item_id,
                "available": new_stock
            }
            # Usa self.get_headers() para obtener las cabeceras correctas
            response = requests.post(update_url, json=data, headers=self.get_headers())
            if response.status_code == 200:
                print(f"Stock actualizado para SKU {sku}.")
            else:
                print(f"Error al actualizar stock para SKU {sku}: {response.status_code} - {response.text}")
        else:
            print(f"No se pudo obtener el location_id para inventory_item_id {inventory_item_id}")

        # Espera para evitar saturar la API o violar los límites de la tasa de solicitud
        time.sleep(1)

    def update_price(self, variant_id, new_price, sku):
        update_url = urljoin(self.base_url, f"variants/{variant_id}.json")
        data = {
            "variant": {
                "id": variant_id,
                "price": new_price
            }
        }
        response = requests.put(update_url, json=data, headers=self.get_headers())
        if response.status_code == 200:
            print(f"Precio actualizado para el sku {sku}")
        else:
            print(f"Error al actualizar el precio para el {sku}: {response.text}")

        # Pausa después de cada actualización
        print("durmiendo...")
        time.sleep(1)
    
    def update_price_comparison(self, variant_id, compare_at_price, sku):
        update_url = urljoin(self.base_url, f"variants/{variant_id}.json")
        data = {
            "variant": {
                "id": variant_id,
                "compare_at_price": compare_at_price
            }
        }
        response = requests.put(update_url, json=data, headers=self.get_headers())
        if response.status_code == 200:
            print(f"Precio de comparación actualizado para el sku {sku}")
        else:
            print(f"Error al actualizar el precio de comparación para el {sku}: {response.text}")

        print("durmiendo...")
        time.sleep(1)             

    def delete_complementary_products(self, product_id):
        endpoint = f"products/{product_id}/metafields.json"
        url = urljoin(self.base_url, endpoint)
        
        response = requests.get(url, headers=self.get_headers())
        if response.status_code == 200:
            metafields = response.json().get('metafields', [])
            complementary_metafield = next((m for m in metafields if m['namespace'] == "shopify--discovery--product_recommendation" and m['key'] == "complementary_products"), None)
            
            if complementary_metafield:
                metafield_id = complementary_metafield['id']
                delete_url = urljoin(self.base_url, f"metafields/{metafield_id}.json")
                response = requests.delete(delete_url, headers=self.get_headers())
                
                if response.status_code != 200:
                    print(f"Failed to delete complementary products for product {product_id}: {response.status_code} - {response.text}")
        else:
            print(f"Failed to retrieve metafields for product {product_id}: {response.status_code} - {response.text}")

# AUX Functions

    def export_products_to_json(self, products,path):        
        with open(path, 'w', encoding='utf-8') as file:
            json.dump(products, file, ensure_ascii=False, indent=4)
    
    def map_sku_to_product_id(self):
        sku_to_product_id = {}
        products = self.read_all_products()
        for product in products:
            for variant in product['variants']:
                sku = variant['sku']
                product_id = product['id']
                if sku:
                    sku_to_product_id[sku] = product_id
        return sku_to_product_id
