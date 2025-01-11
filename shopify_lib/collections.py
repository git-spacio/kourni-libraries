import sys
sys.path.append('/home/snparada/Spacionatural/Libraries/shopify_lib')
from api import ShopifyAPI
import requests, json, time
from urllib.parse import urljoin, urlparse

class ShopifyCollections(ShopifyAPI):
    def __init__(self, shop_url=None, api_password=None, api_version="2024-01"):
        super().__init__(shop_url, api_password, api_version)

# CRUD      
    def read_all_collections(self):       
        collections = []
        endpoint = 'custom_collections.json?limit=250'
        while endpoint:
            full_url = urljoin(self.base_url, endpoint)
            response = requests.get(full_url, headers=self.get_headers())

            if response.status_code == 200 and response.content:
                data = json.loads(response.content)
                collections.extend(data['custom_collections'])

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

        return collections

    def read_all_images(self):
        images = []
        collections = self.read_all_collections()
        for collection in collections:
            try:
                image = collection['image']
                image['collection_id'] = collection['id']
                image['description'] = collection['body_html']
                images.append(image)
            except:
                continue
        return images

    def read_collection_id(self, collection_handle):
        url = urljoin(self.base_url, f"custom_collections.json?handle={collection_handle}")
        response = requests.get(url, headers=self.get_headers())
        if response.status_code == 200:
            collections = response.json().get('custom_collections', [])
            if collections:
                return collections[0]['id']
        return None

    def update_image_seo(self, collection_id, new_alt):
        update_url = urljoin(self.base_url, f"custom_collections/{collection_id}.json")
        data = {
            "custom_collection": {
                "id": collection_id,
                "image": {
                    'alt': new_alt
                }
            }
        }
        response = requests.put(update_url, json=data, headers=self.get_headers())
        if response.status_code == 200:
            print(f"{collection_id}'s image was updated")
        else:
            print(f"{collection_id}'s image updating was failed")

    def update_collection_products(self, collection_handle, product_ids):
        # Step 1: Find the collection ID
        collection_id = self.read_collection_id(collection_handle)
        if not collection_id:
            print(f"Collection '{collection_handle}' not found.")
            return

        # Step 2: Clear existing products from the collection
        self.delete_all_collection_products(collection_id)

        # Step 3: Add new products to the collection
        self._add_products_to_collection(collection_id, product_ids)

    def delete_all_collection_products(self, collection_id):
        print(f"Clearing collection {collection_id}...")
        url = urljoin(self.base_url, f"collects.json?collection_id={collection_id}&limit=250")
        while url:
            response = requests.get(url, headers=self.get_headers())
            if response.status_code == 200:
                collects = response.json().get('collects', [])
                for collect in collects:
                    delete_url = urljoin(self.base_url, f"collects/{collect['id']}.json")
                    delete_response = requests.delete(delete_url, headers=self.get_headers())
                    if delete_response.status_code == 200:
                        print(f"Removed product {collect['product_id']} from collection {collection_id}")
                    else:
                        print(f"Failed to remove product {collect['product_id']} from collection {collection_id}: {delete_response.status_code}, {delete_response.text}")
                    time.sleep(0.1)  # Rate limiting

                # Check for pagination
                link_header = response.headers.get('Link')
                url = self._get_next_page_url(link_header)
            else:
                print(f"Failed to get collects for collection {collection_id}: {response.status_code}, {response.text}")
                break
        print(f"Finished clearing collection {collection_id}")

# AUX
    def _get_next_page_url(self, link_header):
        if not link_header:
            return None
        links = link_header.split(', ')
        for link in links:
            if 'rel="next"' in link:
                return link[link.index('<')+1:link.index('>')]
        return None

    def _add_products_to_collection(self, collection_id, product_ids):
        url = urljoin(self.base_url, "collects.json")
        for product_id in product_ids:
            data = {
                "collect": {
                    "product_id": product_id,
                    "collection_id": collection_id
                }
            }
            response = requests.post(url, headers=self.get_headers(), json=data)
            if response.status_code == 201:
                print(f"Added product {product_id} to collection {collection_id}")
            else:
                print(f"Failed to add product {product_id} to collection {collection_id}: {response.status_code}, {response.text}")
            time.sleep(0.1)  # Rate limiting