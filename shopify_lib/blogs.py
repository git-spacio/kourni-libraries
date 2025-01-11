import sys
sys.path.append('/home/snparada/Spacionatural/Libraries/shopify_lib')
from api import ShopifyAPI
import requests, json
from urllib.parse import urljoin, urlparse
import pandas as pd

class ShopifyBlogs(ShopifyAPI):
    def __init__(self, shop_url=None, api_password=None, api_version="2024-01"):
        super().__init__(shop_url, api_password, api_version)

    def read_all_blogs(self):
        """
        Obtiene todos los blogs de la tienda
        """
        endpoint = 'blogs.json'
        full_url = urljoin(self.base_url, endpoint)
        response = requests.get(full_url, headers=self.get_headers())
        
        if response.status_code == 200 and response.content:
            data = json.loads(response.content)
            return data['blogs']
        else:
            print(f"Error al obtener blogs: {response.status_code}, {response.text}")
            return []


    # Método para obtener todos los posts del blog
    def read_all_blog_posts(self, blog_id):
        posts = []
        endpoint = f'blogs/{blog_id}/articles.json?limit=250'
        while endpoint:
            full_url = urljoin(self.base_url, endpoint)
            response = requests.get(full_url, headers=self.get_headers())

            if response.status_code == 200 and response.content:
                data = json.loads(response.content)
                posts.extend(data['articles'])

                next_link = response.links.get('next')
                if next_link:
                    next_url = next_link['url']
                    parsed_url = urlparse(next_url)
                    endpoint = parsed_url.path + "?" + parsed_url.query
                else:
                    endpoint = None
            else:
                print(f"Error al obtener posts del blog: {response.status_code}, {response.text}")
                break

        return posts

    def read_all_blog_posts_df(self, blog_id):
        """
        Obtiene todos los posts de un blog específico y los devuelve como DataFrame
        """
        endpoint = f'blogs/{blog_id}/articles.json'
        full_url = urljoin(self.base_url, endpoint)
        all_posts = []
        
        try:
            response = requests.get(full_url, headers=self.get_headers())
            
            if response.status_code == 200 and response.content:
                data = json.loads(response.content)
                posts = data.get('articles', [])
                
                # Procesar cada post
                for post in posts:
                    # Extraer todos los campos incluyendo body_html
                    processed_post = {
                        'id': post.get('id'),
                        'title': post.get('title'),
                        'author': post.get('author'),
                        'created_at': post.get('created_at'),
                        'updated_at': post.get('updated_at'),
                        'published_at': post.get('published_at'),
                        'tags': post.get('tags'),
                        'handle': post.get('handle'),
                        'body_html': post.get('body_html'),
                        'summary_html': post.get('summary_html'),
                        'url': post.get('url'),
                        'image_url': post.get('image', {}).get('src') if post.get('image') else None
                    }
                    all_posts.append(processed_post)
                    
                return pd.DataFrame(all_posts)
            else:
                print(f"Error al obtener posts del blog: {response.status_code}, {response.text}")
                return pd.DataFrame()
            
        except Exception as e:
            print(f"Error al procesar los posts del blog: {str(e)}")
            return pd.DataFrame()


    # Método para actualizar los tags de un post del blog
    def update_blog_post_tags(self, post_id, new_tags):
        update_url = urljoin(self.base_url, f"articles/{post_id}.json")
        data = {
            "article": {
                "id": post_id,
                "tags": ", ".join(new_tags)
            }
        }
        response = requests.put(update_url, json=data, headers=self.get_headers())
        if response.status_code == 200:
            print(f"Tags actualizados para el post {post_id}")
        else:
            print(f"Error al actualizar tags para el post {post_id}: {response.status_code} - {response.text}")

