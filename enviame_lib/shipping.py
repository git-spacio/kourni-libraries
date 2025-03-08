import os
import requests
from dotenv import load_dotenv
from urllib.parse import urljoin
import pandas as pd
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from tqdm import tqdm


class EnviameAPI:
    def __init__(self, environment="prod", company_id=None):
        load_dotenv('/home/snparada/Spacionatural/Libraries/enviame_lib/.env')
        self.api_key = os.getenv('ENVIAME_API_KEY')
        self.company_id = os.getenv('COMPANY_ID')
        self.base_url = "https://stage.api.enviame.io/api/s2/v2/" if environment == "stage" else "https://api.enviame.io/api/s2/v2/"
        
        # Configurar headers
        self.headers = {
            'Accept': 'application/json',
            'api-key': self.api_key,
            'Content-Type': 'application/json'
        }
        
        # Configurar reintentos automáticos
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    # CREATE
    def create_shipping_price(self, shipping_data):
        endpoint = f"companies/{self.company_id}/deliveries/calculate"
        url = urljoin(self.base_url, endpoint)
        
        response = requests.post(
            url,
            headers=self.headers,
            json=shipping_data
        )
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error calculating shipping: {response.status_code} - {response.text}")
            return None

    def create_delivery(self, delivery_data):
        endpoint = f"companies/{self.company_id}/deliveries"
        url = urljoin(self.base_url, endpoint)
        
        response = requests.post(
            url,
            headers=self.headers,
            json=delivery_data
        )
        
        if response.status_code == 201:
            return response.json()
        else:
            print(f"Error creating delivery: {response.status_code} - {response.text}")
            return None

    # READ
    def read_all_deliveries(self, max_pages=30):
        """
        Obtiene los envíos del seller usando paginación
        Args:
            max_pages (int): Número máximo de páginas a obtener (por defecto 30)
                           Cada página contiene 20 envíos, así que por defecto se obtienen 600 envíos

        """
        endpoint = f"companies/{self.company_id}/deliveries"
        base_url = self.base_url if self.base_url.endswith('/') else f"{self.base_url}/"
        url = urljoin(base_url, endpoint)
        
        all_deliveries = []
        
        # Obtener información de la primera página para metadata
        try:
            response = requests.get(
                url,
                headers=self.headers,
                params={'page': 0}
            )
            
            if response.status_code == 200:
                response_json = response.json()
                meta = response_json.get('meta', {}).get('pagination', {})
                total_pages = meta.get('total_pages', 0)
                total_envios = meta.get('total', 0)
                # Procesar primera página
                for delivery in response_json.get('data', []):
                    delivery_info = {
                        'ID': delivery.get('identifier'),
                        'Nº Referencia': delivery.get('imported_id'),
                        'Tracking': delivery.get('tracking_number'),
                        'Estado': delivery.get('status', {}).get('name'),
                        'Código Estado': delivery.get('status', {}).get('code'),
                        'Info Estado': delivery.get('status', {}).get('info'),
                        'Fecha Estado': delivery.get('status', {}).get('created_at'),
                        'Transportista': delivery.get('carrier'),
                        'Servicio': delivery.get('service'),
                        'Cliente': delivery.get('customer', {}).get('full_name'),
                        'Teléfono': delivery.get('customer', {}).get('phone'),
                        'Email': delivery.get('customer', {}).get('email'),
                        'Dirección': delivery.get('shipping_address', {}).get('full_address'),
                        'Ciudad': delivery.get('shipping_address', {}).get('place'),
                        'Tipo Dirección': delivery.get('shipping_address', {}).get('type'),
                        'País': delivery.get('country'),
                        'Fecha Creación': delivery.get('created_at'),
                        'Última Actualización': delivery.get('updated_at'),
                        'Fecha Límite': delivery.get('deadline_at'),
                        'PDF Etiqueta': delivery.get('label', {}).get('PDF')
                    }
                    all_deliveries.append(delivery_info)
                
                # Obtener el resto de las páginas con barra de progreso
                for current_page in tqdm(range(1, min(max_pages, total_pages)), desc="Obteniendo envíos"):
                    response = requests.get(
                        url,
                        headers=self.headers,
                        params={'page': current_page}
                    )
                    
                    if response.status_code == 200:
                        data = response.json().get('data', [])
                        if not data:
                            break
                            
                        for delivery in data:
                            delivery_info = {
                                'ID': delivery.get('identifier'),
                                'Nº Referencia': delivery.get('imported_id'),
                                'Tracking': delivery.get('tracking_number'),
                                'Estado': delivery.get('status', {}).get('name'),
                                'Código Estado': delivery.get('status', {}).get('code'),
                                'Info Estado': delivery.get('status', {}).get('info'),
                                'Fecha Estado': delivery.get('status', {}).get('created_at'),
                                'Transportista': delivery.get('carrier'),
                                'Servicio': delivery.get('service'),
                                'Cliente': delivery.get('customer', {}).get('full_name'),
                                'Teléfono': delivery.get('customer', {}).get('phone'),
                                'Email': delivery.get('customer', {}).get('email'),
                                'Dirección': delivery.get('shipping_address', {}).get('full_address'),
                                'Ciudad': delivery.get('shipping_address', {}).get('place'),
                                'Tipo Dirección': delivery.get('shipping_address', {}).get('type'),
                                'País': delivery.get('country'),
                                'Fecha Creación': delivery.get('created_at'),
                                'Última Actualización': delivery.get('updated_at'),
                                'Fecha Límite': delivery.get('deadline_at'),
                                'PDF Etiqueta': delivery.get('label', {}).get('PDF')
                            }
                            all_deliveries.append(delivery_info)
                    else:
                        print(f"\nError en página {current_page}: {response.status_code}")
                        break
                        
            if all_deliveries:
                df = pd.DataFrame(all_deliveries)
                
                # Convertir fechas
                date_columns = ['Fecha Estado', 'Fecha Creación', 'Última Actualización', 'Fecha Límite']
                for col in date_columns:
                    if col in df.columns:
                        df[col] = pd.to_datetime(df[col], format='%Y-%m-%d %H:%M:%S', errors='coerce')
                return df
                
        except Exception as e:
            print(f"\nError: {str(e)}")
            return None

    def read_tracking_by_reference(self, reference):
        """
        Obtiene el historial de tracking de un envío usando su número de referencia de Shopify
        Args:
            reference (str): Número de referencia (ej: 'sn-64556', 'Sn64556', 'S64556', '64556')
        Returns:
            dict: Información de tracking del envío o None si hay error
        """
        # Primero obtener el envío usando la referencia
        delivery = self.read_delivery_by_reference(reference)
        
        if delivery is None:
            print(f"No se encontró envío con la referencia: {reference}")
            return None
            
        delivery_id = delivery['ID']
        
        # Construir la URL para el tracking
        endpoint = f"deliveries/{delivery_id}/tracking"
        url = urljoin(self.base_url, endpoint)
        
        try:
            response = self.session.get(
                url,
                headers=self.headers
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Error obteniendo tracking: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            print(f"Error al obtener tracking: {str(e)}")
            return None

    def read_delivery_by_reference(self, reference):
        """
        Busca un envío por su número de referencia de Shopify (imported_id)
        Acepta formatos como: 'sn-64556', 'Sn64556', 'S64556', '64556'
        """
        def extract_number(ref):
            """Extrae solo los números de una cadena"""
            if not ref:
                return None
            return int(''.join(filter(str.isdigit, str(ref))))
        
        try:
            # Extraer el número de la referencia proporcionada
            search_number = extract_number(reference)
            if not search_number:
                print(f"No se pudo extraer un número válido de la referencia: {reference}")
                return None
                
            print(f"Buscando número de orden: {search_number}")
            
            df = self.read_all_deliveries()
            
            if df is not None and not df.empty:
                # Convertir la columna de referencias a números
                df['Nº Orden'] = df['Nº Referencia'].apply(extract_number)
                
                # Filtrar por el número extraído
                envio = df[df['Nº Orden'] == search_number]
                
                if not envio.empty:
                    resultado = envio.iloc[0]
                    print(f"\nEnvío encontrado:")
                    print(f"Referencia original: {resultado['Nº Referencia']}")
                    print(f"Número extraído: {resultado['Nº Orden']}")
                    return resultado
                else:
                    print(f"No se encontró ningún envío con el número de orden: {search_number}")
                    return None
            else:
                print("Error al obtener los envíos")
                return None
                
        except Exception as e:
            print(f"Error al procesar la referencia: {str(e)}")
            return None
    
    # UPDATE
    def update_delivery(self, delivery_id, update_data):
        endpoint = f"companies/{self.company_id}/deliveries/{delivery_id}"
        url = urljoin(self.base_url, endpoint)
        
        response = requests.put(
            url,
            headers=self.headers,
            json=update_data
        )
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error updating delivery: {response.status_code} - {response.text}")
            return None

    # DELETE
    def delete_delivery(self, delivery_id):
        endpoint = f"companies/{self.company_id}/deliveries/{delivery_id}/cancel"
        url = urljoin(self.base_url, endpoint)
        
        response = requests.post(
            url,
            headers=self.headers
        )
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error canceling delivery: {response.status_code} - {response.text}")
            return None
    


