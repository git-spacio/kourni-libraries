import sys
import pandas as pd
sys.path.append('/home/sam/Spacionatural/Libraries')

from shopify_lib.api import ShopifyConnection
from shopify_lib.creds.config import SHOPIFY_SHOP_URL, SHOPIFY_PASSWORD
from laudus_lib.products import LaudusProduct

# Ruta completa al archivo CSV
archivo_csv = '/home/sam/Spacionatural/Data/Dimensions/products_laudus.csv'

# Lee el archivo CSV
df = pd.read_csv(archivo_csv)

# Función para verificar si un valor es numérico
def es_numerico(sku):
    try:
        float(sku)  # Intenta convertir a float
        return True
    except ValueError:
        return False

# Filtra por 'sku' numérico y 'discontinued' igual a False
df_filtrado = df[df['sku'].apply(es_numerico) & (df['discontinued'] == False)]

# Selecciona las columnas 'sku', 'productId', y 'unitPriceWithTaxes', y convierte 'unitPriceWithTaxes' a entero
df_filtrado['unitPriceWithTaxes'] = df_filtrado['unitPriceWithTaxes'].astype(int)
columnas_seleccionadas = df_filtrado[['sku', 'productId', 'unitPriceWithTaxes']]

# Exporta a un archivo JSON (sobrescribe si ya existe)
ruta_json = '/home/sam/Spacionatural/Data/Dimensions/skus_filtrados.json'
columnas_seleccionadas.to_json(ruta_json, orient='records', lines=False)

print(f"Archivo JSON exportado a {ruta_json}")

# Crear una instancia de ShopifyConnection
# conn = ShopifyConnection(SHOPIFY_SHOP_URL, SHOPIFY_PASSWORD)
# print(conn)