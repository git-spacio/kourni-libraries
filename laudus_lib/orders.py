import sys
sys.path.append('/home/snparada/Spacionatural/Libraries/laudus_lib')
from api import LaudusAPI
import requests
import json
import pandas as pd
from datetime import datetime

class LaudusOrders(LaudusAPI):
    def __init__(self):
        super().__init__()
        self.header_authentication()
    
#CRUD
    def create_new_order(self, order_number,order_df):
        order_exists = read_order_exists(order_number)
        if order_exists == True:
            url = 'https://api.laudus.cl/sales/orders'
            json_order_data = create_json_order(order_data)

            create_order = requests.post(
                url,
                headers=self.headers_auth,
                json=json_order_data
            )

            if create_order.status_code == 200:
                print('Pedido creado con éxito')
                return True
            else:
                print(f'Hubo un error al crear el pedido: {json_order_data}')
                return False
        if order_exists == False:
            print(f'La orden {order_number} ya existe')
        
    def read_check_order_exists(self, order_number):
        url = 'https://api.laudus.cl/sales/orders/list'
        headers_auth = self.headers_auth  # Asegúrate de que headers_auth esté definido correctamente
        parametros = {
            "options": {
                "offset": 0,
                "limit": 0
            },
            "fields": [
                "salesOrderId"
            ],
            "filterBy": [
                {
                    "field": "purchaseOrderNumber",
                    "operator": "=",
                    "value": order_number
                }
            ]
        }

        # Realizar la petición POST
        response = requests.post(url, headers=headers_auth, json=parametros)
        result = {'status': False, 'response': None}

        # Procesar la respuesta
        if response.status_code == 200:
            result['status'] = True
            result['response'] = response.json()
        elif response.status_code == 204:
            result['status'] = False
        else:
            print(f'Error en la busqueda de la url: {url}, respuesta del servidor: {response.text}')

        return result
    
    def read_all_orders_without_items(self):
        url_lista_facturas = 'https://api.laudus.cl/sales/invoices/list'

        parametros_lista = {
            "options": {
                "offset": 0,
                "limit": 0
            },
            "fields": [
                "salesInvoiceId",
                "doctype.name",
                "docnumber",
                "customer.customerid",
                "customer.name",
                "customer.vatid",
                "salesman.name",
                "term.name",
                "warehouse.name",
                "totals.net",
                "totals.vat",
                "total.total",
                "issuedDate"
            ],
            "filterBy": [
                {
                    "field": "issuedDate",
                    "operator": ">",
                    "value": "2015-12-31T23:59:59"
                }
            ]
        }

        lista_factura = requests.post(
            url_lista_facturas, 
            headers=self.headers_auth, 
            json=parametros_lista
            )
        lista_factura_json = lista_factura.json()
        df = pd.DataFrame(lista_factura_json)

        return df

    def read_all_orders_with_items(self):
        url_lista_facturas = 'https://api.laudus.cl/sales/invoices/list'

        all_data = []
        offset = 0
        limit = 0
        aux = 0

        # Empezamos con la fecha inicial
        latest_date = "2015-12-31T23:59:59"

        while True:
            parametros_lista = {
                "options": {
                    "offset": offset,
                    "limit": limit
                },
                "fields": [
                    "salesInvoiceId",
                    "doctype.name",
                    "docnumber",
                    "customer.customerid",
                    "customer.name",
                    "customer.vatid",
                    "salesman.name",
                    "term.name",
                    "warehouse.name",
                    "totals.net",
                    "totals.vat",
                    "total.total",
                    "items.product.description",
                    "items.product.sku",
                    "items.quantity",
                    "items.unitPrice",
                    "issuedDate"
                ],
                "filterBy": [
                    {
                        "field": "issuedDate",
                        "operator": ">",
                        "value": latest_date
                    }
                ]
            }

            lista_factura = requests.post(
                url_lista_facturas, 
                headers=self.headers_auth, 
                json=parametros_lista
            )

            lista_factura_json = lista_factura.json()
            lista_factura = str(lista_factura)

            # Agrega los datos a all_data
            all_data.extend(lista_factura_json)

            # Actualiza latest_date con la fecha más reciente de los nuevos datos
            df_new = pd.DataFrame(lista_factura_json)
            latest_date = df_new['issuedDate'].max()
            print(latest_date)
            aux+=1
            print(aux)

            # Si no hay más datos, sal del bucle
            if lista_factura != '<Response [200]>' or latest_date == "2024-07-31T18:09:35":
                break

        df = pd.DataFrame(all_data)
        return df

    def read_lastest_orders(self,file_path):
        url_lista_facturas = 'https://api.laudus.cl/sales/invoices/list' 

        # Leer los datos existentes
        df_old = pd.read_csv(file_path, dtype={13: object})
        # Convertir 'issuedDate' a datetime si aún no lo es
        df_old['issuedDate'] = pd.to_datetime(df_old['issuedDate'], format='mixed')
        # Obtener la fecha más reciente de los datos existentes
        latest_date = df_old['issuedDate'].max()
        
        # Preparar los parámetros para solicitar solo los datos más recientes de la API
        parametros_lista = {
            "options": {
                "offset": 0,
                "limit": 0  # O cualquier otro límite que te convenga
            },
            "fields": [
                "salesInvoiceId",
                "doctype.name",
                "docnumber",
                "customer.customerid",
                "customer.name",
                "customer.vatid",
                "salesman.name",
                "term.name",
                "warehouse.name",
                "totals.net",
                "totals.vat",
                "total.total",
                "items.product.description",
                "items.product.sku",
                "items.quantity",
                "items.unitPrice",
                "issuedDate"
            ],
            "filterBy": [
                {
                    "field": "issuedDate",
                    "operator": ">",
                    "value": latest_date.isoformat()  # Convertir la fecha a una cadena en el formato correcto
                }
            ]
        }

        lista_factura = requests.post(
            url_lista_facturas, 
            headers=self.headers_auth, 
            json=parametros_lista
        )
        a = str(lista_factura)
        if a != '<Response [204]>':
                print(a)
                lista_factura_json = lista_factura.json()

                # Convertir los nuevos datos en un DataFrame y agregarlos a los datos existentes
                df_new = pd.DataFrame(lista_factura_json)
                df_updated = pd.concat([df_old, df_new])

                # Eliminar posibles duplicados
                df_updated.drop_duplicates(inplace=True)

                # Guardar los datos actualizados en el archivo .csv
                df_updated.to_csv(file_path, index=False)
        else:
            # Obtén los títulos de las columnas del DataFrame original
            column_names = df_old.columns.tolist()

            # Crea un nuevo DataFrame vacío con los títulos de las columnas
            df_new = pd.DataFrame(columns=column_names)
            df_updated = pd.concat([df_old, df_new])

        return df_updated
    
#AUX Functions
    def calculate_total_in_uf(self,sales_df, uf_df):
        # Convertir la columna 'Fecha' en el dataframe UF al formato datetime.
        uf_df['Fecha'] = pd.to_datetime(uf_df['Fecha']).dt.normalize()
        # Convertir la columna 'issuedDate' en el dataframe de ventas al formato dateime.
        sales_df['issuedDate'] = pd.to_datetime(sales_df['issuedDate']).dt.normalize()
        # Unir los dos dataframes utilizando la fecha como clave.
        print(sales_df)
        merged_df = sales_df.merge(uf_df, left_on='issuedDate', right_on='Fecha', how='left', suffixes=('_sales', '_uf'))
        # Reemplazar los valores NaN con ceros.
        merged_df.fillna(0, inplace=True)
        print(merged_df)
        # Crear la nueva columna 'Total en UF'.
        merged_df['Precio'] = merged_df['Precio_uf'].replace('[\$,]',"", regex = True).astype('float')
        merged_df['Total Neto (UF)'] = (merged_df['totals_net'] / merged_df['Precio']).round(2) 
        merged_df['Total Productos ($ CLP)'] = (merged_df['items_unitPrice'] * merged_df['items_quantity']).round(0) 
        merged_df['Precio Unitario (UF)'] = (merged_df['items_unitPrice'] / merged_df['Precio']).round(2) 
        merged_df['Total Productos (UF)'] = (merged_df['items_quantity'] * merged_df['Precio Unitario (UF)']).round(2)

        for index, row in merged_df.iterrows():
            if row['Precio'] == 0:
                merged_df.loc[index, 'Total Neto (UF)'] = 0
                merged_df.loc[index, 'Precio Unitario (UF)'] = 0
                merged_df.loc[index, 'Total Productos (UF)'] = 0
                merged_df.loc[index, 'Total Productos ($ CLP)'] = 0

        return merged_df

    def adding_sales_channel(self,sales_df):

        for row in sales_df.itertuples():
            if row.term_name == 'PAYKU' or row.term_name == 'TRANSBANK' or row.term_name == 'Mercado Pago' or row.salesman_name == 'Mercado Libre' or row.term_name == 'KLAP CHECKOUT' or (row.salesman_name == 'VENTAS WEB' and row.warehouse_name == 'PICKING'):
                if row.salesman_name == 'Mercado Libre' or row.term_name == 'Mercado Pago' or row.salesman_name == 'Mercado Libre':
                    sales_df.loc[row.Index, 'sales_channel'] = 'Mercado Libre'
                else:
                    sales_df.loc[row.Index, 'sales_channel'] = 'E-Commerce'
 
            elif row.term_name == 'EFECTIVO' or row.term_name == 'TARJETA DE DEBITO' or row.term_name == 'TARJETA DE CREDITO' or (row.salesman_name == 'Nohemi Lando' and row.warehouse_name == 'TIENDA'):
                sales_df.loc[row.Index, 'sales_channel'] = 'Tienda Sabaj'

            elif row.salesman_name == 'Damarys' or row.salesman_name == 'Ventas Cotizaciones':
                sales_df.loc[row.Index, 'sales_channel'] = 'Cotizaciones'

            elif row.salesman_name == 'VENTAS WEB':
                sales_df.loc[row.Index, 'sales_channel'] = 'E-Commerce' 

            #De aquí en adelante son dudosas porque hay inconsistencias, como por ej: vendedor VENTAS WEB pero bodega de venta TIENDA.
            elif row.warehouse_name == 'TIENDA'or row.warehouse_name == 'SALA DE VENTAS':
                sales_df.loc[row.Index, 'sales_channel'] = 'Tienda Sabaj' 

            elif row.warehouse_name == 'PICKING' and row.salesman_name != 'Mercado Libre':
                sales_df.loc[row.Index, 'sales_channel'] = 'E-Commerce' 

            else:
                sales_df.loc[row.Index, 'sales_channel'] = 'Sin Clasificar'

        return sales_df
