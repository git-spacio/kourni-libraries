import datetime
import pandas as pd
import json
import requests
import sys
sys.path.append('/home/snparada/Spacionatural/Libraries/laudus_lib')
from api import LaudusAPI


class LaudusCustomers(LaudusAPI):
    def __init__(self):
        super().__init__()
        self.header_authentication()
    
# CRUD

    def create_new_user(self, customer_data):
        # Crear el JSON del cliente
        json_customer = self.create_json_customer(customer_data)
        
        # URL para la creación de un nuevo cliente en la API de Laudus
        url = 'https://api.laudus.cl/sales/customers'
        
        # Realizar la petición POST directamente aquí
        try:
            response = requests.post(url, headers=self.headers_auth, json=json_customer)
            
            # Verificar el estado de la respuesta y actuar en consecuencia
            if response.status_code == 200 or response.status_code == 201:
                return {'status': True, 'response': response.json()}
            else:
                print(f'Error al crear usuario: {response.text}')
                return {'status': False, 'response': response.text}
        except Exception as e:
            print(f"Excepción al realizar petición POST: {e}")
            return {'status': False, 'response': None}

    def create_address_for_customer(self, customer_id, address_data):
        # Generar el JSON para la dirección
        json_address_user = self.create_json_address(address_data)
        
        # Construir la URL para añadir una dirección al cliente existente
        address_url = f'https://api.laudus.cl/sales/customers/{customer_id}/addresses'
        
        # Intentar crear la dirección
        try:
            response = requests.post(address_url, headers=self.headers_auth, json=json_address_user)
            if response.status_code in [200, 201]:
                print("La dirección fue creada con éxito para el cliente.")
                return {'status': True, 'response': response.json()}
            else:
                print(f'Error al crear la dirección: {response.text}')
                return {'status': False, 'response': response.text}
        except Exception as e:
            print(f"Excepción al realizar la petición POST: {e}")
            return {'status': False, 'response': None}

    def read_check_customer_exists(self, rut_formateado):
        url = 'https://api.laudus.cl/sales/customers/list'
        parametros = {
            "options": {
                "offset": 0,
                "limit": 0
            },
            "fields": [
                "customerId"
            ],
            "filterBy": [
                {
                    "field": "VATId",
                    "operator": "=",
                    "value": rut_formateado
                }
            ]
        }

        # Realizar la petición POST aquí mismo
        response = requests.post(url, headers=self.headers_auth, json=parametros)
        if response.status_code == 200:
            return {'status': True, 'response': response.json()}
        elif response.status_code == 204:
            return {'status': True, 'response': None}  # Indica éxito pero sin contenido (no existe el cliente)
        else:
            print(f'Error en la petición a la URL: {url}, respuesta del servidor: {response.text}')
            return {'status': False, 'response': None}

    @staticmethod 
    def create_json_address(address_data):
        json_data = {
            "description": "direccion cargada desde el API",
            "address": "",
            "county": "",
            "zipCode": "",
            "city": "",
            "state": "",
            "country": "Chile",
            "notes": ""
        }

        json_data['address'] = address_data['address']['direccion']
        json_data['county'] = address_data['address']['comuna']
        json_data['state'] = address_data['address']['region']

        return json_data

    @staticmethod    
    def create_json_customer(customer_data):
        json_data = {
            "name": "",
            "legalName": "",
            "VATId": "",
            "activityName": "",
            "account": None,
            "dealer": None,
            "term": {"termId": ""},
            "priceList": None,
            "customerCategory": None,
            "salesman": {"salesmanId": 24},
            "address": "",
            "city": "",
            "county": "",
            "zipCode": "",
            "state": "",
            "country": "Chile",
            "foreigner": False,
            "addressBilling": "",
            "cityBilling": "",
            "countyBilling": "",
            "zipCodeBilling": "",
            "stateBilling": "",
            "countryBilling": "Chile",
            "phone1": "",
            "phone2": "",
            "fax": "",
            "DTEEmail": "",
            "email": "",
            "webPage": "",
            "schedule": "",
            "creditLimit": 0,
            "collectWeekday": 0,
            "daysToExpiration": 0,
            "discount": 0,
            "discountIsAdditive": False,
            "blocked": False,
            "blockedIfOverCreditLimit": False,
            "blockedIfOverdueInvoices": False,
            "defaultQuoteHeader": "",
            "defaultQuoteFooter": "",
            "comercioNetBranchCode": "",
            "comercioNetCode": "",
            "notes": "",
            "createdBy": {"userId": "07"},
            "customFields": {}
        }

        # sustituir la key 'name'
        json_data['name'] = customer_data['customer']['firstname'] + \
            " " + customer_data['customer']['lastname']
        json_data['VATId'] = customer_data['customer']['dni']

        if customer_data['customer']['payment'] == 'Payku':
            json_data['term']['termId'] = '18'
        elif customer_data['customer']['payment'] == 'Pagos por transferencia bancaria':
            json_data['term']['termId'] = '12'
        elif customer_data['customer']['payment'] == 'Klap Checkout (2.0.0)':
            json_data['term']['termId'] = '19'
        elif customer_data['customer']['payment'] == 'Webpay Plus':
            json_data['term']['termId'] = '1B'

        json_data['address'] = customer_data['customer']['address']
        json_data['addressBilling'] = customer_data['customer']['address']
        json_data['state'] = customer_data['customer']['region']
        json_data['stateBilling'] = customer_data['customer']['region']
        json_data['county'] = customer_data['customer']['comuna']
        json_data['countyBilling'] = customer_data['customer']['comuna']
        json_data['cityBilling'] = customer_data['customer']['comuna']
        json_data['email'] = customer_data['customer']['email']
        json_data['phone1'] = customer_data['customer']['phone']

        return json_data