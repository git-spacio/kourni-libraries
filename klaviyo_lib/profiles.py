import pandas as pd
import sys, json
sys.path.append('/home/snparada/Spacionatural/Libraries/klaviyo_lib')
from api import KlaviyoConnection

class KlaviyoProfiles(KlaviyoConnection):
    def __init__(self):
        super().__init__()

    def read_all_profiles(self):
        endpoint_base = "profiles"
        profiles_data = []
        page = 100
        cursor=''
        endpoint = endpoint_base + f'/?page[cursor]={cursor}&page[size]={page}'
        count = 100

        while True:            
            try:
                response = self.make_request(endpoint, method='GET')
                url = response['links']['next']
                endpoint = self.extract_endpoint_and_params(url)
                profiles = response.get("data", [])
                print(count)
                count += 100
                
                if not profiles:
                    break
                
                profiles_data.extend(profiles)
            
            except Exception as e:
                print(f"Error occurred: {str(e)}")
                break

        df = pd.DataFrame(profiles_data)
        df.to_csv('/home/snparada/Spacionatural/Data/Historical/historic_customers_klaviyo.csv', index=False)
        
        return df

    def read_profile_by_id(self, profile_id):
        endpoint = f"v1/person/{profile_id}"
        return self.make_request(endpoint)


    def update_profile_by_id(self, profile_id, email=None, phone_number=None, external_id=None,
                            first_name=None, last_name=None, organization=None, title=None,
                            image=None, location=None, preferencias= None, ticket_classification= None, num_ps_orders = None, 
                            last_purchase = None, monetary_classification = None):
        url = f"profiles/{profile_id}"

        payload = {
            "data": {
                "type": "profile",
                "id": profile_id,
                "attributes": {}
            }
        }

        if email:
            payload["data"]["attributes"]["email"] = email
        if phone_number:
            payload["data"]["attributes"]["phone_number"] = phone_number
        if external_id:
            payload["data"]["attributes"]["external_id"] = external_id
        if first_name:
            payload["data"]["attributes"]["first_name"] = first_name
        if last_name:
            payload["data"]["attributes"]["last_name"] = last_name
        if organization:
            payload["data"]["attributes"]["organization"] = organization
        if title:
            payload["data"]["attributes"]["title"] = title
        if image:
            payload["data"]["attributes"]["image"] = image
        if location:
            payload["data"]["attributes"]["location"] = location

        if preferencias:
            properties = {}
            properties['Preferencias'] = json.loads(preferencias)  # Convierte la cadena JSON en un diccionario
            payload["data"]["attributes"]["properties"] = properties

        if num_ps_orders:
            if properties:
                properties['ps_num_orders'] = num_ps_orders
                payload["data"]["attributes"]["properties"] = properties
            else:
                properties = {}
                properties['ps_num_orders'] = num_ps_orders
                payload["data"]["attributes"]["properties"] = properties

        if last_purchase:
            if properties:
                properties['ps_last_purchase'] = last_purchase
                payload["data"]["attributes"]["properties"] = properties
            else:
                properties = {}
                properties['ps_last_purchase'] = num_ps_orders
                payload["data"]["attributes"]["properties"] = properties

        if monetary_classification:
            if properties:
                properties['ps_monetary_clasification'] = monetary_classification
                payload["data"]["attributes"]["properties"] = properties
            else:
                properties = {}
                properties['ps_monetary_clasification'] = num_ps_orders
                payload["data"]["attributes"]["properties"] = properties


        if ticket_classification:
            if properties:
                properties['ps_ticket_clasification'] = ticket_classification
                payload["data"]["attributes"]["properties"] = properties
            else:
                properties = {}
                properties['ps_ticket_clasification'] = num_ps_orders
                payload["data"]["attributes"]["properties"] = properties


        # Verificar si hay atributos para actualizar
        if not payload["data"]["attributes"]:
            print("No hay atributos para actualizar.")
            return None
        
        response = self.make_request(url, method='PATCH', data=payload)
        return response
