import sys
sys.path.append('/home/snparada/Spacionatural/Libraries/laudus_lib')
from api import LaudusAPI
import requests
import json
import pandas as pd
from datetime import datetime

class LaudusCollaborators(LaudusAPI):
    def __init__(self):
        super().__init__()
        self.header_authentication()
# CRUD

    def read_all_collaborators(self):
        url_collaborators = 'https://api.laudus.cl/HR/employees/list'

        parametros_lista = {
            "options": {
                "offset": 0,
                "limit": 0
            },
            "fields": [
                "employeeId",
                "firstName",
                "lastName1",
                "contractStartDate",
            ],
            "filterBy": [
                {
                    "field": "contractStartDate",
                    "operator": ">",
                    "value": "2015-12-31T23:59:59"
                }
            ]
        }

        collaborators_list = requests.post(
            url_collaborators, 
            headers=self.headers_auth, 
            json=parametros_lista
            )
        print(collaborators_list)
        collaborators_list_json = collaborators_list.json()
        df = pd.DataFrame(collaborators_list_json)

        return df
    
    def read_all_payrolls(self):
        url_collaborators = 'https://api.laudus.cl/HR/Payroll/list'

        parametros_lista = {
            "options": {
                "offset": 0,
                "limit": 0
            },
            "fields": [
                "date",
                "lines.employee.firstName",
                "lines.employee.lastName1",
                "lines.taxableIncomeUnemploymentInsurance",
                "lines.taxableIncomeLPE",
                "lines.taxableIncomeWithoutLPE",
                "remunerationAmounts.amount"
            ],
            "filterBy": [
                {
                    "field": "date",
                    "operator": ">",
                    "value": "2022-12-31T23:59:59"
                }
            ]
        }

        collaborators_list = requests.post(
            url_collaborators, 
            headers=self.headers_auth, 
            json=parametros_lista
            )
        print(collaborators_list)
        collaborators_list_json = collaborators_list.json()
        df = pd.DataFrame(collaborators_list_json)

        return df