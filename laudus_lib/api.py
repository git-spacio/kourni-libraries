import requests
import json

class LaudusAPI():
    def __init__(self):
        self.token_file='/home/snparada/Spacionatural/Libraries/laudus_lib/creds/laudusToken.json'

    def header_authentication(self):
        with open(self.token_file, 'r') as f:
                token = json.load(f)['token']

        auth_token = 'Bearer '+ token

        self.headers_auth = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Authorization': auth_token
        }
