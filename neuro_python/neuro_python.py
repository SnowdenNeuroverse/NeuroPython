import requests
import json
import os

#private classes and methods
class Neuro_Python:
    def __init__(self):
        self.token = os.environ['JUPYTER_TOKEN']
        if 'prd' in os.environ['NV_DOMAIN']:
            #this will need to be updated when the certificate expires
            self.domain = 'https://15ded47f-ef38-4ee3-b989-685820ca3d36.cloudapp.net'
        elif 'tst' in os.environ['NV_DOMAIN']:
            self.domain = 'https://launchau.snowdenonline.com.au'
        elif 'sit' in os.environ['NV_DOMAIN']:
            self.domain = 'https://neurosit.snowdenonline.com.au'
        else:
            self.domain = 'https://neurodev.snowdenonline.com.au'
        self.home_dir = '/home/jovyan/session/'
        
    def neuro_call(self,port,service,method,requestbody,timeout=1200):
        url = self.domain + ":8080/NeuroApi/" + port + "/" + service + "/api/" + service.lower().replace("service","") + "/" + method
        msg_data = json.dumps(requestbody, default=lambda o: o.__dict__)
        msg_data_length = len(msg_data)
        headers = {'Content-Length' : str(msg_data_length), 'Token' : self.token}
        response = requests.post(url, headers=headers, data=msg_data, verify=False)
        if response.status_code != 200:
            if response.status_code == 401:
                raise ValueError('Session has expired: Log into Neuroverse and connect to your Notebooks session or reload the Notebooks page in Neuroverse')
            else:
                raise ValueError('Neuroverse connection error: Http code ' + str(response.status_code))
        response_obj = response.json()
        return response_obj
