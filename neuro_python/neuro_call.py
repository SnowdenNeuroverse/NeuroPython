"""
The neuro_call model contains the neuro_call function
"""
import os
import json
import requests
import urllib3
import neuro_python

def neuro_call_v2(service, method, requestbody, timeout=1200, retry=True, controller=None):
    """
    The neuro_call function provides a way of making authorised calls to the Neuroverse api
    """
    port="80"
    token = os.environ['JUPYTER_TOKEN']
    if 'prd' in os.environ['NV_DOMAIN']:
        #this will need to be updated when the certificate expires
        domain = 'https://24ef3f75-9b77-4240-b756-d682b7d12a83.cloudapp.net'
    elif 'tst' in os.environ['NV_DOMAIN']:
        domain = 'https://neuroqa.d3s.com.au'
    elif 'sit' in os.environ['NV_DOMAIN']:
        domain = 'https://neurosit.d3s.com.au'
    elif 'dev' in os.environ['NV_DOMAIN']:
        domain = 'https://neurodev.d3s.com.au'
    else:
        domain = 'http://localhost'

    if controller==None:
        url = domain + ":8080/NeuroApi/" + port + "/" + service + ".Api/api/"
        url += service + "/" + method
        if domain == "http://localhost":
            url = domain + ":8082/NeuroApi/" + port + "/" + service
            url += ".Api/api/" + controller + "/" + method
    else:
        url = domain + ":8080/NeuroApi/" + port + "/" + service + ".Api/api/"
        url += controller + "/" + method
        if domain == "http://localhost":
            url = domain + ":8082/NeuroApi/" + port + "/" + service
            url += ".Api/api/" + service + "/" + method
    msg_data = json.dumps(requestbody, default=lambda o: o.__dict__)
    msg_data_length = len(msg_data)
    headers = {'Content-Length' : str(msg_data_length), 'Token' : token}
    urllib3.disable_warnings()
    if neuro_python.debug_val:
        print(service)
        print(controller)
        print(method)
        print("Request")
        print(url)
        print(str(headers))
        print(msg_data)
    try:
        response = requests.post(url, headers=headers, data=msg_data, verify=False,
                                 timeout=timeout)
    except Exception as err:
        if retry:
            response = requests.post(url, headers=headers, data=msg_data, verify=False,
                                 timeout=timeout)
        else:
            raise err
    if neuro_python.debug_val:
        print("Response")
        print(response.status_code)
    try:
        response_obj = response.json()
    except:
        response_obj = None
    if neuro_python.debug_val:
        print(str(response_obj))
    if 300<=response.status_code or response.status_code<200:
        if retry and 600>response.status_code>500:
            response = requests.post(url, headers=headers, data=msg_data, verify=False,
                                     timeout=timeout)
        if 300<=response.status_code or response.status_code<200:
            raise Exception('Neuroverse error: Http code ' + str(response.status_code) +' Message: ' + str(response_obj))
    try:
        if response_obj["ErrorCode"] is not 0:
            errMsg=""
            if response_obj["Error"] is not None:
                errMsg=response_obj["Error"]
            raise Exception("Neuroverse Error(%s): " % str(response_obj["ErrorCode"]) + errMsg)
    except:
        pass
    return response_obj
    
def neuro_call(port, service, method, requestbody, timeout=1200, retry=True, controller=None):
    """
    The neuro_call function provides a way of making authorised calls to the Neuroverse api
    """
    token = os.environ['JUPYTER_TOKEN']
    if 'prd' in os.environ['NV_DOMAIN']:
        #this will need to be updated when the certificate expires
        domain = 'https://24ef3f75-9b77-4240-b756-d682b7d12a83.cloudapp.net'
    elif 'tst' in os.environ['NV_DOMAIN']:
        domain = 'https://neuroqa.d3s.com.au'
    elif 'sit' in os.environ['NV_DOMAIN']:
        domain = 'https://neurosit.d3s.com.au'
    elif 'dev' in os.environ['NV_DOMAIN']:
        domain = 'https://neurodev.d3s.com.au'
    else:
        domain = 'http://localhost'

    if controller==None:
        url = domain + ":8080/NeuroApi/" + port + "/" + service + "/api/"
        url += service.lower().replace("service", "") + "/" + method
        if domain == "http://localhost":
            url = domain + ":8082/NeuroApi/" + port + "/" + service
            url += "/api/" + service.lower().replace("service", "") + "/" + method
    else:
        url = domain + ":8080/NeuroApi/" + port + "/" + service + "/api/"
        url += controller + "/" + method
        if domain == "http://localhost":
            url = domain + ":8082/NeuroApi/" + port + "/" + service
            url += "/api/" + controller + "/" + method
    msg_data = json.dumps(requestbody, default=lambda o: o.__dict__)
    msg_data_length = len(msg_data)
    headers = {'Content-Length' : str(msg_data_length), 'Token' : token}
    urllib3.disable_warnings()
    if neuro_python.debug_val:
        print("Request")
        print(url)
        print(str(headers))
        print(msg_data)
    try:
        response = requests.post(url, headers=headers, data=msg_data, verify=False,
                                 timeout=timeout)
    except Exception as err:
        if retry:
            response = requests.post(url, headers=headers, data=msg_data, verify=False,
                                 timeout=timeout)
        else:
            raise err
    if neuro_python.debug_val:
        print("Response")
        print(response.status_code)
    if response.status_code != 200:
        if retry:
            response = requests.post(url, headers=headers, data=msg_data, verify=False,
                                     timeout=timeout)
        if response.status_code != 200:
            if response.status_code == 401:
                raise Exception("""
                Session has expired:
                Log into Neuroverse and connect to your Notebooks session or
                reload the Notebooks page in Neuroverse
                """)
            elif response.status_code == 404:
                raise Exception("""
                Session has expired:
                Log into Neuroverse and connect to your Notebooks session or
                reload the Notebooks page in Neuroverse
                """)
            else:
                raise Exception('Neuroverse connection error: Http code ' + str(response.status_code))
    try:
        response_obj = response.json()
        if neuro_python.debug_val:
            print(str(response_obj))
        errCode = response_obj["ErrorCode"]
    except Exception as err:
        if retry:
            response = requests.post(url, headers=headers, data=msg_data, verify=False,
                                     timeout=timeout)
            if response.status_code != 200:
                if response.status_code == 401:
                    raise Exception("""
                    Session has expired:
                    Log into Neuroverse and connect to your Notebooks session or
                    reload the Notebooks page in Neuroverse
                    """)
                elif response.status_code == 404:
                    raise Exception("""
                    Session has expired:
                    Log into Neuroverse and connect to your Notebooks session or
                    reload the Notebooks page in Neuroverse
                    """)
                else:
                    raise Exception('Neuroverse connection error: Http code ' + str(response.status_code))
            response_obj = response.json()
            errCode = response_obj["ErrorCode"]
        else:
            raise err
    if errCode is not 0:
        errMsg=""
        if response_obj["Error"] is not None:
            errMsg=response_obj["Error"]
        raise Exception("Neuroverse Error(%s): " % str(errCode) + errMsg)
    return response_obj
