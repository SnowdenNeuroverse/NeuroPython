from neuro_python.neuro_call import neuro_call
from enum import Enum

class DateTimeLevels(Enum):
    NA = 0
    Year = 1
    Month = 2
    Day = 3
    Hour = 4

class PartitionIdLevels(Enum):
    NA = 0
    Top = 1
    Bottom = 2

def create_event_hub_namespace(name:str):
    request={"NameSpaceName":name}
    neuro_call("80", "endpointmanagement", "createnamespace", request)
    
def list_event_hub_namespaces():
    request={}
    return neuro_call("80", "endpointmanagement", "GetNameSpace", request)['EventHubNamespaces']
    
def delete_event_hub_namespace(name:str):
    #Require interactive
    check = input("Are you sure you want to delete %s (y/n)"%name)
    if check=='y':
        request={"NameSpaceName":name}
        neuro_call("80", "endpointmanagement", "DeleteNamespace", request)
        return "%s has been deleted"%name

def create_event_hub(namespace_name:str,event_hub_name:str):
    request = {'EndpointName':event_hub_name,
    'EndPointType':2,
    'NameSpaceName':namespace_name,
    'Description':'',
    'ScaleTierTypeId':0,
    'DataIngestionTypeId':0}
    neuro_call('80','endpointmanagement','CreateEndpoint',request)

def list_event_hubs(namespace_name:str):
    request = {}
    return [hub for hub in neuro_call('80','endpointmanagement','GetEndpoints',request)['EndPointInfo'] if hub['EndpointTypeId']==2 and hub['EventHubNamespace']==namespace_name]
    
def delete_event_hub(namespace_name:str,event_hub_name:str):
    endpoint = next(obj for obj in list_event_hubs(namespace_name)["EndPointInfo"] if obj["EventHubNamespace"]==namespace_name and obj["Name"]==event_hub_name) 
    #Require interactive
    check = input("Are you sure you want to delete %s:%s (y/n)"%(namespace_name,event_hub_name))
    if check=='y':
        request = {'EndPointId': endpoint['EndPointId']}
        neuro_call('80','endpointmanagement','DeleteEndpoint',request)
        return "%s:%s has been deleted"%(namespace_name,event_hub_name)

def create_update_event_hub_raw_data_capture(namespace_name:str,event_hub_name:str,datalake_name:str,
                                   datetime_partition_level:"DateTimeLevels"=DateTimeLevels.NA,
                                   partition_id_level:"PartitionByIdLevel"=PartitionIdLevels.NA,
                                   max_file_in_minutes:int=None,
                                   max_file_in_MB:int=None):
    endpoint = next(obj for obj in list_event_hubs(namespace_name) if obj["EventHubNamespace"]==namespace_name and obj["Name"]==event_hub_name and obj['EndpointTypeId']==2)
    datastore = neuro_call('80','datastoremanager','getdatastores',{"StoreName":datalake_name})['DataStores'][0]
    request = {'EndPointId': endpoint['EndPointId'],
    'DataStoreId':datastore['DataStoreId'],
    'PartitionByDateTimeLevel':datetime_partition_level.value,
    'PartitionByIdLevel':partition_id_level.value,
    'FileTimeMinutesMax': max_file_in_minutes,
    'FileSizeMBMax': max_file_in_MB}
    neuro_call('80','endpointmanagement','PutRawData',request)
    
def delete_event_hub_raw_data_capture(namespace_name:str,event_hub_name:str):
    endpoint = next(obj for obj in list_event_hubs(namespace_name) if obj["EventHubNamespace"]==namespace_name and obj["Name"]==event_hub_name and obj['EndpointTypeId']==2) 
    #Require interactive
    check = input("Are you sure you want to delete data capture on %s:%s (y/n)"%(namespace_name,event_hub_name))
    if check=='y':
        request = {'EndPointId': endpoint['EndPointId']}
        neuro_call('80','endpointmanagement','DeleteRawData',request)
        return "Data capture on %s:%s has been deleted"%(namespace_name,event_hub_name)

def create_iot_hub(iot_hub_name:str):
    request = {'EndpointName':iot_hub_name,
    'EndPointType':1,
    'Description':'',
    'ScaleTierTypeId':0,
    'DataIngestionTypeId':0}
    neuro_call('80','endpointmanagement','CreateEndpoint',request)

def list_iot_hubs():
    request = {}
    return [hub for hub in neuro_call('80','endpointmanagement','GetEndpoints',request)['EndPointInfo'] if hub['EndpointTypeId']==1]
    
def delete_iot_hub(iot_hub_name:str):
    endpoint = next(obj for obj in list_event_hubs()["EndPointInfo"] if obj["Name"]==iot_hub_name and obj['EndpointTypeId']==1)
    #Require interactive
    check = input("Are you sure you want to delete %s (y/n)"%iot_hub_name)
    if check=='y':
        request = {'EndPointId': endpoint['EndPointId']}
        neuro_call('80','endpointmanagement','DeleteEndpoint',request)
        return "%s has been deleted"%iot_hub_name

def create_iot_hub_device(iot_hub_name:str,device_name:str):
    endpoint = next(obj for obj in list_event_hubs()["EndPointInfo"] if obj["Name"]==iot_hub_name and obj['EndpointTypeId']==1) 
    request = {'EndpointId':endpoint['EndPointId'],
    'DeviceId':device_name}
    neuro_call('80','endpointmanagement','RegisterDevice',request)

def list_iot_hub_devices(iot_hub_name:str):
    endpoint = next(obj for obj in list_event_hubs()["EndPointInfo"] if obj["Name"]==iot_hub_name and obj['EndpointTypeId']==1) 
    request = {'EndpointId':endpoint['EndPointId']}
    neuro_call('80','endpointmanagement','GetRegisterDevices',request)["DeviceInfo"]
    
def delete_iot_hub_devce(iot_hub_name:str,device_name:str):
    endpoint = next(obj for obj in list_event_hubs()["EndPointInfo"] if obj["Name"]==iot_hub_name and obj['EndpointTypeId']==1)
    #Require interactive
    check = input("Are you sure you want to delete %s:%s (y/n)"%(iot_hub_name,device_name))
    if check=='y':
        request = {'EndpointId':endpoint['EndPointId'],'DeviceId':device_name}
        neuro_call('80','endpointmanagement','DeregisterDevice',request)
        return "%s:%s has been deleted"%(iot_hub_name,device_name)
   
    
def create_update_iot_hub_raw_data_capture(iot_hub_name:str,datalake_name:str,
                                   datetime_partition_level:"DateTimeLevels"=DateTimeLevels.NA,
                                   partition_id_level:"PartitionByIdLevel"=PartitionIdLevels.NA,
                                   max_file_in_minutes:int=None,
                                   max_file_in_MB:int=None):
    endpoint = next(obj for obj in list_event_hubs()["EndPointInfo"] if obj["Name"]==iot_hub_name and obj['EndpointTypeId']==1)
    datastore = neuro_call('80','datastoremanager','getdatastores',{"StoreName":datalake_name})['DataStores'][0]
    request = {'EndPointId': endpoint['EndPointId'],
    'DataStoreId':datastore['DataStoreId'],
    'PartitionByDateTimeLevel':datetime_partition_level.value,
    'PartitionByIdLevel':partition_id_level.value,
    'FileTimeMinutesMax': max_file_in_minutes,
    'FileSizeMBMax': max_file_in_MB}
    neuro_call('80','endpointmanagement','PutRawData',request)
    
def delete_iot_hub_raw_data_capture(iot_hub_name:str):
    endpoint = next(obj for obj in list_event_hubs()["EndPointInfo"] if obj["Name"]==iot_hub_name and obj['EndpointTypeId']==1)
    #Require interactive
    check = input("Are you sure you want to delete data capture on %s (y/n)"%(iot_hub_name))
    if check=='y':
        request = {'EndPointId': endpoint['EndPointId']}
        neuro_call('80','endpointmanagement','DeleteRawData',request)
        return "Data capture on %s has been deleted"%(iot_hub_name)
    
