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
    neuro_call("80", "endpointmanagement", "GetNameSpace", request)
    
def delete_event_hub_namespace(name:str):
    request={"NameSpaceName":name}
    neuro_call("80", "endpointmanagement", "GetNameSpace", request)

def create_event_hub(namespace_name:str,event_hub_name:str):
    request = {'EndpointName':event_hub_name,
    'EndPointType':2,
    'NameSpaceName':namespace_name,
    'Description':'',
    'ScaleTierTypeId':0,
    'DataIngestionTypeId':0}
    neuro_call('80','endpointmanagement','CreateEndpoint',request)

def list_event_hubs():
    request = {}
    neuro_call('80','endpointmanagement','GetEndpoints',request)
    
def delete_event_hub(namespace_name:str,event_hub_name:str):
    endpoint = next(obj for obj in list_event_hubs()["EndPointInfo"] if obj["EventHubNamespace"]==namespace_name and obj["Name"]==event_hub_name) 
    request = {'EndPointId': endpoint['EndPointId']}
    neuro_call('80','endpointmanagement','DeleteEndpoint',request)

def create_update_raw_data_capture(namespace_name:str,event_hub_name:str,datalake_name:str,
                                   datetime_partition_level:"DateTimeLevels"=DateTimeLevels.NA,
                                   partition_id_level:"PartitionByIdLevel"=PartitionIdLevels.NA,
                                   max_file_in_minutes:int=None,
                                   max_file_in_MB:int=None):
    endpoint = next(obj for obj in list_event_hubs()["EndPointInfo"] if obj["EventHubNamespace"]==namespace_name and obj["Name"]==event_hub_name)
    datastore = nc('80','datastoremanager','getdatastores',{"StoreName":datalake_name})['DataStores'][0]
    request = {'EndPointId': endpoint['EndPointId'],
    'DataStoreId':datastore['DataStoreId'],
    'PartitionByDateTimeLevel':datetime_partition_level.value,
    'PartitionByIdLevel':partition_id_level.value,
    'FileTimeMinutesMax': max_file_in_minutes,
    'FileSizeMBMax': max_file_in_MB}
    nc('80','endpointmanagement','PutRawData',request)
    
def delete_raw_data_capture(namespace_name:str,event_hub_name:str):
    endpoint = next(obj for obj in list_event_hubs()["EndPointInfo"] if obj["EventHubNamespace"]==namespace_name and obj["Name"]==event_hub_name) 
    request = {'EndPointId': endpoint['EndPointId']}
    neuro_call('80','endpointmanagement','DeleteRawData',request)
