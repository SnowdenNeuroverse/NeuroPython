from neuro_python.neuro_call import neuro_call
from enum import Enum

class DataStoreType(Enum):
    Sql = 2
    DataLakeGen2 = 7
    
class StoreTierType(Enum):
    Small = 1
    Medium = 2
    Large = 3

def create_data_store(data_store_name:str, data_store_type:"DataStoreType", data_store_tier:"StoreTierType"=StoreTierType.Small):
    request = {"StoreName":data_store_name, "DataStoreTypeId":data_store_type.value, "StoreTierTypeId":data_store_tier.value}
    neuro_call('80','datastoremanager','createdatastore',request)
    
def list_data_stores():
    return neuro_call('80','datastoremanager','getdatastores',{})['DataStores']

def delete_data_store(data_store_name:str):
    datastore = [ds for ds in list_data_stores()['DataStores'] if ds['StoreName']==data_store_name]
    #Require interactive
    check = input("Are you sure you want to delete %s (y/n)"%data_store_name)
    if check=='y':
        request = {'DataStoreId':datastore['DataStoreId']}
        neuro_call('80','datastoremanager','deletedatastore',request)
        return "%s has been deleted"%data_store_name
