"""
Helper commands for a datalake
"""

import os
import time
import uuid
import pandas
from neuro_python import home_directory
from neuro_python.neuro_call import neuro_call
from neuro_python.neuro_data import schema_manager as sm
from neuro_python.neuro_data import source_sink as ss
from neuro_python.neuro_data import stream_table as st

def delete_datalake_file(store_name: str, table_name: str, file_name_including_partition: str):
    """
    Delete a file from a processed datalake table in Neuroverse
    """
    table_def = sm.get_table_definition(store_name, table_name)
    schema_type = list(sm.SCHEMA_TYPE_MAP.keys())[list(sm.SCHEMA_TYPE_MAP.values()).index(table_def["SchemaType"])]
    file_path = "/managed/" + schema_type + "/table/" + table_name + "/"
    file_path = file_path.lower()
    file_path += file_name_including_partition.strip('/')

    request = {"DataStoreName" : store_name, "TableName" : table_name, "FilePath" : file_path}
    response = neuro_call("80", "DataMovementService", "DataLakeDeleteFile", request)

    check_request = {"JobId" : response["JobId"]}
    status = 0
    errormsg = ""
    while status == 0:
        time.sleep(1)
        response_c = neuro_call("80", "DataMovementService", "CheckJob", check_request)
        status = response_c["Status"]
        if status > 1:
            errormsg = response_c["Message"]

    neuro_call("80", "DataMovementService", "FinaliseJob", check_request)

    if status != 1:
        raise Exception("Neuroverse error: " + errormsg)

    return {"JobId" : response["JobId"], "TimeStamp" : response["TimeStamp"]}

def list_datalake_table_files_with_partitions(store_name: str, table_name: str):
    """
    List all the files associated with a datalake file in Neuroverse
    """
    request = {"DataStoreName" : store_name, "TableName" : table_name}
    files = neuro_call("80", "DataMovementService", "ListDataLakeTableFiles", request)["Files"]
    return_list = []
    for file in files:
        return_list.append(file.split(table_name.lower())[1])
    return return_list

def list_datalake_table_directory_items(store_name: str, table_name: str, directory_path: str):
    """
    List all the items associated with a directory in a table in a datalake file in Neuroverse
    """
    table_def = sm.get_table_definition(store_name, table_name)
    schema_type = list(sm.SCHEMA_TYPE_MAP.keys())[list(sm.SCHEMA_TYPE_MAP.values()).index(table_def["SchemaType"])]
    directory_path = "/managed/" + schema_type + "/table/" + table_name + "/" + directory_path
    request = {"DataStoreName" : store_name, "TableName" : table_name, "DirectoryPath" : directory_path.lower()}
    items = neuro_call("80", "DataMovementService", "ListDataLakeTableDirectoryItems", request)["Items"]
    return_list = []
    for item in items:
        return_list.append(item.split(table_name.lower())[1])
    return return_list

def get_lines_in_datalake_csv(store_name: str, table_name: str, file_name_including_partition: str):
    """
    Get the number of lines for a file in a datalake
    """
    table_def = sm.get_table_definition(store_name, table_name)
    schema_type = list(sm.SCHEMA_TYPE_MAP.keys())[list(sm.SCHEMA_TYPE_MAP.values()).index(table_def["SchemaType"])]
    file_path = "/managed/" + schema_type + "/table/" + table_name + "/"
    file_path = file_path.lower()
    file_path += file_name_including_partition.strip('/')

    request = {"DataStoreName" : store_name, "TableName" : table_name, "FilePath" : file_path}
    response = neuro_call("80", "DataMovementService", "GetLinesInDataLakeCsvFile", request)

    check_request = {"JobId" : response["JobId"]}
    status = 0
    errormsg = ""
    while status == 0:
        time.sleep(1)
        response_c = neuro_call("80", "DataMovementService", "CheckJob", check_request)
        status = response_c["Status"]
        errormsg = response_c["Message"]

    neuro_call("80", "DataMovementService", "FinaliseJob", check_request)

    if status != 1:
        raise Exception("Neuroverse error: " + errormsg)

    return int(errormsg)

def rechunk_datalake_csv(store_name: str, from_table_name: str, file_name_including_partition: str, to_table_name: str):
    """
    Split a datalake table's csv file in files with less than 1 million rows.
    This allows to complete files to be streamed through the DataMovement Service.
    """
    table_def = sm.get_table_definition(store_name, from_table_name)
    schema_type = list(sm.SCHEMA_TYPE_MAP.keys())[list(sm.SCHEMA_TYPE_MAP.values()).index(table_def["SchemaType"])]
    file_path = "/managed/" + schema_type + "/table/" + from_table_name + "/"
    file_path = file_path.lower()
    file_path += file_name_including_partition.strip('/')

    request = {"FromDataStoreName" : store_name, "FromTableName" : from_table_name,
               "FilePath" : file_path, "ToTableName" : to_table_name}
    response = neuro_call("80", "DataMovementService", "DataLakeReChunkCsvFile", request)

    check_request = {"JobId" : response["JobId"]}
    status = 0
    errormsg = ""
    while status == 0:
        time.sleep(1)
        response_c = neuro_call("80", "DataMovementService", "CheckJob", check_request)
        status = response_c["Status"]
        if status > 1:
            errormsg = response_c["Message"]

    neuro_call("80", "DataMovementService", "FinaliseJob", check_request)

    if status != 1:
        raise Exception("Neuroverse error: " + errormsg)

    outfiles=[]

    files=list_datalake_table_files_with_partitions(store_name,to_table_name)

    for file in files:
        if response["JobId"] in file:
            outfiles.append(file)

    return outfiles

def datalake_to_csv(store_name: str, table_name: str, file_name_including_partition: str, file_name: str, data_start_row:str = 2):
    """
    Move a file in a datalake into a csv in your notebook environment
    """
    #Get table schema
    table_def = sm.get_table_definition(store_name, table_name)
    column_names = []
    column_types = []
    table_def["DestinationTableDefinitionColumns"].sort(key=lambda x: x['Index'])
    for col in table_def["DestinationTableDefinitionColumns"]:
        column_name = col["ColumnName"]
        column_data_type = DATA_TYPE_MAP_REV[col['ColumnDataType']]
        if "String" in column_data_type:
            column_data_type += "(" + str(col["ColumnDataTypeSize"]) + ")"
        elif "Decimal" in column_data_type:
            column_data_type += "(" + str(col["ColumnDataTypePrecision"]) + "," + str(col["ColumnDataTypeScale"]) +")"
        column_names.append(column_name)
        column_types.append(column_data_type)
    source=ss.csv_datalake_source_parameters(store_name,table_name,file_name_including_partition,data_start_row)
    sink=ss.csv_notebook_sink_parameters(file_name,column_names,column_types)
    st.stream(source,sink)
    return None

def datalake_to_df(store_name: str, table_name: str, file_name_including_partition: str, data_start_row:str = 2):
    """
    Load datalake file into a dataframe
    """
    if not os.path.exists(home_directory()+"/tmp"):
        os.makedirs(home_directory()+"/tmp")

    file_name = str(uuid.uuid4()) + ".csv"

    count = len(os.getcwd().replace(home_directory(), "").split('/'))-1

    backs = ""
    for c in range(0, count):
        backs += "../"
    datalake_to_csv(store_name, table_name,file_name_including_partition,data_start_row, backs + "tmp/" + file_name)

    df = pandas.read_csv(home_directory() + "/" + "tmp/" + file_name)
    os.remove(home_directory() + "/" + "tmp/" + file_name)
    df = df.drop(columns=['NeuroverseLastModified'])
    return df
