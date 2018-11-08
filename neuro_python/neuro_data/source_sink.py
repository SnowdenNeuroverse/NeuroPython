"""
Tabular source and sink parameter constructors
"""

import os
from neuro_python import home_directory
from neuro_python.neuro_data import schema_manager as sm

def sinktosource(sink, response):
    """
    Convert a sink parameter object and stream response into a source parameter object
    """
    if sink["Type"] == "Sql":
        return sql_source_parameters(sink["DataStoreName"], sink["TableName"])
    elif sink["Type"] == "CsvNotebookFileShare":
        return csv_notebook_source_parameters(sink["FileName"], sink["Headers"],
                                                        sink["Types"], sink["DataStartRow"])
    elif sink["Type"] == "CsvDataLake":
        file_name_including_partition = sink["FolderPath"] + "/" + response["JobId"]
        file_name_including_partition += "_" + response["TimeStamp"].replace(":", "-").replace(".", "-")
        file_name_including_partition += ".csv"
        file_name_including_partition = file_name_including_partition.split(sink["TableName"].lower())[1]
        return csv_datalake_source_parameters(sink["DataStoreName"], sink["TableName"], file_name_including_partition,
                                              2)
    else:
        raise Exception("Sink type: " + sink["Type"] + " not supported")

#Sql
def sql_source_parameters(store_name: str, table_name: str, sql_where_clause: str = None):
    """
    Source table in a sql data store.
    sql_where_clause runs a where function in the sql database before moving the data
    """
    return {"Type" : "Sql", "DataStoreName" : store_name, "TableName" : table_name, "SqlWhereClause" : sql_where_clause}

def sql_sink_parameters(store_name: str, table_name: str, expressions: "List[str]" = None, where_clause: str = None):
    """
    Sink table in a sql data store.
    expressions: a list of expressions of equal length to the column count. Column names can be used to access to the value of the column on that row.
    ROW and RANDOM are available for use in the expressions. An empty string will not recalculate the column.
    where_clause: a clause to determine whether a row is streamed. Column names can be used to access to the value of the column on that row.
    ROW and RANDOM are available for use in the where_clause.
    """
    return {"Type" : "Sql", "DataStoreName" : store_name, "TableName" : table_name, "Expressions" : expressions, "WhereClause" : where_clause}



def csv_notebook_source_parameters(file_name: str, headers: "List[str]", column_data_types: "List[str]", data_start_row: int):
    """
    Source csv file in a notebook session.
    Column data types can be found through neuro_python.neuro_data.schema_manager.get_column_data_types()
    """
    if not os.path.isfile(file_name):
        raise Exception("File doesn't exist")

    file_name = (os.getcwd().replace(home_directory(), "") + "/" + file_name).strip('/')
    path=[]
    for dir in file_name.split('/'):
        if dir=="..":
            path.pop()
        else:
            path.append(dir)
    file_name='/'.join(path)
    file_name

    return {"Type" : "CsvNotebookFileShare", "FileName" : file_name,
            "Headers" : headers, "Types" : column_data_types, "DataStartRow" : data_start_row}

def csv_notebook_sink_parameters(file_name: str, headers: "List[str]", column_data_types: "List[str]",
                                           expressions: "List[str]" = None, where_clause: str = None):
    """
    Sink csv file in a notebook session.
    If the file exists the data will be appended otherwise a new file will be created and the headers inserted.
    Column data types can be found through neuro_python.neuro_data.schema_manager.get_column_data_types()
    expressions: a list of expressions of equal length to the column count. Column names can be used to access to the value of the column on that row.
    ROW and RANDOM are available for use in the expressions. An empty string will not recalculate the column.
    where_clause: a clause to determine whether a row is streamed. Column names can be used to access to the value of the column on that row.
    ROW and RANDOM are available for use in the where_clause.
    """
    file_name = (os.getcwd().replace(home_directory(), "") + "/" + file_name).strip('/')
    path=[]
    for dir in file_name.split('/'):
        if dir=="..":
            path.pop()
        else:
            path.append(dir)
    file_name='/'.join(path)
    file_name
    return {"Type" : "CsvNotebookFileShare", "FileName" : file_name, "Headers" : headers,
            "Types" : column_data_types, "Expressions" : expressions, "WhereClause" : where_clause}

def csv_datalake_source_parameters(store_name: str, table_name: str, file_name_including_partition: str,
                                   data_start_row: str):
    """
    Source file from datalake
    The datalake tables are partitioned into files. The files can be found through
    """
    table_def = sm.get_table_definition(store_name, table_name)
    schema_type = list(sm.SCHEMA_TYPE_MAP.keys())[list(sm.SCHEMA_TYPE_MAP.values()).index(table_def["SchemaType"])]
    file_path = "/managed/" + schema_type + "/table/" + table_name + "/"
    file_path = file_path.lower()
    file_path += file_name_including_partition.strip('/')
    return {"Type" : "CsvDataLake", "DataStoreName" : store_name, "TableName" : table_name,
            "FileName" : file_path, "DataStartRow" : data_start_row}

def csv_datalake_sink_parameters(store_name: str, table_name: str, partition_path: str,
                                 expressions: "List[str]" = None, where_clause: str = None):
    """
    Define a datalake sink.
    A new file will be generated under the partition path
    """
    table_def = sm.get_table_definition(store_name, table_name)
    schema_type = list(sm.SCHEMA_TYPE_MAP.keys())[list(sm.SCHEMA_TYPE_MAP.values()).index(table_def["SchemaType"])]
    folder_path = "/managed/" + schema_type + "/table/" + table_name + "/"
    folder_path = folder_path.lower()
    folder_path += partition_path.strip('/')

    return {"Type" : "CsvDataLake", "DataStoreName" : store_name, "TableName" : table_name, "FolderPath" : folder_path,
            "Expressions" : expressions, "WhereClause" : where_clause}
