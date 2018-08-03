"""
The schema_manager module provides functions to interact with the
Schema Manager in Neuroverse
"""

import typing
from neuro_python.neuro_call import neuro_call

DATA_TYPE_MAP = {"Int" : 11, "Decimal" : 9, "String" : 14, "BigInt" : 1, "Boolean" : 3,
                 "DateTime" : 6, "UniqueIdentifier" : 22, "Int32" : 11, "Int64" : 1, "Double" : 10,
                 "Guid" : 22}
COL_TYPE_MAP = {"Key" : 1, "Value" : 4, "TimeStampKey" : 3, "ForeignKey" : 2}
SCHEMA_TYPE_MAP = {"DataIngestion" : 1, "TimeSeries" : 2, "Processed" : 3}

def index_definition(index_name: str, index_columns: typing.List[str]):
    """
    Object to create Sql table indexes in Neuroverse
    """
    columns = []
    for col in index_columns:
        columns.append({"ColumnName" : col})
    return {"IndexName" : index_name, "IndexColumns" : columns}

def column_definition(name: str, column_data_type: str, column_type: str, is_required: bool):
    """
    Object to create a column in a Neuroverse data store table
    """

    foreign_key_table_name = None
    foreign_key_column_name = None
    col_type_id = None

    if "ForeignKey" in column_type:
        col_type_id = COL_TYPE_MAP["ForeignKey"]
        foreign_key_table_name,foreign_key_column_name = column_type.split('(')[1].strip(')').split(',')
    else:
        col_type_id = COL_TYPE_MAP[column_type]

    data_type = None
    data_type_precision = None
    data_type_scale = None
    data_type_size = None
    if "String" in column_data_type:
        data_type = DATA_TYPE_MAP["String"]
        data_type_size = int(column_data_type.split('(')[1].strip(')'))
    elif "Decimal" in column_data_type:
        data_type = DATA_TYPE_MAP["Decimal"]
        data_type_precision,data_type_scale = list(map(int, column_data_type.
                                                       split('(')[1].strip(')').split(',')))
    else:
        data_type = DATA_TYPE_MAP[column_data_type]

    return {"ColumnName" : name, "ColumnType" : col_type_id, "WasRemoved" : False,
            "ForeignKeyColumnName" : foreign_key_column_name, "IsRequired" : is_required,
            "IsSystemColumn" : False, "ValidationError" : "",
            "ColumnDataType" : data_type, "ColumnDataTypePrecision" : data_type_precision,
            "ColumnDataTypeScale" : data_type_scale, "ColumnDataTypeSize" : data_type_size,
            "ForeignKeyTableName" : foreign_key_table_name}

def table_definition(name: str, columns: "List[table_column]", allow_data_changes: bool, schema_type: str,
                     table_indexes: "List[table_index]", partition_path: str):
    """
    Object to create a Neuroverse data store table
    """
    schema_type_id = None

    for ind in range(0, len(columns)):
        columns[ind]["Index"] = ind

    if schema_type_id is None:
        if schema_type == "DataIngestion":
            schema_type_id = 1
        elif schema_type == "TimeSeries":
            schema_type_id = 2
        elif schema_type == "Processed":
            schema_type_id = 3
        else:
            raise Exception("schematype must be \"DataIngestion\", \"TimeSeries\" or \"Processed\"")

    return {"DestinationTableDefinitionId" : "", "AllowDataLossChanges" : allow_data_changes,
            "DestinationTableDefinitionColumns" : columns,
            "DestinationTableDefinitionIndexes" : table_indexes,
            "DestinationTableName" : name, "DataStoreId" : None, "SchemaType" : schema_type_id,
            "FilePath" : partition_path}

def sql_table_definition(name: str, columns: "List[table_column]", allow_data_changes: bool, schema_type: str,
                         table_indexes: "List[table_index]"):
    """
    Object to create a Neuroverse data store sql table
    """
    return table_definition(name, columns, allow_data_changes, schema_type, table_indexes, "")

def datalake_table_definition(name: str, columns: "List[table_column]", schema_type: str,
                              partition_path: str):
    """
    Object to create a Neuroverse data store datalake table
    """
    return table_definition(name, columns, False, schema_type, [], partition_path)

def create_table(store_name: str, table_definition: "table_definition"):
    data_store_id = ""
    try:
        data_store_id = neuro_call("80", "datastoremanager", "GetDataStores", {"StoreName" : store_name})["DataStores"][0]["DataStoreId"]
    except:
        raise Exception("Data Store name is not valid")
    table_definition["DataStoreId"] = data_store_id
    neuro_call("8080", "datapopulationservice", "CreateDestinationTableDefinition", table_definition)
