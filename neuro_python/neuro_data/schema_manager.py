"""
The schema_manager module provides functions to interact with the
Schema Manager in Neuroverse
"""

import typing
import json
from neuro_python.neuro_call import neuro_call

DATA_TYPE_MAP = {"Int" : 11, "Decimal" : 9, "String" : 14, "BigInt" : 1, "Boolean" : 3,
                 "DateTime" : 6, "UniqueIdentifier" : 22, "Int32" : 11, "Int64" : 1, "Double" : 10,
                 "Guid" : 22}
COL_TYPE_MAP = {"Key" : 1, "Value" : 4, "TimeStampKey" : 3, "ForeignKey" : 2}
SCHEMA_TYPE_MAP = {"DataIngestion" : 1, "TimeSeries" : 2, "Processed" : 3}

def get_column_data_types():
    "Get available data types for columns in Neuroverse tabular data"
    return ["Boolean", "Int32", "Int64", "Decimal", "Double", "DateTime", "Guid", "String"]

def get_column_types():
    "Get available column types in Neuroverse tabular data"
    return list(COL_TYPE_MAP.keys())

def get_schema_types():
    "Get available schema types for Neuroverse tabular data"
    return list(SCHEMA_TYPE_MAP.keys())

def index_definition(index_name: str, index_column_names: typing.List[str]):
    """
    Object to create Sql table indexes in Neuroverse
    """
    columns = []
    for col in index_column_names:
        columns.append({"ColumnName" : col})
    return {"IndexName" : index_name, "IndexColumns" : columns}


def column_definition(name: str, column_data_type: str, column_type: str = "Value", is_required: bool = True):
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

def table_definition(columns: "List[table_column]", schema_type: str,
                     allow_data_changes: bool = False, partition_path: str = "", table_name: str = "table_name"):
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
    
    file_path = partition_path

    return {"DestinationTableDefinitionId" : "", "AllowDataLossChanges" : allow_data_changes,
            "DestinationTableDefinitionColumns" : columns,
            "DestinationTableDefinitionIndexes" : [],
            "DestinationTableName" : "", "DataStoreId" : None, "SchemaType" : schema_type_id,
            "FilePath" : file_path}

def create_table(store_name: str, table_name: str, table_def: "table_definition"):
    """
    Create a table in a Neuroverse data store
    """
    data_stores = neuro_call("80", "datastoremanager", "GetDataStores", {"StoreName" : store_name})
    if len(data_stores["DataStores"]) == 0:
        raise Exception("Data Store name is not valid")

    columns = []
    for col in table_def["DestinationTableDefinitionColumns"]:
        if col["ColumnName"] != "NeuroverseLastModified":
            column_name = col["ColumnName"]
            column_type = list(COL_TYPE_MAP.keys())[list(COL_TYPE_MAP.values()).index(col["ColumnType"])]
            if "ForeignKey" in column_type:
                column_type += "(" + col["ForeignKeyTableName"] + "," + col["ForeignKeyColumnName"] + ")"
            column_data_type = str(list(DATA_TYPE_MAP.keys())[list(DATA_TYPE_MAP.values()).index(col["ColumnDataType"])])
            if "String" in column_data_type:
                column_data_type += "(" + str(col["ColumnDataTypeSize"]) + ")"
            elif "Decimal" in column_data_type:
                column_data_type += "(" + str(col["ColumnDataTypePrecision"]) + "," + str(col["ColumnDataTypeScale"]) +")"
            is_required = col["IsRequired"]
            columns.append(column_definition(column_name, column_data_type, column_type, is_required))
    schema_type = list(SCHEMA_TYPE_MAP.keys())[list(SCHEMA_TYPE_MAP.values()).index(table_def["SchemaType"])]
    allow_data_changes = table_def["AllowDataLossChanges"]

    partition_path = ''
    if "/managed/"+schema_type.lower() in table_def["FilePath"]:
        path_list = table_def["FilePath"].split('/')
        partition_path = '/'.join(path_list[5:len(path_list)])

    table_def1 = table_definition(columns,schema_type,allow_data_changes,partition_path,table_name=table_name)

    table_def1["DataStoreId"] = data_stores["DataStores"][0]["DataStoreId"]
    table_def1["DestinationTableName"] = table_name

    neuro_call("8080", "datapopulationservice", "CreateDestinationTableDefinition", table_def1)

def get_table_definition(store_name: str, table_name: str):
    """
    Get an existing table definition for a table in a Neuroverse data store
    """
    data_stores = neuro_call("80", "datastoremanager", "GetDataStores", {"StoreName" : store_name})["DataStores"]
    if len(data_stores) == 0:
        raise Exception("Data store doesn't exist")

    table_defs = neuro_call("8080", "DataPopulationService", "GetDestinationTableDefinition", {"TableName" : table_name, "DataStoreId" : data_stores[0]["DataStoreId"]})
    if len(table_defs["DestinationTableDefinitions"]) == 0:
        raise Exception("Table doesn't exist")
    table_def = table_defs["DestinationTableDefinitions"][0]

    table_def["DestinationTableDefinitionIndexes"] = []
    
    table_def['DestinationTableDefinitionColumns'].sort(key=lambda y: y['Index'] )
    return table_def

def add_table_indexes(store_name: str, table_name: str, table_indexes: "List[index_definition]"):
    """
    Add indexes to a table in a Neuroverse SQL data store
    """
    table_def = get_table_definition(store_name, table_name)
    table_def["DestinationTableDefinitionIndexes"].append(table_indexes)
    neuro_call("8080", "datapopulationservice", "UpdateDestinationTableDefinition", table_def)

def save_table_definition(file_name: str, table_def: "table_definition"):
    """
    Save a table definintion to a file
    """
    json_data = json.dumps(table_def, default=lambda o: o.__dict__)
    def_file = open(file_name, "w+")
    def_file.write(json_data)
    def_file.close()

def load_table_definition(file_name: str):
    """
    Load a table definition from a file
    """
    return json.loads(open(file_name).read())

def create_stream_to_table_mapping(store_name: str, table_name: str, mapping_name: str,
                                   source_dest_name_pairs: "List[tuple]"):
    """
    Creates a mapping between a stream job and a data store table in Neuroverse
    """
    table_def = get_table_definition(store_name, table_name)
    table_columns = table_def["DestinationTableDefinitionColumns"]

    for col in table_columns:

        if len([x for x in source_dest_name_pairs if x[1] == col["ColumnName"]]) == 0:
            if col["IsRequired"]:
                raise Exception(col["ColumnName"] + " is a required column, please supply a mapping")

    column_pairs = []
    for pair in source_dest_name_pairs:
        col_def = next(i for i in table_columns if i["ColumnName"] == pair[1])
        column_pairs.append({"DestinationColumnInfo" : col_def,
                             "SourceColumnName" : pair[0],
                             "DestinationColumnName" : pair[1],
                             "IsMapped" : True})

    neuro_call("8080", "datapopulationservice", "CreateDataPopulationMapping",
               {"DestinationTableDefinitionId" : table_def["DestinationTableDefinitionId"],
                "MappingName" : mapping_name,
                "DataPopulationMappingSourceColumns" : column_pairs})

def delete_processed_table(store_name: str, table_name: str):
    """
    Delete a table with schema type "Processed" from a Neuroverse data store
    """
    table_def = get_table_definition(store_name, table_name)
    if table_def["SchemaType"] != 3:
        raise Exception("Table schema type is not processed")
    neuro_call("8080", "datapopulationservice", "DeleteDestinationTableDefinition",
               {"DestinationTableDefinitionId" : table_def["DestinationTableDefinitionId"]})
