"""
Helper sql commands
"""

import time
import os
import uuid
import pandas
import pyodbc
from neuro_python import home_directory
from neuro_python.neuro_call import neuro_call
import sqlalchemy as db

def transformation(store_name: str, sql_query: "sql_query", sink_table_name: str):
    """
    Execute a sql query on a database and store the results in another table in the same database
    """
    request = {"SqlTransformationParameters" : {"DataStoreName" : store_name, "SqlQuery" : sql_query},
               "SinkTableName" : sink_table_name}
    response = neuro_call("80", "DataMovementService", "SqlTransformation", request)

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

def delete_rows(store_name: str, table_name: str, where_clause: str = None):
    """
    Delete rows of a sql table using a where clause. If no where clause is supplied all rows are deleted
    """
    request = {"DataStoreName" : store_name, "TableName" : table_name,
               "WhereClause" : where_clause}
    response = neuro_call("80", "DataMovementService", "SqlDelete", request)

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

    return None

def sql_to_csv(store_name: str, sql_query: "sql_query", file_name: str):
    """
    Execute a sql query and have the result put in a csv file in your notebook session
    """
    file_name = (os.getcwd().replace(home_directory(), "") + "/" + file_name).strip('/')

    path_list = file_name.split('/')

    indices = [i for i, x in enumerate(path_list) if x == ".."]
    new_indices = []

    for ind in indices:
        new_indices.append(ind-1)
        new_indices.append(ind)

    new_path_list = []
    for i in range(0,len(path_list)):
        if i not in new_indices:
            new_path_list.append(path_list[i])

    file_name = "/".join(new_path_list)

    request = {"SqlParameters" : {"DataStoreName" : store_name, "SqlQuery" : sql_query},
               "FileName" : file_name}
    response = neuro_call("80", "DataMovementService", "SqlQueryToCsvNotebookFileShare", request)

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

    return None

def build_sql(sql_query: "sql_query"):
    query=''
    query+='select '+sql_query['SelectClause']
    if sql_query['FromTableName'] is not None:
        query+=' from '+sql_query['FromTableName']
    else:
        query+=' from ('+build_sql(sql_query['FromSubQuery'])+')'
    if sql_query['FromAlias'] is not None:
         query+=' '+sql_query['FromAlias']
    #joins
    if sql_query['Joins'] is not None:
        for join in sql_query['Joins']:
            if join['JoinType'] is not None:
                query+=' '+ join['JoinType'] +' join'
            else:
                query+=' join'
            if join['JoinTableName'] is not None:
                query+=' '+join['JoinTableName']
            else:
                query+=' ('+build_sql(join['JoinSubQuery'])+')'
            query+=' as '+join['JoinAlias']
            query+=' on '+join['JoinClause']
    if sql_query['WhereClause'] is not None:
        query+=' where '+sql_query['WhereClause']
    if sql_query['GroupByClause'] is not None:
        query+=' group by '+sql_query['GroupByClause']
    if sql_query['HavingClause'] is not None:
        query+=' having '+sql_query['HavingClause']
    if sql_query['OrderByClause'] is not None:
        query+=' order by '+sql_query['OrderByClause']
    return query

def sql_to_df(store_name: str, sql_query: "sql_query",use_pyodbc=True):
    """
    Execute a sql query and have the result put into a pandas dataframe in the notebook
    """
    if use_pyodbc:
        connstrbits=neuro_call('80','datastoremanager','GetDataStores',{'StoreName':store_name})['DataStores'][0]['ConnectionString'].split(';')
        server=connstrbits[0].split(':')[1].split(',')[0]
        database=connstrbits[1].split('=')[1]
        username=database
        password=connstrbits[3].split('=')[1]
        driver= '{ODBC Driver 13 for SQL Server}'
        with pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password) as cnxn:
            with cnxn.cursor() as cursor:
                return pandas.read_sql(build_sql(sql_query),cnxn)
    else:
        if not os.path.exists(home_directory()+"/tmp"):
            os.makedirs(home_directory()+"/tmp")

        file_name = str(uuid.uuid4()) + ".csv"

        count = len(os.getcwd().replace(home_directory(), "").split('/'))-1

        backs = ""
        for c in range(0, count):
            backs += "../"
        sql_to_csv(store_name, sql_query, backs + "tmp/" + file_name)

        df = pandas.read_csv(home_directory() + "/" + "tmp/" + file_name)
        os.remove(home_directory() + "/" + "tmp/" + file_name)
        return df
def df_to_sql(store_name: str,table_name: str, data: "pandas.DataFrame"):
    connstrbits=neuro_call('80','datastoremanager','GetDataStores',{'StoreName':store_name})['DataStores'][0]['ConnectionString'].split(';')
    server=connstrbits[0].split(':')[1].split(',')[0]
    database=connstrbits[1].split('=')[1]
    domain=server.split('.')[0]
    username=database
    password=connstrbits[3].split('=')[1]
    driver= 'ODBC Driver 13 for SQL Server'
    engine = db.create_engine('mssql+pyodbc://%s@%s:%s@%s:1433/%s?driver=%s'%(username,domain,password,server,database,driver), echo=False)
    data.to_sql(table_name, engine, if_exists='append', index=False)
    
    
