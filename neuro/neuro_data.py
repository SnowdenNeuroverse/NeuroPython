import requests
import json
import os
from pathlib import Path
import time
import pandas
#private classes and methods
class Neuro_Data:
    def __init__(self):
        self.token = os.environ['JUPYTER_TOKEN']
        self.domain = os.environ['NV_DOMAIN'] + ':8082/NeuroApi/datamovementservice/api/datamovement/'
        self.home_dir = '/home/jovyan/session/'

    class SqlJoin:
        def __init__(self, JoinType, JoinTableName, JoinSubQuery, JoinAlias, JoinClause):
            self.JoinType = JoinType
            self.JoinTableName = JoinTableName
            self.JoinSubQuery = JoinSubQuery
            self.JoinAlias = JoinAlias
            self.JoinClause = JoinClause

    def sql_join(self,join_type=None,table_name=None,sub_query=None,alias=None,clause=None):
        return SqlJoin(join_type,table_name,sub_query,alias,clause)

    class SqlSourceDefinition:
        def __init__(self, SelectClause, FromTableName, FromSubQuery, FromAlias, Joins, WhereClause, GroupByClause, HavingClause, OrderByClause):
            self.SourceMappingType = 1
            self.SelectClause = SelectClause
            self.FromTableName = FromTableName
            self.FromSubQuery = FromSubQuery
            self.FromAlias = FromAlias
            self.Joins = Joins
            self.WhereClause = WhereClause
            self.GroupByClause = GroupByClause
            self.HavingClause = HavingClause
            self.OrderByClause = OrderByClause

    def sql_query(self,select=None,table_name=None,sub_query=None,alias=None,joins=None,where=None,group_by=None,having=None,order_by=None):
        return SqlSourceDefinition(select,table_name,sub_query,alias,joins,where,group_by,having,order_by)

    class FileShareDestinationDefinition:
        def __init__(self, FolderPath):
            self.DestinationMappingType = 0
            if FolderPath != None:
                FolderPath=FolderPath.strip()
                if FolderPath.startswith("/"): FolderPath = FolderPath[1:]
                if FolderPath.startswith("\\"): FolderPath = FolderPath[1:]
                if (not FolderPath.endswith("/")) or (not FolderPath.endswith("\\")):
                    FolderPath = FolderPath + "/"
            self.FolderPath = FolderPath

    class TransferFromSqlToFileShareRequest:
        def __init__(self, FileShareDestinationDefinition, SqlSourceDefinition):
            self.FileShareDestinationDefinition = FileShareDestinationDefinition
            self.SqlSourceDefinition = SqlSourceDefinition

    def sql_to_file_share(self,transfer_from_sql_to_fileshare_request):
        url = self.domain + 'TransferFromSqlToFileShare'
        msg_data = json.dumps(transfer_from_sql_to_fileshare_request, default=lambda o: o.__dict__)
        msg_data_length = len(msg_data)
        headers = {'Content-Length' : str(msg_data_length), 'Token' : self.token}
        response = requests.post(url, headers=headers, data=msg_data)
        response_obj = response.json()
        if response_obj['Error'] != None:
            raise ValueError('Neuroverse error: ' + response_obj['Error'])
        file_path = self.home_dir + transfer_from_sql_to_fileshare_request.FileShareDestinationDefinition.FolderPath
        file_path = file_path + response_obj['FileName'] +'.info'
        my_file = Path(file_path)
        while 1==1:
            if my_file.is_file():
                time.sleep(0.25)
                with open(file_path) as json_data:
                    d = json.load(json_data)
                    if d['Error'] == None:
                        break
                    else:
                        raise ValueError('Neuroverse error: ' + d['Error'])
                os.remove(file_path)
            time.sleep(0.25)
        return response_obj['FileName']

    def sql_to_csv(self,folder_path=None,file_name=None,sql_query=None):
        fs=FileShareDestinationDefinition(folder_path)
        tr = TransferFromSqlToFileShareRequest(fs,sql_query)
        output_name=sql_to_file_share(tr)
        folder=self.home_dir + transfer_from_sql_to_fileshare_request.FileShareDestinationDefinition.FolderPath
        os.rename(folder + output_name, folder + file_name)
        return folder + file_name

    def sql_to_df(self,sql_query=None):
        fs=FileShareDestinationDefinition(None)
        tr = TransferFromSqlToFileShareRequest(fs,sql_query)
        output_name=sql_to_file_share(tr)
        folder=self.home_dir + transfer_from_sql_to_fileshare_request.FileShareDestinationDefinition.FolderPath
        df = pandas.read_csv(folder + output_name)
        os.remove(folder + output_name)
        return df
