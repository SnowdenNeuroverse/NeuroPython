"""
The spark_manager module provides functions to interact with the
Spark Manager in Neuroverse.
"""

def script_parameter(name: str, value):
    """
    Cmd line parameter value to be used in the pyspark script. eg sys.argv[1]
    """
    return {"Name":name,"Value":value}
  
def import_table(dataframe_name: str, data_store_name: str, table_name: str, partition_paths: "List[str]"):
    """
    Neuroverse datalake data to be used in the pyspark script.
    partition_paths contains a list of string that can be feed into the python eval function and return a str.
    This allows for the partition that is used in a spark job run to be determined at runtime (eg. using the current datetime).
    If you want to use a constant partition path string must be wrapped in quotes. eg "'/2018/1'"
    """
    for path in partition_paths:
        if not isinstance(eval(path), str):
            raise Exception("A string is not returned when evaluating: " + path)
    return {"SparkDataFrameName":dataframe_name, "DataStoreName":data_store_name, "TableName":table_name, "PartitionPaths":partition_paths}
  
def export_table(dataframe_name: str, data_store_name: str, table_name: str, partition_path: str):
    """
    pyspark script data to be outputed into a Neuroverse datalake.
    partition_path contains string that can be feed into the python eval function and return a str.
    This allows for the partition that is used in a spark job run to be determined at runtime (eg. using the current datetime).
    If you want to use a constant partition path string must be wrapped in quotes. eg "'/2018/1'"
    """
    if not isinstance(eval(partition_path), str):
        raise Exception("A string is not returned when evaluating: " + partition_path)
    return {"SparkDataFrameName":dataframe_name, "DataStoreName":data_store_name, "TableName":table_name, "PartitionPath":partition_path}

def submit_job(job_name: str, pyspark_script: str,
               script_parameters: "List[script_parameter]" = None,
               import_tables: "List[import_table]" = None,
               export_tables: "List[export_table]" = None,
               workspace_id: str = None, cluster_id: str = None,
               run_retry: bool = None, max_concurrent_runs: int = None):
    """
    Submit a spark job (template) and recieve back the JobId
    """
    return neuro_call("80", "sparkmanager", "submitjob", 
                                     {
                                       "JobName" : job_name,
                                       "Script" : pyspark_script,
                                       "ScriptLanguage" : 0,
                                       "ScriptParameters" : script_parameters,
                                       "ImportTables" : import_tables,
                                       "ExportTables" : export_tables,
                                       "WorkspaceId" : workspace_id,
                                       "ClusterId" : cluster_id,
                                       "RunRetry" : run_retry,
                                       "MaxConcurrentRuns" : max_concurrent_runs
                                     }
                                    )
    
