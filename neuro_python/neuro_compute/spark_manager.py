"""
The spark_manager module provides functions to interact with the
Spark Manager in Neuroverse.
"""

from neuro_python.neuro_call import neuro_call
import uuid
import os

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

def remove_job(job_id: str):
    """
    Remove a spark manager job
    """
    neuro_call("80", "sparkmanager", "removejob", {"JobId":job_id})

def list_jobs(workspace_id: str = None, cluster_id: str = None, max_returned: int = None):
    """
    List the jobs submitted to spark manager
    """
    list_jobs_response = neuro_call("80", "sparkmanager", "listjobs", 
                                     {
                                         "WorkspaceId" : workspace_id,
                                         "ClusterId" : cluster_id,
                                         "NumberReturned" : max_returned
                                     }
                                   )
    return list_jobs_response["JobSummaries"]

def get_job_details(job_id: str):
    """
    Get details about a submitted job
    """
    get_job_details_response = neuro_call("80", "sparkmanager", "getjobdetails", 
                                     {
                                         "JobId":job_id
                                     }
                                   )
    return get_job_details_response["JobDetails"]

def run_job(job_id: str, run_name: str, 
            override_script_parameters: "List[script_parameter]" = None,
            override_import_tables: "List[import_table]" = None,
            override_export_tables: "List[export_table]" = None):
    """
    Run an instance of a submitted job.
    """
    return neuro_call("80", "sparkmanager", "runjob", 
                      {
                          "JobId" :  job_id,
                          "RunName" : run_name,
                          "OverrideScriptParameters" : override_script_parameters,
                          "OverrideImportTables" : override_import_tables,
                          "OverrideExportTables" : override_export_tables
                      }
                     )

def list_runs(job_id: str, schedule_id: str = None, submitted_by: str = None, max_returned: int = None, run_id: str = None):
    """
    List runs for a spark manager job
    """
    list_runs_response = neuro_call("80", "sparkmanager", "listruns", 
                                     {
                                         "JobId" : job_id,
                                         "ScheduleId" : schedule_id,
                                         "SubmittedBy" : submitted_by,
                                         "NumberReturned" : max_returned,
                                         "RunId" : run_id
                                     }
                                   )
    return list_runs_response["RunSummaries"]

def run_schedule(job_id: str, schedule_name: str, utc_cron_expression: str, 
            override_script_parameters: "List[script_parameter]" = None,
            override_import_tables: "List[import_table]" = None,
            override_export_tables: "List[export_table]" = None):
    """
    Start a run schedule on a spark manager job. A cron expression is used to specify the schedule based on utc time.
    """
    return neuro_call("80", "sparkmanager", "runschedule", 
                      {
                          "JobId" :  job_id,
                          "ScheduleName" : schedule_name,
                          "UtcCronExpression" : utc_cron_expression,
                          "OverrideScriptParameters" : override_script_parameters,
                          "OverrideImportTables" : override_import_tables,
                          "OverrideExportTables" : override_export_tables
                      }
                     )

def stop_schedule(schedule_id: str):
    """
    Stop a run schedule on a spark manager job
    """
    neuro_call("80", "sparkmanager", "stopschedule", {"ScheduleId":schedule_id})
    
def load_pyspark_notebook_to_str(file_name: str):
    """
    Read a python notebook as a string into a variable. This variable can be given to submit_job
    """
    tmp_file=str(uuid.uuid4())
    os.system("jupyter nbconvert --to script '" + file_name +"' --output '" + tmp_file + "'")
    file=open(tmp_file+".py")
    script=file.read()
    file.close()
    os.remove(tmp_file+".py")
    return script
    
