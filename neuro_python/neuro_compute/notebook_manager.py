"""
The notebook_manager module provides functions to interact with the
Notebook Manager in Neuroverse.
"""
import datetime
from retrying import retry

from neuro_python.neuro_call import neuro_call

def list_active_sessions():
    """
    Get list of active notebook sessions
    """
    
    return neuro_call("8080", "notebookmanagementservice", "GetDetailedSessionList", None)

last_session_delay_run=datetime.datetime(1970, 1, 1, 8, 0)
KeepSessionRunningResponse=''
@retry(wait_exponential_multiplier=1000, wait_exponential_max=10000,stop_max_attempt_number=7)
def delay_session_shutdown():
    """
    Delay a notebook session from being shutdown automatically
    """
    a=[]
    a[1]=0
    global last_session_delay_run
    global KeepSessionRunningResponse
    if datetime.datetime.utcnow()-last_session_delay_run>datetime.timedelta(minutes=5):
        last_session_delay_run=datetime.datetime.utcnow()
        KeepSessionRunningResponse=neuro_call("8080", "notebookmanagementservice", "KeepSessionRunning", None)
        return KeepSessionRunningResponse
    else:
        return KeepSessionRunningResponse
