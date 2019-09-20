"""
The notebook_manager module provides functions to interact with the
Notebook Manager in Neuroverse.
"""
import datetime

from neuro_python.neuro_call import neuro_call

def list_active_sessions():
    """
    Remove a spark manager job
    """
    return neuro_call("8080", "notebookmanagementservice", "GetDetailedSessionList", None)

last_session_delay_run=datetime.datetime(1970, 1, 1, 8, 0)
def delay_session_shutdown():
    if datetime.datetime.utcnow()-last_session_delay_run>datetime.timedelta(minutes=5):
        last_session_delay_run=datetime.datetime.utcnow()
        return neuro_call("8080", "notebookmanagementservice", "KeepSessionRunning", None)
