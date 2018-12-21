"""
The notebook_manager module provides functions to interact with the
Notebook Manager in Neuroverse.
"""

from neuro_python.neuro_call import neuro_call

def list_active_sessions(job_id: str):
    """
    Remove a spark manager job
    """
    neuro_call("80", "notebookmanagementservice", "GetDetailedSessionList", None)
