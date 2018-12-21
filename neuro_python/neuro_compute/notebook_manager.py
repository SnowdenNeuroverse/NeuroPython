"""
The notebook_manager module provides functions to interact with the
Notebook Manager in Neuroverse.
"""

from neuro_python.neuro_call import neuro_call

def list_active_sessions():
    """
    Remove a spark manager job
    """
    neuro_call("8080", "notebookmanagementservice", "GetDetailedSessionList", None)
