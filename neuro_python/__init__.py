"""
The neuro_python package provides access to Neuroverse from within Python.

It currently contains:
    - Neuro_Call class: Gives authorised access to the Neuroverse API
    - neuro_data module(nd): Gives a user of python access to Neuroverse Data Stores
"""

debug_val=False

def home_directory():
    try:
        return os.environ['NEUROSESSIONHOMEDIR']
    except:
        return "/home/jovyan/session"
