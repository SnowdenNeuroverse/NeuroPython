"""
The neuro_python package provides access to Neuroverse from within Python.

It currently contains:
    - Neuro_Call class: Gives authorised access to the Neuroverse API
    - neuro_data module(nd): Gives a user of python access to Neuroverse Data Stores
"""

def home_directory():
    try:
        return os.environ['NEUROSESSIONHOMEDIR']
    except:
        return "/home/jovyan/session"

from . import neuro_data as nd
from . neuro_call import neuro_call
from . neuro_call import debug
