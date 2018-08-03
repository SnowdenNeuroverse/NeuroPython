"""
The neuro_python package provides access to Neuroverse from within Python.

It currently contains:
    - Neuro_Call class: Gives authorised access to the Neuroverse API
    - neuro_data module: Gives a user of python access to Neuroverse Data Stores
"""
import neuro_python.neuro_data
from .neuro_call import neuro_call
