"""
Stream tabular data from a source to sink in Neuroverse
"""

import time
from neuro_python.neuro_call import neuro_call

def stream(source: "SourceParameters", sink: "SinkParameters"):
    """
    Stream data from a tabular data source to a tabular data sink
    """
    request = {"SourceParameters" : source, "SinkParameters" : sink}
    method = source["Type"] + "To" + sink["Type"]
    response = neuro_call("80", "DataMovementService", method, request)

    check_request = {"JobId" : response["JobId"]}
    status = 0
    errormsg = ""
    while status == 0:
        time.sleep(1)
        response_c = neuro_call("80", "DataMovementService", "CheckJob", check_request)
        status = response_c["Status"]
        if status > 1:
            errormsg = response_c["Message"]

    neuro_call("80", "DataMovementService", "FinaliseJob", check_request)

    if status != 1:
        raise Exception("Neuroverse error: " + errormsg)

    return {"JobId" : response["JobId"], "TimeStamp" : response["TimeStamp"]}
