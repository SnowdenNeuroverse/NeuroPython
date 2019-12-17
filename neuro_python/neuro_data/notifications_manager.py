"""
Manage notifications in Neuroverse
"""

from neuro_python.neuro_call import neuro_call_v2

def delivery_descriptor(target_address:str,delivery_method:int=1):
    """
    Specify a delivery endpoint
    """
    return {"TargetAddress":target_address,"DeliveryMethodType":delivery_method}

def create_notification_definition(name:str,delivery_descriptors:"list[delivery_descriptor]",description:str=None):
    """
    Specify a notification definition.
    """
    requestbody={"Name":name,"DeliveryDescriptors":delivery_descriptors,"Description":description}
    response = neuro_call_v2("Notification", "CreateNotificationDefinition", requestbody)
    return {"Id":response['NotificationDefinitionId']}

def list_notification_definitions():
    """
    List notification definitions
    """
    requestbody={}
    response = neuro_call_v2("Notification", "ListNotificationDefinitions", requestbody)
    return [
        {
        "Id":r['NotificationDefinitionId'],
        "Name":r['Name'],
        "DeliveryDescriptors":r['DeliveryDescriptors'],
        "Description":r['Description']
        } for r in response]

def delete_notification_definition(notification_id:str):
    """
    Delete a notification definition.
    """
    requestbody={"NotificationDefinitionId":notification_id}
    response = neuro_call_v2("Notification", "DeleteNotificationDefinition", requestbody)
    return 'Successful'