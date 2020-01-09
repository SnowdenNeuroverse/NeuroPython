"""
Manage event monitoring in Neuroverse
"""
import uuid

from neuro_python.neuro_call import neuro_call_v2

def get_event_feed_store_name():
    return 'NeuroverseEvents'

def create_event_monitor_feed(name:str,filter_columns:"list[str]"):
    """
    Specify an event monitor feed.
    """
    requestbody={"EventMonitorFeedName":name,"FilterColumns":filter_columns}
    response = neuro_call_v2("Event", "CreateEventMonitorFeed", requestbody)
    return {"Id":response['EventMonitorFeedId']}

def delete_event_monitor_feed(feed_id:str):
    """
    Delete an event monitor feed.
    """
    requestbody={"EventMonitorFeedId":feed_id}
    response = neuro_call_v2("Event", "DeleteEventMonitorFeed", requestbody)
    return 'Successful'

def list_event_monitor_feeds():
    """
    List event monitor feeds.
    """
    requestbody={}
    response = neuro_call_v2("Event", "ListEventMonitorFeeds", requestbody)
    return [{"Id":r['EventMonitorFeedId'],"Name":r['EventMonitorFeedName'],"FilterColumns":r['FilterColumns']} for r in response]

def create_event_definition(feed_id:str,name:str,severity:int,sql_filter:str=None,notification_definition_id:str=None,description:str='',notification_batching_filter_column:str=None):
    """
    Specify an event monitor feed.
    """
    requestbody={
        "EventMonitorFeedId":feed_id,
        "EventDefinitionAlias":str(uuid.uuid4()).replace('-',''),
        "EventDefinitionName":name,
        "EventDefinitionDescription":description,
        "EventDefinitionSeverity":severity,
        "SqlFilterQuery":sql_filter,
        "NotificationDefinitionId":notification_definition_id,
        "NotificationBatchingFilterColumn":notification_batching_filter_column
    }
    response = neuro_call_v2("Event", "CreateEventDefinition", requestbody)
    return {"Id":response['EventDefinitionId']}

def delete_event_definition(feed_id:str,event_id:str):
    """
    Delete an event definition.
    """
    requestbody={"EventMonitorFeedId":feed_id,"EventDefinitionId":event_id}
    response = neuro_call_v2("Event", "DeleteEventDefinition", requestbody)
    return 'Successful'

def list_event_definitions(feed_id:str):
    """
    List event definitions.
    """
    requestbody={"EventMonitorFeedId":feed_id}
    response = neuro_call_v2("Event", "ListEventDefinitions", requestbody)
    return [{
        "Id":r['EventDefinitionId'],
        "Name":r['EventDefinitionName'],
        "Description":r['EventDefinitionDescription'],
        "SqlFilter":r['SqlFilterQuery'],
        "NotificationDefinitionId":r['NotificationDefinitionId'],
        "NotificationBatchingFilterColumn":r['NotificationBatchingFilterColumn'],
        "Severity":r['EventDefinitionSeverity'],
        "FeedId":r['EventMonitorFeedId']
    } for r in response]
