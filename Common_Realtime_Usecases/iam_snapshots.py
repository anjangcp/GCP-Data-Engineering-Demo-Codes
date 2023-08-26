import functions_framework
from googleapiclient import discovery
from google.cloud import bigquery
from datetime import datetime
import os

'''
Dependencies to be installed

bigquery
google-api-python-client
'''

@functions_framework.http
def hello_http(request):
    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
        <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <https://flask.palletsprojects.com/en/1.1.x/api/#flask.make_response>.
    """
    request_json = request.get_json(silent=True)
    request_args = request.args
    
    # Get project , Dataset , Table details from Function Env
    PROJECT_ID = os.environ.get("project")
    DATASET_ID = os.environ.get("dataset")
    TABLE_ID = os.environ.get("table")

    #from apiclient.discovery import build
    service = discovery.build('cloudresourcemanager', 'v1')

    # Get IAM roles  from specified project
    policy_request = service.projects().getIamPolicy(resource=PROJECT_ID, body={})
    policy_response = policy_request.execute()
    #print(policy_response['bindings'])
    
    # Deriving current timestamp for snapshot
    now = datetime.now()
    date_time = now.strftime("%m/%d/%Y %H:%M:%S")
    rows_to_insert = [] 

    # Append snapshot time to each row 
    for i in policy_response['bindings']:
        i['snapshot_time'] = date_time
        rows_to_insert.append(i)
    #print(rows_to_insert)
    # Construct a BigQuery client object.
    client = bigquery.Client()

    # load job confuguration
    load_job = client.insert_rows_json("{}.{}.{}".format(PROJECT_ID,DATASET_ID,TABLE_ID), rows_to_insert
    ) 

    if load_job == []:
        print('Data has been loaded successfully')
    else:
        print("Somee issue with loading data")

    name = 'Succeeded'
    return 'Function Execution {}!'.format(name)
