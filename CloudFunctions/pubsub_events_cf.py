
def hello_pubsub(event, context):
    """
    Background Cloud Function to be triggered by Pub/Sub.
    Required dependencies : bigquery
    Configure environment variables while creating and deploying cloud function
    for bigquery dataset and table
    dataset:gcp_dataeng_demos
    table:cf_pubsub_demo
    
    """
    import base64
    import json
    import os,sys
    from google.cloud import bigquery

    if 'data' in event:
        data_buffer = base64.b64decode(event['data']).decode('utf-8')

        message='{'+'"Actvity_Time": "{}"'.format(json.loads(data_buffer)['timestamp']) + ',' +'"Resource_Name": "{}"'.format(json.loads(data_buffer)['protoPayload']['resourceName']) + ',' +'"Actvity_Type": "{}"'.format(json.loads(data_buffer)['protoPayload']['methodName']) + ',' +'"Activity_done_by": "{}"'.format(json.loads(data_buffer)['protoPayload']['authenticationInfo']['principalEmail']) + ',' + '"Change_in_IAM_policies": "{}"'.format(json.loads(data_buffer)['protoPayload']['serviceData']['policyDelta']['bindingDeltas'])+'}'
        bq_data=json.loads(message)
        print(bq_data)
        
        def to_bigquery(dataset, table, document):
            bigquery_client = bigquery.Client()
            dataset_ref = bigquery_client.dataset(dataset)
            table_ref = dataset_ref.table(table)
            table = bigquery_client.get_table(table_ref)
            errors = bigquery_client.insert_rows(table, [document])
            if errors != [] :
                 print(errors, file=sys.stderr)
        to_bigquery(os.environ['dataset'], os.environ['table'], bq_data)
        
    else:
        print('Hello World')
