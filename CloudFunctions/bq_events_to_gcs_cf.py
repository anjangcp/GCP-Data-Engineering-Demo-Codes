import functions_framework
from google.cloud import bigquery
from datetime import datetime

'''
Dependencies to be installed

db-dtypes
fsspec
gcsfs
bigquery

'''

# CloudEvent function to be triggered by an Eventarc Cloud Audit Logging trigger
# Note: this is NOT designed for second-party (Cloud Audit Logs -> Pub/Sub) triggers!
@functions_framework.cloud_event
def hello_auditlog(cloudevent):

   # Print out details from the `protoPayload`
   # This field encapsulates a Cloud Audit Logging entry
   # See https://cloud.google.com/logging/docs/audit#audit_log_entry_structure

   payload = cloudevent.data.get("protoPayload")
   if payload:
       
       # Timestamp in string format 
       now = datetime.now()
       timpstamp = now.strftime("%m%d%Y%H%M%S")
       
       # Build Big Query client 
       bucket_name = 'data_eng_demos'
       project = "gcp-dataeng-demos-365206"
       dataset_id = "gcp_dataeng_demos"
       table_id = "demo_cf"
       
       # Write data into GCS/csv file using dataframe 
       client = bigquery.Client(project=project)
       destination_uri = "gs://{}/{}".format(bucket_name, "bq_to_gcs_extract" + timpstamp + ".csv")
       qry = "select * from " + project + "." + dataset_id + "." + table_id
       df_qry_result = client.query(qry).to_dataframe()
       df_qry_result.to_csv(destination_uri)

       print(
                "Exported {}:{}.{} to {}".format(project, dataset_id, table_id, destination_uri)
            )
