'''
Author : @ Anjan GCP Data Engineering
 Cloud function code to:
 	1. Read data from publicly shared  google sheets
 	2. Load data into Bigquery  using pandas and bigquery APIs 
'''

import functions_framework
import pandas as pd
import pandas_gbq

@functions_framework.http
def hello_http(request):

    message = 'Function executed Successfully'
    # Read Google Sheet data into Pandas dataframe
    # and write that data into Bigquery Table
    sheet_id = '1r9b-CN86hCmwnnm_-6aosLwZpzKyLRjsW9fxr0ALVRE'
    sheet_name = "Sheet1"
    url_1 = "https://docs.google.com/spreadsheets/d/{}/gviz/tq?tqx=out:csv&sheet={}".format(sheet_id,sheet_name)
    #print(url_1)
    df = pd.read_csv(url_1)
    df.to_gbq('gcp_dataeng_demos.public_fruit_to_bq',
              'gcp-dataeng-demos-383407',
              chunksize=10000, 
              if_exists='append'
                      )
    print("Data loaded successfully")
    return message
    
    
 
""" 

Author : @ Anjan GCP Data Engineering
 Cloud function code to:
 	1. Read data from private  google sheets
 	2. grant edit access to compute engine SA on google sheet
 	3. upload SA creds to secret manager
 	4. authenticate SA by downloading SA creds into CF code using python API
 	2. Load data into Bigquery  using pandas and bigquery APIs

Roles required by  SA -

Secret Manager Secret Accessor  

Installations required -

functions-framework==3.*
google-cloud-secret-manager
requests
pandas
gspread_pandas
pandas_gbq

"""

# Imports
import functions_framework
import requests as req
import pandas as pd
from google.cloud import secretmanager
import gspread_pandas
import json
import pandas_gbq

# Cloud Function 
@functions_framework.http
def hello_http(request):
    message = 'Function executed Successfully'
    # Create the Secret Manager client.
    client = secretmanager.SecretManagerServiceClient()
    # Build the resource name of the secret version.
    project_id = '414888653736'
    secret_id = 'sa_cred'
    version_id = '1'
    name = "projects/{}/secrets/{}/versions/{}".format(project_id,secret_id,version_id)
    # Access the secret version.
    response = client.access_secret_version(request={"name": name})
    # Print the secret payload.
    # snippet is showing how to access the secret material.
    payload = response.payload.data.decode("UTF-8")
    # convert secret value into json format
    credentials = json.loads(payload)
	# Defining scopes for gsheet and gdrive APIs
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    # Access gsheet into gspread_pandas varaible
    google_sheet_file_1 = gspread_pandas.Spread('1GmKAaZQS-sLaQRmMzNXaFt6lXOkQbvxGNx2_c3NnoVk', config=credentials)
    # Convert into pandas dataframe
    df = google_sheet_file_1.sheet_to_df(header_rows=1).astype(str)
    df.reset_index(inplace=True)
    # Write values into Bigquery Table with append mode
    df.to_gbq('gcp_dataeng_demos.sa_sheet_to_bq',
             'gcp-dataeng-demos-383407',
              chunksize=10000, 
              if_exists='append'
                )
    print("Data loaded successfully")
    return message
