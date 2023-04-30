"""Demo HTTP operators"""
from __future__ import annotations

import json
import os
from datetime import datetime

from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python import PythonOperator
from google.cloud import storage

# Dag name
DAG_ID = "demo_http_operator_to_gcs"

# ths python funcgion writes data from Xcom to GCS byucket as a JSON file
def WriteToGcs(ti):
    data = ti.xcom_pull(task_ids=['get_http_data'])
    bucket_name = 'gcpdataeng-demos'
    destination_blob_name = 'stock_data.json'
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_string(str(data))

    print(
        f"{destination_blob_name} with contents uploaded to {bucket_name}."
    )
# DAG definitions with all required params
dag = DAG(
    DAG_ID,
    default_args={"retries": 1},
    tags=["example"],
    start_date=datetime(2023, 4, 26),
    catchup=False,
)

# Task to get data from given HTTP end point
get_http_data = SimpleHttpOperator(
    task_id="get_http_data",
    http_conn_id="http_conn_id_demo",
    method="GET",
    endpoint="/query?function=TOURNAMENT_PORTFOLIO&season=2021-09&apikey=demo",
    response_filter = lambda response : json.loads(response.text),
    dag=dag
)
# Task to write data from Xcom to GCS bucket
write_data_to_gcs = PythonOperator(
    task_id = 'write_data_to_gcs',
    python_callable = WriteToGcs
)
# Task dependency set
get_http_data >> write_data_to_gcs
