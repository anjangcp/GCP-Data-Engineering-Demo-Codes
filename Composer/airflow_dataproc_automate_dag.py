"""
Author : @ Anjan GCP Data Engineering

Example Airflow DAG to demo below Dataproc use cases 
Airflow operators for managing a dataproc cluster 
    1. Create Dataproc cluster
    2. Submit PySpark jobs (in parallel) 
    3. Delete dataproc cluster
"""
import os
from datetime import datetime
from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (
    ClusterGenerator,
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator
)

# Param initializations 
DAG_ID = "dataproc_cluster_jobs"
PROJECT_ID = "gcp-dataeng-demos-383407"
BUCKET_NAME = "dataprco-airflow-demos"
CLUSTER_NAME = "dataeng-demos-airflow"
REGION = "asia-south2"
ZONE = "asia-south2-a"

#PySPark scripts paths
SCRIPT_BUCKET_PATH = "gcpdataeng-demos/scripts"
# BQ -> AGGREGATE -> GCS
SCRIPT_NAME_1 = "pyspark_bq_to_gcs.py"
# GCS -> AGGREGATE -> BQ
SCRIPT_NAME_2 = "pyspark_gcs_to_bq.py"

# Cluster definition: Generating Cluster Config for DataprocCreateClusterOperator

INIT_FILE = "goog-dataproc-initialization-actions-asia-south2/connectors/connectors.sh"

# Generating cluster Configurations with this operator
CLUSTER_GENERATOR_CONFIG = ClusterGenerator(
    project_id=PROJECT_ID,
    zone=ZONE,
    master_machine_type="n1-standard-2",
    worker_machine_type="n1-standard-2",
    num_workers=2,
    storage_bucket=BUCKET_NAME,
    init_actions_uris=[f"gs://{INIT_FILE}"],
    metadata={"bigquery-connector-version":"1.2.0","spark-bigquery-connector-version":"0.21.0"}
).make()

# PySpark job configs for Job1
PYSPARK_JOB_1 = {
                "reference": {"project_id": PROJECT_ID},
                "placement": {"cluster_name": CLUSTER_NAME},
                "pyspark_job": {"main_python_file_uri": f"gs://{SCRIPT_BUCKET_PATH}/{SCRIPT_NAME_1}"}
                }
# PySpark job configs for Job2
PYSPARK_JOB_2 = {
                "reference": {"project_id": PROJECT_ID},
                "placement": {"cluster_name": CLUSTER_NAME},
                "pyspark_job": {"main_python_file_uri": f"gs://{SCRIPT_BUCKET_PATH}/{SCRIPT_NAME_2}"}

                }

# DAH definition is here
with models.DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["example", "dataproc"],
) as dag:

    # Create cluster with generates cluster config operator
    create_dataproc_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        cluster_name=CLUSTER_NAME,
        project_id=PROJECT_ID,
        region=REGION,
        cluster_config=CLUSTER_GENERATOR_CONFIG,
    )

    # PySpark task to read data from Bigquery , perform agrregate on data and write data into GCS
    pyspark_task_bq_to_gcs = DataprocSubmitJobOperator(
        task_id="pyspark_task_bq_to_gcs", 
        job=PYSPARK_JOB_1, 
        region=REGION, 
        project_id=PROJECT_ID
    )

    # PySpark task to read data from GCS , perform agrregate on data and write data into Bigquery
    pyspark_task_gcs_to_bq = DataprocSubmitJobOperator(
        task_id="pyspark_task_gcs_to_bq", 
        job=PYSPARK_JOB_2, 
        region=REGION, 
        project_id=PROJECT_ID
    )

    # Delete Cluster once done with jobs
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION
    )

# Set task dependencies
create_dataproc_cluster >> [pyspark_task_bq_to_gcs,pyspark_task_gcs_to_bq] >> delete_cluster
