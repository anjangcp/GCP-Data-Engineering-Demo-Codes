"""
⚠️ Disclaimer

This code and demonstration are created solely for educational and demo purposes.
They are intended to help viewers understand concepts related to Google Cloud, Data, and AI concepts.

While every effort has been made to ensure accuracy,
the author — Anjan GCP Data & AI — assumes no responsibility for any direct or indirect issues, errors, or damages arising from the use or misuse of this code or related materials.

Users are encouraged to review, modify, and validate the code before applying it in any production or business-critical environment.

"""
from __future__ import annotations

import datetime

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.utils.dates import days_ago

# --- Configuration Variables ---
# NOTE: Replace these placeholder values with your actual GCP details
GCP_PROJECT_ID = 'gcp-data-demos'
GCS_BUCKET_NAME = 'raw-customer-data-1'
GCS_OBJECT_PREFIX = 'customers/'
RAW_STAGING_TABLE = 'raw_data.raw_customer_staging'
FINAL_ANALYSIS_TABLE = 'analysis.customer_analysis_final'

# Define today's date placeholder (used for file naming)
TODAY = "{{ ds }}"  # ds is Airflow's 'date string' (e.g., 2025-11-15)

# --- Define the Transformation SQL (In-line for simplicity, typically separate file) ---
# This SQL runs in BigQuery, joining/cleaning the staging data into the final table.
TRANSFORM_SQL = f"""
SELECT
    t1.customer_id,
    t1.email,
    t1.join_date,
    CASE WHEN t1.data_source = 'WEB' THEN 'Web' ELSE 'Mobile' END AS platform,
    'New' AS segment
FROM
    `{GCP_PROJECT_ID}.{RAW_STAGING_TABLE}` AS t1;
"""


# --- DAG Definition ---
with DAG(
    dag_id='ecom_customer_etl_pipeline',
    start_date=days_ago(1),
    schedule="@daily",
    catchup=False,
    tags=['etl', 'bigquery', 'composer'],
) as dag:
    # 1. WAIT FOR THE INPUT FILE
    # Sensor that pauses the DAG until the specific daily file appears in GCS.
    wait_for_file = GCSObjectExistenceSensor(
        task_id='wait_for_file',
        bucket=GCS_BUCKET_NAME,
        object=f'{GCS_OBJECT_PREFIX}{TODAY}.csv',
        poke_interval=60 * 5,
        timeout=60 * 60 * 24,
    )

    # 2. LOAD RAW DATA TO BIGQUERY STAGING
    load_raw_data = GCSToBigQueryOperator(
        task_id='load_raw_data',
        bucket=GCS_BUCKET_NAME,
        source_objects=[f'{GCS_OBJECT_PREFIX}{TODAY}.csv'],
        destination_project_dataset_table=f'{GCP_PROJECT_ID}.{RAW_STAGING_TABLE}',
        autodetect=True,
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
    )

    # 3. RUN THE FINAL TRANSFORMATION SQL
    # Operator to execute the transformation query and write results to the final table.
    transform_data = BigQueryInsertJobOperator(
        task_id='transform_data',
        configuration={
            'query': {
                'query': TRANSFORM_SQL,
                'useLegacySql': False,
                'destinationTable': {
                    'projectId': GCP_PROJECT_ID,
                    'datasetId': FINAL_ANALYSIS_TABLE.split('.')[0],
                    'tableId': FINAL_ANALYSIS_TABLE.split('.')[1],
                },
                # Overwrite the final table with new transformed data
                'writeDisposition': 'WRITE_TRUNCATE',
            }
        },
    )

    # --- Set Task Dependencies ---
    # Define the order of execution:
    # Sensor must succeed before the Load task starts.
    # Load task must succeed before the Transform task starts.
    wait_for_file >> load_raw_data >> transform_data