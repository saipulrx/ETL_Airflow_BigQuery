import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

with DAG(
    dag_id="local_to_gcs_to_bigquery",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
) as dag:
    upload_file = LocalFilesystemToGCSOperator(
        task_id = 'upload_file_local_to_gcs',
        src = './dataset/online_Retail.csv',
        dst = 'raw/online_Retail.csv',
        bucket = BUCKET,
        mime_type='text/csv',
    )

    load_data_gcs_bigquery = GCSToBigQueryOperator(
        task_id = 'gcs_to_bigquery',
        bucket = BUCKET,
        source_objects = 'raw/online_Retail.csv',
        destination_project_dataset_table = PROJECT_ID + ".demo2.online_retail",
        field_delimiter = ','
    )

upload_file >> load_data_gcs_bigquery
