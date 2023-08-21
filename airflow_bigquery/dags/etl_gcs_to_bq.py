import os
import logging
import pandas as pd
from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
#from google.cloud import storage
from airflow.utils.task_group import TaskGroup

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
year = '2023'
month = '05'
dataset_file = f"yellow_tripdata_{year}-{month}.parquet"
dataset_transform_file = f"yellow_tripdata_{year}-{month}_transform.parquet"
dataset_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{dataset_file}"
path_to_raw_dataset = '/opt/airflow/dataset/raw'
path_to_transform_dataset = '/opt/airflow/dataset/transform'
zone_map = 'taxi+_zone_lookup.csv'

def transform_data():
    df = pd.read_parquet(path_to_raw_dataset + '/' + dataset_file)
    filter_df = df.query('trip_distance > 0')
    save_filter_df = filter_df.to_parquet(f"{path_to_transform_dataset}/{dataset_transform_file}")
    return save_filter_df

""" def upload_transform_data_to_gcs(bucket, object_name, local_file):
    #upload to gcs bucket
    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file) """


with DAG(
    dag_id="etl_local_to_gcs_to_bigquery",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
) as dag:
    with TaskGroup('raw_data') as raw_data:
        download_yellow_taxi_trip = BashOperator(
            task_id="download_dataset_task",
            bash_command=f"curl {dataset_url} > {path_to_raw_dataset}/{dataset_file}"
        )

        upload_yellow_taxi_trip = LocalFilesystemToGCSOperator(
            task_id = 'upload_yellow_taxi_trip',
            src = f"{path_to_raw_dataset}/{dataset_file}",
            dst = f"raw/{dataset_file}",
            bucket = BUCKET
        )

        upload_zone_map = LocalFilesystemToGCSOperator(
            task_id = 'upload_zone_map',
            src = f"{path_to_raw_dataset}/{zone_map}",
            dst = f"raw/{zone_map}",
            bucket = BUCKET
        )

        load_zone_map_to_bigquery = GCSToBigQueryOperator(
            task_id = 'load_zone_map_to_bigquery',
            bucket = BUCKET,
            source_objects = f"raw/{zone_map}",
            destination_project_dataset_table = PROJECT_ID + ".demo_etl.taxi_zone_lookup",
            field_delimiter = ',',
            write_disposition = 'WRITE_TRUNCATE'
        )

        download_yellow_taxi_trip >> upload_yellow_taxi_trip
        download_yellow_taxi_trip >> upload_zone_map >> load_zone_map_to_bigquery

    with TaskGroup('transforms_data') as transforms_data:
        transform_raw_data = PythonOperator(
            task_id = 'transform_raw_data',
            python_callable = transform_data
        )

        upload_transform_data_to_gcs = LocalFilesystemToGCSOperator(
            task_id = 'upload_transform_data_to_gcs',
            src = f"{path_to_transform_dataset}/{dataset_transform_file}",
            dst = f"transform/{dataset_transform_file}",
            bucket = BUCKET
        )

        transform_raw_data >> upload_transform_data_to_gcs

    load_transform_data_to_bigquery = GCSToBigQueryOperator(
        task_id = 'load_transform_data_to_bigquery',
        bucket = BUCKET,
        source_format="PARQUET",
        source_objects = f"transform/{dataset_transform_file}",
        destination_project_dataset_table = PROJECT_ID + ".demo_etl.green_trip_data"
        )
    
raw_data >> transforms_data >> load_transform_data_to_bigquery

    


