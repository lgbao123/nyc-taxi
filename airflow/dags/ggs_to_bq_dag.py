from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator,BigQueryInsertJobOperator
from airflow.utils.dates import days_ago


import os
import logging



HOME_DIRECTORY = os.getenv("AIRFLOW_HOME")
BUCKET = os.getenv("GCP_GCS_BUCKET")

DATASET_NAME = 'tripdata_all'
TABLES = {'yellow': 'tpep_pickup_datetime', 'green': 'lpep_pickup_datetime','fhv': 'pickup_datetime'}

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
    
}

with DAG(
   dag_id="ggs_to_bq",
   default_args=default_args,
   max_active_runs=1,
   schedule_interval='@daily',
   tags=['nyc-taxi'],
) as dag:
   for name,col in TABLES.items():
      create_exernal_table= BigQueryCreateExternalTableOperator(
         task_id=f"bq_{name}_create_external_table_task",
         destination_project_dataset_table=f"{DATASET_NAME}.{name}_tripdata_external_table",
         bucket=BUCKET,
         source_objects=[f'raw/{name}_tripdata/*'],
         source_format='PARQUET'
      )
      CREATE_BQ_TABLE_SQL = f"""
            create or replace table {DATASET_NAME}.{name}_tripdata
            partition by date({col})
            as 
            select * from  {DATASET_NAME}.{name}_tripdata_external_table
      """
      bq_create_partitioned_table_job = BigQueryInsertJobOperator(
         task_id=f"bq_{name}_create_partitioned_table_task",
         configuration={
               "query": {
                  "query": CREATE_BQ_TABLE_SQL,
                  "useLegacySql": False,
               }
         },
      )
      create_exernal_table >> bq_create_partitioned_table_job