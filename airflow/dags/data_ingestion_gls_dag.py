from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from google.cloud import storage
import os
import pyarrow.csv as pv
import pyarrow.parquet as pq
from datetime import  timedelta,datetime
import logging



HOME_DIRECTORY = os.getenv("AIRFLOW_HOME")
BUCKET = os.getenv("GCP_GCS_BUCKET")

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

def download_file_to_local_task(url,gz_path,csv_path):
    if url.endswith('.csv'):
        os.system(f"curl -fL {url} -o {csv_path}")
    if url.endswith('.gz'):
        os.system(f"curl -fL {url} -o {gz_path} \
                    && gzip -dvc {gz_path}  > {csv_path}  ")

def convert_to_parquet_task(localFilePath:str):

    if not localFilePath.endswith('.csv'):
        print(localFilePath)
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(localFilePath)
    pq.write_table(table, localFilePath.replace('csv', 'parquet'))


def upload_to_gls(bucket_name,blob_name,path_to_file):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(path_to_file)


def download_convert_upload_dag(
        dag,
        url,
        file_csv_path,
        file_gz_path,
        file_parquet_path,
        file_cloud_path,
        bucket
    ):
   with dag :
        # download_gz_file_task = BashOperator(
        #     task_id="download_gz_file_task",
        #     bash_command=f"curl -fL {url} -o {file_gz_path} \
        #                 && gzip -dvc {file_gz_path}  > {file_csv_path}  "
        # )
        # download_csv_file_task = BashOperator(
        #     task_id="download_csv_file_task",
        #     bash_command=f"curl -fL {url} -o {file_csv_path}"
        # )
        download_file_task = PythonOperator(
            task_id = "download_file_task",
            python_callable= download_file_to_local_task,
            op_kwargs={
                "url":  url,
                "gz_path": file_gz_path,
                "csv_path": file_csv_path
            }
        )        
        convert_task = PythonOperator(
            task_id = "convert_to_parquet_task",
            python_callable= convert_to_parquet_task,
            op_kwargs={
                "localFilePath":  file_csv_path
            }
        )
        upload_to_cloud_task = PythonOperator(
            task_id = "upload_to_gls",
            python_callable= upload_to_gls,
            op_kwargs={
                "bucket_name":bucket  ,
                "blob_name": file_cloud_path,
                "path_to_file": file_parquet_path
            }
        )
        remove_file_local_task = BashOperator(
            task_id="remove_file_local_task",
            bash_command=f"rm -rf {file_parquet_path} {file_csv_path} {file_gz_path}"
        )

        download_file_task >> convert_task >> upload_to_cloud_task >> remove_file_local_task





# Ingest yellow taxi data
# FILE_NAME = 'yellow_tripdata_'+'{{ execution_date.strftime("%Y-%m")}}'
FILE_NAME = 'yellow_tripdata_2019-01'
URL_TEMPLATE = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/{FILE_NAME}.csv.gz'
FILE_LOCAL_CSV_PATH_TEMPLATE = f'{HOME_DIRECTORY}/{FILE_NAME}.csv'
FILE_LOCAL_GZ_PATH_TEMPLATE = f'{HOME_DIRECTORY}/{FILE_NAME}.gz'
FILE_LOCAL_PARQUET_PATH_TEMPLATE = f'{HOME_DIRECTORY}/{FILE_NAME}.parquet'
FILE_CLOUD_PATH_TEMPLATE = f'raw/yellow_tripdata/{FILE_NAME}.parquet'

yellow_taxi_dag = DAG(dag_id='ingest_yellow_taxi_to_cloud',
            default_args=default_args,
            max_active_runs=3,
            start_date=datetime(2024,1,1),
            schedule_interval='0 3 2 * *',
            tags=['nyc-taxi'],
        )

ingest_yellow_taxi_data = download_convert_upload_dag(dag=yellow_taxi_dag,url=URL_TEMPLATE,
                                                      file_gz_path=FILE_LOCAL_GZ_PATH_TEMPLATE,
                                                      file_csv_path=FILE_LOCAL_CSV_PATH_TEMPLATE,
                                                      file_parquet_path=FILE_LOCAL_PARQUET_PATH_TEMPLATE,
                                                      file_cloud_path=FILE_CLOUD_PATH_TEMPLATE,
                                                      bucket=BUCKET
                                                    )
# Ingest green taxi data
FILE_NAME = 'green_tripdata_2019-01'
URL_TEMPLATE = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/{FILE_NAME}.csv.gz'
FILE_LOCAL_CSV_PATH_TEMPLATE = f'{HOME_DIRECTORY}/{FILE_NAME}.csv'
FILE_LOCAL_GZ_PATH_TEMPLATE = f'{HOME_DIRECTORY}/{FILE_NAME}.gz'
FILE_LOCAL_PARQUET_PATH_TEMPLATE = f'{HOME_DIRECTORY}/{FILE_NAME}.parquet'
FILE_CLOUD_PATH_TEMPLATE = f'raw/green_tripdata/{FILE_NAME}.parquet'

green_taxi_dag = DAG(dag_id='ingest_green_taxi_to_cloud',
            default_args=default_args,
            max_active_runs=3,
            start_date=datetime(2024,1,1),
            schedule_interval='0 3 2 * *',
            tags=['nyc-taxi'],
        )

ingest_green_taxi_data = download_convert_upload_dag(dag=green_taxi_dag,url=URL_TEMPLATE,
                                                      file_gz_path=FILE_LOCAL_GZ_PATH_TEMPLATE,
                                                      file_csv_path=FILE_LOCAL_CSV_PATH_TEMPLATE,
                                                      file_parquet_path=FILE_LOCAL_PARQUET_PATH_TEMPLATE,
                                                      file_cloud_path=FILE_CLOUD_PATH_TEMPLATE,
                                                      bucket=BUCKET
                                                    )

# Ingest zone taxi data
FILE_NAME = 'taxi_zone_lookup'
URL_TEMPLATE = f'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv'
FILE_LOCAL_CSV_PATH_TEMPLATE = f'{HOME_DIRECTORY}/{FILE_NAME}.csv'
FILE_LOCAL_GZ_PATH_TEMPLATE = f'{HOME_DIRECTORY}/{FILE_NAME}.gz'
FILE_LOCAL_PARQUET_PATH_TEMPLATE = f'{HOME_DIRECTORY}/{FILE_NAME}.parquet'
FILE_CLOUD_PATH_TEMPLATE = f'raw/taxi_zone/{FILE_NAME}.parquet'

zone_taxi_dag = DAG(dag_id='ingest_zone_taxi_to_cloud',
            default_args=default_args,
            max_active_runs=3,
            start_date=days_ago(1),
            tags=['nyc-taxi'],
        )

ingest_green_taxi_data = download_convert_upload_dag(dag=zone_taxi_dag,url=URL_TEMPLATE,
                                                      file_gz_path=FILE_LOCAL_GZ_PATH_TEMPLATE,
                                                      file_csv_path=FILE_LOCAL_CSV_PATH_TEMPLATE,
                                                      file_parquet_path=FILE_LOCAL_PARQUET_PATH_TEMPLATE,
                                                      file_cloud_path=FILE_CLOUD_PATH_TEMPLATE,
                                                      bucket=BUCKET
                                                    )

# Ingest fhv taxi data
FILE_NAME = 'fhv_tripdata_2019-01'
URL_TEMPLATE = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{FILE_NAME}.csv.gz'

FILE_LOCAL_CSV_PATH_TEMPLATE = f'{HOME_DIRECTORY}/{FILE_NAME}.csv'
FILE_LOCAL_GZ_PATH_TEMPLATE = f'{HOME_DIRECTORY}/{FILE_NAME}.gz'
FILE_LOCAL_PARQUET_PATH_TEMPLATE = f'{HOME_DIRECTORY}/{FILE_NAME}.parquet'
FILE_CLOUD_PATH_TEMPLATE = f'raw/fhv_tripdata/{FILE_NAME}.parquet'

fhv_taxi_dag = DAG(dag_id='ingest_fhv_taxi_to_cloud',
            default_args=default_args,
            max_active_runs=3,
            start_date=datetime(2024,1,1),
            schedule_interval='0 3 2 * *',
            tags=['nyc-taxi'],
        )

ingest_green_taxi_data = download_convert_upload_dag(dag=fhv_taxi_dag,url=URL_TEMPLATE,
                                                      file_gz_path=FILE_LOCAL_GZ_PATH_TEMPLATE,
                                                      file_csv_path=FILE_LOCAL_CSV_PATH_TEMPLATE,
                                                      file_parquet_path=FILE_LOCAL_PARQUET_PATH_TEMPLATE,
                                                      file_cloud_path=FILE_CLOUD_PATH_TEMPLATE,
                                                      bucket=BUCKET
                                                    )
