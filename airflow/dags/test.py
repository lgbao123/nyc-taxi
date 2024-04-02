from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def push_function(**context):
    return "Hello, world!"

def pull_function(**context):
    value_received = context['ti'].xcom_pull(task_ids='push_task')
    print(value_received)

with DAG('xcom_example', start_date=datetime(2024, 1, 1)) as dag:
    push_task = PythonOperator(
        task_id='push_task',
        python_callable=push_function
    )

    pull_task = PythonOperator(
        task_id='pull_task',
        python_callable=pull_function
    )

    push_task >> pull_task