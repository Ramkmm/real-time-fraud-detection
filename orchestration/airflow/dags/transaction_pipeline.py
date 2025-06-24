from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def ingest():
    print("Ingesting transaction data...")

def transform():
    print("Running Spark fraud detection job...")

with DAG('transaction_pipeline', start_date=datetime(2024, 1, 1), schedule_interval='@hourly', catchup=False) as dag:
    ingest_task = PythonOperator(task_id='ingest_data', python_callable=ingest)
    transform_task = PythonOperator(task_id='transform_data', python_callable=transform)

    ingest_task >> transform_task
