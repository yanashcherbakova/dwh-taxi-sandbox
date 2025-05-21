from dotenv import load_dotenv
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from New_trips_generation import generate_trip
from clickhouse_connect import get_client

load_dotenv()
clickhouse_pass = os.getenv("CLICKHOUSE_PASSWORD")

default_args = {
    'owner' : 'yana',
    'retries' : 1,
    'retry_delay' : timedelta(minutes=5)
}

with DAG(
    dag_id = 'generate_taxi_trip',
    default_args=default_args,
    start_date=datetime(2025,5,19),
    schedule_interval= timedelta(minutes=15),
    catchup=False
) as dag:
    
    task_generate_trip = PythonOperator(
        task_id='generate_trip_task',
        python_callable=generate_trip,
        op_kwargs={
            'host': 'clickhouse',
            'database': 'analytics',
            'username': 'yana',
            'password': clickhouse_pass
        }
    )