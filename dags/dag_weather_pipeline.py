from airflow import DAG
from airflow.utils import dates

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from tasks.fetch_weather import fetch_weather_data
from tasks.load_to_sql import load_to_postgres

# Defining DAG
dag = DAG(
  dag_id="weather_data_pipeline",
  start_date=dates.days_ago(0),
  schedule_interval="@daily",
)

# Task 1 => Fetch data from API
fetch_weather_task = PythonOperator(
    task_id="fetch-weather",
    python_callable=fetch_weather_data,
    op_kwargs={
        "province": "Daerah Khusus Ibukota Jakarta",
        "start_date": "2020-01-01",
        "end_date": "2020-01-31",
        "output": "raw_data",
        "wait_time": 30
    },
    dag=dag
)

# Task 2 => Load to postgresql
load_to_sql = PythonOperator(
    task_id="load-to-sql",
    python_callable=load_to_postgres,
    op_kwargs={
        "conn_id": "weather-data-conn",
        "query": ""
    }
)


fetch_weather_task