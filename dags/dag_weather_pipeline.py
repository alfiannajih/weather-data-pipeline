from airflow import DAG
from airflow.utils import dates

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

from tasks.fetch_weather import fetch_weather_data
from tasks.load_to_sql import load_to_postgres

# Defining DAG
dag = DAG(
  dag_id="weather_data_pipeline",
  start_date=dates.days_ago(0),
  schedule_interval="@daily",
)

# Task 1.1 => create lcoation table
create_location_table = PostgresOperator(
    task_id="create-location-table",
    postgres_conn_id="weather-data-conn",
    sql="sql/geo_location_table.sql",
    dag=dag
)

# Task 1.2 => create weather table
create_weather_table = PostgresOperator(
    task_id="create-weather-table",
    postgres_conn_id="weather-data-conn",
    sql="sql/weather_table.sql",
    dag=dag
)



[create_location_table, create_weather_table] >> 