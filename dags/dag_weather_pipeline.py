from airflow import DAG
from airflow.utils import dates

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

from tasks.fetch_weather import fetch_weather_data

from tasks.extract_location import fetch_location_data
from tasks.transform_location import transform_geo_location
from tasks.load_location import load_geo_location

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

# Task 2 => extract geo location
extracting_geo_location = PythonOperator(
    task_id="extract-geo-location",
    python_callable=fetch_location_data,
    op_kwargs={"filename": "jawa_timur", "province": "jawa timur"},
    dag=dag
)

# Task 3 => transform geo location
transforming_geo_location = PythonOperator(
    task_id="transform-geo-location",
    python_callable=transform_geo_location,
    op_kwargs={"filename": "jawa_timur"},
    dag=dag
)

# Task 4 => load geo location
loading_geo_location = PythonOperator(
    task_id="load-geo-location",
    python_callable=load_geo_location,
    op_kwargs={"filename": "jawa_timur", "postgres_conn_id": "weather-data-conn", "table_name": "geo_location"},
    dag=dag
)

# Task 5 => create weather table
create_weather_table = PostgresOperator(
    task_id="create-weather-table",
    postgres_conn_id="weather-data-conn",
    sql="sql/weather_table.sql",
    dag=dag
)

create_location_table >> extracting_geo_location >> transforming_geo_location >> loading_geo_location