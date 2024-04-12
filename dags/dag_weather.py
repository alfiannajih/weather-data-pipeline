from airflow import DAG
from airflow.utils import dates

from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models.param import Param

from tasks.extract_weather import fetch_batch_weather_data
from tasks.transform_and_load_weather import transform_weather_data

from datetime import datetime, timedelta

current_date = (datetime.now() - timedelta(2)).strftime('%Y-%m-%d')

# Defining DAG
dag = DAG(
    dag_id="weather-pipeline",
    start_date=datetime(2024, 4, 12),
    schedule_interval="@daily",
    params={
        "province": Param("Jawa Timur", type="string"),
        "timezone": Param("Asia/Jakarta", type="string"),
        "postgres_conn_id": Param("weather-data-conn", type="string"),
        "weather_table_name": Param("weather", type="string"),
        "geo_location_table_name": Param("geo_location", type="string"),
    }
    )

# Task 1 => create weather table
create_location_table = PostgresOperator(
    task_id="create-location-table",
    postgres_conn_id="{{ params.postgres_conn_id }}",
    sql="sql/weather_table.sql",
    dag=dag
)

# Task 2 => extract weather data from api
extract_weather_data = PythonOperator(
    task_id="extract-weather-data",
    python_callable=fetch_batch_weather_data,
    op_kwargs={
        "batch_size": 10,
        "postgres_conn_id": "{{ params.postgres_conn_id }}",
        "geo_location_table_name": "{{ params.geo_location_table_name }}",
        "province": "{{ params.province }}",
        "timezone": "{{ params.timezone }}",
        "start_date": current_date,
        "end_date": current_date
    },
    dag=dag
)

# Task 3 => transform and load weather data to db
transform_and_load_weather_data = PythonOperator(
    task_id="transform-and-load-weather-data",
    python_callable=transform_weather_data,
    op_kwargs={
        "start_date": current_date,
        "postgres_conn_id": "{{ params.postgres_conn_id }}",
        "table_name": "{{ params.weather_table_name }}"
    },
    dag=dag
)

# Task dependencies
create_location_table >> extract_weather_data >> transform_and_load_weather_data