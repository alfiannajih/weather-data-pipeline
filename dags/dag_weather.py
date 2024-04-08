from airflow import DAG
from airflow.utils import dates

from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from tasks.extract_weather import batch_extract
from tasks.transform_and_load_weather import transform_weather_data

# Defining DAG
dag = DAG(
  dag_id="weather-pipeline",
  start_date=dates.days_ago(0),
  schedule_interval="@daily",
)

# Task 1 => create weather table
create_location_table = PostgresOperator(
    task_id="create-location-table",
    postgres_conn_id="weather-data-conn",
    sql="sql/weather_table.sql",
    dag=dag
)

# Task 2 => extract weather data from api
extract_weather_data = PythonOperator(
    task_id="extract-weather-data",
    python_callable=batch_extract,
    op_kwargs={
        "batch_size": 10,
        "postgres_conn_id": "weather-data-conn",
        "table_name": "geo_location",
        "province": "Jawa Timur",
        "timezone": "Asia/Jakarta",
        "start_date": "2020-01-01",
        "end_date": "2020-01-31",
        "output": "weather"
    },
    dag=dag
)

# Task 3 => transform and load weather data to db
transform_and_load_weather_data = PythonOperator(
    task_id="transform-and-load-weather-data",
    python_callable=transform_weather_data,
    op_kwargs={
        "start_date": "2020-01-01",
        "postgres_conn_id": "weather-data-conn",
        "table_name": "weather"
    },
    dag=dag
)

create_location_table >> extract_weather_data >> transform_and_load_weather_data