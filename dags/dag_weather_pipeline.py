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

# Task 0
create_weather_table = PostgresOperator(
    task_id="create-weather-table",
    postgres_conn_id="weather-data-conn",
    sql="""
        CREATE TABLE IF NOT EXISTS weather (
          "date" TEXT,
          "weather_code" FLOAT,
          "temperature_2m_max" FLOAT,
          "temperature_2m_min" FLOAT,
          "temperature_2m_mean" FLOAT,
          "apparent_temperature_max" FLOAT,
          "apparent_temperature_min" FLOAT,
          "apparent_temperature_mean" FLOAT,
          "sunrise" INTEGER,
          "sunset" INTEGER,
          "daylight_duration" FLOAT,
          "sunshine_duration" FLOAT,
          "precipitation_sum" FLOAT,
          "rain_sum" FLOAT,
          "snowfall_sum" FLOAT,
          "precipitation_hours" FLOAT,
          "wind_speed_10m_max" FLOAT,
          "wind_gusts_10m_max" FLOAT,
          "wind_direction_10m_dominant" FLOAT,
          "shortwave_radiation_sum" FLOAT,
          "et0_fao_evapotranspiration" FLOAT,
          "city" TEXT,
          "longitude" TEXT,
          "latitude" TEXT
        );""",
    dag=dag
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
        "wait_time": 5
    },
    dag=dag
)

# Task 2 => Load to postgresql
load_to_sql = PythonOperator(
    task_id="load-to-db",
    python_callable=load_to_postgres,
    op_kwargs={
        "conn_id": "weather-data-conn",
        "table": "weather",
        "file_name": "/home/alfiannajih/de-projects/weather-data-pipeline/raw_data/2020/01/Cakung.csv"
    }
)


create_weather_table >> fetch_weather_task >> load_to_sql