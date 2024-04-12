from airflow import DAG
from airflow.utils import dates

from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models.param import Param

from tasks.extract_location import fetch_location_data
from tasks.transform_location import transform_geo_location
from tasks.load_location import load_geo_location

# Defining DAG
dag = DAG(
    dag_id="geo-loc-pipeline",
    schedule_interval=None,
    params={
      "province": Param("Jawa Timur", type="string"),
      "postgres_conn_id": Param("weather-data-conn", type="string"),
      "location_table_name": Param("geo_location", type="string")
  }
)

# Task 1 => create lcoation table
create_location_table = PostgresOperator(
    task_id="create-location-table",
    postgres_conn_id="{{ params.postgres_conn_id }}",
    sql="sql/geo_location_table.sql",
    dag=dag
)

# Task 2 => extract geo location
extracting_geo_location = PythonOperator(
    task_id="extract-geo-location",
    python_callable=fetch_location_data,
    op_kwargs={"province": "{{ params.province }}"},
    dag=dag
)

# Task 3 => transform geo location
transforming_geo_location = PythonOperator(
    task_id="transform-geo-location",
    python_callable=transform_geo_location,
    op_kwargs={"province": "{{ params.province }}"},
    dag=dag
)

# Task 4 => load geo location
loading_geo_location = PythonOperator(
    task_id="load-geo-location",
    python_callable=load_geo_location,
    op_kwargs={
        "postgres_conn_id": "{{ params.postgres_conn_id }}",
        "table_name": "{{ params.location_table_name }}",
        "province": "{{ params.province }}"
    },
    dag=dag
)

# Task dependencies
create_location_table >> extracting_geo_location >> transforming_geo_location >> loading_geo_location