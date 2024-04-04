from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

def load_to_postgres(conn_id, table, file_name):
    postgres_hook = PostgresHook(postgres_conn_id=conn_id)
    postgres_hook.bulk_load(table, file_name)