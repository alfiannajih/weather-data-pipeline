from airflow.providers.postgres.hooks.postgres import PostgresHook

def load_geo_location(
        postgres_conn_id: str,
        filename: str,
        table_name: str
    ):
    postgres_hook = PostgresHook(postgres_conn_id)
    conn = postgres_hook.get_conn()
    cur = conn.cursor()

    with  open(f"/opt/airflow/data/transformed/{filename}.csv", "r") as fp:
        cur.copy_expert(
            f"COPY {table_name} FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
            fp,
        )
    conn.commit()