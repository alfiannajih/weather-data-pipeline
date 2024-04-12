from airflow.providers.postgres.hooks.postgres import PostgresHook

def load_geo_location(
        postgres_conn_id: str,  # The Airflow connection ID for PostgreSQL
        province: str,  # The name of the CSV file to load
        table_name: str  # The name of the table to load the data into
    ):
    """
    Load a geographic location CSV file into a PostgreSQL table.

    Args:
        postgres_conn_id (str): The Airflow connection ID for PostgreSQL.
        province (str): The name of the province CSV file to load.
        table_name (str): The name of the table to load the data into.
    """
    # Establish a connection to the PostgreSQL database
    postgres_hook = PostgresHook(postgres_conn_id)
    conn = postgres_hook.get_conn()
    cur = conn.cursor()

    # Open the CSV file for reading
    with  open(f"/opt/airflow/data/transformed/location/geo_location_({province}).csv", "r") as fp:
        # Use the COPY command to load the data into the table
        cur.copy_expert(
            f"COPY {table_name} FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
            fp,
        )

    # Commit the transaction and close the connection
    conn.commit()
    conn.close()
