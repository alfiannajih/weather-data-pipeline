import pandas as pd
import json

import os
import pandas as pd

from airflow.providers.postgres.hooks.postgres import PostgresHook

def transform_weather_data(
        postgres_conn_id: str,
        table_name: str,
        start_date: str
    ):
    year, month, date = start_date.split("-")
    json_files = get_json_list(year, month)

    for json_file in json_files:
        with open(f"/opt/airflow/data/raw/weather/{year}/{month}/{json_file}", "r") as fp:
            daily_weather = json.load(fp)["data"]

        input_dict = daily_weather[0]["data"]
        input_dict["location_id"] = daily_weather[0]["location_id"]
        df = pd.DataFrame.from_dict(
            input_dict
        )

        df = transform_timezone(df, daily_weather[0]["timezone"])

        for weather in daily_weather[1:]:
            input_dict = weather["data"]
            
            input_dict["location_id"] = weather["location_id"]
            
            temp_df = pd.DataFrame.from_dict(
                input_dict
            )
            temp_df = transform_timezone(temp_df, daily_weather[0]["timezone"])

            df = pd.concat([df, temp_df], ignore_index=True)

        with open(f"/opt/airflow/data/transformed/temp.csv", "w") as fp:
            df.to_csv(fp, index=False)

        load_to_db(postgres_conn_id, table_name)

def timezone_aware(date, timezone):
    return pd.to_datetime(date).dt.tz_localize(timezone)

def transform_timezone(df, timezone):
    df["sunrise"] = timezone_aware(df["sunrise"], timezone)
    df["sunset"] = timezone_aware(df["sunset"], timezone)
    df["time"] = timezone_aware(df["time"], timezone)

    return df

def get_json_list(year, month):
    folder_path = f"/opt/airflow/data/raw/weather/{year}/{month}"
    json_files = [pos_json for pos_json in os.listdir(folder_path) if pos_json.endswith('.json')]

    return json_files

def load_to_db(
        postgres_conn_id: str,
        table_name: str
    ):
    postgres_hook = PostgresHook(postgres_conn_id)
    conn = postgres_hook.get_conn()
    cur = conn.cursor()

    with  open(f"/opt/airflow/data/transformed/temp.csv", "r") as fp:
        cur.copy_expert(
            f"""COPY {table_name} (time, weather_code, temperature_2m_max, temperature_2m_min, temperature_2m_mean, apparent_temperature_max, apparent_temperature_min, apparent_temperature_mean, sunrise, sunset, daylight_duration, sunshine_duration, precipitation_sum, rain_sum, snowfall_sum, precipitation_hours, wind_speed_10m_max, wind_gusts_10m_max, wind_direction_10m_dominant, shortwave_radiation_sum, et0_fao_evapotranspiration, location_id)
            FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'""",
            fp,
        )
    conn.commit()