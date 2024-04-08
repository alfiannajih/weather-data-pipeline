import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

import json
import os

from airflow.providers.postgres.hooks.postgres import PostgresHook

def fetch_weather_data(
        start_date: str,
        end_date: str,
        location: dict,
        timezone: str
    ):
    # 1. Setting up retry requests if failed
    retry_strategy = Retry(
        total=5,
        backoff_factor=2
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session = requests.Session()
    session.mount('http://', adapter)
    session.mount('https://', adapter)


    weather_url = "https://archive-api.open-meteo.com/v1/archive"
    payload = {
        "latitude": location["lat"],
        "longitude": location["lon"],
        "start_date": start_date,
        "end_date": end_date,
        "daily": [
            "weather_code",
            "temperature_2m_max",
            "temperature_2m_min",
            "temperature_2m_mean",
            "apparent_temperature_max",
            "apparent_temperature_min",
            "apparent_temperature_mean",
            "sunrise",
            "sunset",
            "daylight_duration",
            "sunshine_duration",
            "precipitation_sum",
            "rain_sum",
            "snowfall_sum",
            "precipitation_hours",
            "wind_speed_10m_max",
            "wind_gusts_10m_max",
            "wind_direction_10m_dominant",
            "shortwave_radiation_sum",
            "et0_fao_evapotranspiration"
        ],
       "timezone": timezone
    }

    response = session.get(url=weather_url, params=payload).json()
    
    return response

def get_location(
        postgres_conn_id: str,
        table_name: str,
        province: str,
        start_row: int,
        batch_size: int
    ):
    postgres_hook = PostgresHook(postgres_conn_id)
    conn = postgres_hook.get_conn()
    cur = conn.cursor()

    cur.execute(f"""
SELECT
    id,
    longitude,
    latitude
FROM {table_name}
WHERE province = '{province}' AND longitude IS NOT NULL
LIMIT {batch_size} OFFSET {start_row}""")
    
    result = cur.fetchall()

    return result

def batch_extract(
        batch_size: int,
        postgres_conn_id: str,
        table_name: str,
        province: str,
        timezone: str,
        start_date: str,
        end_date: str
    ):
    year, month, date = start_date.split("-")
    start_row = 0
    while True:
        location = get_location(
            postgres_conn_id=postgres_conn_id,
            table_name=table_name,
            province=province,
            start_row=start_row,
            batch_size=batch_size
        )

        if location == []:
            break
        
        print(f"Processing batch {int((start_row + batch_size)/batch_size)}...")

        coordinates = {}
        coordinates["lon"] = [lon[1] for lon in location]
        coordinates["lat"] = [lat[2] for lat in location]

        query_result = fetch_weather_data(start_date, end_date, coordinates, timezone)
        
        result = []
        for i, entity in enumerate(query_result):
            result.append({"data": entity["daily"], "timezone": timezone, "location_id": location[i][0]})

        directory = f"/opt/airflow/data/raw/weather/{year}/{month}"
        if not os.path.exists(directory):
            os.makedirs(directory)

        with open(f"{directory}/weather_{start_row}.json", "w") as fp:
            json.dump({
                "data": result
            }, fp)
        
        start_row += batch_size