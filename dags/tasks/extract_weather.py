import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

import json
import os
import time
from airflow.providers.postgres.hooks.postgres import PostgresHook

def fetch_weather_data(
        start_date: str,
        end_date: str,
        location: dict,
        timezone: str
    ):
    """
    Fetches weather data from the Open-Meteo API.

    Args:
        start_date (str): Start date of the weather data in 'YYYY-MM-DD' format.
        end_date (str): End date of the weather data in 'YYYY-MM-DD' format.
        location (dict): Dictionary containing latitude and longitude coordinates.
        timezone (str): Timezone for the weather data.

    Returns:
        dict: JSON response containing the weather data.
    """

    # Define the retry strategy with a total of 5 attempts and a backoff factor of 2.
    retry_strategy = Retry(
        total=5,
        backoff_factor=2
    )

    # Mount the retry strategy to the HTTPAdapter.
    adapter = HTTPAdapter(max_retries=retry_strategy)

    # Create a session object and mount the adapter to HTTP and HTTPS protocols.
    session = requests.Session()
    session.mount('http://', adapter)
    session.mount('https://', adapter)


    # Define the URL for the Open-Meteo API.
    weather_url = "https://archive-api.open-meteo.com/v1/archive"

    # Define the payload containing the weather data parameters.
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

    # Send a GET request to the Open-Meteo API with the defined payload and return the JSON response.
    response = session.get(url=weather_url, params=payload).json()
    
    return response

def get_location(
        postgres_conn_id: str,
        geo_location_table_name: str,
        province: str,
        start_row: int,
        batch_size: int
    ):
    """
    Retrieves location data from a PostgreSQL table.

    Args:
        postgres_conn_id (str): The Airflow connection ID for the PostgreSQL database.
        geo_location_table_name (str): The name of the table to retrieve data from.
        province (str): The province to filter the data by.
        start_row (int): The starting row number for the query.
        batch_size (int): The number of rows to retrieve in each query.

    Returns:
        List[Tuple[int, float, float]]: A list of tuples containing the id, longitude, and latitude of each location.
    """
    # Establish a connection to the PostgreSQL database
    postgres_hook = PostgresHook(postgres_conn_id)
    conn = postgres_hook.get_conn()
    cur = conn.cursor()

    # Execute a SELECT query to retrieve the desired data
    cur.execute(
        f"""
        SELECT
            id,
            longitude,
            latitude
        FROM {geo_location_table_name}
        WHERE province = '%s' AND longitude IS NOT NULL
        LIMIT %s OFFSET %s
        """ % (province, batch_size, start_row)
    )

    # Fetch the results of the query
    result = cur.fetchall()

    return result

def fetch_batch_weather_data(
        batch_size: int,  # Number of rows to retrieve in each query
        postgres_conn_id: str,  # Airflow connection ID for the PostgreSQL database
        geo_location_table_name: str,  # Name of the table to retrieve data from
        province: str,  # Province to filter the data by
        timezone: str,  # Timezone for the weather data
        start_date: str,  # Start date of the weather data in 'YYYY-MM-DD' format
        end_date: str  # End date of the weather data in 'YYYY-MM-DD' format
    ):
    """
    Extract weather data from a PostgreSQL table and fetch corresponding weather data.
    Results are stored in json files in a directory structured by year and month.

    Args:
        batch_size (int): Number of rows to retrieve in each query.
        postgres_conn_id (str): Airflow connection ID for the PostgreSQL database.
        geo_location_table_name (str): Name of the table to retrieve data from.
        province (str): Province to filter the data by.
        timezone (str): Timezone for the weather data.
        start_date (str): Start date of the weather data in 'YYYY-MM-DD' format.
        end_date (str): End date of the weather data in 'YYYY-MM-DD' format.
    """
    year, month, date = start_date.split("-")
    start_row = 0  # Starting row number for the query
    
    while True:
        # Retrieve location data from the PostgreSQL table
        location = get_location(
            postgres_conn_id=postgres_conn_id,
            geo_location_table_name=geo_location_table_name,
            province=province,
            start_row=start_row,
            batch_size=batch_size
        )

        if location == []:  # If no more location data, exit the loop
            break
        
        print(f"Processing batch {int((start_row + batch_size)/batch_size)}...")

        coordinates = {}  # Coordinates for fetching weather data
        coordinates["lon"] = [lon[1] for lon in location]  # Longitude coordinates
        coordinates["lat"] = [lat[2] for lat in location]  # Latitude coordinates

        # Fetch weather data using the coordinates and time period
        query_result = fetch_weather_data(start_date, end_date, coordinates, timezone)
        
        result = []  # List to store the result
        for i, entity in enumerate(query_result):
            result.append({"data": entity["daily"], "timezone": timezone, "location_id": location[i][0]})

        directory = f"/opt/airflow/data/raw/weather/{year}/{month}"  # Directory to store the results
        if not os.path.exists(directory):
            os.makedirs(directory)  # Create the directory if it doesn't exist

        with open(f"{directory}/weather_{start_row}.json", "w") as fp:
            json.dump({  # Store the result in a json file
                "data": result
            }, fp)
        
        start_row += batch_size  # Update the starting row number for the next query
        time.sleep(5)