import logging
import argparse

from retry_requests import retry
import openmeteo_requests
import requests_cache
import requests

import pyarrow as pa
import pyarrow.parquet as pq

import pandas as pd

import time

import os

logger = logging.getLogger(__name__)

def fetch_location_data(province, start_row):
    geo_url = "http://api.geonames.org/searchJSON"
    payload = {
        "q": province,
        "country": "ID",
        "maxRows": 10,
        "startRow": start_row,
        "featureClass": "P",
        "username": "alfiannajih"
    }

    cities_location = requests.get(geo_url, params=payload)

    cities = []
    longitude = []
    latitude = []
    
    for city in cities_location.json()["geonames"]:
        cities.append(city["name"])
        longitude.append(city["lng"])
        latitude.append(city["lat"])

    return {"name": cities, "longitude": longitude, "latitude": latitude, "start_row": start_row + payload["maxRows"]}

def fetch_weather_data(args):
    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 2)
    openmeteo = openmeteo_requests.Client(session = retry_session)

    # Make sure all required weather variables are listed here
    # The order of variables in hourly or daily is important to assign them correctly below
    url = "https://archive-api.open-meteo.com/v1/archive"
    temp_row = 0
    for i in range(98):
        logger.info(f"Batch {i+1} is started...")
        location = fetch_location_data(province=args.province, start_row=temp_row)
        temp_row = location["start_row"]
        params = {
            "latitude": location["latitude"],
            "longitude": location["longitude"],
            "start_date": args.start_date,
            "end_date": args.end_date,
            "daily": ["weather_code", "temperature_2m_max", "temperature_2m_min", "temperature_2m_mean", "apparent_temperature_max", "apparent_temperature_min", "apparent_temperature_mean", "sunrise", "sunset", "daylight_duration", "sunshine_duration", "precipitation_sum", "rain_sum", "snowfall_sum", "precipitation_hours", "wind_speed_10m_max", "wind_gusts_10m_max", "wind_direction_10m_dominant", "shortwave_radiation_sum", "et0_fao_evapotranspiration"],
            "timezone": "Asia/Bangkok"
        }

        year = args.start_date.split("-")[0]
        path = os.path.join(args.output, f"{year}")
        if not os.path.isdir(path):
            os.makedirs(path)

        responses = openmeteo.weather_api(url, params=params)
        for j, response in enumerate(responses):
            data_dict = read_response(response)
            size = len(data_dict["date"])
            
            data_dict["city"] = [location["name"][j]] * size
            data_dict["longitude"] = [location["longitude"][j]] * size
            data_dict["latitude"] = [location["latitude"][j]] * size

            daily_data = pd.DataFrame(data_dict)

            full_path = os.path.join(path, f"{location['name'][j]}.parquet")

            save_parquet(
                df=daily_data,
                file_name=full_path
            )
        time.sleep(40)
        logger.info(f"Batch {i+1} is finished...")

def read_response(response):
    # Process daily data. The order of variables needs to be the same as requested.
    daily = response.Daily()
    daily_weather_code = daily.Variables(0).ValuesAsNumpy()
    daily_temperature_2m_max = daily.Variables(1).ValuesAsNumpy()
    daily_temperature_2m_min = daily.Variables(2).ValuesAsNumpy()
    daily_temperature_2m_mean = daily.Variables(3).ValuesAsNumpy()
    daily_apparent_temperature_max = daily.Variables(4).ValuesAsNumpy()
    daily_apparent_temperature_min = daily.Variables(5).ValuesAsNumpy()
    daily_apparent_temperature_mean = daily.Variables(6).ValuesAsNumpy()
    daily_sunrise = daily.Variables(7).ValuesAsNumpy()
    daily_sunset = daily.Variables(8).ValuesAsNumpy()
    daily_daylight_duration = daily.Variables(9).ValuesAsNumpy()
    daily_sunshine_duration = daily.Variables(10).ValuesAsNumpy()
    daily_precipitation_sum = daily.Variables(11).ValuesAsNumpy()
    daily_rain_sum = daily.Variables(12).ValuesAsNumpy()
    daily_snowfall_sum = daily.Variables(13).ValuesAsNumpy()
    daily_precipitation_hours = daily.Variables(14).ValuesAsNumpy()
    daily_wind_speed_10m_max = daily.Variables(15).ValuesAsNumpy()
    daily_wind_gusts_10m_max = daily.Variables(16).ValuesAsNumpy()
    daily_wind_direction_10m_dominant = daily.Variables(17).ValuesAsNumpy()
    daily_shortwave_radiation_sum = daily.Variables(18).ValuesAsNumpy()
    daily_et0_fao_evapotranspiration = daily.Variables(19).ValuesAsNumpy()

    daily_data = {"date": pd.date_range(
        start = pd.to_datetime(daily.Time(), unit = "s", utc = True),
        end = pd.to_datetime(daily.TimeEnd(), unit = "s", utc = True),
        freq = pd.Timedelta(seconds = daily.Interval()),
        inclusive = "left"
    )}

    logger.info(daily_data)
    
    daily_data["weather_code"] = daily_weather_code
    daily_data["temperature_2m_max"] = daily_temperature_2m_max
    daily_data["temperature_2m_min"] = daily_temperature_2m_min
    daily_data["temperature_2m_mean"] = daily_temperature_2m_mean
    daily_data["apparent_temperature_max"] = daily_apparent_temperature_max
    daily_data["apparent_temperature_min"] = daily_apparent_temperature_min
    daily_data["apparent_temperature_mean"] = daily_apparent_temperature_mean
    daily_data["sunrise"] = daily_sunrise
    daily_data["sunset"] = daily_sunset
    daily_data["daylight_duration"] = daily_daylight_duration
    daily_data["sunshine_duration"] = daily_sunshine_duration
    daily_data["precipitation_sum"] = daily_precipitation_sum
    daily_data["rain_sum"] = daily_rain_sum
    daily_data["snowfall_sum"] = daily_snowfall_sum
    daily_data["precipitation_hours"] = daily_precipitation_hours
    daily_data["wind_speed_10m_max"] = daily_wind_speed_10m_max
    daily_data["wind_gusts_10m_max"] = daily_wind_gusts_10m_max
    daily_data["wind_direction_10m_dominant"] = daily_wind_direction_10m_dominant
    daily_data["shortwave_radiation_sum"] = daily_shortwave_radiation_sum
    daily_data["et0_fao_evapotranspiration"] = daily_et0_fao_evapotranspiration

    return daily_data

def save_parquet(df, file_name):
    table = pa.Table.from_pandas(df)
    pq.write_table(table, f'{file_name}')

def main(args):
    logger.info("Job is started!")
    fetch_weather_data(args=args)
    logger.info("Job is finished!")

if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.INFO
    )

    parser = argparse.ArgumentParser(description="Extract raw data.")
    parser.add_argument("--province", type=str, help="The name of province")
    parser.add_argument("--start_date", type=str, help="The start of recorded date (YYYY-MM-DD)")
    parser.add_argument("--end_date", type=str, help="The end of recorded date (YYYY-MM-DD)")
    parser.add_argument("--output", type=str, help="The output of the file")

    args = parser.parse_args()
    
    main(args)