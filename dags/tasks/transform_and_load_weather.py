import pandas as pd
import json

import os
import pandas as pd

from airflow.providers.postgres.hooks.postgres import PostgresHook

def transform_weather_data(
        postgres_conn_id: str,  # Airflow connection ID for the PostgreSQL database
        table_name: str,  # Name of the table to load the weather data into
        start_date: str  # Start date of the weather data in 'YYYY-MM-DD' format
    ):
    """
    Transforms weather data from JSON files into a CSV file and loads it into a PostgreSQL database table.

    Args:
        postgres_conn_id (str): Airflow connection ID for the PostgreSQL database.
        table_name (str): Name of the table to load the weather data into.
        start_date (str): Start date of the weather data in 'YYYY-MM-DD' format.
    """
    # Extract year, month, and date from the start_date
    year, month, date = start_date.split("-")

    # Get the list of JSON files for the given month
    json_files = get_json_list(year, month)

    # Iterate over each JSON file
    for json_file in json_files:
        # Load the weather data from the JSON file
        with open(f"/opt/airflow/data/raw/weather/{year}/{month}/{json_file}", "r") as fp:
            daily_weather = json.load(fp)["data"]

        # Extract the first record from the weather data and create a DataFrame
        input_dict = daily_weather[0]["data"]
        input_dict["location_id"] = daily_weather[0]["location_id"]
        df = pd.DataFrame.from_dict(
            input_dict
        )

        # Transform the timezone of the DataFrame
        df = transform_timezone(df, daily_weather[0]["timezone"])

        # Concatenate the data from the remaining records into the DataFrame
        for weather in daily_weather[1:]:
            input_dict = weather["data"]
            input_dict["location_id"] = weather["location_id"]
            temp_df = pd.DataFrame.from_dict(
                input_dict
            )
            temp_df = transform_timezone(temp_df, daily_weather[0]["timezone"])
            df = pd.concat([df, temp_df], ignore_index=True)

        directory = f"/opt/airflow/data/transformed/weather"  # Directory to store the results
        if not os.path.exists(directory):
            os.makedirs(directory)  # Create the directory if it doesn't exist

        # Write the transformed DataFrame into a CSV file
        with open(f"{directory}/temp.csv", "w") as fp:
            df.to_csv(fp, index=False)

        # Load the CSV file into the specified table in the PostgreSQL database
        load_to_db(directory, postgres_conn_id, table_name)

def timezone_aware(date, timezone):
    """
    Converts a date to a timezone-aware datetime.

    Args:
        date (str): The date string to be converted.
        timezone (str): The timezone to which the date should be converted.

    Returns:
        pandas.Series: A series with the date converted to the specified timezone.
    """

    # Use pandas to_datetime function to convert the date string to a datetime series.
    # Then, use the dt.tz_localize function to convert the series to the specified timezone.
    return pd.to_datetime(date).dt.tz_localize(timezone)

def transform_timezone(df, timezone):
    """
    Transforms the 'sunrise', 'sunset' and 'time' columns of a DataFrame to a
    given timezone.

    Args:
        df (pandas.DataFrame): The DataFrame to transform.
        timezone (str): The timezone to which the columns should be transformed.

    Returns:
        pandas.DataFrame: The transformed DataFrame.
    """
    # Transform the 'sunrise' column to the specified timezone
    df["sunrise"] = timezone_aware(df["sunrise"], timezone)

    # Transform the 'sunset' column to the specified timezone
    df["sunset"] = timezone_aware(df["sunset"], timezone)

    # Transform the 'time' column to the specified timezone
    df["time"] = timezone_aware(df["time"], timezone)

    return df

def get_json_list(year, month):
    """
    Retrieve a list of JSON files in a specified month and year.

    Args:
        year (str): The year of the month.
        month (str): The month.

    Returns:
        List[str]: A list of JSON file names.
    """
    # Define the folder path where the JSON files are located.
    folder_path = f"/opt/airflow/data/raw/weather/{year}/{month}"

    # Get a list of all files in the folder path.
    json_files = [
        pos_json for pos_json in os.listdir(folder_path)
        if pos_json.endswith('.json')
    ]

    # Return the list of JSON file names.
    return json_files

def load_to_db(
        directory: str,  # Directory where the CSV file is stored
        postgres_conn_id: str,  # Airflow connection ID for the PostgreSQL database
        weather_table_name: str,  # Name of the table to load the weather data into
    ):
    """
    Load transformed weather data from a CSV file into a PostgreSQL database table.

    Args:
        directory (str): Directory where the CSV file is stored.
        postgres_conn_id (str): Airflow connection ID for the PostgreSQL database.
        weather_table_name (str): Name of the table to load the weather data into.
    """
    # Create a PostgreSQL hook and get a database connection
    postgres_hook = PostgresHook(postgres_conn_id)
    conn = postgres_hook.get_conn()
    cur = conn.cursor()

    # Open the CSV file and start a COPY operation to load the data into the database table
    with open(f"{directory}/temp.csv", "r") as fp:
        cur.copy_expert(
            f"""COPY {weather_table_name} (time, weather_code, temperature_2m_max, temperature_2m_min, temperature_2m_mean, apparent_temperature_max, apparent_temperature_min, apparent_temperature_mean, sunrise, sunset, daylight_duration, sunshine_duration, precipitation_sum, rain_sum, snowfall_sum, precipitation_hours, wind_speed_10m_max, wind_gusts_10m_max, wind_direction_10m_dominant, shortwave_radiation_sum, et0_fao_evapotranspiration, location_id)
            FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'""",
            fp,
        )

    # Commit the changes to the database
    conn.commit()
