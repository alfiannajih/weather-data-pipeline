import json
import requests
import pandas as pd

from dotenv import load_dotenv
import os

load_dotenv()

def get_add_info(address):
    """
    This function fetches geographical information and timezone for a given address.

    Args:
        address (str): The address for which information is needed.

    Returns:
        dict: A dictionary containing the coordinates and timezone of the address.
    """
    # Initialize the output dictionary with default values
    output = {
        "coordinates": [None, None],  # Longitude and Latitude of the address
        "timezone": None  # Timezone of the address
    }

    # URL of the Geoapify API
    geo_url = "https://api.geoapify.com/v1/geocode/search"

    try:
        # Send a GET request to the Geoapify API with the address and API key
        response = requests.get(
            geo_url,
            {
                "text": address,  # Address to search for
                "limit": 1,  # Limit the number of results to 1
                "country": "Indonesia",  # Search only in Indonesia
                "apiKey": os.getenv("GEOAPIFY_API_KEY")  # API key for authentication
            }
        ).json()["features"][0]  # Get the first feature from the response

        # Extract the coordinates and timezone from the response
        output["coordinates"] = response["geometry"]["coordinates"]
        output["timezone"] = response["properties"]["timezone"]["name"]

    except:
        # If there is an error, do nothing
        pass
    
    return output

def transform_geo_location(province: str):
    """
    Transforms geographical location data from a JSON file into a CSV file.

    Args:
        province (str): The name of the province to be transformed.
    """
    # Initialize lists to store data
    provinces = []  # List to store province names
    cities = []  # List to store city names
    districts = []  # List to store district names
    id = []  # List to store district IDs
    latitude = []  # List to store latitude coordinates
    longitude = []  # List to store longitude coordinates
    timezone = []  # List to store timezones

    # Open the JSON file and load its contents
    with open(f"/opt/airflow/data/raw/location/geo_location_({province}).json", "r") as fp:
        # Load the contents of the JSON file
        hierachy_loc = json.load(fp)

    # Extract location data from the JSON file and store in lists
    print(f"Get {len(hierachy_loc['cities'])} cities")
    for i, city in enumerate(hierachy_loc["cities"]):
        print(f"Transforming {city['city']} ({i+1}/{len(hierachy_loc['cities'])})...")
        print(f"Get {len(city['districts'])} districts from {city['city']}")
        
        for district in city["districts"]:
            print(f"Transforming {district['district']} ({i+1}/{len(city['districts'])})...")
            # Province
            provinces.append(hierachy_loc["province"].title())
            # City
            cities.append(city["city"].title().replace("KAB.", ""))
            # District
            districts.append(district["district"].title())
            # ID
            id.append(district["id"])

            # Address
            address = f"{district['district']}, {city['city']}, {hierachy_loc['province']}"
            # Get additional information about the address
            add_info = get_add_info(address)

            # Longitude and Latitude
            longitude.append(add_info["coordinates"][0])
            latitude.append(add_info["coordinates"][1])

            # Timezone
            timezone.append(add_info["timezone"])

    # Create a dictionary of the extracted data
    raw_dict = {
        "id": id,
        "province": provinces,
        "city": cities,
        "district": districts,
        "longitude": longitude,
        "latitude": latitude,
        "timezone": timezone
    }
    
    # Create a DataFrame from the dictionary
    transformed_df = pd.DataFrame.from_dict(raw_dict)

    directory = f"/opt/airflow/data/transformed/location"  # Directory to store the results
    if not os.path.exists(directory):
        os.makedirs(directory)  # Create the directory if it doesn't exist

    # Save the transformed DataFrame to a CSV file
    with open(f"{directory}/geo_location_({province}).csv", "w") as fp:
        # Save the transformed DataFrame to a CSV file
        transformed_df.to_csv(fp, index=False)
