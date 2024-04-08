import json
import requests
import pandas as pd

from dotenv import load_dotenv
import os

load_dotenv()

def get_add_info(address):
    output = {
        "coordinates": [None, None],
        "timezone": None
    }

    geo_url = "https://api.geoapify.com/v1/geocode/search"

    try:
        response = requests.get(
            geo_url,
            {
                "text": address,
                "limit": 1,
                "country": "Indonesia",
                "apiKey": os.getenv("GEOAPIFY_API_KEY")
            }
        ).json()["features"][0]
        
        output["coordinates"] = response["geometry"]["coordinates"]
        output["timezone"] = response["properties"]["timezone"]["name"]

    except:
        pass
    
    return output

def transform_geo_location(filename: str):
    provinces = []
    cities = []
    districts = []
    id = []
    latitude = []
    longitude = []
    timezone = []

    with open(f"/opt/airflow/data/raw/{filename}.json", "r") as fp:
        hierachy_loc = json.load(fp)

    for city in hierachy_loc["cities"]:
        for district in city["districts"]:
            provinces.append(hierachy_loc["province"].title())
            cities.append(city["city"].title().replace("KAB.", ""))
            districts.append(district["district"].title())
            id.append(district["id"])

            address = f"{district['district']}, {city['city']}, {hierachy_loc['province']}"
            add_info = get_add_info(address)
            
            longitude.append(add_info["coordinates"][0])
            latitude.append(add_info["coordinates"][1])

            timezone.append(add_info["timezone"])
            
            break

    raw_dict = {
        "id": id,
        "province": provinces,
        "city": cities,
        "district": districts,
        "longitude": longitude,
        "latitude": latitude,
        "timezone": timezone
    }
    
    transformed_df = pd.DataFrame.from_dict(raw_dict)

    with open(f"/opt/airflow/data/transformed/{filename}.csv", "w") as fp:
        transformed_df.to_csv(fp, index=False)