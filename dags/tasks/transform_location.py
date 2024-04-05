import json
import requests
import pandas as pd

def get_lon_lat(address):
    geo_url = "https://api.geoapify.com/v1/geocode/search"
    response = requests.get(
        geo_url,
        {
            "text": address,
            "limit": 1,
            "country": "Indonesia",
            "apiKey": "ff2255fac5754aff859d970f6bbc37a3"
        }
    ).json()["features"][0]["geometry"]
    
    return response["coordinates"]

def transform_geo_location(filename: str):
    provinces = []
    cities = []
    districts = []
    id = []
    latitude = []
    longitude = []

    with open(f"/opt/airflow/data/raw/{filename}.json", "r") as fp:
        hierachy_loc = json.load(fp)

    for city in hierachy_loc["cities"]:
        for district in city["districts"]:
            provinces.append(hierachy_loc["province"].title())
            cities.append(city["city"].title().replace("KAB.", ""))
            districts.append(district["district"].title())
            id.append(district["id"])

            address = f"{district['district']}, {city['city']}, {hierachy_loc['province']}"
            loc = get_lon_lat(address)
            
            longitude.append(loc[0])
            latitude.append(loc[1])
        break

    raw_dict = {
        "province": provinces,
        "city": cities,
        "district": districts,
        "id": id,
        "longitude": longitude,
        "latitude": latitude
    }
    
    transformed_df = pd.DataFrame.from_dict(raw_dict)

    with open(f"/opt/airflow/data/transformed/{filename}.csv", "w") as fp:
        transformed_df.to_csv(fp, index=False)