import json
import os
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

def fetch_location_data(province: str) -> None:
    """
    Fetch location data for a given province and write the result to a file.

    Args:
        province (str): The name of the province to fetch data for.

    Returns:
        None
    """
    # 1. Find Province Code
    # URL of the location service API
    loc_url = "https://sig.bps.go.id/rest-bridging-dagri/getwilayah"

    # Fetch provinces data from the location service API
    provinces_response = requests.get(
        loc_url, {"level": "provinsi"}
    ).json()

    # Find the code of the province given in the argument
    for province_json in provinces_response:
        if province_json["nama_bps"] == province.upper():
            province_code = province_json["kode_bps"]
            break

    # 2. Find all city from province code
    # Fetch cities data for the province
    cities_response = requests.get(
        loc_url,
        {"level": "kabupaten", "parent": f"{province_code}."}
    ).json()

    # 3. Find all district from each city code
    # Extract district data for each city
    cities = []
    for city in cities_response:
        districts_response = requests.get(
            loc_url,
            {"level": "kecamatan", "parent": f"{city['kode_dagri']}"}
        ).json()

        districts = []
        for district in districts_response:
            districts.append({
                "district": district['nama_dagri'],
                "id": district['kode_bps']
            })

        cities.append({
            "city": city['nama_dagri'],
            "id": city['kode_bps'],
            "districts": districts
        })

    directory = f"/opt/airflow/data/raw/location"  # Directory to store the results
    if not os.path.exists(directory):
        os.makedirs(directory)  # Create the directory if it doesn't exist

    # Write result to a file
    with open(f"{directory}/geo_location_({province}).json", "w") as fp:
        json.dump({
            "province": province,
            "id": province_json["kode_bps"],
            "cities": cities
        }, fp)
