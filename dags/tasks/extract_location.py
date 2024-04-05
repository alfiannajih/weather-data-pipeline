import json

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

def fetch_location_data(
        filename: str,
        province: str
    ):
    # 1. Find Province Code
    loc_url = "https://sig.bps.go.id/rest-bridging-dagri/getwilayah"
    provinces_response = requests.get(
        loc_url,
        {"level": "provinsi"}
    ).json()

    for province_json in provinces_response:
        if province_json["nama_bps"] == province.upper():
            province_code = province_json["kode_bps"]
            break
    
    # 2. Find all city from province code
    cities_response = requests.get(
        loc_url,
        {
            "level": "kabupaten",
            "parent": f"{province_code}."
        }
    ).json()

    # 3. Find all district from each city code
    cities = []
    for city in cities_response:
        districts_response = requests.get(
            loc_url,
            {
                "level": "kecamatan",
                "parent": f"{city['kode_dagri']}"
            }
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

    with open(f"/opt/airflow/data/raw/{filename}.json", "w") as fp:
        json.dump(
            {"province": province, "id": province_json["kode_bps"], "cities": cities},
            fp
        )