import requests
from src.ingestion.upload_to_landing import upload_api_data

REST_COUNTRIES_URL = "https://restcountries.com/v3.1/all"

def fetch_countries() -> bytes:
    response = requests.get(REST_COUNTRIES_URL)
    response.raise_for_status()

    return response.content

def upload_countries() -> None:
    data = fetch_countries()

    path = f"rest_countries/countries.json"

    upload_api_data(data, path)

if __name__ == "__main__":
    upload_countries()