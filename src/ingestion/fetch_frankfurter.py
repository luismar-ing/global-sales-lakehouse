import requests
from src.ingestion.upload_to_landing import upload_api_data

FRANKFURTER_URL = "https://api.frankfurter.app"

def fetch_rates(start_date: str, end_date: str, base_currency: str) -> bytes:
    url = f"{FRANKFURTER_URL}/{start_date}..{end_date}"

    response = requests.get(
        url,
        params={"from": base_currency}
    )
    response.raise_for_status()

    return response.content

def upload_updated_rates(start_date: str, end_date: str, base_currency: str) -> None:
    data = fetch_rates(start_date, end_date, base_currency)

    path = f"frankfurter/rates_{start_date}_{end_date}.json"

    upload_api_data(data, path)

if __name__ == "__main__":
    upload_updated_rates("2009-12-01", "2014-12-31", "USD")
