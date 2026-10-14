import requests
import json
from src.common import config
from src.ingestion.upload_to_landing import upload_api_data

PAGE_SIZE = 100
MAX_REQUESTS = 10
REST_COUNTRIES_URL = "https://api.restcountries.com/countries/v5"

def validate_countries_page(parsed: dict) -> tuple[list, dict]:
    data = parsed.get("data", {})

    countries = data.get("objects")  if isinstance(data, dict) else None
    meta = data.get("meta") if isinstance(data, dict) else None

    if not isinstance(countries, list) or not isinstance(meta, dict):
        raise ValueError(f"Respuesta inesperada de la API: {parsed}")

    return countries, meta

def fetch_countries_page(api_key: str, limit: int, offset: int) -> tuple[list, dict]:
    headers = {
        "Authorization": f"Bearer {api_key}"
    }

    params = {
        "limit": limit,
        "offset": offset
    }

    response = requests.get(
        REST_COUNTRIES_URL,
        headers=headers,
        params=params,
        timeout=30
    )

    response.raise_for_status()
    parsed = response.json()

    return validate_countries_page(parsed)

def fetch_countries(api_key: str) -> bytes:
    all_countries = []
    offset = 0
    meta = None
    request_count = 1


    while True:
        page, meta = fetch_countries_page(api_key, PAGE_SIZE, offset=offset)

        all_countries.extend(page)

        if meta["more"] is False:
            break

        if request_count >= MAX_REQUESTS:
            break

        offset += PAGE_SIZE
        request_count += 1

    if len(all_countries) != meta["total"]:
        raise ValueError(f"Paginacion incompleta: se recibieron {len(all_countries)} paises, pero la API indica un total de {meta['total']}")

    return json.dumps(
        all_countries,
        ensure_ascii=False
    ).encode("utf-8")

def upload_countries() -> None:
    data = fetch_countries(config.REST_COUNTRIES_API_KEY)

    path = f"rest_countries/countries.json"

    upload_api_data(data, path)

if __name__ == "__main__":
    upload_countries()