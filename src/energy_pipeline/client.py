import json

from datetime import date

import requests

BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1"
DATASET = "FUELHH"

def fetch_generation(settlement_date: str) -> list[dict]:
    """Fetch half-hourly electricity generation data for one day."""

    # Validate the date format before making an API request.
    date.fromisoformat(settlement_date)

    url = f"{BASE_URL}/datasets/{DATASET}"

    params = {
        "settlementDateFrom": settlement_date,
        "settlementDateTo": settlement_date,
    }

    response = requests.get(
        url, 
        params=params,
        timeout=(5,30),
    )

    # Raise an exception if the HTTP was unsuccesful.
    response.raise_for_status()

    # Convert the JSON response into Python objects.
    payload = response.json()

    if not isinstance(payload, dict):
        raise ValueError("Unexpected API response format.")

    records = payload.get("data")

    if not isinstance(records, list):
        raise ValueError("API response does not contain a data list.")

    return records

if __name__ == "__main__":
    records = fetch_generation("2026-11-03")

    print(f"Total records received: {len(records)}")

    if records:
        print("\nFirst record:")
        print(json.dumps(records[0], indent=2))