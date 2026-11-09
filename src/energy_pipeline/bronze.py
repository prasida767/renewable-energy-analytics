import json
from datetime import date
from pathlib import Path

BRONZE_ROOT = Path("data/bronze/fuelhh")

def save_raw_records(
        records: list[dict],
        settlement_date: str
) -> Path:
    """Save Elexon records to a date-partitioned JSON file."""

    date.fromisoformat(settlement_date)

    if not records:
        raise ValueError("Cannot save an empty dataset.")

    # Create a directory for the settlement date.
    directory = (
        BRONZE_ROOT
        / f"settlement_date={settlement_date}"
    )

    directory.mkdir(parents=True, exist_ok=True)

    # Define the output file.
    file_path = directory / "records.json"

    # Serialize the Python objects to JSON.
    with file_path.open("w", encoding="utf-8") as file:
        json.dump(records, file, indent=2)

    return file_path