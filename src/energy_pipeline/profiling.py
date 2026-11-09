import json
from pathlib import Path
from collections import Counter


RAW_FILE = Path(
    "data/bronze/fuelhh/"
    "settlement_date=2026-11-03/records.json"
)

def load_records(file_path: Path) -> list[dict]:
    """Load a JSON array from our Bronze staging file."""

    with file_path.open("r", encoding="utf-8") as file:
        records = json.load(file)

        if not isinstance(records, list):
            raise ValueError("Expected a list of records")

        return records

if __name__ == "__main__":
    # Load Data
    records = load_records(RAW_FILE)

    print(f"Total records: {len(records)}")

    if records:
        # Inspect Available Fields
        print("\nFirst record:")
        print(json.dumps(records[1], indent=2))

        print("\nAvailable fields:")
        print(list(records[0].keys()))

        # Count Records by Fuel Type
        fuel_counts = Counter(
            record.get("fuelType")
            for record in records
        )

        print("\nFuel types and record counts:")

        for fuel_type, count in sorted(
            fuel_counts.items(),
            key=lambda item: str(item[0])
        ):
            print(f"{fuel_type}: {count}")

        # Count unique settlement periods
        period_counts = Counter(
            record.get("settlementPeriod")
            for record in records
        )
        print("\nSettlement period analysis:")
        print(f"Unique periods: {len(period_counts)}")
        print(f"First 10 periods: {sorted(period_counts)[:10]}")
        print(f"Last 10 periods: {sorted(period_counts)[-10:]}")

        print("\nRecords per period:")
        for period, count in sorted(period_counts.items()):
            print(f"Period {period}: {count}")

        # Count missing generation values
        missing_generation = sum(
            record.get("generation") is None
            for record in records
        )

        print(
            f"Missing generation values: {missing_generation}"
        )

        # Find potentital duplicate business keys
        business_keys = [
            (
                record.get("settlementDate"),
                record.get("settlementPeriod"),
                record.get("fuelType"),
            )
            for record in records
        ]

        key_counts = Counter(business_keys)

        repeated_keys = {
            key: count
            for key, count in key_counts.items()
            if count > 1
        }

        print(
            f"\nDistinct business keys: {len(key_counts)}"
        )

        print(
            f"Keys appearing more than once: {len(repeated_keys)}"
        )

        for key, count in list(repeated_keys.items())[:5]:
            print(f"Repeated key: {key}, count: {count}")
    else:
        print("No records for profiling.")