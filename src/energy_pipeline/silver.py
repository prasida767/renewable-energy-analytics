import json
from collections import Counter
from pathlib import Path

from energy_pipeline.profiling import load_records
from energy_pipeline.validation import validate_record


BUSINESS_KEY_FIELDS = {
    "settlementDate",
    "settlementPeriod",
    "fuelType",
}

def get_business_key(record: dict) -> tuple:
    """Build the candidates key for a FUELHH measurement."""

    return tuple(
        record[field]
        for field in BUSINESS_KEY_FIELDS
    )

def process_records(
        records: list[dict]
) -> tuple[list[dict], list[dict]]:
    """Separate accepted records from quantined records."""
    eligible = []
    quarantined = []

    # PASS 1: Valdiate individual records.
    for row_number, record in enumerate(records, start=1):
        errors = validate_record(record)

        if errors:
            quarantined.append({
                "row_number": row_number,
                "errors": errors,
                "record": record,
            })
        else:
            eligible.append((row_number, record))

    # Count candidate business keys among eligible records.
    key_counts = Counter(
        get_business_key(record)
        for _, record in eligible
    )

    # PASS 2: Check uniqueness across the dataset.
    accepted = []

    for row_number, record in eligible:
        key = get_business_key(record)

        if key_counts[key] > 1:
            quarantined.append({
                "row_number": row_number,
                "errors": [
                    "Repeated business key: revision review required"
                ],
                "record": record,
            })
        else:
            accepted.append(record)

    return accepted, quarantined

def save_json(data: list | dict, output_path: Path) -> None:
    """Write JSON to disk using a temporary file."""

    output_path.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    temporary_path = output_path.with_suffix(".json.tmp")

    with temporary_path.open("w", encoding="utf-8") as file:
        json.dump(data, file, indent=2)

    temporary_path.replace(output_path)


def run_silver(settlement_date: str) -> dict:
    """Process one settlement date from bronze to silver."""

    bronze_path = Path(
        f"data/bronze/fuelhh/"
        f"settlement_date={settlement_date}/records.json"
    )

    # Load Bronze records.
    records = load_records(bronze_path)

    if not records:
        raise ValueError(
            f"No Bronze records for {settlement_date}"
        )

    # Validate and separate records.
    accepted, quarantined = process_records(records)

    # Calculate quality metrics.
    total = len(records)

    pass_rate = round(
        len(accepted) / total * 100, 2
    )

    report = {
        "settlement_date": settlement_date,
        "total_records": total,
        "accepted_records": len(accepted),
        "quarantined_records": len(quarantined),
        "pass_rate_percent": pass_rate,
    }

    # Build output paths.
    silver_path = Path(
        f"data/silver/fuelhh"
        f"settlement_date={settlement_date}/records.json"
    )

    quarantine_path = Path(
        f"data/quarantine/fuelhh/"
        f"settlement_date={settlement_date}/records.json"
    )

    report_path = Path(
        f"data/quality/fuelhh/"
        f"settlement_date={settlement_date}/report.json"
    )

    # Save results
    save_json(accepted, silver_path)
    save_json(quarantined, quarantine_path)
    save_json(report, report_path)

    print(json.dumps(report, indent=2))

    return report

if __name__ == "__main__":
    run_silver("2026-11-03")