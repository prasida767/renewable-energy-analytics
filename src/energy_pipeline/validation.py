import math
import re
from datetime import date, datetime
from energy_pipeline.profiling import RAW_FILE, load_records

REQUIRED_FIELDS = {
    "dataset",
    "publishTime",
    "startTime",
    "settlementDate",
    "settlementPeriod",
    "fuelType",
    "generation",
}

def validate_record(record: dict) -> list[str]:
    """Return validation errors for one FUELHH record."""

    errors = []

    if not isinstance(record, dict):
        return ["Record must be a dictionary."]

    # Check that every required field exists.
    for field in REQUIRED_FIELDS:
        if field not in record:
            errors.append(f"Missing required field: {field}")

    # Verify dataset identity.
    if "dataset" in record and record["dataset"] != "FUELHH":
        errors.append("Invalid dataset")

    # Validate settlement date
    if "settlementDate" in record:
        value = record["settlementDate"]

        try:
            if not isinstance(value, str):
                raise ValueError
            if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
                raise ValueError()

            date.fromisoformat(value)
        except ValueError:
            errors.append("Invalid settlementDate")

    # Validate settlement period.
    if "settlementPeriod" in record:
        period = record["settlementPeriod"]

        if type(period) is not int or not 1 <= period <=50:
            errors.append("Invalid settlementPeriod")

    # Validate fuel type.
    if "fuelType" in record:
        fuel = record["fuelType"]

        if not isinstance(fuel, str) or not fuel.strip():
            errors.append("Invalid fuelType")

    # Validate generation.
    if "generation" in record:
        generation = record["generation"]

        if(
            type(generation) not in (int, float)
            or not math.isfinite(generation)
        ):
            errors.append("Invalid generation")

    # Validate timestamp fields.
    for field in("publishTime", "startTime"):
        if field not in record:
            continue

        try:
            value = record[field]

            if not isinstance(value, str):
                raise ValueError()

            timestamp = datetime.fromisoformat(
                value.replace("Z", "+00:00")
            )
            if timestamp.tzinfo is None:
                raise ValueError()

        except ValueError:
            errors.append(f"Invalid {field}")

    return errors

if __name__ == "__main__":
    records = load_records(RAW_FILE)

    valid_count = 0
    invalid_count = 0

    for record in records:
        errors = validate_record(record)

        if errors:
            invalid_count += 1
            print(f"Invalid record: {errors}")
        else:
            valid_count += 1

    print(f"\nTotal records: {len(records)}")
    print(f"Valid records: {valid_count}")
    print(f"Invalid records: {invalid_count}")

    bad_record = records[0].copy()
    bad_record["settlementPeriod"] = "48"
    bad_record["generation"] = None
    print("\nTesting an intentionally invalid record:")
    print(validate_record(bad_record))
