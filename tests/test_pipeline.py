import pytest

from energy_pipeline.validation import validate_record
from energy_pipeline.silver import process_records

# TEST DATA FACTORY 

def make_record(**changes):
    """Create a sample valid Elexon record for testing."""

    record = {
        "dataset": "FUELHH",
        "publishTime": "2026-11-04T00:00:00Z",
        "startTime": "2026-11-03T23:30:00Z",
        "settlementDate": "2026-11-03",
        "settlementPeriod": 48,
        "fuelType": "BIOMASS",
        "generation": 3324,
    }

    record.update(changes)

    return record

# RECORD LEVEL VALIDATION TESTS
def test_valid_record():
    record = make_record()

    errors = validate_record(record)

    assert errors == []

@pytest.mark.parametrize(
    "changes, expected_error",
    [
        (
            {"generation": None},
            "Invalid generation"
        ),
        (
            {"generation": "unknown"},
            "Invalid generation"
        ),
        (
            {"settlementPeriod": 99},
            "Invalid settlementPeriod"
        ),
        (
            {"settlementDate": "2026-02-30"},
            "Invalid settlementDate"
        ),
        (
            {"fuelType": ""},
            "Invalid fuelType"
        )
    ]
)
def test_invalid_record(changes, expected_error):
    # Arrange
    record = make_record(**changes)

    # Act
    errors = validate_record(record)

    # Assert
    assert expected_error in errors

def test_missing_required_field():

    record = make_record()

    del record["generation"]

    errors = validate_record(record)

    assert "Missing required field: generation" in errors

# DATASET-LEVEL VALIDATION TESTS

def test_duplicate_business_keys():

    record_1 = make_record(
        generation=3324
    )

    record_2 = make_record(
        generation=3500,
        publishTime="2026-11-04T01:00:00Z"
    )

    accepted, quarantined = process_records(
        [record_1, record_2]
    )

    assert len(accepted) == 0
    assert len(quarantined) == 2

    for item in quarantined:
        assert(
            "Repeated business key: revision review required"
            in item["errors"]
        )

def test_mixed_valid_and_invalid_records():

    valid_1 = make_record(
        settlementPeriod=1
    )

    invalid = make_record(
        settlementPeriod=2,
        generation=None
    )

    valid_2 = make_record(
        settlementPeriod=3
    )

    accepted, quarantined = process_records(
        [valid_1, invalid, valid_2]
    )

    assert len(accepted) == 2
    assert len(quarantined) == 1

    assert "Invalid generation" in quarantined[0]["errors"]
