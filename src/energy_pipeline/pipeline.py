import argparse
from datetime import date, timedelta

from energy_pipeline.ingest import run_ingestion
from energy_pipeline.silver import run_silver


def parse_date(value: str) -> date:
    """Parse a date supplied through the command line."""

    try:
        parsed = date.fromisoformat(value)

        if parsed.isoformat() != value:
            raise ValueError()

        return parsed

    except ValueError:
        raise argparse.ArgumentTypeError(
            "Date must be in YYYY-MM-DD format"
        )

def iter_dates(start: date, end: date):
    """Generate dates within and inclusive date range."""

    if end < start:
        raise ValueError("End date cannot precede start date")

    current = start

    while current <= end:
        yield current.isoformat()
        current += timedelta(days=1)


def main():
    """Run ingestion and Silver processing for a date range."""

    parser = argparse.ArgumentParser(
        description="UK Renewable Energy ETL Pipeline"
    )

    parser.add_argument(
        "--start",
        type=parse_date,
        required=True,
        help="Start settlement date (YYYY-MM-DD)"
    )

    parser.add_argument(
        "--end",
        type=parse_date,
        required=True,
        help="End settlement date (YYYY-MM-DD)"
    )

    args = parser.parse_args()

    if args.end < args.start:
        parser.error("End date cannot precede start date")

    for settlement_date in iter_dates(
        args.start,
        args.end
    ):
        print(f"\nProcessing {settlement_date}")

        # Extract and start Bronze.
        run_ingestion(settlement_date)

        # Validate and store Silver.
        run_silver(settlement_date)

    print("\nPipeline completed successfully.")

if __name__ == "__main__":
    main()