from energy_pipeline.client import fetch_generation
from energy_pipeline.bronze import save_raw_records

def run_ingestion(settlement_date: str) -> None:
    print(f"Starting ingestion for {settlement_date}")

    # Extract data from Elexon.
    records = fetch_generation(settlement_date)

    print(f"Fetched {len(records)} records")

    # Store the data.
    output_path = save_raw_records(
        records,
        settlement_date
    )

    print(f"Saved raw records to: {output_path}")

if __name__ == "__main__":
    run_ingestion("2026-11-03")