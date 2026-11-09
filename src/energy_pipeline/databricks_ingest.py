import argparse
import json
import os
from pathlib import Path
from datetime import timedelta


from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv

from energy_pipeline.ingest import run_ingestion
from energy_pipeline.pipeline import parse_date, iter_dates

# Configuration

BRONZE_ROOT = Path("data/bronze/fuelhh")

VOLUME = "raw_energy"

# Upload one settlement date
def upload_settlement_date(
        settlement_date: str,
        client: WorkspaceClient,
        catalog: str,
        schema: str
):
    """
    Fetch Elexon generation data for one date, save it locally, and upload it
    to a Databricks Unity Catalog Volume.
    """

    print(f"\nProcessing settlement datE: {settlement_date}")

    # Fetch data from Elexon API using our existing ingestion pipeline.
    run_ingestion(settlement_date)

    # Locate the locally saved JSON.
    local_file = (
        BRONZE_ROOT
        / f"settlement_date={settlement_date}"
        / "records.json"
    )

    if not local_file.is_file():
        raise FileNotFoundError(
            f"Bronze file not found: {local_file}"
        )

    # Validate the basic file structure

    with local_file.open("r", encoding="utf-8") as file:
        records = json.load(file)

    if not isinstance(records, list) or not records:
        raise ValueError(
            f"No valid records found for {settlement_date}"
        )

    print(f"Fetched records: {len(records)}")

    # Define the remote Databricks path.

    remote_directory = (
        f"/Volumes/{catalog}/{schema}/{VOLUME}/"
        f"settlement_date={settlement_date}"
    )

    remote_file = (
        f"{remote_directory}/records.json"
    )

    # Create the destination directory.
    # Safe if directory already exists.

    client.files.create_directory(
        remote_directory
    )

    # Upload the JSON file.

    client.files.upload_from(
        file_path=remote_file,
        source_path=str(local_file),
        overwrite=True
    )

    print(f"Successfully uploaded: {remote_file}")

    return remote_file

# Main Orchestration Function

def main():
    # Configure command-line arguements.

    parser = argparse.ArgumentParser(
        description=(
            "Fetch Elexon electricity generation "
            "data and upload it to Databricks."
        )
    )

    parser.add_argument(
        "--start",
        type=parse_date,
        required=True,
        help="Start settlement date (YYYY-MM-DD)",
    )

    parser.add_argument(
        "--end",
        type=parse_date,
        required=True,
        help="End settlement date (YYYY-MM-DD)",
    )

    parser.add_argument(
        "--run-job",
        action="store_true",
        help="Run the Databricks workflow after uploading data."
    )

    args = parser.parse_args()

    if args.end < args.start:
        parser.error(
            "End date cannot precede start date"
        )

    # Load environment variables.

    load_dotenv()

    hostname = os.environ[
        "DATABRICKS_SERVER_HOSTNAME"
    ].strip()

    catalog = os.environ[
        "DATABRICKS_CATALOG"
    ].strip()

    schema = os.environ[
        "DATABRICKS_SCHEMA"
    ].strip()

    # Set up Databricks authentication.
    # No databricks CLI or access token required.

    host = (
        hostname
        if hostname.startswith(("https://", "http://"))
        else f"https://{hostname}"
    )

    client = WorkspaceClient(
        host=host,
        auth_type="external-browser",
    )

    print("Databricks client configured.")
    print(f"Destination: {catalog}.{schema}.{VOLUME}")

    # Process every requested date.

    for settlement_date in iter_dates(
        args.start,
        args.end,
    ):
        upload_settlement_date(
            settlement_date=settlement_date,
            client=client,
            catalog=catalog,
            schema=schema
        )

    if args.run_job:
        job_id = int(
            os.environ["DATABRICKS_JOB_ID"]
        )

    print("\nAll uploads completed successfully!")

    print(f"\nStarting Databricks Job: {job_id}")

    # Trigger the existing Databricks workflow.
    waiter = client.jobs.run_now(
        job_id=job_id
    )

    print(f"Databricks Run ID: {waiter.run_id}")

    # Wait for the Job to finish.
    run = waiter.result(
        timeout=timedelta(minutes=20)
    )

    # Check the final execution result.
    result_state = (
        run.state.result_state
        if run.state is not None
        else None
    )

    result_value = getattr(
        result_state, "value", result_state
    )

    if result_value != "SUCCESS":
        raise RuntimeError(
            f"Databricks Job failed: {result_state}"
        )

    print("Databricks Job completed successfully!")

    if run.run_page_url:
        print(f"Job details: {run.run_page_url}")


   

# Entry point

if __name__ == "__main__":
    main()