import os

import pandas as pd
from databricks import sql
from dotenv import load_dotenv

load_dotenv()

ALLOWED_TABLES = {
    "fuelhh_gold_daily_fuel",
    "fuelhh_gold_period_trends",
}

def read_gold_table(table_name: str) -> pd.DataFrame:
    """Read an approved Gold table from Databricks."""

    if table_name not in ALLOWED_TABLES:
        raise ValueError("Table is not approved for querying.")

    hostname = os.environ["DATABRICKS_SERVER_HOSTNAME"]
    http_path = os.environ["DATABRICKS_HTTP_PATH"]
    catalog = os.environ["DATABRICKS_CATALOG"]
    schema = os.environ["DATABRICKS_SCHEMA"]

    # Quote catalog and schema identifiers safely
    def quote_identifier(value):
        return "`" + value.replace("`", "``") + "`"

    qualified_table = ".".join([
        quote_identifier(catalog),
        quote_identifier(schema),
        quote_identifier(table_name)
    ])

    query = f"SELECT * FROM {qualified_table}"

    with sql.connect(
        server_hostname=hostname,
        http_path=http_path,
        auth_type="databricks-oauth",
    ) as connection:

        with connection.cursor() as cursor:
            cursor.execute(query)

            rows = cursor.fetchall()

            columns = [
                column[0]
                for column in cursor.description
            ]

    return pd.DataFrame.from_records(
        rows,
        columns=columns
    )

if __name__ == "__main__":

    data = read_gold_table(
        "fuelhh_gold_daily_fuel"
    )

    print(data.head())
    print(f"\nRecords fetched: {len(data)}")