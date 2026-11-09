# Databricks notebook source
from pyspark.sql import functions as F
# Config

CATALOG = "workspace"
SCHEMA = "default"
VOLUME = "raw_energy"

bronze_table = (
    f"{CATALOG}.{SCHEMA}.fuelhh_bronze"
)

# Read all date-specific JSON files.
source_path = (
    f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/"
    "settlement_date=*/records.json"
)

print("Reading from:", source_path)

# Extract: Read JSON files
raw_df = (
    spark.read
    .option("multiline", "true")
    .json(source_path)
)

print("Records loaded:", raw_df.count())

# Add ingestion metadata
bronze_df = (
    raw_df
    .select(
        "*",
        F.col("_metadata.file_path").alias(
            "source_file"
        )
    )
    .drop("settlement_date")
    .withColumn(
        "ingested_at",
        F.current_timestamp()
    )
)

# Basic validation
record_count = bronze_df.count()

source_file_count = (
    bronze_df
    .select("source_file")
    .distinct()
    .count()
)

print("Total records:", record_count)
print("Source files:", source_file_count)

if record_count == 0:
    raise ValueError(
        "No records found in source files"
    )

# Check the records grouped by date.
display(
    bronze_df
    .groupBy("settlementDate")
    .count()
    .orderBy("settlementDate")
)

# Load: Write Bronze Delta table
(
    bronze_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(bronze_table)
)

print("Bronze table updated successfully!")
print("Table:", bronze_table)

# Veirfy the saved table
saved_df = spark.table(bronze_table)

print("Saved records:", saved_df.count())

display(saved_df.limit(10))

# COMMAND ----------

