# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = "workspace"

SCHEMA = "default"

bronze_table = f"{CATALOG}.{SCHEMA}.fuelhh_bronze"

bronze_df = spark.table(bronze_table)

print("Bronze records:", bronze_df.count())

bronze_df.printSchema()

display(bronze_df.limit(5))

# COMMAND ----------

typed_df = (
    bronze_df

    .withColumn(
        "settlement_date",
        F.expr("try_cast(settlementDate AS DATE)")
    )
    .withColumn(
        "settlement_period",
        F.expr("try_cast(settlementPeriod AS INT)")
    )

    .withColumn(
        "fuel_type",
        F.upper(F.trim(F.col("fuelType")))
    )
    .withColumn(
        "generation_mw",
        F.expr("try_cast(generation AS DOUBLE)")
    )
    .withColumn(
        "publish_time",
        F.expr("try_cast(publishTime AS TIMESTAMP)")
    )
    .withColumn(
        "start_time",
        F.expr("try_cast(startTime AS TIMESTAMP)")
    )
)

typed_df.printSchema()

display(typed_df.limit(5))

# COMMAND ----------

validated_df = typed_df.withColumn(
    "validation_errors",
    F.filter(
        F.array(

            F.when(
                F.col("dataset").isNull()
                | (F.col("dataset") != "FUELHH"),
                F.lit("Invalid dataset")
            ),

            F.when(
                F.col("settlement_date").isNull(),
                F.lit("Invalid settlement date")
            ),

            F.when(
                F.col("settlement_period").isNull()
                | ~F.col("settlement_period").between(1, 50),
                F.lit("Invalid settlement period")
            ),

            F.when(
                F.col("fuel_type").isNull()
                | (F.col("fuel_type") == ""),
                F.lit("Invalid fuel type")
            ),
            F.when(
                F.col("generation_mw").isNull()
                | F.isnan("generation_mw")
                | (F.abs(F.col("generation_mw")) == float("inf")),
                F.lit("Invalid generation")
            ),
            F.when(
                F.col("publish_time").isNull(),
                F.lit("Invalid publish time")
            ),
            F.when(
                F.col("start_time").isNull(),
                F.lit("Invalid start time")
            )
        ),
        lambda error: error.isNotNull()
    )
)

display(
    validated_df.select(
        "settlementDate",
        "fuelType",
        "generation",
        "validation_errors"
    ).limit(10)
)

# COMMAND ----------

# Separate individually valid and invalid records.

eligible_df = validated_df.filter(
    F.size("validation_errors") == 0
)

invalid_df = validated_df.filter(
    F.size("validation_errors") > 0
)

# Candidate business key.
business_key_window = Window.partitionBy(
    "settlement_date",
    "settlement_period",
    "fuel_type"
)

# Count how many eligible records share each key.
eligible_with_counts = eligible_df.withColumn(
    "key_count",
    F.count("*").over(business_key_window)
)

# Accept only unique candidate business keys.
silver_df = eligible_with_counts.filter(
    F.col("key_count") == 1
)

# Quarantine conflicting keys.
duplicate_df = (
    eligible_with_counts
    .filter(F.col("key_count") > 1)
    .withColumn(
        "validation_errors",
        F.array(
            F.lit("Repeated business key: revision review required")
        )
    )
)

# Use the same columns for both quarantine categories.
quarantine_columns = bronze_df.columns + ["validation_errors"]

quarantine_df = (
    invalid_df.select(*quarantine_columns)
    .unionByName(
        duplicate_df.select(*quarantine_columns)
    )
)

print("Bronze records:", bronze_df.count())
print("Accepted records:", silver_df.count())
print("Quarantined records:", quarantine_df.count())

# COMMAND ----------

silver_table = f"{CATALOG}.{SCHEMA}.fuelhh_silver"
quarantine_table = f"{CATALOG}.{SCHEMA}.fuelhh_quarantine"

# Select the standardised Silver schema.
silver_output = silver_df.select(
    "settlement_date",
    "settlement_period",
    "fuel_type",
    "generation_mw",
    "publish_time",
    "start_time",
    "ingested_at",
    "source_file"
)

# Write to the Silver table.
(
    silver_output.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(silver_table)
)

# Write rejected records with their original values.
(
    quarantine_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(quarantine_table)
)

print("Created:", silver_table)
print("Created:", quarantine_table)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     settlement_date,
# MAGIC     fuel_type,
# MAGIC     COUNT(*) AS record_count,
# MAGIC     ROUND(AVG(generation_mw), 2) AS average_mw
# MAGIC FROM fuelhh_silver
# MAGIC GROUP BY
# MAGIC     settlement_date,
# MAGIC     fuel_type
# MAGIC ORDER BY fuel_type;

# COMMAND ----------


