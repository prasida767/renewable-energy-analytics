# Databricks notebook source
from pyspark.sql import functions as F

CATALOG = "workspace"

SCHEMA = "default"

silver_table = f"{CATALOG}.{SCHEMA}.fuelhh_silver"

silver_df = spark.table(silver_table)

print("Silver record count:", silver_df.count())

silver_df.printSchema()

display(silver_df.limit(10))

# COMMAND ----------

energy_df = silver_df.withColumn(
    "energy_mwh",
    F.col("generation_mw") * F.lit(0.5)
)

display(
    energy_df.select(
        "settlement_date",
        "settlement_period",
        "fuel_type",
        "generation_mw",
        "energy_mwh"
    ).limit(10)
)

# COMMAND ----------

daily_fuel_df = (
    energy_df
    .groupBy(
        "settlement_date",
        "fuel_type"
    )
    .agg(
        F.count("*").alias("period_count"),

        F.round(
            F.avg("generation_mw"), 2
        ).alias("average_mw"),

        F.round(
            F.max("generation_mw"), 2
        ).alias("peak_mw"),

        F.round(
            F.sum("energy_mwh"), 2
        ).alias("total_energy_mwh")
    )
    .orderBy("settlement_date", "fuel_type")
)

display(daily_fuel_df)

# COMMAND ----------

period_trends_df = (
    silver_df
    .groupBy(
        "settlement_date",
        "settlement_period"
    )
    .pivot(
        "fuel_type",
        ["WIND", "CCGT", "NUCLEAR"]
    )
    .agg(
        F.sum("generation_mw")
    )
    .withColumnRenamed("WIND", "wind_mw")
    .withColumnRenamed("CCGT", "gas_ccgt_mw")
    .withColumnRenamed("NUCLEAR", "nuclear_mw")
    .orderBy(
        "settlement_date",
        "settlement_period"
    )
)

display(period_trends_df)

# COMMAND ----------

daily_table = (
    f"{CATALOG}.{SCHEMA}.fuelhh_gold_daily_fuel"
)

# Development only: remove the old Gold table.
#spark.sql(f"DROP TABLE IF EXISTS {daily_table}")

# Recreate it using the corrected DataFrame schema.
#(
#    daily_fuel_df.write
#    .format("delta")
#    .mode("overwrite")
#    .saveAsTable(daily_table)
#)

trends_table = (
    f"{CATALOG}.{SCHEMA}.fuelhh_gold_period_trends"
)

# Save daily fuel metrics.
(
    daily_fuel_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(daily_table)
)

# Save half-hourly trends.
(
    period_trends_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(trends_table)
)

print("Created:", daily_table)
print("Created:", trends_table)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     settlement_date,
# MAGIC     fuel_type,
# MAGIC     period_count,
# MAGIC     average_mw,
# MAGIC     peak_mw,
# MAGIC     total_energy_mwh
# MAGIC FROM fuelhh_gold_daily_fuel
# MAGIC ORDER BY total_energy_mwh DESC;

# COMMAND ----------

silver_count = silver_df.count()

gold_count = daily_fuel_df.agg(
    F.sum("period_count").alias("total")
).first()["total"]

assert silver_count == gold_count

print("Gold reconciliation passed!")
