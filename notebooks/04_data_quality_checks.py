# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Data Quality Checks
# MAGIC Runs a set of assertions against the silver and gold tables: row-count
# MAGIC reconciliation between layers, null checks on columns that should never
# MAGIC be null post-cleaning, range checks on filtered numeric columns, and
# MAGIC category-completeness checks on the gold aggregates. Raises if anything
# MAGIC fails, so this can be wired into a job as a pipeline gate.

# COMMAND ----------

dbutils.widgets.text("database", "nyc_taxi", "Database name")
database = dbutils.widgets.get("database")
spark.sql(f"USE {database}")

# COMMAND ----------

from pyspark.sql import functions as F

results = []


def check(description, condition):
    status = "PASS" if condition else "FAIL"
    results.append((status, description))
    print(f"[{status}] {description}")

# COMMAND ----------

# MAGIC %md ## Load tables

# COMMAND ----------

bronze = spark.table(f"{database}.bronze_yellow_tripdata")
silver = spark.table(f"{database}.silver_trips")

bronze_count = bronze.count()
silver_count = silver.count()

# COMMAND ----------

# MAGIC %md ## Silver checks
# MAGIC These hold by construction given the filters in `02_silver_transform.py`
# MAGIC — if any of them fail, the transform logic has regressed.

# COMMAND ----------

check("silver_trips is non-empty", silver_count > 0)
check(
    "silver_trips row count does not exceed bronze (cleaning only removes rows)",
    silver_count <= bronze_count,
)

not_null_cols = [
    "tpep_pickup_datetime", "tpep_dropoff_datetime", "fare_amount",
    "trip_distance", "total_amount", "passenger_count",
    "PULocationID", "DOLocationID", "trip_duration_min",
    "pickup_hour", "pickup_date", "tip_pct", "payment_type_label",
]
null_counts = silver.select(
    [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in not_null_cols]
).collect()[0].asDict()
for col, n_nulls in null_counts.items():
    check(f"silver_trips.{col} has no nulls", n_nulls == 0)

check(
    "silver_trips.fare_amount is always positive",
    silver.filter(F.col("fare_amount") <= 0).count() == 0,
)
check(
    "silver_trips.trip_distance is always positive",
    silver.filter(F.col("trip_distance") <= 0).count() == 0,
)
check(
    "silver_trips.total_amount is always positive",
    silver.filter(F.col("total_amount") <= 0).count() == 0,
)
check(
    "silver_trips.passenger_count is always positive",
    silver.filter(F.col("passenger_count") <= 0).count() == 0,
)
check(
    "silver_trips.trip_duration_min is within [0.5, 360] minutes",
    silver.filter(~F.col("trip_duration_min").between(0.5, 360)).count() == 0,
)
check(
    "silver_trips.pickup_hour is within [0, 23]",
    silver.filter(~F.col("pickup_hour").between(0, 23)).count() == 0,
)
check(
    "silver_trips.tpep_pickup_datetime is always before tpep_dropoff_datetime",
    silver.filter(F.col("tpep_pickup_datetime") >= F.col("tpep_dropoff_datetime")).count() == 0,
)

# COMMAND ----------

# MAGIC %md ## Gold checks — row-count reconciliation
# MAGIC Every gold table is a full aggregation over `silver_trips` with no
# MAGIC filtering, so each one's `total_trips` should sum back to the exact
# MAGIC silver row count. This catches silent row loss/duplication in the
# MAGIC gold aggregation logic.

# COMMAND ----------

gold_tables = [
    "gold_daily_summary", "gold_hourly_pattern", "gold_day_of_week_pattern",
    "gold_zone_summary", "gold_payment_summary", "gold_distance_buckets",
]

for t in gold_tables:
    total = spark.table(f"{database}.{t}").agg(F.sum("total_trips")).collect()[0][0]
    check(
        f"{t}: SUM(total_trips) ({total}) reconciles to silver_trips count ({silver_count})",
        total == silver_count,
    )

# COMMAND ----------

# MAGIC %md ## Gold checks — category completeness & value sanity

# COMMAND ----------

check(
    "gold_hourly_pattern has exactly 24 rows (one per hour of day)",
    spark.table(f"{database}.gold_hourly_pattern").count() == 24,
)
check(
    "gold_day_of_week_pattern has exactly 7 rows (one per weekday)",
    spark.table(f"{database}.gold_day_of_week_pattern").count() == 7,
)
check(
    "gold_distance_buckets has exactly 5 rows (one per bucket)",
    spark.table(f"{database}.gold_distance_buckets").count() == 5,
)

payment_pct_total = (
    spark.table(f"{database}.gold_payment_summary")
    .agg(F.sum("pct_of_total_trips"))
    .collect()[0][0]
)
check(
    f"gold_payment_summary.pct_of_total_trips sums to ~100 (got {payment_pct_total})",
    abs(payment_pct_total - 100) < 0.5,
)

check(
    "gold_daily_summary has no negative total_revenue",
    spark.table(f"{database}.gold_daily_summary").filter(F.col("total_revenue") < 0).count() == 0,
)
check(
    "gold_zone_summary has no negative total_revenue",
    spark.table(f"{database}.gold_zone_summary").filter(F.col("total_revenue") < 0).count() == 0,
)

# COMMAND ----------

# MAGIC %md ## Summary

# COMMAND ----------

failures = [desc for status, desc in results if status == "FAIL"]
print(f"\n{len(results) - len(failures)}/{len(results)} checks passed.")

if failures:
    raise AssertionError(
        f"{len(failures)} data quality check(s) failed:\n"
        + "\n".join(f"  - {f}" for f in failures)
    )

print("All data quality checks passed.")
