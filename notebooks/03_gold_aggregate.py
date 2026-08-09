# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold Aggregates
# MAGIC Builds the small, pre-aggregated tables that back the dashboard. Each table
# MAGIC answers one analytical question, so the dashboard queries stay trivial.

# COMMAND ----------

dbutils.widgets.text("database", "nyc_taxi", "Database name")
database = dbutils.widgets.get("database")
spark.sql(f"USE {database}")

# COMMAND ----------

from pyspark.sql import functions as F

silver = spark.table(f"{database}.silver_trips")

# COMMAND ----------

# MAGIC %md ## Daily summary

# COMMAND ----------

daily_summary = (
    silver.groupBy("pickup_date")
    .agg(
        F.count("*").alias("total_trips"),
        F.sum("total_amount").alias("total_revenue"),
        F.avg("fare_amount").alias("avg_fare"),
        F.avg("tip_pct").alias("avg_tip_pct"),
        F.avg("trip_distance").alias("avg_distance_miles"),
        F.avg("trip_duration_min").alias("avg_duration_min"),
    )
    .orderBy("pickup_date")
)
daily_summary.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{database}.gold_daily_summary")

# COMMAND ----------

# MAGIC %md ## Hourly demand pattern

# COMMAND ----------

hourly_pattern = (
    silver.groupBy("pickup_hour")
    .agg(
        F.count("*").alias("total_trips"),
        F.avg("fare_amount").alias("avg_fare"),
        F.avg("trip_duration_min").alias("avg_duration_min"),
        F.avg("tip_pct").alias("avg_tip_pct"),
    )
    .orderBy("pickup_hour")
)
hourly_pattern.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{database}.gold_hourly_pattern")

# COMMAND ----------

# MAGIC %md ## Day-of-week pattern

# COMMAND ----------

dow_pattern = (
    silver.groupBy("pickup_day_of_week", "is_weekend")
    .agg(
        F.count("*").alias("total_trips"),
        F.avg("fare_amount").alias("avg_fare"),
        F.avg("tip_pct").alias("avg_tip_pct"),
    )
)
dow_pattern.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{database}.gold_day_of_week_pattern")

# COMMAND ----------

# MAGIC %md ## Pickup zone / borough summary

# COMMAND ----------

zone_summary = (
    silver.groupBy("pickup_borough", "pickup_zone")
    .agg(
        F.count("*").alias("total_trips"),
        F.sum("total_amount").alias("total_revenue"),
        F.avg("fare_amount").alias("avg_fare"),
        F.avg("tip_pct").alias("avg_tip_pct"),
    )
    .orderBy(F.desc("total_trips"))
)
zone_summary.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{database}.gold_zone_summary")

# COMMAND ----------

# MAGIC %md ## Payment type summary

# COMMAND ----------

total_trips = silver.count()

payment_summary = (
    silver.groupBy("payment_type_label")
    .agg(
        F.count("*").alias("total_trips"),
        F.avg("tip_pct").alias("avg_tip_pct"),
    )
    .withColumn("pct_of_total_trips", F.round(F.col("total_trips") / F.lit(total_trips) * 100, 2))
    .orderBy(F.desc("total_trips"))
)
payment_summary.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{database}.gold_payment_summary")

# COMMAND ----------

# MAGIC %md ## Trip distance buckets

# COMMAND ----------

distance_buckets = (
    silver
    .withColumn(
        "distance_bucket",
        F.when(F.col("trip_distance") <= 1, "0-1 mi")
         .when(F.col("trip_distance") <= 3, "1-3 mi")
         .when(F.col("trip_distance") <= 6, "3-6 mi")
         .when(F.col("trip_distance") <= 10, "6-10 mi")
         .otherwise("10+ mi")
    )
    .groupBy("distance_bucket")
    .agg(
        F.count("*").alias("total_trips"),
        F.avg("fare_amount").alias("avg_fare"),
        F.avg("trip_duration_min").alias("avg_duration_min"),
    )
)
distance_buckets.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{database}.gold_distance_buckets")

# COMMAND ----------

print("Gold tables built:")
for t in ["gold_daily_summary", "gold_hourly_pattern", "gold_day_of_week_pattern",
          "gold_zone_summary", "gold_payment_summary", "gold_distance_buckets"]:
    n = spark.table(f"{database}.{t}").count()
    print(f"  {t}: {n} rows")
