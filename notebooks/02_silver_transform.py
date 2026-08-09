# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver Transform
# MAGIC Cleans the bronze trip data, removes bad/outlier records, enriches it with
# MAGIC derived time fields and pickup/dropoff zone names, and writes a conformed
# MAGIC `silver_trips` Delta table.

# COMMAND ----------

dbutils.widgets.text("database", "nyc_taxi", "Database name")
database = dbutils.widgets.get("database")
spark.sql(f"USE {database}")

# COMMAND ----------

from pyspark.sql import functions as F

trips = spark.table(f"{database}.bronze_yellow_tripdata")
zones = spark.table(f"{database}.bronze_taxi_zone_lookup")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data quality filters
# MAGIC Drop records with impossible or clearly erroneous values: non-positive fares,
# MAGIC distances, or passenger counts; trips with zero/negative duration; and trips
# MAGIC lasting under 30 seconds or over 6 hours (sensor/meter errors).

# COMMAND ----------

clean = (
    trips
    .withColumn("trip_duration_min",
                (F.col("tpep_dropoff_datetime").cast("long") - F.col("tpep_pickup_datetime").cast("long")) / 60.0)
    .filter(F.col("fare_amount") > 0)
    .filter(F.col("total_amount") > 0)
    .filter(F.col("trip_distance") > 0)
    .filter(F.col("passenger_count") > 0)
    .filter(F.col("trip_duration_min").between(0.5, 360))
    .filter(F.col("tpep_pickup_datetime") < F.col("tpep_dropoff_datetime"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Derived columns + zone enrichment

# COMMAND ----------

payment_labels = {
    1: "Credit card",
    2: "Cash",
    3: "No charge",
    4: "Dispute",
    5: "Unknown",
    6: "Voided trip",
}
payment_map = F.create_map([F.lit(x) for pair in payment_labels.items() for x in pair])

pu_zones = zones.select(
    F.col("LocationID").alias("PULocationID"),
    F.col("Zone").alias("pickup_zone"),
    F.col("Borough").alias("pickup_borough"),
)
do_zones = zones.select(
    F.col("LocationID").alias("DOLocationID"),
    F.col("Zone").alias("dropoff_zone"),
    F.col("Borough").alias("dropoff_borough"),
)

enriched = (
    clean
    .withColumn("avg_speed_mph",
                F.when(F.col("trip_duration_min") > 0,
                       F.col("trip_distance") / (F.col("trip_duration_min") / 60.0)))
    .withColumn("tip_pct",
                F.when(F.col("fare_amount") > 0,
                       (F.col("tip_amount") / F.col("fare_amount")) * 100))
    .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
    .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
    .withColumn("pickup_day_of_week", F.date_format("tpep_pickup_datetime", "EEEE"))
    .withColumn("is_weekend", F.dayofweek("tpep_pickup_datetime").isin(1, 7))
    .withColumn("payment_type_label", payment_map[F.col("payment_type")])
    .join(pu_zones, on="PULocationID", how="left")
    .join(do_zones, on="DOLocationID", how="left")
)

# COMMAND ----------

(
    enriched.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{database}.silver_trips")
)

print(f"silver_trips rows: {enriched.count()} (bronze had {trips.count()})")

# COMMAND ----------

display(spark.table(f"{database}.silver_trips").limit(10))
