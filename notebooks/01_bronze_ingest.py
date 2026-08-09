# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze Ingest
# MAGIC Downloads one month of NYC Yellow Taxi trip data (public TLC dataset) plus the
# MAGIC taxi zone lookup table, and lands them as Delta tables in the `bronze` layer
# MAGIC with no transformations applied.

# COMMAND ----------

dbutils.widgets.text("year_month", "2023-01", "Trip month (YYYY-MM)")
dbutils.widgets.text("database", "nyc_taxi", "Database name")

year_month = dbutils.widgets.get("year_month")
database = dbutils.widgets.get("database")

trip_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year_month}.parquet"
zone_lookup_url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

print(f"Trip data source: {trip_url}")
print(f"Zone lookup source: {zone_lookup_url}")

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
spark.sql(f"USE {database}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download raw files to local disk, then stage on DBFS
# MAGIC Databricks Spark can't read directly from an `https://` URL, so we pull the
# MAGIC files to the driver's local filesystem first and copy them into DBFS.

# COMMAND ----------

import urllib.request
import os

local_dir = "/tmp/nyc_taxi_raw"
os.makedirs(local_dir, exist_ok=True)

local_trip_path = f"{local_dir}/yellow_tripdata_{year_month}.parquet"
local_zone_path = f"{local_dir}/taxi_zone_lookup.csv"

urllib.request.urlretrieve(trip_url, local_trip_path)
urllib.request.urlretrieve(zone_lookup_url, local_zone_path)

dbfs_trip_path = f"dbfs:/FileStore/nyc_taxi/raw/yellow_tripdata_{year_month}.parquet"
dbfs_zone_path = "dbfs:/FileStore/nyc_taxi/raw/taxi_zone_lookup.csv"

dbutils.fs.cp(f"file:{local_trip_path}", dbfs_trip_path)
dbutils.fs.cp(f"file:{local_zone_path}", dbfs_zone_path)

print("Files staged on DBFS:")
print(dbfs_trip_path)
print(dbfs_zone_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Land as bronze Delta tables (raw, untouched schema)

# COMMAND ----------

trips_df = spark.read.parquet(dbfs_trip_path)
zones_df = spark.read.option("header", True).option("inferSchema", True).csv(dbfs_zone_path)

(
    trips_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{database}.bronze_yellow_tripdata")
)

(
    zones_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{database}.bronze_taxi_zone_lookup")
)

print(f"bronze_yellow_tripdata rows: {trips_df.count()}")
print(f"bronze_taxi_zone_lookup rows: {zones_df.count()}")

# COMMAND ----------

display(spark.table(f"{database}.bronze_yellow_tripdata").limit(10))
