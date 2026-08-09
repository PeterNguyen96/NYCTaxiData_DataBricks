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
# MAGIC ## Download raw files into memory, convert to Spark DataFrames
# MAGIC Databricks Spark can't read directly from an `https://` URL. On serverless
# MAGIC compute the Python execution environment and the Spark/`dbutils.fs` runtime
# MAGIC don't share a local disk, so writing to `/tmp` or `/Workspace` and then
# MAGIC copying via `dbutils.fs.cp` fails with a `FileNotFoundException`. Instead we
# MAGIC pull the bytes straight into memory with `urllib`, load them with pandas,
# MAGIC and hand the result to Spark via `createDataFrame` — no disk involved.

# COMMAND ----------

import io
import urllib.request

import pandas as pd

trip_bytes = urllib.request.urlopen(trip_url).read()
zone_bytes = urllib.request.urlopen(zone_lookup_url).read()

trips_pdf = pd.read_parquet(io.BytesIO(trip_bytes))
zones_pdf = pd.read_csv(io.BytesIO(zone_bytes))

print(f"Downloaded {len(trips_pdf)} trip rows, {len(zones_pdf)} zone rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Land as bronze Delta tables (raw, untouched schema)

# COMMAND ----------

trips_df = spark.createDataFrame(trips_pdf)
zones_df = spark.createDataFrame(zones_pdf)

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
