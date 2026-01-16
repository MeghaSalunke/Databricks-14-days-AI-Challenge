# Databricks notebook source
dbutils.widgets.text("source_path", "")
dbutils.widgets.dropdown("layer", "bronze", ["bronze", "silver", "gold"])

source_path = dbutils.widgets.get("source_path")
layer = dbutils.widgets.get("layer")


# COMMAND ----------

# DBTITLE 1,Cell 2
from pyspark.sql import functions as F

# 1. Read raw CSV
raw_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/Volumes/workspace/ecommerce/ecommerce_data/2019-Oct.csv")

# 2. Add ingestion timestamp
bronze_df = raw_df.withColumn(
    "ingestion_ts",
    F.current_timestamp()
)

# 3. Write Bronze table
bronze_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_events")

print("✅ Bronze ingestion completed")
