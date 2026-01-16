# Databricks notebook source
dbutils.widgets.text("source_path", "")
dbutils.widgets.dropdown("layer", "bronze", ["bronze", "silver", "gold"])

source_path = dbutils.widgets.get("source_path")
layer = dbutils.widgets.get("layer")


# COMMAND ----------

# DBTITLE 1,Cell 2
from pyspark.sql import functions as F

# 1. Read Silver table
silver_df = spark.read.table("silver_events")

# 2. Aggregate
gold_df = (
    silver_df
    .groupBy("product_id", "product_name")
    .agg(
        F.sum("price").alias("total_revenue")
    )
)

# 3. Write Gold table
gold_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_product_revenue")

print("✅ Gold aggregation completed")
