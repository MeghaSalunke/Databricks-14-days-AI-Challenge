# Databricks notebook source
dbutils.widgets.text("source_path", "")
dbutils.widgets.dropdown("layer", "bronze", ["bronze", "silver", "gold"])

source_path = dbutils.widgets.get("source_path")
layer = dbutils.widgets.get("layer")


# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import col

# 1. Read Bronze table
bronze_df = spark.read.table("bronze_events")

# 2. Clean & validate
silver_df = (
    bronze_df
    .withColumn("price_clean", F.expr("try_cast(price as double)"))
    .filter(col("price_clean").isNotNull())
    .filter(col("price_clean") > 0)
    .dropDuplicates(["user_session", "event_time"])
    .withColumn(
        "product_name",
        F.coalesce(
            F.element_at(F.split(col("category_code"), "\\."), -1),
            F.lit("Other")
        )
    )
    .drop("price")
    .withColumnRenamed("price_clean", "price")
)

# 3. Write Silver table
silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_events")

print("✅ Silver processing completed")
