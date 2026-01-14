# Databricks notebook source
# DBTITLE 1,Cell 1
#Read CSV
bronze_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/Volumes/workspace/ecommerce/ecommerce_data/2019-Oct.csv")
)


# COMMAND ----------

#Add ingestion metadata
from pyspark.sql.functions import current_timestamp

bronze_df = bronze_df.withColumn(
    "ingestion_timestamp",
    current_timestamp()
)


# COMMAND ----------

#Write to Bronze Delta table
bronze_df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("workspace.ecommerce.bronze_ecommerce_events")


# COMMAND ----------

#Read from Bronze
from pyspark.sql.functions import col, to_timestamp

bronze_df = spark.table(
    "workspace.ecommerce.bronze_ecommerce_events"
)


# COMMAND ----------

#Fix timestamps
silver_df = bronze_df.withColumn(
    "event_time",
    to_timestamp("event_time")
)


# COMMAND ----------

#Remove invalid records
silver_df = silver_df.filter(
    col("event_time").isNotNull() &
    col("event_type").isNotNull() &
    col("user_id").isNotNull()
)


# COMMAND ----------

#Apply business validations (price range)
silver_df = silver_df.filter(
    (col("price") > 0) & (col("price") < 10000)
)


# COMMAND ----------

silver_df = silver_df.dropDuplicates(
    ["user_session", "event_time"]
)


# COMMAND ----------

# DBTITLE 1,Cell 9
#Add derived columns
silver_df = spark.table(
    "workspace.ecommerce.silver_ecommerce_events"
)

from pyspark.sql import functions as F
from pyspark.sql.functions import col

silver_df = silver_df \
    .withColumn("event_date", F.to_date("event_time")) \
    .withColumn(
        "price_tier",
        F.when(col("price") < 10, "budget")
         .when(col("price") < 50, "mid")
         .otherwise("premium")
    )


# COMMAND ----------

#Write to Silver Delta table
silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.ecommerce.silver_ecommerce_events")


# COMMAND ----------

silver_df = spark.table(
    "workspace.ecommerce.silver_ecommerce_events"
)

# COMMAND ----------

# DBTITLE 1,Cell 12
#Product-level aggregations
from pyspark.sql.functions import col

product_perf = (
    silver_df
    .groupBy("product_id")
    .agg(
        F.countDistinct(
            F.when(col("event_type") == "view", col("user_id"))
        ).alias("views"),

        F.countDistinct(
            F.when(col("event_type") == "purchase", col("user_id"))
        ).alias("purchases"),

        F.sum(
            F.when(col("event_type") == "purchase", col("price"))
        ).alias("revenue")
    )
)


# COMMAND ----------

# DBTITLE 1,Cell 13
product_perf = product_perf.withColumn(
    "conversion_rate",
    F.when(col("views") != 0, (col("purchases") / col("views")) * 100)
     .otherwise(None)
)

# COMMAND ----------

#Write to Gold Delta table
product_perf.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.ecommerce.gold_product_performance")
