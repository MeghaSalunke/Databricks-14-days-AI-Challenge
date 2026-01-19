# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# COMMAND ----------

# DBTITLE 1,Cell 2
events_table = spark.table("events_table")
events_table.select("price").describe().show()

# COMMAND ----------

# DBTITLE 1,Cell 3
events_with_flag = events_table.withColumn(
    "is_weekend",
    F.dayofweek("event_time").isin([1, 7])
)

# COMMAND ----------

# MAGIC %md
# MAGIC Compare Event Counts

# COMMAND ----------

events_with_flag \
    .groupBy("is_weekend", "event_type") \
    .count() \
    .orderBy("is_weekend", "event_type") \
    .show()


# COMMAND ----------

# MAGIC %md
# MAGIC Identify Correlations

# COMMAND ----------

# DBTITLE 1,Cell 6
corr_value = events_table.stat.corr("price", "user_id")
print(f"Correlation between price and user_id: {corr_value}")

# COMMAND ----------

# MAGIC %md
# MAGIC Time-Based Features

# COMMAND ----------

# DBTITLE 1,Cell 9
features_df = events_table \
    .withColumn("hour", F.hour("event_time")) \
    .withColumn("day_of_week", F.dayofweek("event_time"))

# COMMAND ----------

# MAGIC %md
# MAGIC Price Transformation (Log Scale)

# COMMAND ----------

features_df = features_df.withColumn(
    "price_log",
    F.log(F.col("price") + 1)
)


# COMMAND ----------

# MAGIC %md
# MAGIC Behavioral Feature (Time Since First Event)

# COMMAND ----------

user_window = Window.partitionBy("user_id").orderBy("event_time")


# COMMAND ----------

features_df = features_df.withColumn(
    "time_since_first_view",
    F.unix_timestamp("event_time") -
    F.unix_timestamp(F.first("event_time").over(user_window))
)


# COMMAND ----------

# MAGIC %md
# MAGIC Final Feature Check

# COMMAND ----------

# DBTITLE 1,Cell 16
user_window = Window.partitionBy("user_id").orderBy("event_time")
features_df = features_df.withColumn(
    "time_since_first_view",
    F.unix_timestamp("event_time") -
    F.unix_timestamp(F.first("event_time").over(user_window))
)
features_df = features_df.withColumn(
    "price_log",
    F.log(F.col("price") + 1)
)
features_df.select(
    "user_id",
    "price",
    "price_log",
    "hour",
    "day_of_week",
    "time_since_first_view"
).show(5)