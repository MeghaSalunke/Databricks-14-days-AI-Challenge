# Databricks notebook source
#Read incremental (new) data
updates_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/Volumes/workspace/ecommerce/ecommerce_data/2019-Nov.csv")


# COMMAND ----------

# DBTITLE 1,Untitled
display(updates_df.limit(5))


# COMMAND ----------

#Load Delta table using DeltaTable
from delta.tables import DeltaTable

deltaTable = DeltaTable.forName(
    spark,
    "workspace.ecommerce.events_delta"
)


# COMMAND ----------

# MAGIC %md
# MAGIC user_session + event_time
# MAGIC

# COMMAND ----------

# DBTITLE 1,Untitled
# Deduplicate source DataFrame to avoid ambiguous matches
updates_df_dedup = updates_df.dropDuplicates(["user_session", "event_time"])

deltaTable.alias("t").merge(
    updates_df_dedup.alias("s"),
    "t.user_session = s.user_session AND t.event_time = s.event_time"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

# COMMAND ----------

#Verify MERGE Output
display(
    spark.table("workspace.ecommerce.events_delta")
    .orderBy("event_time")
    .limit(10)
)


# COMMAND ----------

#Check Delta Transaction History
%sql
DESCRIBE HISTORY workspace.ecommerce.events_delta;


# COMMAND ----------

v0 = spark.read.format("delta") \
    .option("versionAsOf", 0) \
    .table("workspace.ecommerce.events_delta")

display(v0.limit(5))


# COMMAND ----------

# DBTITLE 1,Cell 9
yesterday = spark.read.format("delta") \
    .option("timestampAsOf", "2026-01-12 08:05:19") \
    .table("workspace.ecommerce.events_delta")

display(yesterday.limit(5))

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE workspace.ecommerce.events_delta
# MAGIC ZORDER BY (event_type, user_id);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY workspace.ecommerce.events_delta;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM workspace.ecommerce.events_delta RETAIN 168 HOURS;
# MAGIC