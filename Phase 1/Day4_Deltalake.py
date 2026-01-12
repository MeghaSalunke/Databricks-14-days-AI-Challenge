# Databricks notebook source
#Paths
csv_path = "/Volumes/workspace/ecommerce/ecommerce_data/2019-Nov.csv"
delta_path = "/Volumes/workspace/ecommerce/ecommerce_data/delta/ecommerce_events"


# COMMAND ----------

# Read CSV Data
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(csv_path).limit(10)
display(df)



# COMMAND ----------

# Convert CSV to Delta
df.write.format("delta").mode("overwrite").save(delta_path)


# COMMAND ----------

# Created managed Delta Table
df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("ecommerce.events_table")


# COMMAND ----------

#Create Delta Table using SQl
spark.sql("""
CREATE OR REPLACE TABLE ecommerce.events_delta
USING DELTA
AS
SELECT * FROM ecommerce.events_table
""")


# COMMAND ----------

#verify tables
%sql
SHOW TABLES IN ecommerce;


# COMMAND ----------

#Test Schema Enforcement
try:
    bad_df = spark.createDataFrame(
        [("test", 123)],
        ["col1", "col2"]
    )

    bad_df.write \
        .format("delta") \
        .mode("append") \
        .save(delta_path)

except Exception as e:
    print("Schema enforcement working:")
    print(e)


# COMMAND ----------

# Prepare New Data for MERGE
new_data = df.limit(100)

new_data.createOrReplaceTempView("new_data")


# COMMAND ----------

#Handle Duplicate Inserts using MERGE
spark.sql("""
MERGE INTO ecommerce.events_delta AS target
USING new_data AS source
ON target.user_id = source.user_id
AND target.event_time = source.event_time
WHEN NOT MATCHED THEN
  INSERT *
""")


# COMMAND ----------

spark.sql("""
    CREATE TABLE IF NOT EXISTS events_delta
    USING DELTA
    AS SELECT * FROM system.compute.warehouse_events
""")

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS events_delta")
spark.sql("""
    CREATE TABLE events_delta
    USING DELTA
    AS SELECT * FROM system.compute.warehouse_events
""")

# COMMAND ----------

#Check Delta History
display(
    spark.sql("DESCRIBE HISTORY ecommerce.events_delta")
)
