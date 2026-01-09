# Databricks notebook source
# MAGIC %pip install kaggle

# COMMAND ----------

import os

os.environ["KAGGLE_USERNAME"] = "meghasalunke"
os.environ["KAGGLE_KEY"] = "85025dbf871855535eb490b2bc669c5e"
print("Kaggle credentials configured!")



# COMMAND ----------

spark.sql("""
CREATE SCHEMA IF NOT EXISTS workspace.ecommerce
""")

# COMMAND ----------

# MAGIC %sh
# MAGIC cd /Volumes/workspace/ecommerce/ecommerce_data
# MAGIC kaggle datasets download -d mkechinov/ecommerce-behavior-data-from-multi-category-store
# MAGIC

# COMMAND ----------

# MAGIC %sh
# MAGIC cd /Volumes/workspace/ecommerce/ecommerce_data
# MAGIC unzip -o ecommerce-behavior-data-from-multi-category-store.zip
# MAGIC ls -lh

# COMMAND ----------

# MAGIC %sh
# MAGIC cd /Volumes/workspace/ecommerce/ecommerce_data
# MAGIC rm -f ecommerce-behavior-data-from-multi-category-store.zip
# MAGIC ls -lh

# COMMAND ----------

df_n = spark.read.csv("/Volumes/workspace/ecommerce/ecommerce_data/2019-Nov.csv")



# COMMAND ----------

df = spark.read.csv("/Volumes/workspace/ecommerce/ecommerce_data/2019-Oct.csv")

# COMMAND ----------

print(f"October 2019 - Total Events: {df.count():,}")
print("\n" + "="*60)
print("SCHEMA:")
print("="*60)
df.printSchema()

# COMMAND ----------

