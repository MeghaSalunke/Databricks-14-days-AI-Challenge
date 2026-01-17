# Databricks notebook source
# MAGIC %sql
# MAGIC -- Create catalog
# MAGIC CREATE CATALOG IF NOT EXISTS ecommerce;
# MAGIC
# MAGIC -- Use catalog
# MAGIC USE CATALOG ecommerce;
# MAGIC
# MAGIC -- Create schemas (Bronze, Silver, Gold)
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS gold;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Create Bronze, Silver, Gold tables (fixed)
# MAGIC %sql
# MAGIC -- Bronze table
# MAGIC CREATE TABLE IF NOT EXISTS bronze.events
# MAGIC USING DELTA;
# MAGIC
# MAGIC -- Silver table
# MAGIC CREATE TABLE IF NOT EXISTS silver.events
# MAGIC USING DELTA;
# MAGIC
# MAGIC -- Gold table
# MAGIC CREATE TABLE IF NOT EXISTS gold.products
# MAGIC USING DELTA;

# COMMAND ----------

# DBTITLE 1,Cell 3
# MAGIC %sql
# MAGIC GRANT SELECT
# MAGIC ON TABLE gold.products
# MAGIC TO `meghasalunke2003@gmail.com`;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT ALL PRIVILEGES
# MAGIC ON SCHEMA silver
# MAGIC TO `meghasalunke2003@gmail.com`;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.products (
# MAGIC      product_name STRING,
# MAGIC      revenue DOUBLE,
# MAGIC      conversion_rate DOUBLE
# MAGIC    );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO gold.products (product_name, revenue, conversion_rate)
# MAGIC    VALUES
# MAGIC      ('Product A', 1000.0, 0.15),
# MAGIC      ('Product B', 850.0, 0.12),
# MAGIC      ('Product C', 1200.0, 0.18);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold.top_products AS
# MAGIC SELECT
# MAGIC     product_name,
# MAGIC     revenue,
# MAGIC     conversion_rate
# MAGIC FROM gold.products
# MAGIC ORDER BY revenue DESC
# MAGIC LIMIT 100;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT SELECT
# MAGIC ON VIEW gold.top_products
# MAGIC TO `meghasalunke2003@gmail.com`;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM gold.top_products
# MAGIC LIMIT 5;
# MAGIC