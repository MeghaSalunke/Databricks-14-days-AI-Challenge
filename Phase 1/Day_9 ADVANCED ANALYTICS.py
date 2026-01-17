# Databricks notebook source
# DBTITLE 1,Cell 1
# MAGIC %sql
# MAGIC SHOW TABLES IN ecommerce.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM ecommerce.gold.products
# MAGIC LIMIT 10;

# COMMAND ----------

# DBTITLE 1,Cell 3
# MAGIC %sql
# MAGIC SELECT
# MAGIC   ROUND(SUM(revenue), 2) AS total_revenue
# MAGIC FROM ecommerce.gold.products;

# COMMAND ----------

# DBTITLE 1,Cell 4
# MAGIC %sql
# MAGIC SELECT
# MAGIC   product_name AS brand,
# MAGIC   ROUND(revenue, 2) AS revenue
# MAGIC FROM ecommerce.gold.products
# MAGIC ORDER BY revenue DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# DBTITLE 1,Calculate revenue percentage per product
# MAGIC %sql
# MAGIC SELECT
# MAGIC   product_name AS brand,
# MAGIC   ROUND(revenue / SUM(revenue) OVER () * 100, 2) AS revenue_percentage
# MAGIC FROM ecommerce.gold.products
# MAGIC ORDER BY revenue_percentage DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# DBTITLE 1,Visualize brand revenue percentage
import pandas as pd
import matplotlib.pyplot as plt

# Fetch the SQL results into a pandas DataFrame
df = spark.sql('SELECT product_name AS brand, ROUND(revenue / SUM(revenue) OVER () * 100, 2) AS revenue_percentage FROM ecommerce.gold.products ORDER BY revenue_percentage DESC LIMIT 10').toPandas()

plt.figure(figsize=(10, 6))
plt.bar(df['brand'], df['revenue_percentage'], color='skyblue')
plt.xlabel('Brand')
plt.ylabel('Revenue Percentage (%)')
plt.title('Top 10 Brands by Revenue Percentage')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# COMMAND ----------

# DBTITLE 1,Cell 7
# MAGIC %sql
# MAGIC WITH daily AS (
# MAGIC   SELECT
# MAGIC     event_date,
# MAGIC     SUM(revenue) AS rev
# MAGIC   FROM ecommerce.gold.products
# MAGIC   GROUP BY event_date
# MAGIC )
# MAGIC SELECT
# MAGIC   event_date,
# MAGIC   rev,
# MAGIC   AVG(rev) OVER (
# MAGIC     ORDER BY event_date
# MAGIC     ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
# MAGIC   ) AS ma7
# MAGIC FROM daily
# MAGIC ORDER BY event_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   category_code,
# MAGIC   SUM(views) AS views,
# MAGIC   SUM(purchases) AS purchases,
# MAGIC   ROUND(SUM(purchases) * 100.0 / SUM(views), 2) AS conversion_rate
# MAGIC FROM ecommerce.gold.products
# MAGIC GROUP BY category_code
# MAGIC ORDER BY conversion_rate DESC;
# MAGIC