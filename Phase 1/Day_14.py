# Databricks notebook source
# MAGIC %md
# MAGIC Which products have the highest conversion rate?
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cell 2
# MAGIC %sql
# MAGIC SELECT
# MAGIC   product_id,
# MAGIC   conversion_rate
# MAGIC FROM workspace.ecommerce.gold_product_performance
# MAGIC ORDER BY conversion_rate DESC
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC What's the trend of daily purchases over time?
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cell 4
# MAGIC %sql
# MAGIC SELECT
# MAGIC   SUM(purchases) AS total_purchases
# MAGIC FROM workspace.ecommerce.gold_product_performance;

# COMMAND ----------

# MAGIC %md
# MAGIC Find customers who viewed but never purchased
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cell 6
# MAGIC %sql
# MAGIC SELECT DISTINCT user_id
# MAGIC FROM workspace.ecommerce.silver_ecommerce_events
# MAGIC GROUP BY user_id
# MAGIC HAVING
# MAGIC   SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) = 0
# MAGIC   AND
# MAGIC   SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) > 0
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC Build Simple NLP Task

# COMMAND ----------

# MAGIC %pip install transformers
# MAGIC from transformers import pipeline
# MAGIC import mlflow
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cell 9
import torch
from transformers import pipeline
classifier = pipeline("sentiment-analysis")

# COMMAND ----------

reviews = [
    "This product is amazing!",
    "Terrible quality, waste of money"
]

results = classifier(reviews)
results


# COMMAND ----------

# MAGIC %md
# MAGIC Log NLP Experiment to MLflow

# COMMAND ----------

# DBTITLE 1,Cell 12
import mlflow
with mlflow.start_run(run_name="sentiment_model"):
    mlflow.log_param("model", "distilbert-sentiment")
    mlflow.log_param("task", "sentiment_analysis")
    # Example metric (demo)
    mlflow.log_metric("accuracy", 0.95)

# COMMAND ----------

# MAGIC %md
# MAGIC