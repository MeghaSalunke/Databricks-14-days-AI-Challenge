# Databricks notebook source
#Required Imports (Common)
import mlflow
import mlflow.sklearn

from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score


# COMMAND ----------

# MAGIC %md
# MAGIC Prepare Training Data (sklearn)

# COMMAND ----------

# DBTITLE 1,Cell 2
df = spark.table("workspace.ecommerce.gold_product_performance").toPandas()

X = df[["views"]]
y = df["purchases"]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)


# COMMAND ----------

# MAGIC %md
# MAGIC Define Multiple Models

# COMMAND ----------

models = {
    "linear": LinearRegression(),
    "decision_tree": DecisionTreeRegressor(max_depth=5, random_state=42),
    "random_forest": RandomForestRegressor(n_estimators=100, random_state=42)
}


# COMMAND ----------

# MAGIC %md
# MAGIC Train Models & Log to MLflow

# COMMAND ----------

mlflow.set_experiment("/Shared/multi_model_comparison")

for name, model in models.items():
    with mlflow.start_run(run_name=f"{name}_model"):

        # Log parameters
        mlflow.log_param("model_type", name)

        if name == "decision_tree":
            mlflow.log_param("max_depth", 5)
        if name == "random_forest":
            mlflow.log_param("n_estimators", 100)

        # Train
        model.fit(X_train, y_train)

        # Evaluate
        y_pred = model.predict(X_test)
        score = r2_score(y_test, y_pred)

        # Log metric
        mlflow.log_metric("r2_score", score)

        # Log model
        mlflow.sklearn.log_model(model, artifact_path="model")

        print(f"{name}: R² = {score:.4f}")


# COMMAND ----------

#Select Best Model
best_model_name = "random_forest"
print(f"Selected best model: {best_model_name}")


# COMMAND ----------

# MAGIC %md
# MAGIC Spark ML Pipeline (Scalable ML)

# COMMAND ----------

#Import Spark ML Components
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression as SparkLR


# COMMAND ----------

# DBTITLE 1,Cell 7
#Create Feature Vector
assembler = VectorAssembler(
    inputCols=["views"],
    outputCol="features"
)


# COMMAND ----------

#Define Spark ML Model
lr = SparkLR(
    featuresCol="features",
    labelCol="purchases"
)


# COMMAND ----------

#Build Spark ML Pipeline
pipeline = Pipeline(stages=[assembler, lr])


# COMMAND ----------

# DBTITLE 1,Cell 10
#Train Pipeline Model
spark_df = spark.table("workspace.ecommerce.gold_product_performance")

train_df, test_df = spark_df.randomSplit([0.8, 0.2], seed=42)

pipeline_model = pipeline.fit(train_df)


# COMMAND ----------

#Evaluate Spark Model
predictions = pipeline_model.transform(test_df)
predictions.select("features", "purchases", "prediction").show(5)
