# Databricks notebook source
#Import Required Libraries
import mlflow
import mlflow.sklearn

from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score


# COMMAND ----------

# DBTITLE 1,Cell 2
#Prepare Data (From Delta Table)
df = spark.table("workspace.ecommerce.gold_product_performance").toPandas()

X = df[["views"]]
y = df["purchases"]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)


# COMMAND ----------

mlflow.set_experiment("/Shared/ecommerce_purchase_prediction")


# COMMAND ----------

# DBTITLE 1,Cell 4
from mlflow.models import infer_signature

with mlflow.start_run(run_name="linear_regression_v1"):
    # Log parameters
    mlflow.log_param("model_type", "LinearRegression")
    mlflow.log_param("test_size", 0.2)
    mlflow.log_param("features", "views, cart_adds")

    # Train model
    model = LinearRegression()
    model.fit(X_train, y_train)

    # Predict & evaluate
    y_pred = model.predict(X_test)
    score = r2_score(y_test, y_pred)

    # Log metric
    mlflow.log_metric("r2_score", score)

    # Infer and log model signature
    signature = infer_signature(X_train, y_train)
    mlflow.sklearn.log_model(
        model,
        artifact_path="model",
        registered_model_name="purchase_prediction_lr",
        signature=signature
    )

print(f"R² Score: {score:.4f}")
