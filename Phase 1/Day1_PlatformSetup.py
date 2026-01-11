# Databricks notebook source
# Create simple DataFrame
data = [("iPhone", 999), ("Samsung", 799), ("MacBook", 1299)]
df = spark.createDataFrame(data, ["product", "price"])
df.show()

# Filter expensive products
df.filter(df.price > 1000).show()


# COMMAND ----------

# Create simple DataFrame
data = [
    ("Megha", "Tech", 45000),
    ("Pratik", "HR", 35000),
    ("Shriya", "Tech", 55000),
    ("Priya", "Sales", 40000)
]

df = spark.createDataFrame(data, ["name", "department", "salary"])

# Show DataFrame
df.show()




# COMMAND ----------

df.filter(df.salary > 40000).show()


# COMMAND ----------

df.select("name", "salary").show()
