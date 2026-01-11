# Databricks notebook source
#Read the CSV file into a Spark DataFrame
df = spark.read.csv("/Volumes/workspace/ecommerce/ecommerce_data/products.csv", header=True)
display(df.limit(30))



# COMMAND ----------

#Display the Schema
df.printSchema()

# COMMAND ----------

#Select()
display(
  df.select(
    "Product ID",
    "Product Name",
    "Product Category"
  ).limit(20)
)
                                                                        


# COMMAND ----------

#filter()
display(df.filter(df.Price > 300.00).limit(10))


# COMMAND ----------

#Orderby()
display(df.orderBy(df.Price.desc()).limit(10))


# COMMAND ----------

#GroupBy()
display(
  df.groupBy("Product Category")
    .count()
)