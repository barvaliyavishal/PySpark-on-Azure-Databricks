# Databricks notebook source
users = spark.read.csv("dbfs:/Workspace/Repos/c0851464@mylambton.ca/PySpark-on-Azure-Databricks/Reading and writing data from files using Spark's Dataframe/users.csv")

# COMMAND ----------

# MAGIC %fs ls "/public/retail_db"

# COMMAND ----------


