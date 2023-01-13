# Databricks notebook source
users = spark.read.json('/public/retail_db/user.json')

# COMMAND ----------

u = spark.read.parquet("/public/retail_db/write_parquet/users.parquet")

# COMMAND ----------

schema = """
    age int,
    first_name string,
    id int,
    last_name string,
    salary int
"""

# COMMAND ----------

spark.read.schema(schema).csv("/public/retail_db/write/csv/users.csv",sep='|').show()

# COMMAND ----------


