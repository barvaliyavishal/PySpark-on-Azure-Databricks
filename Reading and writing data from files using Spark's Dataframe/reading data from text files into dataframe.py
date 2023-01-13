# Databricks notebook source
users = spark.read.json('/public/retail_db/user.json')

# COMMAND ----------

users.show()

# COMMAND ----------

schema = """
    age int,
    first_name string,
    id int,
    last_name string,
    salary int
"""

# COMMAND ----------

# MAGIC %fs ls "/public/retail_db"

# COMMAND ----------

users.write.parquet("/public/retail_db/write_parquet/users.parquet")

# COMMAND ----------

# MAGIC %fs ls "/public/retail_db/write_parquet"

# COMMAND ----------

u = spark.read.parquet("/public/retail_db/write_parquet/users.parquet")

# COMMAND ----------

u.show()

# COMMAND ----------

users.show()

# COMMAND ----------

users.write.csv("/public/retail_db/write/csv/users.csv",mode="overwrite",sep='|')

# COMMAND ----------

# MAGIC %fs ls "/public/retail_db/write/csv/"

# COMMAND ----------

spark.read.schema(schema).csv("/public/retail_db/write/csv/users.csv",sep='|').show()

# COMMAND ----------

spark.read.text("/public/retail_db/write/csv/users.csv").show(truncate=False)

# COMMAND ----------


