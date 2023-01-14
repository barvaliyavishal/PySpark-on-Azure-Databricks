# Databricks notebook source
df = spark.read.parquet("/public/courses/write/")

# COMMAND ----------

df.show()

# COMMAND ----------

first = spark.udf.register('first_char',lambda x : x[0])

# COMMAND ----------

df.select(first('suitable_for').alias("First_char")).show()

# COMMAND ----------

df.createOrReplaceTempView('user')

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select (first_char(suitable_for)) from user

# COMMAND ----------


