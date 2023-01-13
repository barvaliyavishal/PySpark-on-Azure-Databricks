# Databricks notebook source
df = spark.read.json('/public/courses/course.json')

# COMMAND ----------

df.show()

# COMMAND ----------

df.select('stars').distinct().count()

# COMMAND ----------

df.write.parquet('/public/courses/write/',mode='overwrite',partitionBy=['stars','suitable_for'])

# COMMAND ----------

# MAGIC %fs ls /S  '/public/courses/write/'

# COMMAND ----------


df.write.parquet('/public/courses/write/',mode='overwrite',partitionBy=['stars'])

# COMMAND ----------


