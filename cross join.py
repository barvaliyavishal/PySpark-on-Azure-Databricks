# Databricks notebook source
# MAGIC %run "./Dataset For Joins"

# COMMAND ----------

users_df.crossJoin(courses_df).count()

# COMMAND ----------

users_df.crossJoin(courses_df).show(100)

# COMMAND ----------

users_df.join(courses_df).show()

# COMMAND ----------

users_df.join(courses_df).count()

# COMMAND ----------


