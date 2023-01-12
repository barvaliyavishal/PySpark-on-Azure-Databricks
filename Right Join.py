# Databricks notebook source
# MAGIC %run "./Dataset For Joins"

# COMMAND ----------

users_df. \
    join(course_enrolments_df,'user_id','right'). \
    show()

# COMMAND ----------

users_df. \
    join(course_enrolments_df,'user_id','right_outer'). \
    show()

# COMMAND ----------


