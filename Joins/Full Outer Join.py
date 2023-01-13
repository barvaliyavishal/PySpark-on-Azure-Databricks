# Databricks notebook source
# MAGIC %run "./Dataset For Joins"

# COMMAND ----------

users_df.join(course_enrolments_df,'user_id','full_outer').show()

# COMMAND ----------

users_df.join(course_enrolments_df,'user_id','inner').count()

# COMMAND ----------

users_df. \
    join(course_enrolments_df,'user_id','left'). \
    union(users_df.join(course_enrolments_df,'user_id','right')). \
    distinct(). \
    show()

# COMMAND ----------


