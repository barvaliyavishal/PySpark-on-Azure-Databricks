# Databricks notebook source
# MAGIC %run "./Dataset For Joins"

# COMMAND ----------

users_df.join(course_enrolments_df, 'user_id').show()

# COMMAND ----------

users_df. \
    join(course_enrolments_df, 'user_id'). \
    select(users_df['*'],course_enrolments_df['price_paid'],course_enrolments_df['course_enrolment_id']). \
    show()

# COMMAND ----------

users_df.alias('u'). \
    join(course_enrolments_df.alias('c'), 'user_id'). \
    select('u.*','c.price_paid','c.course_enrolment_id'). \
    show()

# COMMAND ----------

users_df.alias('u'). \
    join(course_enrolments_df.alias('c'), 'user_id'). \
    groupBy('u.user_id'). \
    count(). \
    show()

# COMMAND ----------

users_df.alias('u'). \
    join(course_enrolments_df.alias('c'), users_df.user_id == course_enrolments_df.user_id). \
    select('u.*','c.price_paid','c.course_enrolment_id'). \
    show()

# COMMAND ----------

users_df.join(course_enrolments_df,'user_id').groupBy('user_id').count().show()

# COMMAND ----------

users_df.join(course_enrolments_df,'user_id','left').show()

# COMMAND ----------

users_df.join(course_enrolments_df,'user_id','left').filter("course_id is null or user_id in (1,2,6)").show()

# COMMAND ----------

users_df.join(course_enrolments_df,'user_id','left').filter("course_id is not null").show()

# COMMAND ----------

users_df.join(course_enrolments_df,'user_id','left').groupby('user_id').agg(sum(when(course_enrolments_df['course_enrolment_id'].isNull(),0).otherwise(1)).alias('course_count')).sort(['course_count','user_id'],ascending=[0,1]).show()

# COMMAND ----------

users_df. \
    join(course_enrolments_df,'user_id','left'). \
    groupby('user_id'). \
    agg(sum(expr("""
        case when course_enrolment_id is null then 0 
        else 1
        end """
                )).alias('course_count')). \
    sort(['course_count','user_id'],ascending=[0,1]). \
    show()

# COMMAND ----------


