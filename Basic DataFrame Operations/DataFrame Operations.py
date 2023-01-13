# Databricks notebook source
from pyspark.sql import Row
import datetime
from pyspark.sql.functions import col,date_format
import pandas as pd
from pyspark.sql.functions import *

# COMMAND ----------

spark.conf.set('spark.sql.execution.arrow.pyspark.enabled',False)

# COMMAND ----------

users_dict = [
    {
        'user_id':1,
        'first_name':"Vishal",
        'last_name':'patel',
        'email':'vishalpatel@gmail.com',
        'gender':'male',
        'courses':[],
        'is_customer':True,
        'amount_paid':None,
        'country':"Canada",
        'age':23,
        'phone' : Row(mobile="+1 647 894 4598", home="+1 564 897 4855"),
        'last_updated_timestamp' : datetime.datetime(2023, 4, 10, 17, 45, 30)
    },
    {
        'user_id':2,
        'first_name':"Karina",
        'last_name':'patel',
        'email':'karinapatel@gmail.com',
        'gender':'female',
        'courses':[3,4],
        'is_customer':False,
        'amount_paid':350,
        'country':"India",
        'age':25,
        'phone' : Row(mobile="+1 647 894 4598",home=None),
        'last_updated_timestamp' : datetime.datetime(2021, 3, 20, 7, 5, 0)
        
    },
    {
        'user_id':3,
        'first_name':"Jay",
        'last_name':'Wick',
        'email':'jaywick@gmail.com',
        'gender':'male',
        'courses':[5,4],
        'is_customer':True,
        'amount_paid':None,
        'country':"USA",
        'age':28,
        'phone' : Row(mobile="+1 647 894 4598", home="+1 564 897 4855"),
        'last_updated_timestamp' : datetime.datetime(2022, 4, 10, 2, 45, 30)
    },
    {
        'user_id':4,
        'first_name':"krishna",
        'last_name':'Desai',
        'email':'jaykishandesai@gmail.com',
        'gender':'female',
        'courses':[5],
        'is_customer':True,
        'amount_paid':1000,
        'country':"Canada",
        'age':23,
        'phone':Row(mobile=None,home=None),
        'last_updated_timestamp' : datetime.datetime(2023, 4, 10, 17, 45, 30)
    },
    {
        'user_id':5,
        'first_name':"Jonny",
        'last_name':'depp',
        'email':'jonnydepp@gmail.com',
        'gender':'male',
        'courses':[4,5,6,7],
        'is_customer':False,
        'amount_paid':500,
        'country':"USA",
        'age':24,
        'phone':Row(mobile="+1 456 4586 254",home="+91 125 455 523"),
        'last_updated_timestamp' : datetime.datetime(2023, 4, 10, 0, 45, 18)
    },
    {
        'user_id':6,
        'first_name':"jenny",
        'last_name':'depp',
        'email':'jennydepp@gmail.com',
        'gender':'female',
        'courses':[4,5,6,7],
        'is_customer':False,
        'amount_paid':1,
        'country':"USA",
        'age':24,
        'phone':Row(mobile="+1 456 456 4569",home="+91 125 256 1455"),
        'last_updated_timestamp' : datetime.datetime(2023, 4, 10, 0, 45, 18)
    }
] 

# COMMAND ----------

df = spark.createDataFrame(pd.DataFrame(users_dict))

# COMMAND ----------

df.show(truncate=False)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.filter(df.age > 25).show()

# COMMAND ----------

df.where('age > 25').show()

# COMMAND ----------

df.filter(df['age']==25).show()

# COMMAND ----------

df.createOrReplaceTempView('user')

# COMMAND ----------

spark.sql("select user_id, concat(first_name,' ', last_name) as full_name,  age from user where age == 25").show()

# COMMAND ----------

spark.sql("select user_id, concat(first_name,' ', last_name) as full_name,  age, is_customer from user where is_customer is not True or is_customer = 'true'").show()

# COMMAND ----------

df.filter(df['is_customer'] == True).show()

# COMMAND ----------

df.filter('is_customer = False').show()

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from user

# COMMAND ----------

df.filter(isnan('amount_paid') == 'false').show()

# COMMAND ----------

df.select('user_id', 'first_name', 'country').show()

# COMMAND ----------

df.select('user_id',concat('first_name',lit(' '),'last_name').alias('full_name'), 'country','age').filter("country != 'Canada' and age > 25").show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.coalesce(1).show()

# COMMAND ----------

df.filter(col('last_updated_timestamp').between('2021-03-19','2021-03-22')).show()

# COMMAND ----------

df.filter("last_updated_timestamp between '2021-03-19' and '2021-03-22'").show()

# COMMAND ----------

df. \
select('first_name', 'last_name'). \
show()

# COMMAND ----------

df. \
select('user_id', 'first_name', 'last_name', 'amount_paid'). \
show()

# COMMAND ----------

df. \
select('user_id', 'first_name', 'last_name', 'amount_paid'). \
filter("amount_paid between '350' and '550'"). \
show()

# COMMAND ----------

df.filter((col('country') == 'Canada') & (col('gender') == 'female')).show()

# COMMAND ----------

df.drop('phone','last_updated_timestamp','vishal').show()

# COMMAND ----------

l = ['email','courses','last_updated_timestamp']

# COMMAND ----------



# COMMAND ----------

users_dict = [
    {
        'user_id':1,
        'first_name':"Vishal",
        'last_name':'patel',
        'email':'vishalpatel@gmail.com',
        'gender':'male',
        'courses':[],
        'is_customer':True,
        'amount_paid':None,
        'country':"Canada",
        'age':23,
        'phone' : Row(mobile="+1 647 894 4598", home="+1 564 897 4855"),
        'last_updated_timestamp' : datetime.datetime(2023, 4, 10, 17, 45, 30)
    },
    {
        'user_id':2,
        'first_name':"Karina",
        'last_name':'patel',
        'email':'karinapatel@gmail.com',
        'gender':'female',
        'courses':[3,4],
        'is_customer':False,
        'amount_paid':350,
        'country':'India',
        'age':25,
        'phone' : Row(mobile="+1 647 894 4598",home=None),
        'last_updated_timestamp' : datetime.datetime(2021, 3, 20, 7, 5, 0)
        
    },
    {
        'user_id':3,
        'first_name':"Jay",
        'last_name':'Wick',
        'email':'jaywick@gmail.com',
        'gender':'male',
        'courses':[5,4],
        'is_customer':True,
        'amount_paid':None,
        'country':"USA",
        'age':28,
        'phone' : Row(mobile="+1 647 894 4598", home="+1 564 897 4855"),
        'last_updated_timestamp' : datetime.datetime(2022, 4, 10, 2, 45, 30)
    },
    {
        'user_id':4,
        'first_name':"krishna",
        'last_name':'Desai',
        'email':'jaykishandesai@gmail.com',
        'gender':'female',
        'courses':[5],
        'is_customer':True,
        'amount_paid':1000,
        'country':"Canada",
        'age':23,
        'phone':Row(mobile=None,home=None),
        'last_updated_timestamp' : datetime.datetime(2023, 4, 10, 17, 45, 30)
    },
    {
        'user_id':5,
        'first_name':"Jonny",
        'last_name':'depp',
        'email':'jonnydepp@gmail.com',
        'gender':'male',
        'courses':[4,5,6,7],
        'is_customer':False,
        'amount_paid':500,
        'country':None,
        'age':24,
        'phone':Row(mobile="+1 456 4586 254",home="+91 125 455 523"),
        'last_updated_timestamp' : datetime.datetime(2023, 4, 10, 0, 45, 18)
    },
    {
        'user_id':6,
        'first_name':"jenny",
        'last_name':'depp',
        'email':'jennydepp@gmail.com',
        'gender':'female',
        'courses':[4,5,6,7],
        'is_customer':False,
        'amount_paid':1,
        'country':None,
        'age':24,
        'phone':Row(mobile="+1 456 456 4569",home="+91 125 256 1455"),
        'last_updated_timestamp' : datetime.datetime(2023, 4, 10, 0, 45, 18)
    }
] 

# COMMAND ----------

df = spark.createDataFrame(pd.DataFrame(users_dict))

# COMMAND ----------

df.show()

# COMMAND ----------

df = df.drop('email','lat_updated_timestamp')

# COMMAND ----------

df.select('user_id','first_name','last_name','gender','amount_paid','country','age').show()

# COMMAND ----------

df.dropna(how = 'all',subset=['age','country','amount_paid']).show()

# COMMAND ----------

df.sort(col('country').asc_nulls_first()).show()

# COMMAND ----------

df.sort(size('courses'),ascending=False).withColumn('courses_enrolled',size('courses')).show()

# COMMAND ----------

df.sort(desc('first_name')).show()

# COMMAND ----------

c = when(col('country') == 'India',0).otherwise(when(col('country') == 'USA', 1).otherwise(2))

# COMMAND ----------

c

# COMMAND ----------

df.sort(c,'age').show()

# COMMAND ----------

level = expr("""
    case
        when gender == 'male' then 0
        when gender == 'frmale' then 1
        else 2
        end
""")

# COMMAND ----------

df.sort(level).show()

# COMMAND ----------

# MAGIC %fs ls /public/retail_db/orders

# COMMAND ----------

data = [
      {
        "id": "1",
		"city":"Canada"
      },
      {
        "id": "2",
		"city":"USA"
      },
      {
        "id": "3",
		"city":"Toronto"
      },
	  {
        "id": "4",
		"city":"York"
      },
	  {
        "id": "5",
		"city":"Canada"
      }
  
]

# COMMAND ----------

city = spark.createDataFrame(pd.DataFrame(data))

# COMMAND ----------

user =  [
      {
        "id": "1",
        "firstName": "Tom",
        "lastName": "Cruise",
        "age": 24,
        "salary" : 10000
      },
      {
        "id": "2",
        "firstName": "Maria",
        "lastName": "Sharapova",
        "age": 23,
        "salary" : 20000
      },
      {
        "id": "3",
        "firstName": "Robert",
        "lastName": "Downey Jr.",
        "age": 20,
        "salary" : 25000
      },
	  {
        "id": "4",
        "firstName": "Vishal",
        "lastName": "Patel",
        "age": 28,
        "salary" : 5000
      },
	  {
        "id": "5",
        "firstName": "Jay",
        "lastName": "Depp",
        "age": 26,
        "salary" : 10000
      }
    ]

# COMMAND ----------

user = spark.createDataFrame(pd.DataFrame(user))

# COMMAND ----------

user.show()

# COMMAND ----------

city.show()

# COMMAND ----------

city.write.json('/public/retail_db/city.json',mode="overwrite")

# COMMAND ----------

user.write.json('/public/retail_db/user.json',mode="overwrite")

# COMMAND ----------

# MAGIC %fs ls '/public/retail_db/'

# COMMAND ----------

cities = spark.read.json('/public/retail_db/city.json')

# COMMAND ----------

cities.show()

# COMMAND ----------

users = spark.read.json('/public/retail_db/user.json')

# COMMAND ----------

users.show()

# COMMAND ----------

users.select(count("*")).show()

# COMMAND ----------

users.groupBy('salary').agg(sum('salary').alias("nums")).show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from 'uu'

# COMMAND ----------

import datetime
courses = [
    {
        'course_id': 1,
        'course_title': 'Mastering Python',
        'course_published_dt': datetime.date(2021, 1, 14),
        'is_active': True,
        'last_updated_ts': datetime.datetime(2021, 2, 18, 16, 57, 25)
    },
    {
        'course_id': 2,
        'course_title': 'Data Engineering Essentials',
        'course_published_dt': datetime.date(2021, 2, 10),
        'is_active': True,
        'last_updated_ts': datetime.datetime(2021, 3, 5, 12, 7, 33)
    },
    {
        'course_id': 3,
        'course_title': 'Mastering Pyspark',
        'course_published_dt': datetime.date(2021, 1, 7),
        'is_active': True,
        'last_updated_ts': datetime.datetime(2021, 4, 6, 10, 5, 42)
    },
    {
        'course_id': 4,
        'course_title': 'AWS Essentials',
        'course_published_dt': datetime.date(2021, 3, 19),
        'is_active': False,
        'last_updated_ts': datetime.datetime(2021, 4, 10, 2, 25, 36)
    },
    {
        'course_id': 5,
        'course_title': 'Docker 101',
        'course_published_dt': datetime.date(2021, 2, 28),
        'is_active': True,
        'last_updated_ts': datetime.datetime(2021, 3, 21, 7, 18, 52)
    }
]

courses_df = spark.createDataFrame([Row(**course) for course in courses])

# COMMAND ----------

users = [{
  "user_id": 1,
  "user_first_name": "Sandra",
  "user_last_name": "Karpov",
  "user_email": "skarpov0@ovh.net"
}, {
  "user_id": 2,
  "user_first_name": "Kari",
  "user_last_name": "Dearth",
  "user_email": "kdearth1@so-net.ne.jp"
}, {
  "user_id": 3,
  "user_first_name": "Joanna",
  "user_last_name": "Spennock",
  "user_email": "jspennock2@redcross.org"
}, {
  "user_id": 4,
  "user_first_name": "Hirsch",
  "user_last_name": "Conaboy",
  "user_email": "hconaboy3@barnesandnoble.com"
}, {
  "user_id": 5,
  "user_first_name": "Loreen",
  "user_last_name": "Malin",
  "user_email": "lmalin4@independent.co.uk"
}, {
  "user_id": 6,
  "user_first_name": "Augy",
  "user_last_name": "Christon",
  "user_email": "achriston5@mlb.com"
}, {
  "user_id": 7,
  "user_first_name": "Trudey",
  "user_last_name": "Choupin",
  "user_email": "tchoupin6@de.vu"
}, {
  "user_id": 8,
  "user_first_name": "Nadine",
  "user_last_name": "Grimsdell",
  "user_email": "ngrimsdell7@sohu.com"
}, {
  "user_id": 9,
  "user_first_name": "Vassily",
  "user_last_name": "Tamas",
  "user_email": "vtamas8@businessweek.com"
}, {
  "user_id": 10,
  "user_first_name": "Wells",
  "user_last_name": "Simpkins",
  "user_email": "wsimpkins9@amazon.co.uk"
}]

users_df = spark.createDataFrame([Row(**user) for user in users])

# COMMAND ----------

course_enrolments = [{
  "course_enrolment_id": 1,
  "user_id": 10,
  "course_id": 2,
  "price_paid": 9.99
}, {
  "course_enrolment_id": 2,
  "user_id": 5,
  "course_id": 2,
  "price_paid": 9.99
}, {
  "course_enrolment_id": 3,
  "user_id": 7,
  "course_id": 5,
  "price_paid": 10.99
}, {
  "course_enrolment_id": 4,
  "user_id": 9,
  "course_id": 2,
  "price_paid": 9.99
}, {
  "course_enrolment_id": 5,
  "user_id": 8,
  "course_id": 2,
  "price_paid": 9.99
}, {
  "course_enrolment_id": 6,
  "user_id": 5,
  "course_id": 5,
  "price_paid": 10.99
}, {
  "course_enrolment_id": 7,
  "user_id": 4,
  "course_id": 5,
  "price_paid": 10.99
}, {
  "course_enrolment_id": 8,
  "user_id": 7,
  "course_id": 3,
  "price_paid": 10.99
}, {
  "course_enrolment_id": 9,
  "user_id": 8,
  "course_id": 5,
  "price_paid": 10.99
}, {
  "course_enrolment_id": 10,
  "user_id": 3,
  "course_id": 3,
  "price_paid": 10.99
}, {
  "course_enrolment_id": 11,
  "user_id": 7,
  "course_id": 5,
  "price_paid": 10.99
}, {
  "course_enrolment_id": 12,
  "user_id": 3,
  "course_id": 2,
  "price_paid": 9.99
}, {
  "course_enrolment_id": 13,
  "user_id": 5,
  "course_id": 2,
  "price_paid": 9.99
}, {
  "course_enrolment_id": 14,
  "user_id": 4,
  "course_id": 3,
  "price_paid": 10.99
}, {
  "course_enrolment_id": 15,
  "user_id": 8,
  "course_id": 2,
  "price_paid": 9.99
}]

course_enrolments_df = spark.createDataFrame([Row(**ce) for ce in course_enrolments])

# COMMAND ----------

courses_df.show()

# COMMAND ----------

users_df.show()

# COMMAND ----------

course_enrolments_df.show()

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


