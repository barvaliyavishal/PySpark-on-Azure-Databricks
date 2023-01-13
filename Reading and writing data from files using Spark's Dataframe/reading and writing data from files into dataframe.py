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

users.coalesce(1).write.csv("/public/retail_db/write/csv/users.csv",mode="overwrite",sep='|')

# COMMAND ----------

# MAGIC %fs ls "/public/retail_db/write/csv/"

# COMMAND ----------

spark.read.schema(schema).csv("/public/retail_db/write/csv/users.csv",sep='|').show()

# COMMAND ----------

spark.read.text("/public/retail_db/write/csv/users.csv").show(truncate=False)

# COMMAND ----------

spark.read.format('csv').schema(schema).load('/public/retail_db/write/csv/users.csv',sep = '|').show()

# COMMAND ----------

from pyspark.sql.types import IntegerType,StringType,FloatType,DoubleType,DataType,TimestampType,StructField,StructType

# COMMAND ----------

schema = StructType([
    StructField('age',IntegerType()),
    StructField('first_name',StringType()),
    StructField('id',IntegerType()),
    StructField('last_name',StringType()),
    StructField('salary',IntegerType())
])

# COMMAND ----------

spark.read.schema(schema).csv("/public/retail_db/write/csv/users.csv",sep='|').show()

# COMMAND ----------

spark.read.csv("/public/retail_db/write/csv/users.csv",sep='|').show()

# COMMAND ----------

spark.read.csv("/public/retail_db/write/csv/users.csv",schema=schema,sep='|').show()

# COMMAND ----------

columns = ['age','first_name','id','last_name','salary']

# COMMAND ----------

spark.read.option('inferSchema',True).csv("/public/retail_db/write/csv/users.csv",sep='|').show()

# COMMAND ----------

spark.read.option('inferSchema',True).csv("/public/retail_db/write/csv/users.csv",sep='|').dtypes

# COMMAND ----------

spark.read.option('inferSchema',True).csv("/public/retail_db/write/csv/users.csv",sep='|').toDF(*columns).show()

# COMMAND ----------


spark.read.option('inferSchema',True).csv("/public/retail_db/write/csv/users.csv",sep='|').toDF('age','first_name','id','last_name','salary').show()

# COMMAND ----------


spark.read.csv("/public/retail_db/write/csv/users.csv",sep='|',inferSchema=True).toDF('age','first_name','id','last_name','salary').show()

# COMMAND ----------

spark.read.options(sep = '|',header=None,inferSchema=True).csv('/public/retail_db/write/csv/users.csv').show()

# COMMAND ----------

spark.read.options(sep = '|',header=None,inferSchema=True).csv('/public/retail_db/write/csv/users.csv').toDF('age','first_name','id','last_name','salary').show()

# COMMAND ----------

options = {
    'sep' : '|',
    'header' : None,
    'inferSchema':True
}

# COMMAND ----------

df = spark.read.options(**options).csv('/public/retail_db/write/csv/users.csv').toDF('age','first_name','id','last_name','salary')

# COMMAND ----------

df.inputFiles()

# COMMAND ----------

schema = """
age INT,
firstname STRING,
id STRING,
lastName STRING,
salary INT
"""

# COMMAND ----------

df = spark.read.option('header',False).schema(schema).json('/public/retail_db/user.json')

# COMMAND ----------

df.show()

# COMMAND ----------

df.dtypes

# COMMAND ----------

df = spark.read.json('/public/retail_db/user.json')

# COMMAND ----------

df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------


