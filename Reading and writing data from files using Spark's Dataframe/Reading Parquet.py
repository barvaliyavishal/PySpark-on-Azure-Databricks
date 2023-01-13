# Databricks notebook source
df = spark.read.parquet("/public/retail_db/write_parquet/users.parquet")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.show()

# COMMAND ----------

df.dtypes

# COMMAND ----------

schema = """
    age long,
    firstName string,
    id string,
    lastName string,
    salary long
    """

# COMMAND ----------

df = spark.read.schema(schema).parquet("/public/retail_db/write_parquet/users.parquet")

# COMMAND ----------

df.show()

# COMMAND ----------

from pyspark.sql.types import IntegerType,FloatType,StringType,DataType,StructType,StructField,LongType

# COMMAND ----------

schema1 = StructType([
    StructField('age',LongType()),
    StructField('firstName',StringType()),
    StructField('id',StringType()),
    StructField('lastName',StringType()),
    StructField('salary',LongType())
])

# COMMAND ----------

df = spark.read.schema(schema1).parquet("/public/retail_db/write_parquet/users.parquet")

# COMMAND ----------

df.show()

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

dff = df.withColumn('age',col('age').cast('int')).withColumn('id',col('id').cast('int')).withColumn('salary',col('salary').cast('int'))

# COMMAND ----------

dff.show()

# COMMAND ----------

dff.printSchema()

# COMMAND ----------


