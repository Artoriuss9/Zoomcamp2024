#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pyspark
from pyspark.sql import SparkSession

#Q1

pyspark.__version__

#Q2

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

df_fhv = spark.read \
    .option("header", "true") \
    .csv('/home/artoriuss9/Homework_5/fhv_tripdata_2019-10.csv.gz')

df_fhv.printSchema()

df_fhv.show(5)

from pyspark.sql import types

schema = types.StructType([
	types.StructField('dispatching_base_num', types.StringType(), True), 
	types.StructField('pickup_datetime', types.TimestampType(), True), 
	types.StructField('dropOff_datetime', types.TimestampType(), True), 
	types.StructField('PUlocationID', types.IntegerType(), True), 
	types.StructField('DOlocationID', types.IntegerType(), True), 
	types.StructField('SR_Flag', types.StringType(), True)
])

df_fhv = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv('/home/artoriuss9/Homework_5/fhv_tripdata_2019-10.csv.gz')

df_fhv.printSchema()

df_fhv.repartition(6)

df_fhv.coalesce(6).write \
    .parquet('/home/artoriuss9/Homework_5/fhvhv/2019/10', mode = 'overwrite')

get_ipython().system('ls -lh /home/artoriuss9/Homework_5/fhvhv/2019/10')

#Q3

df_fhv.printSchema()

from pyspark.sql import functions as F

df_fhv \
    .withColumn('pickup_date', F.to_date(df_fhv.pickup_datetime)) \
    .filter("pickup_date = '2019-10-15'") \
    .count()

#Q4

df_fhv.show(5)

df_fhv.printSchema()

df_fhv.columns

df_fhv \
    .withColumn('duration', df_fhv.dropOff_datetime.cast('long') - df_fhv.pickup_datetime.cast('long')) \
    .withColumn('pickup_date', F.to_date(df_fhv.pickup_datetime)) \
    .groupBy('pickup_date') \
        .max('duration') \
    .orderBy('max(duration)', ascending=False) \
    .limit(10) \
    .show()

df_fhv.registerTempTable('df_fhv_longest_trip')

spark.sql("""
SELECT
    to_date(pickup_datetime) AS pickup_date,
    MAX((CAST(dropOff_datetime AS LONG) - CAST(pickup_datetime AS LONG)) / 60) AS duration
FROM 
    df_fhv_longest_trip
GROUP BY
    1
ORDER BY
    2 DESC
LIMIT 10;
""").show()

#Q5

4040

#Q6

df_fhv_parq = spark.read.parquet('/home/artoriuss9/Homework_5/fhvhv/2019/10')

df_zones = spark.read.parquet('/home/artoriuss9/notebooks/zones')

df_result = df_fhv_parq.join(df_zones, df_fhv_parq.PUlocationID == df_zones.LocationID)

df_result.drop('dispatching_base_num', 'PUlocationID').show()

df_result.registerTempTable('df_fhv_least')

spark.sql("""
SELECT
    LocationID,
    COUNT(1)
FROM 
    df_fhv_least
GROUP BY
    1
ORDER BY
    2 ASC
LIMIT 5;
""").show()

df_result \
    .groupBy('LocationID') \
        .count() \
    .orderBy('count') \
    .limit(5) \
    .show()

df_result \
    .filter("LocationID = 2") \
    .count()

