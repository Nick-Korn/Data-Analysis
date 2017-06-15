from __future__ import print_function

"""
cassandraTest.py 

Created by Niko Liimatainen: 13.6.2017
Modified by Niko Liimatainen: 14.6.2017
            Niko Liimatainen: 15.6.2017

Short first script testing Cassandra integration to Spark.
"""


from pyspark.sql import SparkSession
from datetime import *


spark = SparkSession \
    .builder \
    .appName("cassandraTest") \
    .getOrCreate()

# creating SparkSession for spark.sql


df = spark.read.format("org.apache.spark.sql.cassandra")\
     .options(table="harmitus_logs", keyspace="kaa").load()

# giving the proper parameters to spark.read in order to connect to the right
#  cassandra table

df.show()

tDayData = df.filter(df["date"] == date.today())

# function to filter today's data

print(str(df.filter(df["date"] == date.today()).count()) + '\n\n')

print(str(df.filter(df["harmitus_level"] == 3).count()) + '\n\n')

print(str(df.filter(df["harmitus_level"] == 2).count()) + '\n\n')

print(str(df.filter(df["harmitus_level"] == 1).count()) + '\n\n')

print(str(df.filter(df["harmitus_level"] == 0).count()) + '\n\n')

# filtering to get the count of certain "frustration levels"

