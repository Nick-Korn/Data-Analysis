"""
IOTcassandraAVG.py

Created by: Niko Liimatainen 26.6.2017
Modified by: Niko Liimatainen 27.6.2017
             Niko Liimatainen 29.6.2017


This script is used to access the Mysticons IOT-departments Cassandra database
and calculating averages on their Raspberry Pi weather sensor data.
"""

from pyspark.sql import SparkSession, SQLContext
from time import gmtime, strftime
from json import dumps


def avgCounter(df, firstStamp, gmtime, strftime, dumps, dataFile):
    weekMs = 604800000
    # one week in Epoch milliseconds
    x = 0
    # for counting loops
    device = df.select(df['entity_id']).head()[0]
    # getting device name for future usage
    key = df.select(df['key']).head()[0]
    # getting key, which is the name of the data type being filtered
    while x < 6:
        # the loop for going through x-1 weeks worth of data
        week = firstStamp + weekMs
        weekDf = df.filter(df['ts'] > firstStamp)
        weekDf = weekDf.filter(weekDf['ts'] < week)
        # using the start day timestamp and the weekMs variable to filter a
        # specific week
        dfAvg = weekDf.groupBy().avg('dbl_v')
        # getting the avg of the double values as a data frame
        avg = dfAvg.head()[0]
        # this line gets the first value of the column

        start = strftime('%d.%m.%Y %H:%M:%S', gmtime(firstStamp / 1000))
        end = strftime('%d.%m.%Y %H:%M:%S', gmtime(week / 1000))
        # converting the epoch timestamps used to the specified format. We
        # divide by 1000 to get rid of the milliseconds

        data = {"Device": device,
                "Time frame": start + '-' + end,
                "Datatype": key,
                "Avg value": avg}
        # putting the data to a dict
        dump = dumps(data)
        # dict to .json dump
        dataFile.write(dump + '\n')
        # writing the dump to a datafile

        firstStamp = week
        x = x + 1
        # allocating variables for next loop. Shifting the firstStamp value
        # to the one that was calculated earlier to get the right time frame

spark = SparkSession \
    .builder \
    .appName("IOTcassandraAVG") \
    .getOrCreate()

# configuring Spark session for SparkSql

sqlc = SQLContext(spark)
# prototyping SQLContext for use with cahce clearing

df = spark.read.format("org.apache.spark.sql.cassandra") \
    .options(table="ts_kv_cf", keyspace="thingsboard").load()

# setting the read format to cassandra, setting the right table and keyspace
# in order to get the right data, loading that data to a Spark data frame

officeRasp = "8990d510-3faa-11e7-a809-c53cc0543175"
officeRuuvi = "6a46de80-4aa8-11e7-a8f9-c53cc0543175"

# assigning device id's to variables for better usage

firstStamp = 1495411200000

# defining the start of the first week with Epoch time in milliseconds

dataFile = open('/media/k8908/J_CENA_X64FREV_EN-US_DV5/CF2017/Python/Data'
                '/Weekly_avg.json', 'w')

# open .json file for writing

df.cache()

# using sparks .cache() function to cahce the data  that was acquired from
# cassandra to ram for superior processing speeds

filterRasp = df.filter(df['entity_id'] == officeRasp)
filterRuuvi = df.filter(df['entity_id'] == officeRuuvi)

# filtering the database to find the correct entry

tempRasp = filterRasp.filter(df['key'] == 'temperature')
pressureRasp = filterRasp.filter(df['key'] == 'air_pressure')
humidityRasp = filterRasp.filter(df['key'] == 'humidity')


tempRuuvi = filterRuuvi.filter(df['key'] == 'temperature')
pressureRuuvi = filterRuuvi.filter(df['key'] == 'air_pressure')
humidityRuuvi = filterRuuvi.filter(df['key'] == 'humidity')

# assigning value filters to variables from both devices

avgCounter(tempRasp, firstStamp, gmtime, strftime, dumps, dataFile)
avgCounter(pressureRasp, firstStamp, gmtime, strftime, dumps, dataFile)
avgCounter(humidityRasp, firstStamp, gmtime, strftime, dumps, dataFile)

avgCounter(tempRuuvi, firstStamp, gmtime, strftime, dumps, dataFile)
avgCounter(pressureRuuvi, firstStamp, gmtime, strftime, dumps, dataFile)
avgCounter(humidityRuuvi, firstStamp, gmtime, strftime, dumps, dataFile)

# calling the main function to specified data frames other required arguments

sqlc.clearCache()

# clearing the cache after the function has served it's purpose

dataFile.close()

# TODO: sending the data back to Cassandra
