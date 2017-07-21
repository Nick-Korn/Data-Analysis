"""
IOTcassandraAVG.py

Created by: Niko Liimatainen 26.6.2017
Modified by: Niko Liimatainen 27.6.2017
             -||- 29.6.2017
             -||- 3.7.2017
             -||- 4.7.2017
             -||- 5.7.2017
             -||- 6.7.2017
             -||- 12.7.2017


This script is used to access the Mysticons IOT-departments Cassandra database
and calculating averages on their Raspberry Pi weather sensor data.

Documentation for this project can be found here:
https://cybertrust.labranet.jamk.fi/data-analysis/documentation/wikis/cassandra-integration
"""

from pyspark import SparkConf
from pyspark.sql import SparkSession, SQLContext
from time import gmtime, strftime
from json import dumps


def avgCounter(df, firstStamp, gmtime, strftime, dumps, dataFile):
    # one week in Epoch milliseconds
    weekMs = 604800000

    # getting device name for future usage
    device = df.select(df['entity_id']).head()[0]
    # getting key, which is the name of the data type being filtered
    key = df.select(df['key']).head()[0]
    # getting the amount of weeks that have passed since the first timestamp
    weeks = (df.groupBy().max('ts').head()[0] - firstStamp)/weekMs
    # the loop for going through weeks + 1 worth of data
    for x in range(0, int(weeks)):
        week = firstStamp + weekMs
        # using the start day timestamp and the weekMs variable to filter a
        # specific week
        weekDf = df.filter(df['ts'] > firstStamp)
        weekDf = weekDf.filter(weekDf['ts'] < week)
        # getting the avg of the double values as a data frame
        dfAvg = weekDf.groupBy().avg('dbl_v')
        # this line gets the first value of the column
        avg = dfAvg.head()[0]

        # converting the epoch timestamps used to the specified format. We
        # divide by 1000 to get rid of the milliseconds
        start = strftime('%d.%m.%Y %H:%M:%S', gmtime(firstStamp / 1000))
        end = strftime('%d.%m.%Y %H:%M:%S', gmtime(week / 1000))

        # putting the data to a dict
        data = {"Device": device,
                "Time frame": start + '-' + end,
                "Datatype": key,
                "Avg value": avg}
        # dict to .json dump
        dump = dumps(data)
        # writing the dump to a datafile
        dataFile.write(dump + '\n')

        # allocating variables for next loop. Shifting the firstStamp value
        # to the one that was calculated earlier to get the right time frame
        firstStamp = week

conf = SparkConf().set('spark.cassandra.connection.host', -ip-)

# configuring Spark session for SparkSql
spark = SparkSession \
    .builder \
    .appName("IOTcassandraAVG") \
    .config(conf=conf) \
    .getOrCreate()


# prototyping SQLContext for use with cahce clearing
sqlc = SQLContext(spark)

# setting the read format to cassandra, setting the right table and keyspace
# in order to get the right data, loading that data to a Spark data frame
df = spark.read.format("org.apache.spark.sql.cassandra") \
    .options(table="ts_kv_cf", keyspace="thingsboard").load()


# assigning device id's to variables for better usage
officeRasp = "8990d510-3faa-11e7-a809-c53cc0543175"
officeRuuvi = "6a46de80-4aa8-11e7-a8f9-c53cc0543175"


# defining the start of the first week with Epoch time in milliseconds
firstStamp = 1495411200000


# open .json file for writing
dataFile = open(-localfilepath-,'w')


# using sparks .cache() function to cahce the data  that was acquired from
# cassandra to ram for superior processing speeds
df.cache()


# filtering the database to find the correct entry
filterRasp = df.filter(df['entity_id'] == officeRasp)
filterRuuvi = df.filter(df['entity_id'] == officeRuuvi)


tempRasp = filterRasp.filter(df['key'] == 'temperature')
pressureRasp = filterRasp.filter(df['key'] == 'air_pressure')
humidityRasp = filterRasp.filter(df['key'] == 'humidity')


# assigning value filters to variables from both devices
tempRuuvi = filterRuuvi.filter(df['key'] == 'temperature')
pressureRuuvi = filterRuuvi.filter(df['key'] == 'air_pressure')
humidityRuuvi = filterRuuvi.filter(df['key'] == 'humidity')


# calling the main function to specified data frames other required arguments
avgCounter(tempRasp, firstStamp, gmtime, strftime, dumps, dataFile)
avgCounter(pressureRasp, firstStamp, gmtime, strftime, dumps, dataFile)
avgCounter(humidityRasp, firstStamp, gmtime, strftime, dumps, dataFile)

avgCounter(tempRuuvi, firstStamp, gmtime, strftime, dumps, dataFile)
avgCounter(pressureRuuvi, firstStamp, gmtime, strftime, dumps, dataFile)
avgCounter(humidityRuuvi, firstStamp, gmtime, strftime, dumps, dataFile)


# clearing the cache after the function has served it's purpose
sqlc.clearCache()


dataFile.close()

# TODO: sending the data back to Cassandra
