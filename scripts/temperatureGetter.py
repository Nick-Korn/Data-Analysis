from __future__ import print_function

"""
temperatureGetter.py

Created by Niko Liimatainen: 7.7.2017
Modified by Niko Liimatainen 10.7.2017
            -||- 11.7.2017
            -||- 13.7.2017
            -||- 17.7.2017
            -||- 18.7.2017
            -||- 21.7.2017

Script for accessing a second cassandra database with weather data and
getting relevant data filtered by frustrationCorrelation.py
"""

from frustrationCorrelation import FrustrationGetter
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import LongType
from pyspark import SparkConf
from time import strftime, gmtime

import pickle


class TemperatureGetter:
    # Establishing connection to a Cassandra node
    conf = SparkConf().set('spark.cassandra.connection.host',
                           -ip-)

    # creating SparkSession for spark.sql
    spark = SparkSession \
        .builder \
        .appName("cassandraTest") \
        .config(conf=conf) \
        .getOrCreate()

    sqlc = SQLContext(spark)

    # reading the data from Cassandra and turning it into a Spark data frame
    df = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table="data", keyspace="weather_data").load()

    def __init__(self):
        pass

    def dataGetter(self):

        fg = FrustrationGetter()

        # One hour in epoch time
        epocHour = 3600

        # Casting the time column of the data frame to longtype in order to
        # conduct calculation based on epoch time
        changedDf = self.df.withColumn('time', self.df['time'].cast(LongType()))

        # ORDER!
        ascDf = changedDf.orderBy('time', ascending=True)

        # Getting a list of the time values in the data frame
        timeList = ascDf.select(ascDf['time']).collect()

        # Getting the last and  first timestamp respectively
        stamp = timeList[0]
        startStamp = stamp.time

        stamp2 = timeList[-1]
        endStamp = stamp2.time

        # Calculates how many hours worth of data there are in the data frame
        hour = (endStamp - startStamp) / epocHour

        # The loop for going thorough the data frame
        for _ in range(0, int(hour) + 1):

            # Printing the starting timestamp for the hour
            print(strftime('%d.%m.%Y %H:%M:%S', gmtime(startStamp)))

            # Calculating the ending timestamp for the hour
            nxtStamp = startStamp + epocHour

            # Filtering the data frame based on the assigned timestamps
            filterDf = ascDf.filter(ascDf['time'] > startStamp)
            filterDf = filterDf.filter(filterDf['time'] < nxtStamp)

            # Getting the hourly average
            filterDf.groupBy().avg('value').show()
            avgTemp = filterDf.groupBy().avg('value').head()[0]

            # Calling the frustration correlation script and passing the
            # required variables
            frustDf = fg.Getter(startStamp, nxtStamp, avgTemp)

            # Assigning the first stamp for next loop
            startStamp = nxtStamp

        # Here we use pickling to save the data frame we got from Frustration
        # correlation script locally
        pickle_out = open(-localfilepath-,'wb')
        pickle.dump(frustDf, pickle_out)
        pickle_out.close()

        # Clearing Spark cache
        self.sqlc.clearCache()

# Prototyping functions
tGet = TemperatureGetter()
tGet.dataGetter()

