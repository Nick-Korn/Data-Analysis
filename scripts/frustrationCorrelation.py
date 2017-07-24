from __future__ import print_function

"""
frustrationCorrelation.py

Created by Niko Liimatainen: 6.7.2017
Modified by Niko Liimatainen: 7.7.2017
            -||-: 10.7.2017
            -||-: 11.7.2017
            -||-: 12.7.2017
            -||-: 13.7.2017
            -||-: 17.7.2017
            -||-: 18.7.2017
            -||-: 21.7.2017

This script accesses IoTitude's Cassandra database to get frustration levels
and tries to make correlations between external data acquired from
temperatureGetter.py

Documentation regarding this Script can be found here:
https://cybertrust.labranet.jamk.fi/data-analysis/documentation/wikis/correlation-with-weather--and-frustration-data
"""

from pyspark.sql import SparkSession, SQLContext
from pyspark import SparkConf
from time import strftime, localtime

import pandas as pd
import requests
import json


class FrustrationGetter:
    # Connecting to the correct Cassandra node
    conf = SparkConf().set('spark.cassandra.connection.host',
                           -cassandra_ip-)

    # creating SparkSession for spark.sql
    spark = SparkSession \
        .builder \
        .appName("cassandraTest") \
        .config(conf=conf) \
        .getOrCreate()

    # Loading the correct data from Cassandra and assigning it to a Spark
    # data frame
    df = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table="harmitus_logs", keyspace="kaa").load()

    # Caching the data frame to memory for faster operations
    df.cache()

    # Creating an empty data frame we use in the loop later
    baseDf = pd.DataFrame({}, index=[0])

    def __init__(self):
        pass

    def Getter(self, fStamp, lStamp, avgTemp):

        # Assigning the right endpoint_keyhash
        hallwayEndpoint = 'Nu4W6wHRhTBifmy64ld74EqDWF4='

        # ORDER!
        aDf = self.df.orderBy('timestamp', ascending=True)

        # Filtering the data frame with the previously assigned keyhash
        filterEndpoint = aDf.filter(aDf['endpoint_keyhash'] == hallwayEndpoint)

        # Using the based timestamps to perform filtering
        start = filterEndpoint.filter(filterEndpoint['timestamp'] > fStamp)
        filterDf = start.filter(start['timestamp'] < lStamp)

        # If-statement to check if there is any frustration data
        if filterDf.count() == 0:
            pass
        else:
            # Grouping the data frame based on the specified column
            frustDf = filterDf.groupBy('harmitus_level').count()

            # Calculating the amount of data entries on the time frame
            frustSum = frustDf.groupBy().sum('count').head()[0]

            print('Data entries recorded: {} \n'.format(frustSum))

            lvlDict = {}
            dfDict = {}

            # Looping the grouped data frame
            for v in range(4):
                # filtering the grouped data based on the iteration
                frustLvl = frustDf.filter(frustDf['harmitus_level'] == v)

                if frustLvl.count() == 0:
                    pass
                else:
                    # getting the frustration value and the type of frustration
                    value = frustLvl.head()[1]
                    frustType = frustLvl.head()[0]

                    print("Amount of type {} frustration: {}. \nPercent "
                          "value: {:.1%} \n"
                          .format(frustType, value, value / frustSum))

                    # Values to dicts in order to construct data frames
                    lvlDict['type{}'.format(frustType)] = int(value)
                    dfDict[str(frustType)] = [value]

            # Assigning the passed average temperature value to one of the dicts
            dfDict['tmp'] = avgTemp

            # Making a data frame from the previous dict
            df1 = pd.DataFrame(dfDict, index=[fStamp])

            # Concacting the two data frames
            midDf = [df1, self.baseDf]
            self.baseDf = pd.concat(midDf)

            # Making a new dict with proper formatting in order to be sent to
            # Thingsboard
            frustDict = {'ts': fStamp * 1000, 'values': lvlDict}

            tempJson = json.dumps(frustDict)

            k = json.loads(tempJson)

            r = requests.post(
                'http://-thingsboard_ip:port-/api/v1/-accestoken-/'
                'telemetry',
                data=json.dumps(k))

            print(r)
            print('\n')

        # Returning the data frame for local storage in cassandraGetter.py
        return self.baseDf
