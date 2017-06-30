from __future__ import print_function

"""
kafkaTest.py
Created by: Niko Liimatainen 28.6.2017
Modified by: Niko Liimatainen 29.6.2017
             Niko Liimatainen 30.6.2017

A script that gets live data and compares outside sensor data to the data 
acquired from the office sensors

_WIP_
"""

import json

from pyspark import SparkContext, RDD
from pyspark.sql import Row, SparkSession, streaming, SQLContext
import pandas as pd
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


def process(time, rdd):
    devices = {'officeRasp': 'raspi-o827ro544093',
               'officeRuuvi': 'ruuvitag-E6BCD58A2A52',
               'd425Rasp': 'raspi-o827rossr2qn',
               'd425Ruuvi': 'ruuvitag-F7AFD68CBFB1',
               'hallway': 'raspi-o827ro3qoqqo',
               'staircase': 'raspi-o827ro1884pn'}
    # setting device identification based on their serialNumber

    try:
        print("========= %s =========" % str(time))
        # printing the time of refresh for more streamlined streaming experience

        df = spark.createDataFrame(rdd)
        # creating a data frame from the received

        df.cache()
        # caching the crated data

        officeRuuvi = df.filter(df['serialNumber'] == devices['officeRuuvi'])
        officeRasp = df.filter(df['serialNumber'] == devices['officeRasp'])
        comp425Ruuvi = df.filter(df['serialNumber'] == devices['d425Ruuvi'])
        comp425Rasp = df.filter(df['serialNumber'] == devices['d425Rasp'])
        compHallway = df.filter(df['serialNumber'] == devices['hallway'])
        compStair = df.filter(df['serialNumber'] == devices['staircase'])
        # creating filtered data frames containing only the specific devices
        # data

        comparison(comp425Rasp, officeRuuvi, officeRasp)
        comparison(comp425Ruuvi, officeRuuvi, officeRasp)
        comparison(compHallway, officeRuuvi, officeRasp)
        comparison(compStair, officeRuuvi, officeRasp)
        # calling all functions

        sqlc.clearCahce()
        # clearing the cache
    except:
        pass


def comparison(df, officeRuuvi, officeRasp):

    outerTemp = df.select(df['temp']).head()[0]
    outerHumidity = df.select(df['humidity']).head()[0]
    outerPressure = df.select(df['pressure']).head()[0]
    # getting the outer values for subtraction

    tempDiff = officeRasp.select(officeRasp['date'],
                                 officeRasp['Temp'] - outerTemp)
    humDiff = officeRasp.select(officeRasp['date'],
                                officeRasp['Humidity'] - outerHumidity)
    pressDiff = officeRasp.select(officeRasp['date'],
                                  officeRasp['Pressure'] - outerPressure)
    tempDiff2 = officeRuuvi.select(officeRuuvi['date'],
                                   officeRuuvi['Temp'] - outerTemp)
    humDiff2 = officeRuuvi.select(officeRuuvi['date'],
                                  officeRuuvi['Humidity'] - outerHumidity)
    pressDiff2 = officeRuuvi.select(officeRuuvi['date'],
                                    officeRuuvi['Pressure'] - outerPressure)
    # subtracting outer from office values to get differences

    placeHolder = tempDiff.join(humDiff, on='date')

    placeHolder2 = placeHolder.join(pressDiff, on='date')

    pandasHolder = placeHolder2.toPandas()

    tmp = tempDiff2.join(humDiff2, on='date')

    tmp2 = tmp.join(pressDiff2, on='date')

    tmpPandas = tmp2.toPandas()
    # joining the values into two separate data frames and then convertting
    # them into pandas data frames

    jsonHolder = pandasHolder.to_json(orient='records')
    jsonTmp = tmpPandas.to_json(orient='records')
    # convert pandas data frames to .json format

    dataFile = open('/media/k8908/ESD-USB/CF2017/Python/Data'
                    '/live_data.json', 'a')

    dataFile.write(jsonHolder + '\n' + jsonTmp + '\n')

    dataFile.close()
    # appending to a local file


sc = SparkContext(appName="kafkaTest")
ssc = StreamingContext(sc, 30)
# creating streaming context and specifying the refresh time in seconds


spark = SparkSession \
    .builder \
    .getOrCreate()
# SparkSession for spark.sql

sqlc = SQLContext(spark)

# prototyping SQLContext for use with cahce clearing

kafkaStream = KafkaUtils.createDirectStream(ssc, ["test-topic"],
                            {"metadata.broker.list": "192.168.51.140:9092"})

# defining the address and port of the Kafka server, passing the topic
# argument in order to find the right stream


words = kafkaStream.map(lambda line: json.loads(line[1]))

# kafka stream returns tuples so we map the data to a .json format for data
# frame processing


words.foreachRDD(process)
# using the function on the RDD:s acquired from the stream


ssc.start()
# tells the program to start streaming

ssc.awaitTerminationOrTimeout(10000)
# telling the program to wait for termination or timeout after specified
# amount of seconds

# TODO: renaming the columns for device identification purposes

# TODO: sending the data back to Cassandra
