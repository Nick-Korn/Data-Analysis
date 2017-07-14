from __future__ import print_function

"""
kafkaTest.py
Created by: Niko Liimatainen 28.6.2017
Modified by: Niko Liimatainen 29.6.2017
             Niko Liimatainen 30.6.2017
             -||- 3.7.2017
             -||- 4.7.2017
             -||- 5.7.2017
             -||- 6.7.2017

A script that gets live data and compares outside sensor data to the data
acquired from the office sensors

Documentation on this script can be found here:
https://cybertrust.labranet.jamk.fi/data-analysis/documentation/wikis/spark-streaming-with-kafka
"""

import json
import requests
import pandas as pd

from pyspark import SparkContext, RDD
from pyspark.sql import Row, SparkSession, streaming, SQLContext
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
        # printing the time when data is processed  for more streamlined streaming experience

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
    if df.count() != 0:
        # this if statment enables the script to work better with fewer data sources
        device = df.select(df['serialNumber']).head()[0]

        if device == 'raspi-o827rossr2qn':
            diffDevice = 'd425Ruuvi'

        elif device == 'ruuvitag-F7AFD68CBFB1':
            diffDevice = 'd425Rasp'

        elif device == 'raspi-o827ro3qoqqo':
            diffDevice = 'hallway'

        elif device == 'raspi-o827ro1884pn':
            diffDevice = 'staircase'

        # if statments for identifying the current device being opertaed
        # and assigning it's name for id purposes

        outerTemp = df.select(df['temp']).head()[0]
        outerHumidity = df.select(df['humidity']).head()[0]
        outerPressure = df.select(df['pressure']).head()[0]
        # getting the external values for subtraction

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
        # subtracting external values from office values to get differences

        placeHolder = tempDiff.join(humDiff, on='date')

        placeHolder2 = placeHolder.join(pressDiff, on='date')

        pandasHolder = placeHolder2.toPandas()

        tmp = tempDiff2.join(humDiff2, on='date')

        tmp2 = tmp.join(pressDiff2, on='date')

        tmpPandas = tmp2.toPandas()

        # joining the values into two separate data frames and then converting
        # them into pandas data frames

        tmpPandas.columns = ['Date', 'RuuviTemp - ' + diffDevice,
                             'RuuviHumidity - ' + diffDevice,
                             'RuuviPressure - ' +
                             diffDevice]
        pandasHolder.columns = ['Date', 'RaspTemp - ' + diffDevice,
                                'RaspHumidity - ' + diffDevice,
                                'RaspPressure - '
                                + diffDevice]

        # renaming columns for better identification purposes in database

        tmpPandas.set_index('Date', inplace=True)
        pandasHolder.set_index('Date', inplace=True)

        jsonHolder = pandasHolder.to_json(orient='records')
        jsonTmp = tmpPandas.to_json(orient='records')

        k = json.loads(jsonHolder)[0]
        l = json.loads(jsonTmp)[0]

        # convert pandas data frames to proper .json formatting

        r = requests.post(
            'http://-ip:port-/api/v1/-apikey-/telemetry',
            data=json.dumps(k))

        r2 = requests.post(
            'http://-ip:port-/api/v1/-apikey-/telemetry',
            data=json.dumps(l))

        # dumping the data to the cassandra server via an api-key acquired 
	# from thingsboard

        print(r, r2)

    else:
        pass


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
                                            {"metadata.broker.list":
                                                    ip-port})


# defining the address and port of the Kafka server, passing the topic
# argument in order to find the right stream


words = kafkaStream.map(lambda line: json.loads(line[1]))

# kafka stream returns tuples so we map the data to a .json format for data
# frame processing


words.foreachRDD(process)
# using the function on the RDD:s acquired from the stream


ssc.start()
# tells the program to start streaming

ssc.awaitTermination()
# telling the program to wait for user termination

# TODO: figure out a way for the script to work with only one office data source
