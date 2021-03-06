from __future__ import print_function

"""
kafkaTest.py
Created by: Niko Liimatainen 14.6.2017
Modified by: Niko Liimatainen 15.6.2017


First attempt at SparkStreaming using Apache Kafka.

Documentation regarding Kafka-spark streaming can be found here:
https://cybertrust.labranet.jamk.fi/data-analysis/documentation/wikis/spark-streaming-with-kafka
"""

import json

from pyspark import SparkContext
from pyspark.sql import Row, SparkSession, streaming
import matplotlib.pyplot as plt
import pandas as pd
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


# the function for converting mapped rdd:s to data frames and the
# showing the resulting data frame w/SQL-query
def process(time, rdd):
    print("========= %s =========" % str(time))
    try:

        dataFrame = spark.createDataFrame(rdd)

        dataFrame.createOrReplaceTempView("data")

        showDataFrame = spark.sql("select * from data")

        liveData = showDataFrame.toPandas()

        print(liveData)

    except:
        pass


# creating streaming context and specifying the refresh time in seconds
sc = SparkContext(appName="kafkaTest")
ssc = StreamingContext(sc, 30)

# SparkSession for spark.sql
spark = SparkSession \
    .builder \
    .getOrCreate()


# defining the address and port of the Kafka server, passing the topic
# argument in order to find the right stream
kafkaStream = KafkaUtils.createDirectStream(ssc, ["test-topic"],
                            {"metadata.broker.list": -ip:port-})



# kafka stream returns tuples so we map the data to a .json format for data
# frame processing
words = kafkaStream.map(lambda line: json.loads(line[1]))



# using the function on the RDD:s acquired from the stream
words.foreachRDD(process)

# tells the program to start streaming
ssc.start()

# telling the program to wait for termination or timeout after specified
# amount of seconds
ssc.awaitTerminationOrTimeout(10000)
