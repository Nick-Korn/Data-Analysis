from __future__ import print_function

"""
sparkDemo.py

Created by: Niko Liimatainen 2.6.2017
Modified by: Niko Liimatainen 3.6.2017
             Niko Liimatainen 6.6.2017
             Niko Liimatainen 15.6.2017



A first attempt at analysing a .json file with spark and trying to make 'smart'
deductions based on it.
WARNING: This is a first attempt so it might be extremely sucky.

Documentation on thi demo can be found here:
https://cybertrust.labranet.jamk.fi/data-analysis/documentation/wikis/spark-demo
"""
import re
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType


def regex_filter(x):
    # A regex filter to find the temperature values of a month. Could be
    # implemented better
    regexs = [r'2015-05-\d\d\w\d\d:\d\d:\d\d\w']

    if x and x.strip():
        for r in regexs:
            if re.match(r, x, re.IGNORECASE):
                return True

    return False

# Spark conf in order to run the  demo locally
conf = SparkConf().setMaster("local[8]")

spark = SparkSession \
    .builder \
    .appName("sparkDemo") \
    .config(conf=conf) \
    .getOrCreate()

# importing the local file and putting it into a data frame
df = spark.read.json("-localfilepath-")


print('\n\n----------Weather data measured at Jyvaskyla Airport----------\n\n')

print('Amount of observations: \n' + str(df.count()) + '\n\n')

print('Types of data and the amount of times they appear in the database: ')

df.groupBy("type").count().show()

# we divide by the amount of different kinds of observation types to find out
#  the days of observation
print('\nDays of observation: \n' + str(df.count() / 5) + '\n')

first = df.first()  # quite straightforward way of getting the first time stamp

# the method to find the last time stamp
lastRow = df.collect()
last = lastRow[df.count() - 1]

print('Time frame of the observations: ' + first['Time'] + ' - ' +
      last['Time'] + '\n')

print('Days with temperature maximum of over +20 celsius and the specific: ')

tmaxValues = df.filter(df['type'] == 'tmax')  # proper filtering to get the
# desired data
maxCount = tmaxValues.filter(df['value'] > 20).count()  # we do .count() to
# get all the dates to the output
tmaxValues.filter(df['value'] > 20).show(maxCount)

print('Days with temperature minimum value of under -20 celsius '
      'and the specific values: ')

# same comments from the one above apply here
tminValues = df.filter(df['type'] == 'tmin')
minCount = tminValues.filter(df['value'] < -20).count()
tminValues.filter(df['value'] < -20).show(minCount)


print('Daily temperature averages: ')

tDayValue = df.filter(df['type'] == 'tday')  # proper filtering to get the
# desired values
tDayValue.show()  # we could add a value here instead of the default (20)

print('Average temperature of may: ')

filter_udf = udf(regex_filter, BooleanType())
df_filtered = tDayValue.filter(filter_udf(tDayValue['time']))  # usage of the
#  regex filtering method defined at he beginning
df_filtered.groupBy().avg('value').show()  # method for counting the average
# temperature


print('Temperature average of the year: ')

tDayValue.groupBy().avg('value').show()  # method for getting the average

print('Days when precipitation happened, ie. when something rained or snowed: ')

# proper filtering
rrDays = df.filter(df['type'] == 'rrday')
rrDayCount = rrDays.filter(df['value'] >= 0).count()
rrDays.filter(df['value'] >= 0).show(rrDayCount)


print('Days when there was snow on the ground: ')

# proper filtering
snowDays = df.filter(df['type'] == 'snow')
snowCount = snowDays.filter(df['value'] >= 0).count()
snowDays.filter(df['value'] >= 0).show(snowCount)

maxCounts = tmaxValues.filter(df['value'] > 20)
minCounts = tminValues.filter(df['value'] < -20)
rainDays = rrDays.filter(df['value'] >= 0)

# convert data frames to temporary tables in order to run SQL-queries in
# Zeppelin
snowDays.registerTempTable('snow_days')
maxCounts.registerTempTable('max_temps')
minCounts.registerTempTable('min_temps')
rainDays.registerTempTable('rain_days')

