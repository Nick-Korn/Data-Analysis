#!/usr/bin/Python3
"""
pandasAnalysis.py

Created by: Niko Liimatainen 19.7.2017
Modified by: Niko Liimtainen 20.7.2017

This script tries to do the same stuff we've done with Spark but with Pandas
and Matplotlib.
"""

import pickle
import pandas as pd
import matplotlib.pyplot as plt

from matplotlib import style
# TQDM for stylish load bars
from tqdm import tqdm
from time import localtime, strftime

# Specifying the visual style for Matplotlib
style.use('ggplot')

# Loading Pickled data in as a data frame and the dropping na values and
# sorting properly
pickel_in = open(-localfilepath-, 'rb')
df = pickle.load(pickel_in)
df.dropna(how='any', inplace=True)
sortDf = df.sort_values(by='ts', ascending=False)

# Getting the first and latest timestamps to serve as filtering points
minTs = sortDf.min()[0]
maxTs = sortDf.max()[0]

# One day in Epoch for looping day by day. As we acquired the data from
# Cassandra data base built around Cassandra, the timestamps are in milliseconds
epochDay = 86400000

# Calculating how many days worth of data the data frame contains
days = (maxTs - minTs) / epochDay

finalDf = pd.DataFrame({}, index=[0])
datDict = {}
v = 1

# The loop for going through the data frame
for _ in tqdm(range(0, int(days))):
    # Assigning the next hour from start timestamp
    nxtStamp = minTs + epochDay
    # Filtering based on the two timestamps provided
    filteredDf = df.loc[(sortDf['ts'] >= minTs) & (sortDf['ts'] <= nxtStamp)]
    # Converting epoch to localtime
    start = strftime('%d.%m.%Y', localtime(minTs / 1000))
    # Setting new first timestamp for next loop
    minTs = nxtStamp
    # Getting the average of the values
    avg = filteredDf['value'].mean()
    # Assigning values to a dict and then turning that dict to a data frame
    datDict['date'] = start
    datDict['avg_value'] = avg
    df2 = pd.DataFrame(datDict, index=[v])
    # Joining the current and previous dicts
    midDf = [df2, finalDf]
    df2 = pd.concat(midDf)
    v += 1
    # Assigning the current dict for next loop
    finalDf = df2

# Dropping na data
finalDf.dropna(how='any', inplace=True)

# Making a bar chart out of the values
finalDf.plot(kind='bar', x='date', y='avg_value', color='m')

# Renaming labels and naming the diagram
plt.xlabel('Date')
plt.ylabel('Windspeed')
plt.title('Pandas  \n ')

# Invert x-axis to get the dates the right way around
ax = plt.gca()
ax.invert_xaxis()

# Showing the diagram
plt.show()
