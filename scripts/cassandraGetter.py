#!/usr/bin/Python3
"""
cassandraGetter.py

Created by: Niko Liimatainen 19.7.2017
Modified by: Niko Liimatainen 20.7.2017
             -||- Niko Liimatainen 24.7.2017

This script gets data from Cassandra puts it in a Pandas data frame and then
stores it locally via pickling.
"""

import pandas as pd
import pickle
from tqdm import tqdm

from cassandra.cluster import Cluster

# Specifying the address of the Cassandra cluster
cluster = Cluster([-ip-])

# Creating a connection session to Cassandra
session = cluster.connect('thingsboard')

# Using the created session we query the data0base with CQL to get the
# correct data
rows = session.execute("SELECT * FROM ts_kv_cf WHERE key = 'windspeedms' ALLOW "
                       "FILTERING")

datDict = {}
v = 1
emptyDf = pd.DataFrame({}, index=[0])

# Loop for going through the rows that the previous query returned
for thingsboard_row in tqdm(rows):

    # Getting the desired values and making a dict out of them
    datDict['type'] = thingsboard_row[2]
    datDict['ts'] = thingsboard_row[4]
    datDict['value'] = thingsboard_row[6]

    # Turning the dict into a data frame
    df = pd.DataFrame(datDict, index=[v])

    # Joining with the previous data frame
    midDf = [df, emptyDf]
    df = pd.concat(midDf)
    v += 1
    emptyDf = df

# Pickling to save data locally for visualization purposes
pickle_out = open(-localfilepath-, 'wb')

pickle.dump(emptyDf, pickle_out)

pickle_out.close()
