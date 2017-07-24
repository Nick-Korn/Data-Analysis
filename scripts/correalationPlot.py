#!/usr/bin/python3
"""
correlationPlot.py

Created by: Niko Liimtainen 18.7.2017
Modified by: Niko Liimatinen 19.7.2017
             -||- 20.7.2017
             -||- 21.7.2017
             -||- 24.7.2017

This script is used to make a scatter plot out of data gotten from
frustration correlation script.

Documentation regarding this Script can be found here:
https://cybertrust.labranet.jamk.fi/data-analysis/documentation/wikis/correlation-with-weather--and-frustration-data
"""
from matplotlib.ticker import FormatStrFormatter

import matplotlib.pyplot as plt
import pickle

# Loading the data frame via pickling
pickle_in = open(-localfilepath-,
                 'rb')
wd = pickle.load(pickle_in)

# Dropping the empty data frame that was used in data frame construction
wd.dropna(how='any', inplace=True)

# Filtering out exceptionally big values
wd = wd[wd < 25]

# Plotting the different values to a scatter plot and joining them by the
# first ax
ax1 = wd.plot(kind='scatter', x='tmp', y=['0'], c='r', label='type0')
ax2 = wd.plot(kind='scatter', x='tmp', y=['1'], c='g', ax=ax1, label='type1')
ax3 = wd.plot(kind='scatter', x='tmp', y=['2'], c='b', ax=ax1, label='type2')
ax4 = wd.plot(kind='scatter', x='tmp', y=['3'], c='m', ax=ax1, label='type3')

# Naming axi
plt.xlabel('Temperature')
plt.ylabel('Amount of frustration per hour')

plt.gca().xaxis.set_major_formatter(FormatStrFormatter('%d $^\circ$C'))
plt.gca().yaxis.set_major_formatter(FormatStrFormatter('%d f/h'))

plt.show()
