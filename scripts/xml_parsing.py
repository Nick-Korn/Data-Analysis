#!/usr/bin/python3
"""
xml_parser.py

Created:  31.5.2017 by Niko Liimatainen
Modified: 1.6.2017 by Niko Liimatainen
          2.6.2017 by Niko Liimatainen


Simple XML parser and .json converter.

Documentation for use case can be found here:

https://cybertrust.labranet.jamk.fi/data-analysis/documentation/wikis/spark-demo
"""


import xml.etree.ElementTree as etree
import json

root = etree.parse('./Data/weatherDaily').getroot()  # gets the local XML file
# and it's root for parsing

i = 0


dataFile = open('./Data/Weather-Daily_Parsed.json', 'w')

for child in root:  # loop for going through the proper nested lists in the
    # XMl-file
    data = {"Time": root[i][0][1].text,
            "Type": root[i][0][2].text,
            "Value": float(root[i][0][3].text)}
    # setting the data as a dict for better .json compatibility
    i += 1
    dump = json.dumps(data)  # converting data dict to .json dump
    dataFile.write(dump + "\n")  # writing the dump to file with newline


dataFile.close()
