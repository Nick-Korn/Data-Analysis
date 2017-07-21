# A script for getting data from Openweathermap.

import requests
import time
import json

url = "http://api.openweathermap.org/data/2.5/weather?id=655195&units=metric&type=like&APPID=d1d8f04e2041d9dcfd2139ca5a6b1258"
r = requests.get(url)
datall = r.json()

alldata = datall.values()

date = time.strftime('%d.%m.%Y %H:%M:%S', time.gmtime(alldata[7]+10800))

ts = alldata[7]

data = datall["main"]
data['ts'] = ts
data['date'] = date

print(data)

thingsDict = {'ts':data['ts']*1000, 'values':{'date': data['date'],
         'humidity': data['humidity'],
         'pressure': data['pressure'],
         'temp': data['temp']}}
print(thingsDict)
r = requests.post(
    'http://192.168.51.176:8080/api/v1/swWXjGcqRjHsB3rTIbXD/telemetry',
     data=json.dumps(thingsDict))

print(r)
