import requests
import json

url = 'https://locator.sasol.com/api/station.json'

r = requests.get(url)

sampleDict = json.loads(r.text)
print("Checking if nested JSON key exists or not")

if 'stations' in sampleDict:
  for sub in sampleDict['stations']:
    if 'station_address' in sub:
      print(sub['station_address'])
