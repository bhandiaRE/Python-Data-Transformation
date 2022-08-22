import requests
import json

url = 'https://quotes.toscrape.com/api/quotes?page=1'
link = requests.get(url).json()
print(link)


with open('personal.json','w') as json_file:
    json.dump(link, json_file)