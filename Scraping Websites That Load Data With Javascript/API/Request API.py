import json 
import requests

url = 'https://quotes.toscrape.com/api/quotes?page=1'

r = requests.get(url)

person_list = []

data = json.loads(r.text)
for i in range(2,5):
    name = (data['quotes'][i]['author']['name'])
    tags = (data['quotes'][i]['tags'][0])
    text = (data['quotes'][i]['text'])
    
    quotes = {
    'Author': name,
    'tags': tags,
    'text': text
    }
    
    person_list.append(quotes)
    
print(person_list)
    