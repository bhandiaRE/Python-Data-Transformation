import json 

with open('personal.json') as json_file:
    data = json.load(json_file)
    
author = (data['quotes'][0]['author']['goodreads_link'])
tags = (data['quotes'][0]['tags'][0])
text = (data['quotes'][0]['text'])
print(data['top_ten_tags'])

quotes = {
    'Author': author,
    'tags': tags,
    'text': text
    }

print(quotes)