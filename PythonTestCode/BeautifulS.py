# -*- coding: utf-8 -*-
"""
Spyder Editor
"""

import requests
from bs4 import BeautifulSoup

URL = "http://quotes.toscrape.com/"
response = requests.get(URL)

#t = response.text
#print(t)
#print(response)
soup = BeautifulSoup(response.content, 'lxml')

qoutes = soup.find_all("div", class_="quote")
for x in qoutes:
    print(x.find('span').text)

tags = soup.find_all("div", class_="tags")
for x in tags:
    print(x.text)
    
name = soup.find_all("small", class_="author")
for x in name :
    print(x.text)
