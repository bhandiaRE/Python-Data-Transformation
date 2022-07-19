import requests
from bs4 import BeautifulSoup
import pandas as pd
import time


def review_count_scrape():
    url = 'https://www.amazon.com/Best-Sellers/zgbs'
    r = requests.get(url)
    soup = BeautifulSoup(r.text, 'lxml')
    print(r.status_code)
    product_total_review = [i.text for i in soup.findAll("span", {"class": "a-size-small"})]
    df = pd.DataFrame(product_total_review, columns=['Reviews'])
    print(df)
    
    #add timer
    time.sleep(20)
    

end_timer = time.time() + 20 
while time.time() < end_timer:
    review_count_scrape()
