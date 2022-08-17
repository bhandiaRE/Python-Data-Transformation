#import requests
from bs4 import BeautifulSoup
import time
from selenium import webdriver

driver = webdriver.Chrome()
time.sleep(5)
driver.get('https://webscraper.io/test-sites/tables')
time.sleep(2)

page_source = driver.page_source
soup = BeautifulSoup(page_source, 'lxml')

for i in range(0,2):
    xpath = "//*[@id='layout-footer']/div/div[1]/div[1]/ul/li['(i)']/a"
    link = driver.find_elements_by_xpath(xpath)
    print(link[i].get_attribute("href"))

time.sleep(1)

driver.quit()
