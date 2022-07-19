# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
import time
from selenium import webdriver
from bs4 import BeautifulSoup
import pandas as pd

driver = webdriver.Chrome()
time.sleep(5)
driver.get('https://sdsclub.com/')
time.sleep(5)
    
bton_one=driver.find_element_by_xpath('//*[@id="menu-item-456"]/a').click()
time.sleep(25)

bton_two=driver.find_element_by_class_name('close-icon').click()
time.sleep(5)

bton_two=driver.find_element_by_xpath('//*[@id="category-career"]/div/div[2]/div[4]/div/figure/a/img').click()
time.sleep(25)

bton_two=driver.find_element_by_class_name('close-icon').click()
time.sleep(5)

page_source = driver.page_source
soup = BeautifulSoup(page_source, 'lxml')

beaut_one = [i.text for i in soup.findAll('span',{'class':'desc'})]
beaut_two = [i.text for i in soup.findAll('div',{'class':'single-path-article-content'})]
beaut_three = [i.text for i in soup.findAll('p',{'class':'name'})]

df = pd.DataFrame(beaut_one)
df_two = pd.DataFrame(beaut_two)
df_three = pd.DataFrame(beaut_three)

print(df,df_two,df_three)

time.sleep(10)

driver.quit()


df_clean = df.replace('\n',' ',)
df_clean_two = df_two.replace('\n',' ',)
df_clean_three = df_three.replace('\n',' ',)
clean_stack = pd.concat([df_clean, df_clean_two, df_clean_three], axis=1)
