# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
import time
from selenium import webdriver

driver = webdriver.Chrome()
time.sleep(10)

def login():
    driver.get('https://www.superdatascience.com/signup')
    time.sleep(1)
    driver.find_element_by_name('email').send_keys("EMAIL")#Your Email will be entered
    driver.find_element_by_name('password').send_keys("123456")#Password will be enter
    driver.find_element_by_xpath('//*[@id="signup"]/button').click()
    
login()
time.sleep(10)

driver.quit()
