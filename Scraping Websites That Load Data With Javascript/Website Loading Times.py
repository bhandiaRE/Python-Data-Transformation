from bs4 import BeautifulSoup
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec

driver = webdriver.Chrome()
time.sleep(3)
driver.get('https://webscraper.io/test-sites/tables')


page_source = driver.page_source
soup = BeautifulSoup(page_source, 'lxml')

#driver.implicitly_wait(10)

timeout = 15
wait = WebDriverWait(driver, timeout)
waittill= "/html/body/div[1]/div[3]/h2[1]"#Will wait till the element shows and then execute xpath (WORKS WITH LOADING DATA)
xpath = "//*[@id='layout-footer']/div/div[1]/div[1]/ul/li[2]/a"
try:
    wait.until(ec.visibility_of_element_located((By.XPATH,waittill)))
    link = driver.find_elements_by_xpath(xpath)
    print(link[0].text)
except Exception as e:
    print(e)
    print("Element not visible")

driver.quit()
