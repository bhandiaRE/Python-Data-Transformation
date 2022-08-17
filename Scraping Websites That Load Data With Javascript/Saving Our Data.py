from bs4 import BeautifulSoup
import time,os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec

options = webdriver.ChromeOptions()
options.add_argument("headless")
driver = webdriver.Chrome(options=(options))
time.sleep(3)
driver.get('https://www.internetlivestats.com/')


page_source = driver.page_source
soup = BeautifulSoup(page_source, 'lxml')

#driver.implicitly_wait(10)

timeout = 10
wait = WebDriverWait(driver, timeout)
waittill= "/html/body/div[3]/div/div[2]/div/div[2]/a"#Will wait till this shows and then search zpath (WORKS WITH LOADING DATA)
zpath = "/html/body/div[3]/div/div[2]/div/div[1]"
try:
    wait.until(ec.visibility_of_element_located((By.XPATH,waittill)))
    number = driver.find_elements_by_xpath(zpath)
   #print(number[0].text)
except Exception as e:
    print(e)
    print("Element not visible") 
    
Save= 10
while True:
    try:
        number = driver.find_elements_by_xpath(zpath)
        print(number[0].text)
        savedata = ""
        for val in number[0].text:
            savedata += ""+val
        if os.path.isfile("savefile.txt"):
            with open("savefile.txt","a") as out:
                out.write(savedata)
                out.write("\n")
        else:
            with open("savefile.txt","w") as out:
                out.write("Total number of Websites\n")
                out.write(savedata)
                out.write("\n")
    except:
        pass
    time.sleep(5)
   

driver.quit()
