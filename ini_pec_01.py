import os, urllib.request, requests, datetime, time, random, ssl, json, codecs, csv
from urllib.request import Request, urlopen
from urllib.request import urlretrieve
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait 
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoAlertPresentException
from selenium.webdriver.chrome.options import Options
import pandas as pd
from pandas import read_csv
from bs4 import BeautifulSoup
from difflib import SequenceMatcher
from threading import Thread
from multiprocessing.dummy import Pool as ThreadPool
from multiprocessing import Pool
from fake_useragent import UserAgent
from mtranslate import translate

class driver:
##    def driv(self):
##        chromedriver = "chromedriver"
##        os.environ["webdriver.chrome.driver"] = chromedriver
##        chrome_options = webdriver.ChromeOptions()
##        prefs = {"profile.default_content_setting_values.notifications" : 2}
##        chrome_options.add_experimental_option("prefs",prefs)
##        driver = webdriver.Chrome(executable_path=chromedriver, chrome_options=chrome_options)
##        return driver

    def driv(self, pr):
        chromedriver = "chromedriver"
        os.environ["webdriver.chrome.driver"] = chromedriver
        chrome_options = webdriver.ChromeOptions()
        prefs = {"profile.default_content_setting_values.notifications" : 2}
        chrome_options.add_experimental_option("prefs",prefs)
        chrome_options.add_argument('--proxy-server=' + pr)
        driver = webdriver.Chrome(executable_path=chromedriver, chrome_options=chrome_options)
        return driver

class inp_page:
    
    def transl(self, text):
        my_text = translate(text, 'en')
        return my_text
        
    def inp_email(self, email):
        url = 'https://www.inipec.gov.it/cerca-pec/-/pecs/companies'
        driver.get(url)
        driver.find_element_by_xpath('//*[@id="_1_WAR_searchpecsportlet_email-address"]').send_keys(email)
##        time.sleep(1)
        element = driver.find_elements_by_css_selector('iframe')[0]
        driver.switch_to_frame(element)
        driver.find_element_by_xpath('//*[@id="recaptcha-anchor"]/div[5]').click()

    def get_captcha(self):
        time.sleep(3)
        driver.switch_to_default_content()
        element = driver.find_elements_by_css_selector('iframe')[1]
        driver.switch_to_frame(element)
        soup = BeautifulSoup(driver.page_source, "lxml")
        box = soup.find('div',{'class':'rc-image-tile-wrapper'})
        url = box.find('img')['src']
        path = 'C:/Users/Donstat_001/Dropbox/Python/03_SmartItalia/ini_pec/'

        destination = path + '111.jpg'
        urlretrieve(url, destination)


        rows = soup.find_all('div',{'class':'rc-imageselect-instructions'})
        for row in rows:
            print(row)
            try:
                end = row.find('span').text
                task = row.text.replace(end, '')
##                task = row.find('strong').text
            except: continue
        return destination, task

    def solve_captcha(self, file, task):
        task = self.transl(task).title()
        print(task)
        print(file)
        api_url = 'http://api.captchasolutions.com/solve'
        data = {'p': 'recaptcha2',
                'captchatext': task,
                'captchafile': file,
                'key': '4756ee53fd85cfb10243f874b1e18d28',
                'secret': 'fa70ea5b',
                'out': 'text'}
##        files={'captcha': open(file, 'rb')}
##        request = requests.post(api_url,files=files,data=data)
        request = requests.post(api_url, data=data)
        print(request)
        answer = request.content
        print(answer)

    def click_captcha(self):
##        time.sleep(3)
        driver.switch_to_default_content()
        element = driver.find_elements_by_css_selector('iframe')[1]
        driver.switch_to_frame(element)
        
        driver.find_elements_by_css_selector('tr')[0].find_elements_by_css_selector('td')[0].click()
        
##        driver.find_element_by_xpath('//*[@id="rc-imageselect"]/div[2]/div[2]')
##        el = driver.find_elements_by_css_selector('tr')
        
##        
##        print(len(el))
##        print(element)

class ip_proxy:    

    def inp_file(self, path):
        with open(path, "r") as ins:
            array = []
            for line in ins: array.append(line.replace('\n',''))
        return array

    def random_proxy(self):
        return random.randint(0, len(self.proxies) - 1)

    def save_ip(self):
        url = 'http://list.didsoft.com/get?email=smartitalia@gmail.com&pass=yxsjxp&pid=http4000&showcountry=no'
        r = urllib.request.urlopen(url).read()
        aa = str(r, 'utf-8-sig').split('\n')
        df = pd.DataFrame(aa, columns=['IP'])
        df.to_csv('input_ip.csv',sep=';', index=False, encoding='utf-8', mode='a', header=True)

##    def proxyes(self):
##        proxies = []
##        path = "input_ip.csv"
##        rows = self.inp_file(path)[1:500]
##        for row in rows:
##            aa = row.split(':')
##            try: proxies.append({'ip': aa[0], 'port': aa[1]})
##            except: continue
##        return proxies

    def proxyes(self):
        ua = UserAgent()
        proxies = []
        proxies_req = Request('https://www.sslproxies.org/')
        proxies_req.add_header('User-Agent', ua.random)
        proxies_doc = urlopen(proxies_req).read().decode('utf8')
        soup = BeautifulSoup(proxies_doc, 'html.parser')
        proxies_table = soup.find(id='proxylisttable')
        for row in proxies_table.tbody.find_all('tr'):
            proxies.append({'ip':   row.find_all('td')[0].string,
                            'port': row.find_all('td')[1].string})
        return proxies


ipp = ip_proxy()
dr = driver()
inp = inp_page()


proxies = ipp.proxyes()
for ii in proxies[:]:
    try:
        pr = ii.get('ip') + ':' + ii.get('port')
        driver = dr.driv(pr)
        email = 'caffe.cavour.xiaye@pec.it'
        inp.inp_email(email)
        aa = inp.get_captcha()

        inp.solve_captcha(aa[0], aa[1])

        
        break
    except:
        driver.quit()
        print('----------------------------------------------------------------')
        continue



##ind = ipp.random_proxy()
##proxy = ipp.proxies[ind]



##df = pd.read_csv('input.txt', header=None, dtype={0: object})
##df.columns = ['A']
##inp_email = df.A.tolist()
##
##dr = driver()
##inp = inp_page()
##
##driver = dr.driv()
##email = 'caffe.cavour.xiaye@pec.it'
##inp.inp_email(email)
##inp.capcha()


    






######    try:
######        dff = pd.read_csv('last.csv', sep=';', encoding='utf-8-sig')
######        m_list = df['Name'].tolist()
######        last_name = dff['name'].tolist()[0]
######        start = m_list.index(dff.name.tolist()[0])
######        last = int(dff.page.tolist()[0])
######    except:
######        start = 0
######        last = 1
######        last_name = ''
######        
####
####    
######    log_pasw(driver)
######    time.sleep(2)
######    driver.find_element_by_xpath('//*[@id="_3_WAR_ricercaimpreseportlet__ricerca-gratuita"]').click()        
######    for index, row in df[start:].iterrows():
######        vat = row['VAT']
######        name = row['Name']
######        cap = row['CAP']
######        city = row['City']
######        addr = row['Address and number']
######        prov = str(row['Province'])
######        
######        if prov == 'nan': prov = 'NA'
######        time.sleep(3)
######        driver.find_element_by_xpath('//*[@id="inputSearchField"]').clear()
######        time.sleep(2)
######        driver.find_element_by_xpath('//*[@id="inputSearchField"]').send_keys(name)
######        time.sleep(2)
######        driver.find_element_by_xpath('//*[@id="inputSearchField"]').send_keys(Keys.ENTER)
######        get_table(driver, name, vat, cap, addr, prov, last, city, last_name)
######  
##########    driver.quit()
####    
######if __name__ == '__main__':
######    main()
####

    
