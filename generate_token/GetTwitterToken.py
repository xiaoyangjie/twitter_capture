# coding=utf-8
import json
import re
import urlparse

import pymongo
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
import httplib2
import oauth2 as oauth
import time
import requests
from httplib2 import socks
from bs4 import BeautifulSoup

PROXY = '222.197.180.150:8118'

for i in asdas:
    username = 'xiaoyangjie3'
    password = 'yj68034201'
    consumer_key =  "zAiATzIp3IqK5mjJQuo2aUolE"
    consumer_secret =  "OR9J3rsF7iKBPAAiRcUhQDb25muByOjANrvoiWuf0NSjryBmzE"
    request_token_url = 'https://api.twitter.com/oauth/request_token'
    access_token_url = 'https://api.twitter.com/oauth/access_token'
    authorize_url = 'https://api.twitter.com/oauth/authorize'
    proxies = {'https:':'http://192.168.1.66:8111',
               'http:': 'http://192.168.1.66:8111'}
    proxy_info = httplib2.ProxyInfo(socks.PROXY_TYPE_HTTP, "192.168.1.66", 8111)
    # cli = pymongo.MongoClient('mongodb://mongo:123456@222.197.180.150')['']['']
    # for i in cli.
    consumer = oauth.Consumer(key=consumer_key,secret=consumer_secret)
    client = oauth.Client(consumer,proxy_info=proxy_info)
    resp,content = client.request(request_token_url,'GET')
    if resp['status'] == '200':
        request_token = dict(urlparse.parse_qsl(content))
        print request_token
        ouath_url = authorize_url + '?oauth_token=' + request_token['oauth_token']
        print ouath_url

        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('--proxy-server=%s' % PROXY)
        driver = webdriver.Chrome(chrome_options=chrome_options)
        driver.get('https://www.twitter.com')
        WebDriverWait(driver, 180).until(EC.presence_of_element_located((By.CLASS_NAME, "front-bg")))
        driver.find_element_by_id('signin-email').send_keys(username)
        driver.find_element_by_id('signin-password').send_keys(password)
        driver.find_element_by_css_selector('button.submit.btn.primary-btn.flex-table-btn.js-submit').send_keys(Keys.ENTER)
        driver.get(ouath_url)
        WebDriverWait(driver, 180).until(EC.presence_of_element_located((By.CLASS_NAME, "auth")))
        driver.find_element_by_id('allow').send_keys(Keys.ENTER)
        WebDriverWait(driver, 180).until(EC.presence_of_element_located((By.CLASS_NAME, "main")))
        driver.find_element_by_name('consumer_key').send_keys(consumer_key)
        driver.find_element_by_name('consumer_secret').send_keys(consumer_secret)
        driver.find_element_by_class_name('button').send_keys(Keys.ENTER)
        WebDriverWait(driver, 180).until(EC.presence_of_element_located((By.CLASS_NAME, "auth")))
        driver.find_element_by_id('allow').send_keys(Keys.ENTER)
        WebDriverWait(driver, 180).until(EC.presence_of_element_located((By.CLASS_NAME, "main")))
        print driver.page_source
        r = driver.page_source
        access_token = re.search(r'access_token: (.*?)\n', r).group(1)
        access_secret = re.search(r'access_secret: (.*?)\n', r).group(1)
        driver.quit()







    ####################认证的时候要我重定向登陆，蛋疼################################
    # resp, content = client.request(ouath_url,'GET')
    # content = BeautifulSoup(content,"html.parser")
    # value = content.find_all(attrs={"name": "authenticity_token"})[0]['value']
    # print value
    # body = 'authenticity_token=%s&username_or_email=%s&password=%s' %(value,username,password)
    # print body
    # resp, content = client.request(ouath_url, 'POST',body=body)
    # print content
    #########################################################################################
    # data = {'authenticity_token':value,'oauth_token':request_token['oauth_token'],'session[email]':username,'session[password]':password}
    # r = requests.post(authorize_url,data=data,proxies=proxies)
    # print r.content

# url = 'https://api.twitter.com/oauth/request_token?oauth_consumer_key=fO4lXV6LLz5e1Ew6uRDtvXpIQ&oauth_signature_method=HMAC-SHA1&oauth_nonce=%s&oauth_timestamp=%d&oauth_version=1.0oauth_signature=TrKrYdCxmpWdLeZBrifNg5HBGFw' %(oauth_nonce,oauth_timestamp)
