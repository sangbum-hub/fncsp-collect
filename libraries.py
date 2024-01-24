### 라이브러리 ############################################################################################

# 데이터베이스
import pymysql
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, A, Q

# ETC
import numpy as np
import pandas as pd
import time, datetime
from dateutil.relativedelta import relativedelta
from tqdm import tqdm
import copy, itertools, joblib, json, math, operator, os, re, random, types, unicodedata, requests, xmltodict

# OPEN API
import urllib.request, urllib.parse
from urllib.request import Request, urlopen
from urllib.parse import urlencode, quote_plus, unquote
import xml.etree.ElementTree as ET

# WEB CRAWLING
from fake_useragent import UserAgent  # pip install fake-useragent
from bs4 import BeautifulSoup as bs
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.common.keys import Keys

# ERROR LOG
import socket, platform

### 전역변수 ############################################################################################
# 엘라스틱서치 접속정보
es_host = "61.78.63.51"
# es_host = "192.168.120.159"
es_port = 9200
es_http_auth = ("sycns", "rltnfdusrnth")
es_timeout = 36000
es_max_retries = 3
es_retry_on_timeout = True

# 데이터 스크롤 options
es_scroll = "60m"
es_scroll_size = 10000
es_scroll_timeout = "60m"

# mysql 접속정보
my_host = "61.78.63.52"
# my_host = "192.168.120.160"
my_user = "nia"
my_passwd = "nia123!!"
my_database = "nia"
my_connect_timeout = 36000

WEBDRIVER_PATH = f"{os.getcwd()}/Collector/chromedriver.exe"
WEBDRIVER_OPTIONS = webdriver.ChromeOptions()
# WEBDRIVER_OPTIONS.add_argument('headless')
# WEBDRIVER_OPTIONS.add_argument('--disable-dev-shm-usage')
# WEBDRIVER_OPTIONS.add_argument('--no-sandbox')
# WEBDRIVER_OPTIONS.add_argument('--ignore-certificate-errors')

index_name = "test_index"
