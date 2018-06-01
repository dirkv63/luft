"""
This script will get the archive index file and process new information
"""

import logging
import requests
from bs4 import BeautifulSoup
from lib import my_env

cfg = my_env.init_env("luftdaten", __file__)
url_base = cfg["Luftdaten"]["archive"]
res = requests.get(url_base)
soup = BeautifulSoup(res.content, 'html.parser')
print(soup)
refs = soup.find_all("a")
for ref in refs:
    print(ref)
logging.info("End application")
