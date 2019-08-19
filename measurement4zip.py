"""
This script gets a zip file and processes all measurement files in it.
"""

import argparse
import pandas
import pytz
import requests
import zipfile
from datetime import datetime
from lib import my_env
from lib import luft_class
from lib.luft_store import *
from pandas.compat import BytesIO

cfg = my_env.init_env("luftdaten", __file__)
# Configure URL
luft = luft_class.Luft()

sensor = 'esp8266-72077'
measure_file = '/home/dirk/development/python/luft/data/data.zip'
zfile = zipfile.ZipFile(measure_file)
for finfo in zfile.infolist():
    ifile = zfile.open(finfo)
    df = pandas.read_csv(ifile, delimiter=";")
    max_ts = luft.latest_measurement(sensor)
    luft.store_measurements(sensor, df, max_ts)

logging.info("End application")
