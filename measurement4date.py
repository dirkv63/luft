#!/opt/envs/luft/bin/python3
"""
This script will collect measurement file for the sensor for a specific day.
New information will be added to the database. New information is more recent than latest timestamp in the database.
The data is on location https://www.madavi.de/sensor/data_csv/csv-files/2019-07-27/data-esp8266-72077-2019-07-27.csv
"""

import argparse
import pytz
import requests
from datetime import datetime
from lib import my_env
from lib import luft_class
from lib.luft_store import *
from pandas.compat import BytesIO


# Configure command line arguments
parser = argparse.ArgumentParser(
    description="Get today's measurements for sensor."
)
parser.add_argument('-s', '--sensor', type=str, default='sds011_sensor_13889',
                    help='Please provide the sensor id ')
now = datetime.now(pytz.utc).strftime("%Y-%m-%d")
parser.add_argument('-d', '--date', type=str, default=now,
                    help='UTC Date for which measurements need to be collected and processed (YYYY-MM-DD format).')
args = parser.parse_args()
cfg = my_env.init_env("luftdaten", __file__)
logging.info("Arguments: {a}".format(a=args))

# Configure URL
luft = luft_class.Luft()
url_base = os.getenv("URL_BASE")
sensor = args.sensor
ds = args.date
url = f"{url_base}{ds}/{ds}_{sensor}.csv"
logging.debug("URL: {url}".format(url=url))

# Collect and process measurements.
res = requests.get(url)
rec_cnt = 0
if res.status_code == 200:
    # Convert csv file into Pandas dataframe.
    measures = BytesIO(res.content)
    if 'sds011' in sensor:
        luft.store_pm(sensor, measures)
    elif 'dht22' in sensor:
        luft.store_temp(sensor, measures)
else:
    logging.info("Extract for sensor not successful, return code: {}".format(res.status_code))
logging.info("End application")
