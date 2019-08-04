"""
This script will collect today's measurement file for the sensor.
New information will be added to the database. New information is more recent than latest timestamp in the database.
The data is on location https://www.madavi.de/sensor/data_csv/csv-files/2019-07-27/data-esp8266-72077-2019-07-27.csv
"""

import argparse
import pandas
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
parser.add_argument('-s', '--sensor', type=str, default='esp8266-72077',
                    help='Please provide the sensor id (as used on https://www.madavi.de/sensor/csvfiles.php)')
args = parser.parse_args()
# Todo: Get latest my_env collecting data from .env
cfg = my_env.init_env("luftdaten", __file__)
logging.info("Arguments: {a}".format(a=args))

luft = luft_class.Luft()
url_base = os.getenv("URL_BASE")
ds = datetime.now(pytz.utc).strftime("%Y-%m-%d")
url = "{base}/{ds}/data-{sensor}-{ds}.csv".format(base=url_base, sensor=args.sensor, ds=ds)
logging.info("URL: {url}".format(url=url))

max_ts = luft.latest_measurement(args.sensor)
res = requests.get(url)
rec_cnt = 0
if res.status_code == 200:
    # Convert csv file into Pandas dataframe.
    df = pandas.read_csv(BytesIO(res.content), delimiter=";")
    luft.store_measurements(args.sensor, df, max_ts)
else:
    logging.info("Extract for sensor not successful, return code: {}".format(res.status_code))
logging.info("End application")
