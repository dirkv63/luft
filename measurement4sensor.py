"""
This script will collect measurement file for a specific sensor. Measurements will be collected since last measurement
in the database. If no measurements in the database, then start day is first of month.
"""

import argparse
import datetime
import pytz
from lib import my_env
from lib import luft_class
from lib.luft_store import *


# Configure command line arguments
parser = argparse.ArgumentParser(
    description="Get measurements for sensor."
)
parser.add_argument('-s', '--sensor', type=str, default='esp8266-72077',
                    help='Please provide the sensor id (as used on https://www.madavi.de/sensor/csvfiles.php)')
args = parser.parse_args()
cfg = my_env.init_env("luftdaten", __file__)
logging.info("Arguments: {a}".format(a=args))

luft = luft_class.Luft()
sensor = args.sensor
max_ts = luft.latest_measurement(sensor)
today = datetime.datetime.now(pytz.utc)
if max_ts == 0:
    # Initialize ds to first day of month
    start = today.replace(day=1)
else:
    start = datetime.datetime.fromtimestamp(max_ts, pytz.utc)

(fp, filename) = os.path.split(__file__)
for day in range(start.day, today.day+1):
    ds = today.replace(day=day).strftime("%Y-%m-%d")
    my_env.run_script(fp, 'measurement4date.py', '-s', sensor, '-d', ds)

logging.info("End application")
