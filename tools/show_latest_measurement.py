#!/opt/envs/luft/bin/python3
"""
This script shows latest measurement for luft data collection.
"""

import argparse
import datetime
from lib import my_env
from lib import luft_class
from lib.luft_store import *


# Configure command line arguments
parser = argparse.ArgumentParser(
    description="Show date/time for latest measurement for sensor."
)
parser.add_argument('-s', '--sensor', type=str, default='esp8266-72077',
                    help='Please provide the sensor id (as used on https://www.madavi.de/sensor/csvfiles.php)')
args = parser.parse_args()
cfg = my_env.init_env("luftdaten", __file__)
logging.info("Arguments: {a}".format(a=args))

luft = luft_class.Luft()
sensor = args.sensor
max_ts = luft.latest_measurement(sensor)
lm = datetime.datetime.fromtimestamp(max_ts)
print("Latest measurement for {}: {}".format(sensor, lm))

logging.info("End application")
