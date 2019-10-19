#!/opt/envs/luft/bin/python3

"""
This script will collect all measurements for a sensor. Available measurements are on
https://www.madavi.de/sensor/csvfiles.php?sensor=sensor_id

The assumption is that all previous months are available in zip files. Current month measurement data is in csv files.
"""

import argparse
from bs4 import BeautifulSoup
import requests
from lib import my_env
from lib.luft_store import *


# Configure command line arguments
parser = argparse.ArgumentParser(
    description="Get measurements for sensor."
)
parser.add_argument('-s', '--sensor', type=str, default='esp8266-72077',
                    help='Please provide the sensor id (as used on https://www.madavi.de/sensor/csvfiles.php?sensor=)')
args = parser.parse_args()
cfg = my_env.init_env("luftdaten", __file__)
logging.info("Arguments: {a}".format(a=args))

sensor = args.sensor
url_base = os.getenv("URL_BASE")
url = "{base}csvfiles.php?sensor={sensor}".format(base=url_base, sensor=args.sensor)
logging.info("URL: {url}".format(url=url))

# Collect and process measurements.
res = requests.get(url)
(fp, filename) = os.path.split(__file__)
if res.status_code == 200:
    soup = BeautifulSoup(res.content)
    all_refs = soup.find_all('a')
    links = [link.attrs['href'] for link in all_refs]
    zips = [link for link in links if '.zip' in link]
    for url in zips:
        my_env.run_script(fp, 'measurement4zip.py', '-s', sensor, '-u', url)
    my_env.run_script(fp, 'measurement4sensor.py', '-s', sensor)

else:
    logging.info("Extract for sensor not successful, return code: {}".format(res.status_code))
logging.info("End application")
