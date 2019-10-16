"""
This script gets a zip file and processes all measurement files in it.
"""

import argparse
import requests
import zipfile
from lib import my_env
from lib import luft_class
from lib.luft_store import *
from pandas.compat import BytesIO

# Configure command line arguments
parser = argparse.ArgumentParser(
    description="Get zipfile with measurements for a month."
)
parser.add_argument('-s', '--sensor', type=str, default='esp8266-72077',
                    help='Please provide the sensor id (as used on https://www.madavi.de/sensor/csvfiles.php)')
parser.add_argument('-u', '--url', type=str, default='data_csv/2019/05/data-esp8266-72077-2019-05.zip',
                    help='Please provide the url part to append to https://www.madavi.de/sensor/')
args = parser.parse_args()
cfg = my_env.init_env("luftdaten", __file__)
logging.info("Arguments: {a}".format(a=args))

luft = luft_class.Luft()

url_base = os.getenv("URL_BASE")
url = "{base}{url_add}".format(base=url_base, url_add=args.url)
logging.info("URL: {url}".format(url=url))

# Collect and process measurements.
res = requests.get(url)
if res.status_code == 200:
    zfile = zipfile.ZipFile(BytesIO(res.content))
    for finfo in zfile.infolist():
        logging.info("Handling file {}".format(finfo.filename))
        ifile = zfile.open(finfo)
        luft.store_measurements(args.sensor, ifile)

logging.info("End application")
