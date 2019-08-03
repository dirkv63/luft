"""
This script will collect today's measurement file for the sensor.
New information will be added to the database. New information is more recent than latest timestamp in the database.
The data is on location https://www.madavi.de/sensor/data_csv/csv-files/2019-07-27/data-esp8266-72077-2019-07-27.csv
"""

import argparse
import pandas
import requests
from datetime import date
from lib import my_env
from lib import luft_store
from lib.luft_store import *
from pandas.compat import BytesIO
from sqlalchemy.sql import func


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
luft_eng = luft_store.init_session()
url_base = os.getenv("URL_BASE")
ds = date.today().strftime("%Y-%m-%d")
url = "{base}/{ds}/data-{sensor}-{ds}.csv".format(base=url_base, sensor=args.sensor, ds=ds)
logging.info("URL: {url}".format(url=url))
max_ts_rec = luft_eng.query(func.max(Measurement.timestamp).label('max')).filter_by(sensor_id='esp8266-72077').one()
max_ts = max_ts_rec.max
print(max_ts)
res = requests.get(url)
if res.status_code == 200:
    # Convert csv file into Pandas dataframe.
    df = pandas.read_csv(BytesIO(res.content), delimiter=";")
    print("Length before: {}".format(len(df.index)))
    df = df[df.Time > max_ts]
    print("Length after: {}".format(len(df.index)))
    my_loop = my_env.LoopInfo("records from {fn}".format(fn=args.sensor), 100)
    for row in df.iterrows():
        my_loop.info_loop()
        # Get excel/csv row in dict format
        xl = row[1].to_dict()
        measure_inst = Measurement(
            sensor_id=args.sensor,
            timestamp=my_env.date2epoch(xl["Time"]),
            p1=xl["SDS_P1"],
            p2=xl["SDS_P2"],
            temperature=xl["Temp"],
            humidity=xl["Humidity"],
            signal=xl["Signal"],
            samples=xl["Samples"],
            min_cycle=xl["Min_cycle"],
            max_cycle=xl["Max_cycle"]
        )
        luft_eng.add(measure_inst)
    my_loop.end_loop()
    luft_eng.commit()
else:
    logging.info("Extract for sensor not successful, return code: {}".format(res.status_code))
logging.info("End application")
