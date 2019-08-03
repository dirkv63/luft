"""
This script will collect the current measurements from particle/temperature file.
File is from URL_BASE https://www.madavi.de/sensor/csvfiles.php
Start Date is first day of current month. Previous month information is compressed per month.

The script will collect measurement information per sensor and for the additional measurements. Therefore the date/time
of the most recent measurement for the sensor is calculated, then files for last measurement until today are collected
and measurements are added to the database. The assumption is that measurements are available in strict FIFO sequence.
Once a measurement on specific date/time is available, then it is never required to check before this time.
"""

import pandas
import requests
from datetime import date, timedelta
from lib import my_env
from lib import luft_store
from lib.luft_store import *
from pandas.compat import BytesIO

start_date = date.today().replace(day=1)
sensor_ids = ["esp8266-72077"]
cfg = my_env.init_env("luftdaten", __file__)
luft_eng = luft_store.init_session()
known_sensors = []
url_base = os.getenv("URL_BASE")

for sensor_id in sensor_ids:
    # Get timestamp for most recent measurement
    last_measurement = luft_eng.query(Measurement).filter_by(sensor_id=sensor_id).max()
    for td in reversed(range(1, 4)):
        dfc = date.today() - timedelta(td)
        ds = dfc.strftime("%Y-%m-%d")
        fn = "data-{id}-{ds}.csv".format(ds=ds, id=sensor_id)
        url = "{url_base}/{fn}".format(url_base=url_base, fn=fn)
        logging.info("URL: {url}".format(url=url))
        res = requests.get(url)
        if res.status_code == 200:
            df = pandas.read_csv(BytesIO(res.content), delimiter=";")
            my_loop = my_env.LoopInfo("records from {fn}".format(fn=fn), 100)
            for row in df.iterrows():
                my_loop.info_loop()
                # Get excel row in dict format
                xl = row[1].to_dict()
                measure_inst = Measurement(
                    sensor_id=sensor_id,
                    timestamp=my_env.date2epochv2(xl["Time"]),
                    p1=xl["SDS_P1"],
                    p2=xl["SDS_P2"],
                    temperature=xl["Temp"],
                    humidity=xl["Humidity"],
                    signal=xl["Signal"]
                )
                luft_eng.add(measure_inst)
            my_loop.end_loop()
            luft_eng.commit()
        else:
            logging.info("Extract {fn} not available.".format(fn=fn))
logging.info("End application")
