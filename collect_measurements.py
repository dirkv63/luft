"""
This script will collect the current measurement.
File is from URL_BASE https://www.madavi.de/sensor/csvfiles.php
"""

import pandas
import requests
from datetime import date, timedelta
from lib import my_env
from lib import luft_store
from lib.luft_store import *
from pandas.compat import BytesIO
from sqlalchemy.orm.exc import NoResultFound


sensor_ids = ["esp8266-72077"]
cfg = my_env.init_env("luftdaten", __file__)
luft_eng = luft_store.init_session()
known_sensors = []
url_base = os.getenv("URL_BASE")
for td in range(1, 4):
    for sensor_id in sensor_ids:
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
