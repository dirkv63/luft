"""
This script will collect a file from the archive.
"""

import pandas
import requests
from datetime import date, timedelta
from lib import my_env
from lib import luft_store
from lib.luft_store import *
from pandas.compat import BytesIO
from sqlalchemy.orm.exc import NoResultFound


def add_sensor(mr):
    """
    This function will verify and add (if required) new sensor information to the sensor table.

    :param mr: Dictionary row from luftdaten with sensor and measurement information.

    :return:
    """
    try:
        sensor_inst = luft_eng.query(Sensor).filter_by(sensor_id=mr["sensor_id"]).one()
    except NoResultFound:
        logging.info("New sensor ID {id}, will be added to the table".format(id=mr["sensor_id"]))
        sensor_inst = Sensor(
            sensor_id=mr["sensor_id"],
            sensor_type=mr["sensor_type"],
            location=mr["location"],
            lat=mr["lat"],
            lon=mr["lon"]
        )
        luft_eng.add(sensor_inst)
        luft_eng.commit()
    else:
        logging.debug("Found existing sensor ID {id}".format(id=sensor_inst.sensor_id))
    return


sensor_ids = [12988, 13191, 13887, 13889]
cfg = my_env.init_env("luftdaten", __file__)
luft_eng = luft_store.init_session(cfg["Main"]["db"])
known_sensors = []
url_base = cfg["Luftdaten"]["archive"]
for td in range(1, 4):
    for sensor_id in sensor_ids:
        dfc = date.today() - timedelta(td)
        ds = dfc.strftime("%Y-%m-%d")
        fn = "{ds}_sds011_sensor_{id}.csv".format(ds=ds, id=sensor_id)
        url = "{url_base}/{ds}/{fn}".format(url_base=url_base, ds=ds, fn=fn)
        logging.info("URL: {url}".format(url=url))
        res = requests.get(url)
        if res.status_code == 200:
            df = pandas.read_csv(BytesIO(res.content), delimiter=";")
            my_loop = my_env.LoopInfo("records from {fn}".format(fn=fn), 100)
            for row in df.iterrows():
                my_loop.info_loop()
                # Get excel row in dict format
                xl = row[1].to_dict()
                if sensor_id not in known_sensors:
                    add_sensor(xl)
                    known_sensors.append(sensor_id)
                measure_inst = Measurement(
                    sensor_id=xl["sensor_id"],
                    timestamp=my_env.date2epoch(xl["timestamp"]),
                    p1=xl["P1"],
                    p2=xl["P2"]
                )
                luft_eng.add(measure_inst)
            my_loop.end_loop()
            luft_eng.commit()
        else:
            logging.info("Extract {fn} not available.".format(fn=fn))
logging.info("End application")
