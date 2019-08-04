"""
This module consolidates methods (and classes) related to the Luft application.
"""

from lib import luft_store
from lib.luft_store import *
from lib import my_env
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql import func


class Luft:
    def __init__(self):
        self.db_eng = luft_store.init_session()

    def latest_measurement(self, sensor_id):
        """
        This method gets the timestamp of the latest record for the sensor in the measurement table.

        :param sensor_id: Sensor ID
        :return: Timestamp of the latest measurement (epoch) or 0 if no measurements.
        """
        try:
            ts_rec = self.db_eng.query(func.max(Measurement.timestamp).label('max')).filter_by(
                sensor_id=sensor_id).one()
            max_ts = ts_rec.max
            return max_ts
        except NoResultFound:
            return 0

    def store_measurements(self, sensor_id, measures, max_ts=0):
        """
        This method gets measurement information from a sensor and stores the information in the database. If timestamp
        is provided, then only measurements more recent than the timestamp will be stored.

        :param sensor_id: ID of the sensor for which measurements need to be stored.
        :param measures: Measurements in Pandas dataframe format.
        :param max_ts: If specified then only measurements more recent than timestamp will be added.
        :return:
        """
        my_loop = my_env.LoopInfo("records from {fn}".format(fn=sensor_id), 100)
        rec_cnt = 0
        for row in measures.iterrows():
            my_loop.info_loop()
            # Get excel/csv row in dict format
            xl = row[1].to_dict()
            timestamp = my_env.date2epoch(xl["Time"])
            if timestamp > max_ts:
                rec_cnt += 1
                measure_inst = Measurement(
                    sensor_id=sensor_id,
                    timestamp=timestamp,
                    p1=xl["SDS_P1"],
                    p2=xl["SDS_P2"],
                    temperature=xl["Temp"],
                    humidity=xl["Humidity"],
                    signal=xl["Signal"],
                    samples=xl["Samples"],
                    min_cycle=xl["Min_cycle"],
                    max_cycle=xl["Max_cycle"]
                )
                self.db_eng.add(measure_inst)
        total = my_loop.end_loop()
        self.db_eng.commit()
        logging.info("{} records have been added.".format(rec_cnt))
        logging.info("{} records have been processed".format(total))
        return
