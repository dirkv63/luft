"""
This module consolidates methods (and classes) related to the Luft application.
"""

import pandas
from lib import luft_store
from lib.luft_store import *
from lib import my_env
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
        ts_rec = self.db_eng.query(func.max(Measurement.timestamp).label('max')).filter_by(
            sensor_id=sensor_id).one()
        max_ts = ts_rec.max
        if max_ts:
            return max_ts
        else:
            return 0

    def store_measurements(self, sensor_id, measures):
        """
        This method gets measurement information from a sensor and stores the information in the database. If timestamp
        is provided, then only measurements more recent than the timestamp will be stored.

        :param sensor_id: ID of the sensor for which measurements need to be stored.
        :param measures: Measurements that can be fed to pandas.read_csv command..
        :return:
        """
        columns = ["Time", "durP1", "ratioP1", "P1", "durP2", "ratioP2", "P2", "SDS_P1", "SDS_P2", "PMS_P1", "PMS_P2",
                   "Temp", "Humidity", "BMP_temperature", "BMP_pressure", "BME280_temperature", "BME280_humidity",
                   "BME280_pressure", "Samples", "Min_cycle", "Max_cycle", "Signal", "HPM_P1", "HPM_P2"]
        df = pandas.read_csv(measures, delimiter=";", names=columns, skiprows=1)
        # my_loop = my_env.LoopInfo("records from {fn}".format(fn=sensor_id), 100)
        max_ts = self.latest_measurement(sensor_id)
        rec_cnt = 0
        for row in df.iterrows():
            # my_loop.info_loop()
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
        # total = my_loop.end_loop()
        self.db_eng.commit()
        logging.info("{} records have been added.".format(rec_cnt))
        # logging.info("{} records have been processed".format(total))
        return

    def store_pm(self, sensor_id, measures):
        """
        This method gets particulate matter (pm) measurement information from a sensor and stores the information in
        the database.

        :param sensor_id: ID of the sensor for which measurements need to be stored.
        :param measures: Measurements that can be fed to pandas.read_csv command..
        :return:
        """
        df = pandas.read_csv(measures, delimiter=";")
        max_ts = self.latest_measurement(sensor_id)
        rec_cnt = 0
        for row in df.iterrows():
            # Get excel/csv row in dict format
            xl = row[1].to_dict()
            timestamp = my_env.date2epoch(xl["timestamp"])
            if timestamp > max_ts:
                rec_cnt += 1
                measure_inst = Measurement(
                    sensor_id=sensor_id,
                    timestamp=timestamp,
                    p1=xl["P1"],
                    p2=xl["P2"]
                )
                self.db_eng.add(measure_inst)
        self.db_eng.commit()
        logging.info("{} records have been added.".format(rec_cnt))
        return

    def store_temp(self, sensor_id, measures):
        """
        This method gets temperature measurement information from a sensor and stores the information in
        the database.

        :param sensor_id: ID of the sensor for which measurements need to be stored.
        :param measures: Measurements that can be fed to pandas.read_csv command..
        :return:
        """
        df = pandas.read_csv(measures, delimiter=";")
        max_ts = self.latest_measurement(sensor_id)
        rec_cnt = 0
        for row in df.iterrows():
            # Get excel/csv row in dict format
            xl = row[1].to_dict()
            timestamp = my_env.date2epoch(xl["timestamp"])
            if timestamp > max_ts:
                rec_cnt += 1
                measure_inst = Measurement(
                    sensor_id=sensor_id,
                    timestamp=timestamp,
                    temperature=xl["temperature"],
                    humidity=xl["humidity"],
                )
                self.db_eng.add(measure_inst)
        self.db_eng.commit()
        logging.info("{} records have been added.".format(rec_cnt))
        return
