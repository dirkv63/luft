"""
This module consolidates Database access for the lkb project.
"""

import logging
import os
import sqlite3
from sqlalchemy import Column, Integer, Text, create_engine, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


Base = declarative_base()


class Measurement(Base):
    """
    Table containing measurements
    """
    __tablename__ = "measurement"
    id = Column(Integer, primary_key=True, autoincrement=True)
    sensor_id = Column(Integer, ForeignKey("sensor.sensor_id"))
    timestamp = Column(Integer, nullable=False)
    p1 = Column(Integer)
    p2 = Column(Integer)


class Sensor(Base):
    """
    Table with Sensor information.
    """
    __tablename__ = "sensor"
    sensor_id = Column(Integer, primary_key=True)
    sensor_type = Column(Text, nullable=False)
    location = Column(Integer)
    lat = Column(Integer)
    lon = Column(Integer)


class DirectConn:
    """
    This class will set up a direct connection to the database. It allows to reset the database,
    in which case the database will be dropped and recreated, including all tables.
    """

    def __init__(self):
        """
        To drop a database in sqlite3, you need to delete the file.
        """
        self.db = os.path.join(os.getenv("DBDIR"), os.getenv("DB"))
        self.dbConn = ""
        self.cur = ""

    def _connect2db(self):
        """
        Internal method to create a database connection and a cursor. This method is called during object
        initialization.
        Note that sqlite connection object does not test the Database connection. If database does not exist, this
        method will not fail. This is expected behaviour, since it will be called to create databases as well.

        :return: Database handle and cursor for the database.
        """
        logging.debug("Creating Datastore object and cursor")
        db_conn = sqlite3.connect(self.db)
        # db_conn.row_factory = sqlite3.Row
        logging.debug("Datastore object and cursor are created")
        return db_conn, db_conn.cursor()

    def rebuild(self):
        # A drop for sqlite is a remove of the file
        try:
            os.remove(self.db)
            logging.info("Database {db} will be recreated".format(db=self.db))
        except FileNotFoundError:
            logging.info("New database {db} will be created".format(db=self.db))
        # Reconnect to the Database
        self.dbConn, self.cur = self._connect2db()
        # Use SQLAlchemy connection to build the database
        conn_string = "sqlite:///{db}".format(db=self.db)
        engine = set_engine(conn_string=conn_string)
        Base.metadata.create_all(engine)


def init_session(echo=False):
    """
    This function configures the connection to the database and returns the session object.

    :param echo: True / False, depending if echo is required. Default: False
    :return: session object.
    """
    db = os.path.join(os.getenv("DBDIR"), os.getenv("DB"))
    conn_string = "sqlite:///{db}".format(db=db)
    engine = set_engine(conn_string, echo)
    session = set_session4engine(engine)
    return session


def set_engine(conn_string, echo=False):
    engine = create_engine(conn_string, echo=echo)
    return engine


def set_session4engine(engine):
    session_class = sessionmaker(bind=engine)
    session = session_class()
    return session
