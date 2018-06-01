"""
This procedure will rebuild the sqlite lkb database
"""

import logging
from lib import my_env
from lib import luft_store

cfg = my_env.init_env("luftdaten", __file__)
logging.info("Start application")
luft = luft_store.DirectConn(cfg)
luft.rebuild()
logging.info("sqlite database luft rebuild")
logging.info("End application")
