#!/opt/envs/luft/bin/python3
"""
This script will monitor a sensor. If last measurement in the database is older than delta time, a message will be sent.
"""

import argparse
import smtplib
import time
import datetime
from lib import my_env
from lib import luft_class
from lib.luft_store import *
from email.mime.multipart import MIMEMultipartc
from email.mime.text import MIMEText


# Configure command line arguments
parser = argparse.ArgumentParser(
    description="Monitor sensor."
)
parser.add_argument('-s', '--sensor', type=str, default='sds011_sensor_13889',
                    help='Please provide the sensor id (as used on https://www.madavi.de/sensor/csvfiles.php)')
args = parser.parse_args()
cfg = my_env.init_env("luftdaten", __file__)
logging.info("Arguments: {a}".format(a=args))

luft = luft_class.Luft()
sensor = args.sensor
max_ts = luft.latest_measurement(sensor)
now = time.time()
max_delta = 60 * 60 *24 # Delta time in seconds => 1 day
# Send message only for limited amount of time.
stop_msg = max_delta * 4
delta = int(now - max_ts)
delta_str = datetime.timedelta(seconds=delta)
if (delta > max_delta) and (delta < stop_msg):
    logging.info("Delta since last measurement: {}, mail is prepared.".format(delta_str))
    # Mail to user
    msg = MIMEMultipart()
    gmail_user = os.getenv('GMAIL_USER')
    gmail_pwd = os.getenv('GMAIL_PWD')
    recipient = os.getenv('RECIPIENT')

    msg['Subject'] = 'Alert - LUFT measurement unavailable for {}'.format(delta_str)
    msg['From'] = gmail_user
    msg['To'] = recipient
    body = 'See Subject'
    msg.attach(MIMEText(body, 'plain'))

    smtp_server = os.getenv('SMTP_SERVER')
    smtp_port = os.getenv('SMTP_PORT')

    server = smtplib.SMTP(smtp_server, smtp_port)
    server.starttls()
    server.login(gmail_user, gmail_pwd)
    text = msg.as_string()
    server.sendmail(gmail_user, recipient, text)
    logging.debug("Mail sent!")
    server.quit()

else:
    logging.debug("Delta since last measurement: {}, max_delta: {}".format(delta_str,
                                                                           str(datetime.timedelta(seconds=max_delta))))
logging.info("End application")
