import csv
import random
from configparser import ConfigParser
from time import time
import logging
from configparser import ConfigParser
import sqlite3
import pandas as pd
from sqlite3 import Error
import datetime

import vdb.db

#  Load configuration items as Globals.
config_file = '/Users/veejer/dev/projects/scoreDC/data/config/config.ini'
config = ConfigParser()
config.read(config_file)
db_file = config.get('database', 'db_file')
loglevel = config.get('logging', 'loglevel')

con = sqlite3.connect(db_file)

#  Initialize logging.
logger = logging.getLogger()
logger.setLevel(eval(loglevel))

df = pd.read_sql_query('SELECT * FROM V_ALL_DEVICES', con)
with pd.option_context('display.max_rows', None,
                       'display.max_columns', None,
                       'display.precision', 3,
                       ):
    print(df.groupby(['U_NAME', 'DC_NAME', 'CIF_NAME'])['CIF_NAME'].count())
#print(df)