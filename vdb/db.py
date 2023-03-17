import sqlite3
import logging
from configparser import ConfigParser

#  Load configuration items as Globals.
config_file = '/Users/veejer/dev/projects/scoreDC_V2/data/config/config.ini'
config = ConfigParser()
config.read(config_file)
db_file = config.get('database', 'db_file')
loglevel = config.get('logging', 'loglevel')

con = sqlite3.connect(db_file)

#  Initialize logging.
logger = logging.getLogger()
logger.setLevel(eval(loglevel))


def version():
    version = 0
    try:
        con = sqlite3.connect(db_file)
        cursor = con.cursor()
        sqls = ''' SELECT VERSION FROM VERSION'''
        cursor.execute(sqls)
        record = cursor.fetchall()
        for row in record:
            version = row[0]
            cursor.close()
            con.close()
    except sqlite3.Error as error:
        print()
    finally:
        return version


def nextval():
    version = 0
    try:
        con = sqlite3.connect(db_file)
        cursor = con.cursor()
        sqls = ''' SELECT VERSION FROM VERSION'''
        cursor.execute(sqls)
        record = cursor.fetchall()
        for row in record:
            version = row[0]
            version = version + 1
            sqli = '''UPDATE VERSION SET VERSION = ?'''
            rec = (version,)
            cursor.execute(sqli, rec)
            con.commit()
            cursor.close()
            con.close()
    except sqlite3.Error as error:
        print()
    finally:
        return version