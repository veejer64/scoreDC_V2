from configparser import ConfigParser
from itertools import combinations
from time import time
import logging
from configparser import ConfigParser
import sqlite3
import pandas as pd
from sqlite3 import Error
import datetime
from faker import Faker
import vdb.db

#  Load configuration items as Globals.
config_file = '/Users/veejer/dev/projects/scoreDC_V2/data/config/config.ini'
config = ConfigParser()
config.read(config_file)
db_file = config.get('database', 'db_file')
loglevel = config.get('logging', 'loglevel')
version = config.get('directives', 'version')
con = sqlite3.connect(db_file)

#  Initialize logging.
logger = logging.getLogger()
logger.setLevel(eval(loglevel))

#  Globals

currentDateTime = datetime.datetime.now()
fake = Faker()


def init():
    start = time()
    logging.info('scoreDC DR Test Score Generation Starting.')
    logging.info(('scoreDC Report based upon scoreDB version: ' + version))
    calcScore()
    con.close()
    elapsed = time() - start
    logging.info('scoreDC CMDB DR Test Scores Generated in: {}'.format(elapsed))


def calcScore():
    cursor = con.cursor()
    sqld = """DELETE FROM SCORES WHERE SN_ID = 9000 AND VERSION = ?"""
    cursor.execute(sqld, (version,))
    con.commit()
    cursor.close()

    cursorscore = con.cursor()
    sqlscore = '''SELECT SN_ID,SN_NAME,SN_ABBREVIATION,PASSVAL,FAILVAL,IMPORTANCE_WEIGHT FROM SCORE_NAME WHERE 
    SN_ABBREVIATION = 'SDR' AND VERSION = ? '''
    cursorscore.execute(sqlscore, (version,))
    scoreID = None
    scoreName = None
    scoreAbbr = None
    passVal = None
    failVal = None
    impWeight = None
    recordscore = cursorscore.fetchall()
    for rowscore in recordscore:
        scoreID = rowscore[0]
        scoreName = rowscore[1]
        scoreAbbr = rowscore[2]
        passVal = rowscore[3]
        failVal = rowscore[4]
        impWeight = rowscore[5]
    cursorscore.close()
    cursor = con.cursor()
    sql = '''SELECT A_ID,A_NAME,TA_ID FROM APPLICATION WHERE VERSION = ?'''
    cursor.execute(sql, (version,))
    records = cursor.fetchall()
    for row in records:
        aid = row[0]
        aname = row[1]
        taid = row[2]
        cursorscore = con.cursor()
        sqlscore = '''SELECT DR_TEST_RESULT FROM DR_TEST WHERE 
        A_ID = ? AND VERSION = ? '''
        cursorscore.execute(sqlscore, (aid, version,))
        dr_result = None
        score = None
        recordscore = cursorscore.fetchall()
        for rowscore in recordscore:
            dr_result = rowscore[0]
        cursorscore.close()
        if dr_result is None:
            score = failVal
        if dr_result == "PASS":
            score = passVal
        else:
            score = failVal
        ins_cursor = con.cursor()
        sqlite_insert = """INSERT INTO SCORES
                                      (SC_ID, SN_ID, A_ID, SC_SCORE, VERSION, TIME) 
                                      VALUES (?, ?, ?, ?, ?, ?);"""
        sn_id = fake.random_int(min=1, max=999999)
        data_tuple = (sn_id, scoreID, aid, score, version, currentDateTime)
        ins_cursor.execute(sqlite_insert, data_tuple)
        con.commit()
    con.close()


if __name__ == '__main__':
    init()
