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
    logging.info('scoreDC Role Score Generation Starting.')
    logging.info(('scoreDC Report based upon scoreDB version: ' + version))
    calcScoreProdOnly()
    calcScoreProdBCPOnly()
    calcScoreProdAndBCP()
    calcScoreNonProdOnly()
    calcScoreProdAndNonProd()
    con.close()
    elapsed = time() - start
    logging.info('scoreDC CMDB Role Scores Generated in: {}'.format(elapsed))


def calcScoreProdOnly():
    cursor = con.cursor()
    sqld = """DELETE FROM SCORES WHERE SN_ID = 10000 AND VERSION = ?"""
    cursor.execute(sqld, (version,))
    con.commit()
    cursor.close()

    cursorscore = con.cursor()
    sqlscore = '''SELECT SN_ID,SN_NAME,SN_ABBREVIATION,PASSVAL,FAILVAL,IMPORTANCE_WEIGHT FROM SCORE_NAME WHERE 
    SN_ID = 10000 AND VERSION = ? '''
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
        cursor = con.cursor()
        sql = '''SELECT A_ID,A_NAME,TA_ID FROM APPLICATION WHERE VERSION = ?'''
        cursor.execute(sql, (version,))
        records = cursor.fetchall()
        for row in records:
            aid = row[0]
            aname = row[1]
            taid = row[2]
            cursorscoreV = con.cursor()
            sqlscore = '''SELECT CIR_HASH FROM CONFIGURATION_ITEM_ROLE WHERE 
            CIR_NAME = 'PROD' AND VERSION = ? '''
            cursorscore.execute(sqlscore, (version,))
            hashV = None
            recordscore = cursorscoreV.fetchall()
            for rowscore in recordscore:
                hashV = rowscore[0]
            cursorscoreV.close()

            cursorhash = con.cursor()
            sqlhash = '''SELECT ARH_APP_HASH FROM APP_ROLE_HASH WHERE 
            A_ID = ? AND VERSION = ? '''
            cursorhash.execute(sqlhash, (aid, version,))
            hashA = None
            match = 0
            recordshash = cursorhash.fetchall()
            for rowshash in recordshash:
                hashA = rowshash[0]
                if hashA != hashV:
                    match = 1
            cursorhash.close()

            if match == 1:
                score = failVal
            else:
                score = passVal

            ins_cursor = con.cursor()
            sqlite_insert = """INSERT INTO SCORES
                                          (SC_ID, SN_ID, A_ID, SC_SCORE, VERSION, TIME) 
                                          VALUES (?, ?, ?, ?, ?, ?);"""
            sn_id = fake.random_int(min=1, max=999999)
            data_tuple = (sn_id, scoreID, aid, score, version, currentDateTime)
            ins_cursor.execute(sqlite_insert, data_tuple)
            con.commit()
    cursorscore.close()


def calcScoreProdBCPOnly():
    cursor = con.cursor()
    sqld = """DELETE FROM SCORES WHERE SN_ID = 10002 AND VERSION = ?"""
    cursor.execute(sqld, (version,))
    con.commit()
    cursor.close()

    cursorscore = con.cursor()
    sqlscore = '''SELECT SN_ID,SN_NAME,SN_ABBREVIATION,PASSVAL,FAILVAL,IMPORTANCE_WEIGHT FROM SCORE_NAME WHERE 
    SN_ID = 10002 AND VERSION = ? '''
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
        cursor = con.cursor()
        sql = '''SELECT A_ID,A_NAME,TA_ID FROM APPLICATION WHERE VERSION = ?'''
        cursor.execute(sql, (version,))
        records = cursor.fetchall()
        for row in records:
            aid = row[0]
            aname = row[1]
            taid = row[2]
            cursorscoreV = con.cursor()
            sqlscore = '''SELECT CIR_HASH FROM CONFIGURATION_ITEM_ROLE WHERE 
            CIR_NAME = 'PRODBCP' AND VERSION = ? '''
            cursorscoreV.execute(sqlscore, (version,))
            hashV = None
            recordscore = cursorscoreV.fetchall()
            for rowscore in recordscore:
                hashV = rowscore[0]
            cursorscoreV.close()

            cursorhash = con.cursor()
            sqlhash = '''SELECT ARH_APP_HASH FROM APP_ROLE_HASH WHERE 
            A_ID = ? AND VERSION = ? '''
            cursorhash.execute(sqlhash, (aid, version,))
            hashA = None
            match = 0
            recordshash = cursorhash.fetchall()
            for rowshash in recordshash:
                hashA = rowshash[0]
                if hashA != hashV:
                    match = 1
            cursorhash.close()
            if match == 1:
                score = failVal
            else:
                score = passVal

            ins_cursor = con.cursor()
            sqlite_insert = """INSERT INTO SCORES
                                          (SC_ID, SN_ID, A_ID, SC_SCORE, VERSION, TIME) 
                                          VALUES (?, ?, ?, ?, ?, ?);"""
            sn_id = fake.random_int(min=1, max=999999)
            data_tuple = (sn_id, scoreID, aid, score, version, currentDateTime)
            ins_cursor.execute(sqlite_insert, data_tuple)
            con.commit()
    cursorscore.close()


def calcScoreProdAndBCP():
    cursor = con.cursor()
    sqld = """DELETE FROM SCORES WHERE SN_ID = 10001 AND VERSION = ?"""
    cursor.execute(sqld, (version,))
    con.commit()
    cursor.close()

    cursorscore = con.cursor()
    sqlscore = '''SELECT SN_ID,SN_NAME,SN_ABBREVIATION,PASSVAL,FAILVAL,IMPORTANCE_WEIGHT FROM SCORE_NAME WHERE 
    SN_ID = 10001 AND VERSION = ? '''
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
        cursor = con.cursor()
        sql = '''SELECT A_ID,A_NAME,TA_ID FROM APPLICATION WHERE VERSION = ?'''
        cursor.execute(sql, (version,))
        records = cursor.fetchall()
        for row in records:
            aid = row[0]
            aname = row[1]
            taid = row[2]
            cursorscoreV = con.cursor()
            sqlscore = '''SELECT CIR_HASH FROM CONFIGURATION_ITEM_ROLE WHERE 
            CIR_NAME = 'PRODBCP' AND VERSION = ? '''
            cursorscoreV.execute(sqlscore, (version,))
            hashV = None
            recordscore = cursorscoreV.fetchall()
            for rowscore in recordscore:
                hashV = rowscore[0]

            sqlscore = '''SELECT CIR_HASH FROM CONFIGURATION_ITEM_ROLE WHERE 
            CIR_NAME = 'PROD' AND VERSION = ? '''
            cursorscoreV.execute(sqlscore, (version,))
            hashJ = None
            recordscore = cursorscoreV.fetchall()
            for rowscore in recordscore:
                hashJ = rowscore[0]
            cursorscoreV.close()

            comboHash = hashJ + hashV

            cursorhash = con.cursor()
            sqlhash = '''SELECT ARH_APP_HASH FROM APP_ROLE_HASH WHERE 
            A_ID = ? AND VERSION = ? '''
            cursorhash.execute(sqlhash, (aid, version,))
            hashA = None
            match = 0
            recordshash = cursorhash.fetchall()
            for rowshash in recordshash:
                hashA = rowshash[0]
                if hashA == comboHash:
                    match = 1
            cursorhash.close()
            if match == 1:
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
    cursorscore.close()


def calcScoreNonProdOnly():
    cursor = con.cursor()
    sqld = """DELETE FROM SCORES WHERE SN_ID = 10004 AND VERSION = ?"""
    cursor.execute(sqld, (version,))
    con.commit()
    cursor.close()

    cursorscore = con.cursor()
    sqlscore = '''SELECT SN_ID,SN_NAME,SN_ABBREVIATION,PASSVAL,FAILVAL,IMPORTANCE_WEIGHT FROM SCORE_NAME WHERE 
    SN_ID = 10004 AND VERSION = ? '''
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
        cursor = con.cursor()
        sql = '''SELECT A_ID,A_NAME,TA_ID FROM APPLICATION WHERE VERSION = ?'''
        cursor.execute(sql, (version,))
        records = cursor.fetchall()
        for row in records:
            aid = row[0]
            aname = row[1]
            taid = row[2]

            cursorscoreV = con.cursor()
            sqlscore = '''SELECT CIR_HASH FROM CONFIGURATION_ITEM_ROLE WHERE 
            CIR_NAME = 'NONPROD' AND VERSION = ? '''
            cursorscoreV.execute(sqlscore, (version,))
            hashV = None
            recordscore = cursorscoreV.fetchall()
            for rowscore in recordscore:
                hashV = rowscore[0]
            cursorscoreV.close()

            cursorhash = con.cursor()
            sqlhash = '''SELECT ARH_APP_HASH FROM APP_ROLE_HASH WHERE 
            A_ID = ? AND VERSION = ? '''
            cursorhash.execute(sqlhash, (aid, version,))
            hashA = None
            match1 = 0
            recordshash = cursorhash.fetchall()
            for rowshash in recordshash:
                hashA = rowshash[0]
                if hashA != hashV:
                    match1 = 1
            cursorhash.close()

            if match1 == 1:
                score = failVal
            else:
                score = passVal

            ins_cursor = con.cursor()
            sqlite_insert = """INSERT INTO SCORES
                                          (SC_ID, SN_ID, A_ID, SC_SCORE, VERSION, TIME) 
                                          VALUES (?, ?, ?, ?, ?, ?);"""
            sn_id = fake.random_int(min=1, max=999999)
            data_tuple = (sn_id, scoreID, aid, score, version, currentDateTime)
            ins_cursor.execute(sqlite_insert, data_tuple)
            con.commit()
    cursorscore.close()


def calcScoreProdAndNonProd():
    cursor = con.cursor()
    sqld = """DELETE FROM SCORES WHERE SN_ID = 10003 AND VERSION = ?"""
    cursor.execute(sqld, (version,))
    con.commit()
    cursor.close()

    cursorscore = con.cursor()
    sqlscore = '''SELECT SN_ID,SN_NAME,SN_ABBREVIATION,PASSVAL,FAILVAL,IMPORTANCE_WEIGHT FROM SCORE_NAME WHERE 
    SN_ID = 10003 AND VERSION = ? '''
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
        cursor = con.cursor()
        sql = '''SELECT A_ID,A_NAME,TA_ID FROM APPLICATION WHERE VERSION = ?'''
        cursor.execute(sql, (version,))
        records = cursor.fetchall()
        for row in records:
            aid = row[0]
            aname = row[1]
            taid = row[2]
            cursorscoreV = con.cursor()
            sqlscore = '''SELECT CIR_HASH FROM CONFIGURATION_ITEM_ROLE WHERE 
            CIR_NAME = 'PROD' AND VERSION = ? '''
            cursorscoreV.execute(sqlscore, (version,))
            hashV = None
            recordscore = cursorscoreV.fetchall()
            for rowscore in recordscore:
                hashV = rowscore[0]

            sqlscore = '''SELECT CIR_HASH FROM CONFIGURATION_ITEM_ROLE WHERE 
            CIR_NAME = 'NONPROD' AND VERSION = ? '''
            cursorscoreV.execute(sqlscore, (version,))
            hashJ = None
            recordscore = cursorscoreV.fetchall()
            for rowscore in recordscore:
                hashJ = rowscore[0]
            cursorscoreV.close()

            comboHash = hashJ + hashV

            cursorhash = con.cursor()
            sqlhash = '''SELECT ARH_APP_HASH FROM APP_ROLE_HASH WHERE 
            A_ID = ? AND VERSION = ? '''
            cursorhash.execute(sqlhash, (aid, version,))
            hashA = None
            match = 0
            recordshash = cursorhash.fetchall()
            for rowshash in recordshash:
                hashA = rowshash[0]
                if hashA == comboHash:
                    match = 1
            cursorhash.close()
            if match == 1:
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
    cursorscore.close()


if __name__ == '__main__':
    init()
