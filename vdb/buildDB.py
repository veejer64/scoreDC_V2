import csv
import random
from configparser import ConfigParser
from vutil.distance import distancebetween
from vutil.utilities import createHash
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
rebuildAll = config.get('flags', 'rebuildAll')
oorDist = int(config.get('directives', 'oorDist'))
mode = config.get('input_files', 'mode')
if mode == 'test':
    cio_file = config.get('input_files', 'test_cio_file')
    nw_flow_file = config.get('input_files', 'test_nw_flow_file')
    city_file = config.get('input_files', 'us-city-file')
    inventory_file = config.get('input_files', 'test_inventory_file')
    dr_file = config.get('input_files', 'test_dr_results_file')
else:
    cio_file = config.get('input_files', 'cio_file')
    nw_flow_file = config.get('input_files', 'nw_flow_file')
    city_file = config.get('input_files', 'us-city-file')
    inventory_file = config.get('input_files', 'inventory_file')
    dr_file = config.get('input_files', 'dr_results_file')

con = sqlite3.connect(db_file)
fake = Faker()

#  Initialize logging.
logger = logging.getLogger()
logger.setLevel(eval(loglevel))
logging.info('Building scoreDB.')
logging.info('  NW FLOW INPUT FILE: ' + nw_flow_file)
logging.info('  CIO INPUT FILE: ' + cio_file)
logging.info('  CITY INFO FILE: ' + city_file)
logging.info('  INVENTORY FILE: ' + inventory_file)
logging.info('  DR RESULTS FILE: ' + dr_file)
logging.info('  REBUILD ALL:' + rebuildAll)
logging.info('  OOR CONSIDERED TO BE: ' + str(oorDist))

#  Globals
REC_COUNT = 0
version = vdb.db.nextval()
currentDateTime = datetime.datetime.now()


def create_tables():
    cur = con.cursor()
    global version
    version = 1

    cur.execute('''CREATE TABLE RAW_INVENTORY(datacenter text, name text, shortname text, app_guid, app_status text, 
    app_rto text, device_name text, device_guid text, device_ip text, device_function text, environment text, 
    VERSION INT, TIME TIMESTAMP)''')
    cur.execute('''CREATE TABLE RAW_NW_FLOW(name text, shortname text, url_id text, url text, level text, 
    fromip text, frompsr text, fromdvcname text, fromdvcfunction text, fromdvcdatacenter text, fromdvcrole text, 
    toip text, todvcname text, topsr text, todvcfunction text, todvcdatacenter text, todvcrole text, VERSION INT, 
    TIME TIMESTAMP)''')
    cur.execute('''CREATE TABLE RAW_CIO(Application_Name text, Technology_Area text, URL text, VERSION INT, 
    TIME TIMESTAMP)''')
    cur.execute('''CREATE TABLE RAW_DR(Application_Name text, DR_Test_Result text, VERSION INT, 
    TIME TIMESTAMP)''')
    cur.execute('''CREATE TABLE DATA_CENTER(DC_ID INT,DC_NAME TEXT, VERSION INT, TIME TIMESTAMP)''')
    cur.execute('''CREATE TABLE CONFIGURATION_ITEM(CI_ID INT, A_ID INT, DC_ID INT,CIF_ID INT, CIR_ID INT, CI_NAME TEXT, 
    CI_IPV4 TEXT, CI_IPV6 TEXT, VERSION INT, TIME TIMESTAMP)''')
    cur.execute('''CREATE TABLE CONFIGURATION_ITEM_ATTRIBUTE(CIA_ID INT, CI_ID INT, CIA_NAME TEXT, CIA_VALUE TEXT, 
    VERSION INT, TIME TIMESTAMP)''')
    cur.execute('''CREATE TABLE CONFIGURATION_ITEM_FUNCTION(CIF_ID INT, CIF_NAME TEXT, CIF_HASH INT, VERSION INT, 
    TIME TIMESTAMP)''')
    cur.execute('''CREATE TABLE CONFIGURATION_ITEM_ROLE(CIR_ID INT, CIR_NAME TEXT, CIR_HASH INT, 
    VERSION INT, TIME TIMESTAMP)''')
    cur.execute('''CREATE TABLE TECH_AREA(TA_ID INT, TA_NAME TEXT, VERSION INT, TIME TIMESTAMP)''')
    cur.execute('''CREATE TABLE DATA_CENTER_DISTANCES(DCD_NAME1 TEXT, DCD_NAME2 TEXT, 
        DCD_DISTANCE INT, VERSION INT, TIME TIMESTAMP)''')
    cur.execute('''CREATE TABLE APPLICATION(A_ID INT, A_NAME TEXT, TA_ID INT, VERSION INT, TIME TIMESTAMP)''')
    cur.execute('''CREATE TABLE URL(U_ID INT, U_NAME TEXT, A_ID INT, VERSION INT, TIME TIMESTAMP)''')
    cur.execute('''CREATE TABLE APP_DEVICE_MAP(ADM_ID INT, U_ID INT, CI_ID INT, VERSION INT, TIME TIMESTAMP)''')
    cur.execute('''CREATE TABLE SCORES(SC_ID INT, SN_ID INT, A_ID INT, U_ID INT, SC_SCORE INT, VERSION INT, 
    TIME TIMESTAMP)''')
    cur.execute('''CREATE TABLE SCORE_NAME(SN_ID INT, SN_NAME TEXT, SN_ABBREVIATION TEXT, PASSVAL INT, FAILVAL INT, 
    IMPORTANCE_WEIGHT INT, VERSION INT, TIME TIMESTAMP)''')
    cur.execute('''CREATE TABLE APP_DEVICE_FLOW_MAP(ADF_ID INT, U_ID INT, U_LEVEL INT, CI_ID_FROM INT, CI_ID_TO INT, 
    VERSION INT, TIME TIMESTAMP)''')
    cur.execute('''CREATE TABLE APP_FUNCTION_HASH(AFH_ID INT, A_ID INT, DC_ID INT, AFH_APP_HASH INT, 
    AFH_APP_DIMENSIONAL_HASH INT, VERSION INT, TIME TIMESTAMP)''')
    cur.execute('''CREATE TABLE APP_ROLE_HASH(ARH_ID INT, A_ID INT, DC_ID INT, ARH_APP_HASH INT, 
    ARH_APP_DIMENSIONAL_HASH INT, VERSION INT, TIME TIMESTAMP)''')
    cur.execute('''CREATE TABLE FLOW_ROLE_HASH(FRH_ID INT, A_ID INT, U_ID INT, DC_ID INT, FRH_APP_HASH INT, 
    FRH_APP_DIMENSIONAL_HASH INT, VERSION INT, TIME TIMESTAMP)''')
    cur.execute('''CREATE TABLE FLOW_FUNCTION_HASH(FFH_ID INT, A_ID INT, U_ID INT, DC_ID INT, FFH_APP_HASH INT, 
    FFH_APP_DIMENSIONAL_HASH INT, VERSION INT, TIME TIMESTAMP)''')
    cur.execute('''CREATE TABLE CITY_DATA( CITY TEXT, STATE_ID TEXT, STATE_NAME TEXT, COUNTY_NAME TEXT, LAT REAL,
    LON REAL, TIMEZONE TEXT, ZIPS TEXT, ID INT, VERSION INT, TIME TIMESTAMP)''')
    cur.execute('''CREATE TABLE APP_CAPACITY_INFORMATION( A_NAME TEXT, A_ID TEXT, TA_ID TEXT, CIF_NAME,
    CIF_ID, DATACENTER1 TEXT, DEVICE_COUNT1 INT, DATACENTER2 TEXT, DEVICE_COUNT2 INT, 
    PERCENTCAPACITY REAL, OUT_OF_REGION TEXT, VERSION INT, TIME TIMESTAMP)''')
    cur.execute('''CREATE TABLE NWFLOW_CAPACITY_INFORMATION( U_NAME TEXT, U_ID TEXT, A_NAME TEXT, A_ID TEXT, 
    TA_NAME TEXT, TA_ID TEXT, CIF_NAME, CIF_ID, DATACENTER1 TEXT, DEVICE_COUNT1 INT, DATACENTER2 TEXT, DEVICE_COUNT2 
    INT, PERCENTCAPACITY REAL, OUT_OF_REGION TEXT, VERSION INT, TIME TIMESTAMP)''')
    cur.execute('''CREATE TABLE DR_TEST(DR_ID INT, A_ID INT, DR_TEST_RESULT STR, DR_TEST_RESULT_FLAG INT, 
    VERSION INT, TIME TIMESTAMP)''')
    cur.execute('''CREATE TABLE SINGLE_DATA_CENTER (A_NAME TEXT, A_ID INT, TA_ID INT, SCORE INT, VERSION INT, 
    TIME TIMESTAMP)''')

    cur.execute('''CREATE TABLE VERSION(VERSION INT)''')
    cur.execute('''INSERT INTO VERSION (VERSION) VALUES (1)''')

    #######################################################################
    # Note to self - need to rewrite views to include concept of VERSION
    #######################################################################
    cur.execute('''CREATE VIEW V_APP_NWFLOW AS select u.U_NAME, a.A_NAME, adf.U_LEVEL, cifrom.CI_NAME, 
    ciffrom.CIF_NAME, dcfrom.DC_NAME, cirfrom.CIR_NAME, cito.CI_NAME, cifto.CIF_NAME, cirto.CIR_NAME, dcto.DC_NAME 
    from url u inner join APPLICATION a on u.A_ID=a.A_ID inner join APP_DEVICE_FLOW_MAP adf on u.U_ID=adf.U_ID inner 
    join CONFIGURATION_ITEM cifrom on cifrom.CI_ID=adf.CI_ID_FROM inner join CONFIGURATION_ITEM cito on 
    cito.CI_ID=adf.CI_ID_TO inner join CONFIGURATION_ITEM_FUNCTION ciffrom on ciffrom.CIF_ID=cifrom.CIF_ID inner join 
    CONFIGURATION_ITEM_FUNCTION cifto on cifto.CIF_ID=cito.CIF_ID inner join CONFIGURATION_ITEM_ROLE cirfrom on 
    cirfrom.CIR_ID=cifrom.CIR_ID inner join CONFIGURATION_ITEM_ROLE cirto on cirto.CIR_ID=cito.CIR_ID inner join 
    DATA_CENTER dcfrom on dcfrom.DC_ID=cifrom.DC_ID inner join DATA_CENTER dcto on dcto.DC_ID=cito.DC_ID''')

    cur.execute('''CREATE VIEW V_ALL_NWFLOW_DEVICES AS select u.U_NAME, a.A_NAME, t.TA_NAME, ci.CI_NAME, 
    cif.CIF_NAME, cir.CIR_NAME, dc.DC_NAME from url u inner join APPLICATION a on u.A_ID=a.A_ID inner join TECH_AREA 
    t on a.ta_id=t.TA_ID inner join APP_DEVICE_MAP adm on u.U_ID=adm.U_ID inner join CONFIGURATION_ITEM ci on 
    ci.ci_id=adm.CI_ID inner join CONFIGURATION_ITEM_FUNCTION cif on cif.CIF_ID=ci.CIF_ID inner join 
    CONFIGURATION_ITEM_ROLE cir on cir.CIR_ID=ci.CIR_ID inner join DATA_CENTER dc on dc.DC_ID=ci.DC_ID''')

    cur.execute('''CREATE VIEW V_ALL_APP_DEVICES AS select a.A_NAME, t.TA_NAME, ci.CI_NAME,  ci.CI_IPV4, 
    cif.CIF_NAME, cir.CIR_NAME, dc.DC_NAME from APPLICATION a inner join TECH_AREA t on a.ta_id=t.TA_ID inner join 
    CONFIGURATION_ITEM ci on ci.a_id=a.A_ID inner join CONFIGURATION_ITEM_FUNCTION cif on cif.CIF_ID=ci.CIF_ID inner 
    join CONFIGURATION_ITEM_ROLE cir on cir.CIR_ID=ci.CIR_ID inner join DATA_CENTER dc on dc.DC_ID=ci.DC_ID''')

    cur.execute('''CREATE VIEW V_ALL_NWFLOW_DEVICES_EXTENDED AS select u.U_NAME, u.U_ID, a.A_NAME, a.A_ID, t.TA_NAME, 
    t.TA_ID, ci.CI_NAME, ci.CI_ID, cif.CIF_NAME, ci.CI_ID, cif.CIF_NAME, cif.CIF_ID, cif.CIF_HASH, cir.CIR_NAME, 
    cir.CIR_ID, cir.CIR_HASH, dc.DC_NAME, dc.DC_ID from url u inner join APPLICATION a on u.A_ID=a.A_ID inner join 
    TECH_AREA t on a.ta_id=t.TA_ID inner join APP_DEVICE_MAP adm on u.U_ID=adm.U_ID inner join CONFIGURATION_ITEM ci 
    on ci.ci_id=adm.CI_ID inner join CONFIGURATION_ITEM_FUNCTION cif on cif.CIF_ID=ci.CIF_ID inner join 
    CONFIGURATION_ITEM_ROLE cir on cir.CIR_ID=ci.CIR_ID inner join DATA_CENTER dc on dc.DC_ID=ci.DC_ID''')

    cur.execute('''CREATE VIEW V_ALL_APP_DEVICES_EXTENDED AS select a.A_NAME, a.A_ID, t.TA_NAME, t.TA_ID, ci.CI_NAME, 
     ci.CI_ID, ci.CI_IPV4, cif.CIF_NAME, cif.CIF_ID, cif.CIF_HASH, cir.CIR_NAME, cir.CIR_ID, cir.CIR_HASH, dc.DC_NAME, 
     dc.DC_ID from APPLICATION a inner join TECH_AREA t on a.ta_id=t.TA_ID inner join CONFIGURATION_ITEM ci on 
     ci.a_id=a.A_ID inner join CONFIGURATION_ITEM_FUNCTION cif on cif.CIF_ID=ci.CIF_ID inner join 
     CONFIGURATION_ITEM_ROLE cir on cir.CIR_ID=ci.CIR_ID inner join DATA_CENTER dc on dc.DC_ID=ci.DC_ID''')

    cur.execute('''CREATE VIEW V_NWFLOW_DEVICE_COUNTS AS select U_NAME, U_ID, A_NAME, A_ID, TA_NAME, TA_ID, DC_NAME, 
    DC_ID, CIF_NAME, CIF_ID, count(CIF_NAME) as DEVICE_COUNT from V_ALL_NWFLOW_DEVICES_EXTENDED group by U_NAME, 
    DC_NAME, CIF_NAME''')

    cur.execute('''CREATE VIEW V_ALL_DEVICE_COUNTS AS select  A_NAME, A_ID, TA_NAME, TA_ID, DC_NAME, DC_ID, CIF_NAME, 
    CIF_ID, count(CIF_NAME) as DEVICE_COUNT from V_ALL_APP_DEVICES_EXTENDED group by A_NAME, DC_NAME, CIF_NAME''')

    # Save (commit) the changes
    con.commit()
    cur.close()


def delete_tables():
    cur = con.cursor()
    cur.execute('''DROP VIEW V_ALL_DEVICE_COUNTS''')
    cur.execute('''DROP VIEW V_NWFLOW_DEVICE_COUNTS''')
    cur.execute('''DROP VIEW V_ALL_APP_DEVICES_EXTENDED''')
    cur.execute('''DROP VIEW V_ALL_NWFLOW_DEVICES_EXTENDED''')
    cur.execute('''DROP VIEW V_APP_NWFLOW''')
    cur.execute('''DROP VIEW V_ALL_NWFLOW_DEVICES''')
    cur.execute('''DROP VIEW V_ALL_APP_DEVICES''')

    cur.execute('''DROP TABLE SINGLE_DATA_CENTER''')
    cur.execute('''DROP TABLE DR_TEST''')
    cur.execute('''DROP TABLE RAW_DR''')
    cur.execute('''DROP TABLE NWFLOW_CAPACITY_INFORMATION''')
    cur.execute('''DROP TABLE APP_CAPACITY_INFORMATION''')
    cur.execute('''DROP TABLE FLOW_ROLE_HASH''')
    cur.execute('''DROP TABLE FLOW_FUNCTION_HASH''')
    cur.execute('''DROP TABLE APP_ROLE_HASH''')
    cur.execute('''DROP TABLE APP_FUNCTION_HASH''')
    cur.execute('''DROP TABLE DATA_CENTER_DISTANCES''')
    cur.execute('''DROP TABLE RAW_INVENTORY''')
    cur.execute('''DROP TABLE RAW_NW_FLOW''')
    cur.execute('''DROP TABLE RAW_CIO''')
    cur.execute('''DROP TABLE DATA_CENTER''')
    cur.execute('''DROP TABLE CONFIGURATION_ITEM''')
    cur.execute('''DROP TABLE CONFIGURATION_ITEM_ATTRIBUTE''')
    cur.execute('''DROP TABLE CONFIGURATION_ITEM_FUNCTION''')
    cur.execute('''DROP TABLE CONFIGURATION_ITEM_ROLE''')
    cur.execute('''DROP TABLE TECH_AREA''')
    cur.execute('''DROP TABLE APPLICATION''')
    cur.execute('''DROP TABLE URL''')
    cur.execute('''DROP TABLE APP_DEVICE_MAP''')
    cur.execute('''DROP TABLE APP_DEVICE_FLOW_MAP''')
    cur.execute('''DROP TABLE VERSION''')
    cur.execute('''DROP TABLE SCORES''')
    cur.execute('''DROP TABLE SCORE_NAME''')
    cur.execute('''DROP TABLE CITY_DATA''')
    # Save (commit) the changes
    con.commit()
    cur.close()


def rSubset(arr):
    return list(combinations(arr, 2))


def loadRAW():
    # load the data into a Pandas DataFrame
    nwflow = pd.read_csv(nw_flow_file)
    # write the data to a sqlite table
    nwflow.to_sql('RAW_NW_FLOW', con, if_exists='append', index=False)
    con.commit()
    sqli = '''UPDATE RAW_NW_FLOW SET VERSION = ?, TIME = ?'''
    cursor = con.cursor()
    rec = (version, currentDateTime)
    cursor.execute(sqli, rec)
    cursor.close()
    con.commit()

    # load the data into a Pandas DataFrame
    cio = pd.read_csv(cio_file)
    # write the data to a sqlite table
    cio.to_sql('RAW_CIO', con, if_exists='append', index=False)
    con.commit()
    sqli = '''UPDATE RAW_CIO SET VERSION = ?, TIME = ?'''
    cursor = con.cursor()
    rec = (version, currentDateTime)
    cursor.execute(sqli, rec)
    cursor.close()
    con.commit()

    # load the data into a Pandas DataFrame
    dr = pd.read_csv(dr_file)
    # write the data to a sqlite table
    dr.to_sql('RAW_DR', con, if_exists='append', index=False)
    con.commit()
    sqli = '''UPDATE RAW_DR SET VERSION = ?, TIME = ?'''
    cursor = con.cursor()
    rec = (version, currentDateTime)
    cursor.execute(sqli, rec)
    cursor.close()
    con.commit()

    # load the data into a Pandas DataFrame
    city = pd.read_csv(city_file)
    # write the data to a sqlite table
    city.to_sql('CITY_DATA', con, if_exists='append', index=False)
    con.commit()
    sqli = '''UPDATE CITY_DATA SET VERSION = ?, TIME = ?'''
    cursor = con.cursor()
    rec = (version, currentDateTime)
    cursor.execute(sqli, rec)
    cursor.close()
    con.commit()

    # load the data into a Pandas DataFrame
    inv = pd.read_csv(inventory_file)
    # write the data to a sqlite table
    inv.to_sql('RAW_INVENTORY', con, if_exists='append', index=False)
    con.commit()
    sqli = '''UPDATE RAW_INVENTORY SET VERSION = ?, TIME = ?'''
    cursor = con.cursor()
    rec = (version, currentDateTime)
    cursor.execute(sqli, rec)
    cursor.close()
    con.commit()


def loadBaseTables():
    try:
        logging.info(".    Loading TECH_AREA/APPLICATION/DATA_CENTER/CONFIGURATION_ITEM_FUNCTION"
                     "/CONFIGURATION_ITEM_ROLE tables.")
        cursor = con.cursor()
        sql = """SELECT DISTINCT datacenter FROM RAW_INVENTORY WHERE VERSION = ?"""
        cursor.execute(sql, (version,))
        records = cursor.fetchall()

        did = 0
        for row in records:
            datacenter = row[0]
            ins_cursor = con.cursor()
            sqlite_insert = """INSERT INTO DATA_CENTER
                              (DC_ID, DC_NAME, VERSION, TIME) 
                              VALUES (?, ?, ?, ?);"""

            data_tuple = (did, datacenter, version, currentDateTime)
            ins_cursor.execute(sqlite_insert, data_tuple)
            ins_cursor.close()
            did = did + 1
        datacenter = 'URL'
        ins_cursor = con.cursor()
        sqlite_insert = """INSERT INTO DATA_CENTER
                           (DC_ID, DC_NAME, VERSION, TIME) 
                           VALUES (?, ?, ?, ?);"""

        data_tuple = (did, datacenter, version, currentDateTime)
        ins_cursor.execute(sqlite_insert, data_tuple)
        ins_cursor.close()
        cursor.close()
        con.commit()
        logging.info(" .    Loaded DATA_CENTER table.")

        cursor = con.cursor()
        sql = """SELECT DISTINCT Technology_Area FROM RAW_CIO WHERE VERSION = ?"""
        cursor.execute(sql, (version,))
        records = cursor.fetchall()

        tid = 0
        for row in records:
            name = row[0]
            ins_cursor = con.cursor()
            sqlite_insert = """INSERT INTO TECH_AREA
                              (TA_ID, TA_NAME, VERSION, TIME) 
                              VALUES (?, ?, ?, ?);"""

            data_tuple = (tid, name, version, currentDateTime)
            ins_cursor.execute(sqlite_insert, data_tuple)
            ins_cursor.close()
            tid = tid + 1
        cursor.close()
        con.commit()
        logging.info(" .    Loaded TECH_AREA table.")

        cursor = con.cursor()
        sql = """SELECT DISTINCT Application_Name,Technology_Area FROM RAW_CIO WHERE VERSION = ?"""
        cursor.execute(sql, (version,))
        records = cursor.fetchall()

        aid = 0
        for row in records:
            name = row[0]
            ta = row[1]
            taid = None

            scursor = con.cursor()
            scursor.execute(
                "SELECT TA_ID FROM TECH_AREA WHERE TA_NAME = ? AND VERSION = ?", (ta, version))
            srecords = scursor.fetchall()
            for rows in srecords:
                taid = str(rows[0])
            scursor.close()

            ins_cursor = con.cursor()
            sqlite_insert = """INSERT INTO APPLICATION
                              (A_ID, A_NAME, TA_ID, VERSION, TIME) 
                              VALUES (?, ?, ?, ?, ?);"""

            data_tuple = (aid, name, taid, version, currentDateTime)
            ins_cursor.execute(sqlite_insert, data_tuple)
            ins_cursor.close()
            aid = aid + 1
        cursor.close()
        con.commit()
        logging.info(" .    Loaded APPLICATION table.")

        cursor = con.cursor()
        sql = """SELECT Application_Name, DR_Test_Result FROM RAW_DR WHERE VERSION = ?"""
        cursor.execute(sql, (version,))
        records = cursor.fetchall()

        did = 0
        aname = None
        aid = None
        for row in records:
            aname = row[0]
            dres = row[1]

            if dres == 'PASS':
                dresf = 1
            else:
                dresf = 0

            cursora = con.cursor()
            sqla = """SELECT DISTINCT A_ID FROM APPLICATION WHERE A_NAME = ? AND VERSION = ?"""
            cursora.execute(sqla, (aname, version))
            recordsa = cursora.fetchall()
            for rowa in recordsa:
                aid = rowa[0]
            cursora.close()
            ins_cursor = con.cursor()
            sqlite_insert = """INSERT INTO DR_TEST
                              (DR_ID, A_ID, DR_TEST_RESULT, DR_TEST_RESULT_FLAG, VERSION, TIME) 
                              VALUES (?, ?, ?, ?, ?, ?);"""

            data_tuple = (did, aid, dres, dresf, version, currentDateTime)
            ins_cursor.execute(sqlite_insert, data_tuple)
            ins_cursor.close()
            did = did + 1
        cursor.close()
        con.commit()
        logging.info(" .    Loaded DR table.")

        ins_cursor = con.cursor()
        sqlite_insert = """INSERT INTO CONFIGURATION_ITEM_FUNCTION
                                      (CIF_ID, CIF_NAME, CIF_HASH, VERSION, TIME) 
                                      VALUES (0, 'WEB SERVER', ?, ?, ?);"""
        hashf = createHash('WEB SERVER')
        data_tuple = (hashf, version, currentDateTime)
        ins_cursor.execute(sqlite_insert, data_tuple)

        sqlite_insert = """INSERT INTO CONFIGURATION_ITEM_FUNCTION
                                       (CIF_ID, CIF_NAME, CIF_HASH, VERSION, TIME) 
                                       VALUES (1, 'APP SERVER', ?, ?, ?);"""
        hashf = createHash('APP SERVER')
        data_tuple = (hashf, version, currentDateTime)
        ins_cursor.execute(sqlite_insert, data_tuple)

        sqlite_insert = """INSERT INTO CONFIGURATION_ITEM_FUNCTION
                                       (CIF_ID, CIF_NAME, CIF_HASH, VERSION, TIME) 
                                       VALUES (2, 'DATABASE SERVER', ?, ?, ?);"""
        hashf = createHash('DATABASE SERVER')
        data_tuple = (hashf, version, currentDateTime)
        ins_cursor.execute(sqlite_insert, data_tuple)

        sqlite_insert = """INSERT INTO CONFIGURATION_ITEM_FUNCTION
                                       (CIF_ID, CIF_NAME, CIF_HASH, VERSION, TIME) 
                                       VALUES (3, 'SLB', ?, ?, ?);"""
        hashf = createHash('SLB')
        data_tuple = (hashf, version, currentDateTime)
        ins_cursor.execute(sqlite_insert, data_tuple)

        sqlite_insert = """INSERT INTO CONFIGURATION_ITEM_FUNCTION
                                        (CIF_ID, CIF_NAME, CIF_HASH, VERSION, TIME) 
                                        VALUES (4, 'URL', ?, ?, ?);"""
        hashf = createHash('URL')
        data_tuple = (hashf, version, currentDateTime)
        ins_cursor.execute(sqlite_insert, data_tuple)

        sqlite_insert = """INSERT INTO CONFIGURATION_ITEM_FUNCTION
                                        (CIF_ID, CIF_NAME, CIF_HASH, VERSION, TIME) 
                                        VALUES (5, 'DP', ?, ?, ?);"""
        hashf = createHash('DP')
        data_tuple = (hashf, version, currentDateTime)
        ins_cursor.execute(sqlite_insert, data_tuple)

        sqlite_insert = """INSERT INTO CONFIGURATION_ITEM_FUNCTION
                                        (CIF_ID, CIF_NAME, CIF_HASH, VERSION, TIME) 
                                        VALUES (6, 'WAF', ?, ?, ?);"""
        hashf = createHash('WAF')
        data_tuple = (hashf, version, currentDateTime)
        ins_cursor.execute(sqlite_insert, data_tuple)

        sqlite_insert = """INSERT INTO CONFIGURATION_ITEM_FUNCTION
                                        (CIF_ID, CIF_NAME, CIF_HASH, VERSION, TIME) 
                                        VALUES (7, 'MAINFRAME LPAR', ?, ?, ?);"""
        hashf = createHash('MAINFRAME LPAR')
        data_tuple = (hashf, version, currentDateTime)
        ins_cursor.execute(sqlite_insert, data_tuple)

        sqlite_insert = """INSERT INTO CONFIGURATION_ITEM_FUNCTION
                                        (CIF_ID, CIF_NAME, CIF_HASH, VERSION, TIME) 
                                        VALUES (8, 'DNSEntry', ?, ?, ?);"""
        hashf = createHash('DNSEntry')
        data_tuple = (hashf, version, currentDateTime)
        ins_cursor.execute(sqlite_insert, data_tuple)

        con.commit()
        ins_cursor.close()
        logging.info(" .    Loaded CONFIGURATION_ITEM_FUNCTION table.")

        ins_cursor = con.cursor()
        sqlite_insert = """INSERT INTO CONFIGURATION_ITEM_ROLE
                                      (CIR_ID, CIR_NAME, CIR_HASH, VERSION, TIME) 
                                      VALUES (0, 'PROD', ?, ?, ?);"""
        hashf = createHash('PROD')
        data_tuple = (hashf, version, currentDateTime)
        ins_cursor.execute(sqlite_insert, data_tuple)

        sqlite_insert = """INSERT INTO CONFIGURATION_ITEM_ROLE
                                       (CIR_ID, CIR_NAME, CIR_HASH,VERSION, TIME) 
                                       VALUES (1, 'PRODBCP', ?, ?, ?);"""
        hashf = createHash('PRODBCP')
        data_tuple = (hashf, version, currentDateTime)
        ins_cursor.execute(sqlite_insert, data_tuple)

        sqlite_insert = """INSERT INTO CONFIGURATION_ITEM_ROLE
                                       (CIR_ID, CIR_NAME, CIR_HASH, VERSION, TIME) 
                                       VALUES (2, 'PRODFIX', ?, ?, ?);"""
        hashf = createHash('PRODFIX')
        data_tuple = (hashf, version, currentDateTime)
        ins_cursor.execute(sqlite_insert, data_tuple)

        sqlite_insert = """INSERT INTO CONFIGURATION_ITEM_ROLE
                                        (CIR_ID, CIR_NAME, CIR_HASH, VERSION, TIME) 
                                        VALUES (3, 'NONPROD', ?, ?, ?);"""
        hashf = createHash('NONPROD')
        data_tuple = (hashf, version, currentDateTime)
        ins_cursor.execute(sqlite_insert, data_tuple)

#       sqlite_insert = """INSERT INTO CONFIGURATION_ITEM_ROLE
#                                       VALUES (3, 'NONPRODDEV', ?, ?, ?);"""
#        hashf = createHash('NONPRODDEV')
#        ins_cursor.execute(sqlite_insert, data_tuple)

#        sqlite_insert = """INSERT INTO CONFIGURATION_ITEM_ROLE
#                                         (CIR_ID, CIR_NAME, CIR_HASH, VERSION, TIME)
#        hashf = createHash('NONPRODLAB')
#        data_tuple = (hashf, version, currentDateTime)
#        ins_cursor.execute(sqlite_insert, data_tuple)

#        sqlite_insert = """INSERT INTO CONFIGURATION_ITEM_ROLE
#                                         (CIR_ID, CIR_NAME, CIR_HASH, VERSION, TIME)
#                                         VALUES (5, 'NONPRODSIT', ?, ?, ?);"""
#        hashf = createHash('NONPRODSIT')
#        data_tuple = (hashf, version, currentDateTime)
#        ins_cursor.execute(sqlite_insert, data_tuple)

#        sqlite_insert = """INSERT INTO CONFIGURATION_ITEM_ROLE
#                                         (CIR_ID, CIR_NAME, CIR_HASH, VERSION, TIME)
#                                         VALUES (6, 'NONPRODTEST', ?, ?, ?);"""
#        hashf = createHash('NONPRODTEST')
#        data_tuple = (hashf, version, currentDateTime)
#        ins_cursor.execute(sqlite_insert, data_tuple)

#        sqlite_insert = """INSERT INTO CONFIGURATION_ITEM_ROLE
#                                          (CIR_ID, CIR_NAME, CIR_HASH, VERSION, TIME)
#                                          VALUES (7, 'NONPRODUAT', ?, ?, ?);"""
#        hashf = createHash('NONPRODUAT')
#        data_tuple = (hashf, version, currentDateTime)
#        ins_cursor.execute(sqlite_insert, data_tuple)

        con.commit()
        ins_cursor.close()
        logging.info(" .    Loaded CONFIGURATION_ITEM_ROLE table.")

        logging.info(" .    Loading SCORE_NAME table.")
        ins_cursor = con.cursor()
        sqlite_insert = """INSERT INTO SCORE_NAME (SN_ID, SN_NAME, SN_ABBREVIATION, PASSVAL, FAILVAL, 
        IMPORTANCE_WEIGHT, VERSION, TIME) VALUES (?, ?, ?, ?, ?, ?, ?, ?); """

        id = 1000
        name = 'CAPACITY SCORE'
        abbr = 'CPS'
        passval = 10
        failval = 0
        iwt = 20
        data_tuple = (id, name, abbr, passval, failval, iwt, version, currentDateTime)
        ins_cursor.execute(sqlite_insert, data_tuple)

        id = 2000
        name = 'SINGLE DATA CENTER SCORE'
        abbr = 'SDC'
        passval = -15
        failval = 0
        iwt = 16
        data_tuple = (id, name, abbr, passval, failval, iwt, version, currentDateTime)
        ins_cursor.execute(sqlite_insert, data_tuple)

        id = 3000
        name = 'MULTI DATA CENTER SCORE'
        abbr = 'MDC'
        passval = 10
        failval = 0
        iwt = 14
        data_tuple = (id, name, abbr, passval, failval, iwt, version, currentDateTime)
        ins_cursor.execute(sqlite_insert, data_tuple)

        id = 4000
        name = 'SINGLE LOAD BALANCER SCORE'
        abbr = 'SLB'
        passval = 10
        failval = 0
        iwt = 13
        data_tuple = (id, name, abbr, passval, failval, iwt, version, currentDateTime)
        ins_cursor.execute(sqlite_insert, data_tuple)

        id = 5000
        name = 'SINGLE LOAD BALANCER IN MULTI DATA CENTER SCORE'
        abbr = 'MLB'
        passval = 10
        failval = 0
        iwt = 12
        data_tuple = (id, name, abbr, passval, failval, iwt, version, currentDateTime)
        ins_cursor.execute(sqlite_insert, data_tuple)

        id = 6000
        name = 'MULTI SITE SAME STACK SCORE'
        abbr = 'MSS'
        passval = 10
        failval = 0
        iwt = 17
        data_tuple = (id, name, abbr, passval, failval, iwt, version, currentDateTime)
        ins_cursor.execute(sqlite_insert, data_tuple)

        id = 7000
        name = 'LOCAL HIGH AVAILABILITY SCORE'
        abbr = 'LHA'
        passval = 10
        failval = 0
        iwt = 7
        data_tuple = (id, name, abbr, passval, failval, iwt, version, currentDateTime)
        ins_cursor.execute(sqlite_insert, data_tuple)

        id = 8000
        name = 'GLOBAL HIGH AVAILABILITY SCORE'
        abbr = 'GHA'
        passval = 10
        failval = 0
        iwt = 5
        data_tuple = (id, name, abbr, passval, failval, iwt, version, currentDateTime)
        ins_cursor.execute(sqlite_insert, data_tuple)

        id = 9000
        name = 'SUCCESSFUL DR TEST SCORE'
        abbr = 'SDR'
        passval = 10
        failval = 0
        iwt = 8
        data_tuple = (id, name, abbr, passval, failval, iwt, version, currentDateTime)
        ins_cursor.execute(sqlite_insert, data_tuple)

        id = 10000
        name = 'PROD ONLY ROLES SCORE'
        abbr = 'POR'
        passval = 10
        failval = 0
        iwt = 1
        data_tuple = (id, name, abbr, passval, failval, iwt, version, currentDateTime)
        ins_cursor.execute(sqlite_insert, data_tuple)

        id = 10001
        name = 'PROD AND PRODBCP ROLES SCORE'
        abbr = 'PBR'
        passval = 9
        failval = 0
        iwt = 1
        data_tuple = (id, name, abbr, passval, failval, iwt, version, currentDateTime)
        ins_cursor.execute(sqlite_insert, data_tuple)

        id = 10002
        name = 'PRODBCP ONLY ROLES SCORE'
        abbr = 'BOR'
        passval = 8
        failval = 0
        iwt = 1
        data_tuple = (id, name, abbr, passval, failval, iwt, version, currentDateTime)
        ins_cursor.execute(sqlite_insert, data_tuple)

        id = 10003
        name = 'PROD AND NON-PROD ROLES SCORE'
        abbr = 'PNR'
        passval = 4
        failval = 0
        iwt = 1
        data_tuple = (id, name, abbr, passval, failval, iwt, version, currentDateTime)
        ins_cursor.execute(sqlite_insert, data_tuple)

        id = 10004
        name = 'NON-PROD ONLY ROLES SCORE'
        abbr = 'NPR'
        passval = 2
        failval = 0
        iwt = 1
        data_tuple = (id, name, abbr, passval, failval, iwt, version, currentDateTime)
        ins_cursor.execute(sqlite_insert, data_tuple)

        con.commit()
        ins_cursor.close()
        logging.info(" .    Loaded SCORE_NAME table.")

    except sqlite3.Error as error:
        logging.error("Error loading base tables." + error)
    finally:
        return


def loadCITable():
    try:
        logging.info(".     Loading CONFIGURATION_ITEM table")
        cursor = con.cursor()

        sql = """SELECT datacenter, name, shortname, app_guid, app_status, app_rto, device_name, device_guid, 
        device_ip, device_function, environment, VERSION, TIME FROM RAW_INVENTORY WHERE 
        VERSION = ? """
        cursor.execute(sql, (version,))
        records = cursor.fetchall()

        cid = 0
        for row in records:
            ip = row[8]
            aname = row[1]
            name = row[6]
            datacenter = row[0]
            role = row[10]
            function = row[9]

            cifid = None
            cirid = None
            dcid = None

            scursor = con.cursor()
            scursor.execute(
                "SELECT CIF_ID FROM CONFIGURATION_ITEM_FUNCTION WHERE CIF_NAME = ? AND VERSION = ?",
                (function, version))
            srecords = scursor.fetchall()
            for rows in srecords:
                cifid = rows[0]
            scursor.close()

            scursor = con.cursor()
            scursor.execute(
                "SELECT A_ID FROM APPLICATION WHERE A_NAME = ? AND VERSION = ?",
                (aname, version))
            srecords = scursor.fetchall()
            for rows in srecords:
                aid = rows[0]
            scursor.close()

            scursor = con.cursor()
            scursor.execute(
                "SELECT CIR_ID FROM CONFIGURATION_ITEM_ROLE WHERE CIR_NAME = ? AND VERSION = ?", (role, version))
            srecords = scursor.fetchall()
            for rows in srecords:
                cirid = rows[0]
            scursor.close()

            scursor = con.cursor()
            scursor.execute(
                "SELECT DC_ID FROM DATA_CENTER WHERE DC_NAME = ? AND VERSION = ?", (datacenter, version))
            srecords = scursor.fetchall()
            for rows in srecords:
                dcid = rows[0]
            scursor.close()

            ins_cursor = con.cursor()
            sqlite_insert = """INSERT INTO CONFIGURATION_ITEM
                              (CI_ID, CI_NAME, A_ID, DC_ID, CIF_ID, CIR_ID, CI_IPV4, VERSION, TIME) 
                              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);"""

            data_tuple = (cid, name, aid, dcid, cifid, cirid, ip, version, currentDateTime)
            ins_cursor.execute(sqlite_insert, data_tuple)
            ins_cursor.close()
            cid = cid + 1
        cursor.close()
        con.commit()

        logging.info(" .    Loaded CONFIGURATION_ITEM table.")

    except sqlite3.Error as error:
        logging.error("Error loading CI table." + error)
    finally:
        return


def loadURLTable():
    try:
        logging.info(".     Loading URL table")
        cursor = con.cursor()
        sql = """SELECT DISTINCT url, name FROM RAW_NW_FLOW WHERE 
        VERSION = ? """
        cursor.execute(sql, (version,))
        records = cursor.fetchall()

        uid = 0
        for row in records:
            url = row[0]
            name = row[1]
            aid = None

            scursor = con.cursor()
            scursor.execute(
                "SELECT A_ID FROM APPLICATION WHERE A_NAME = ? AND VERSION = ?", (name, version))
            srecords = scursor.fetchall()
            for rows in srecords:
                aid = rows[0]
            scursor.close()

            ins_cursor = con.cursor()
            sqlite_insert = """INSERT INTO URL
                                       (U_ID, U_NAME, A_ID, VERSION, TIME) 
                                       VALUES (?, ?, ?, ?, ?);"""

            data_tuple = (uid, url, aid, version, currentDateTime)
            ins_cursor.execute(sqlite_insert, data_tuple)
            ins_cursor.close()
            uid = uid + 1
        cursor.close()
        con.commit()
        logging.info(" .    Loaded URL table.")


    except sqlite3.Error as error:
        logging.error("Error loading ADM table." + error)
    finally:
        return


def loadADMTable():
    try:
        logging.info(".     Loading ADM table")
        cursor = con.cursor()
        sql = """SELECT DISTINCT url, toip FROM RAW_NW_FLOW WHERE VERSION = ? """
        cursor.execute(sql, (version,))
        records = cursor.fetchall()

        aid = 0
        for row in records:
            url = row[0]
            toip = row[1]

            uid = None
            ci = None

            scursor = con.cursor()
            scursor.execute(
                "SELECT U_ID FROM URL WHERE U_NAME = ? AND VERSION = ?", (url, version))
            srecords = scursor.fetchall()
            for rows in srecords:
                uid = rows[0]
            scursor.close()

            scursor = con.cursor()
            scursor.execute(
                "SELECT CI_ID FROM CONFIGURATION_ITEM WHERE  CI_IPV4 = ? AND VERSION = ?", (toip, version))
            srecords = scursor.fetchall()
            for rows in srecords:
                ci = rows[0]
            scursor.close()

            ins_cursor = con.cursor()
            sqlite_insert = """INSERT INTO APP_DEVICE_MAP
                                       (ADM_ID, U_ID, CI_ID, VERSION, TIME) 
                                       VALUES (?, ?, ?, ?, ?);"""

            data_tuple = (aid, uid, ci, version, currentDateTime)
            ins_cursor.execute(sqlite_insert, data_tuple)
            ins_cursor.close()
            aid = aid + 1
        cursor.close()
        con.commit()
        logging.info(" .    Loaded ADM table.")

    except sqlite3.Error as error:
        logging.error("Error loading ADM table." + error)
    finally:
        return


def loadADFTable():
    try:
        logging.info(".     Loading ADF table")
        cursor = con.cursor()
        sql = """SELECT DISTINCT url, toip, fromip, level FROM RAW_NW_FLOW WHERE VERSION = ? """
        cursor.execute(sql, (version,))
        records = cursor.fetchall()

        aid = 0
        for row in records:
            url = row[0]
            toip = row[1]
            fromip = row[2]
            level = int(row[3])

            uid = None
            ci = None
            cf = None

            scursor = con.cursor()
            scursor.execute(
                "SELECT U_ID FROM URL WHERE U_NAME = ? AND VERSION = ?", (url, version))
            srecords = scursor.fetchall()
            for rows in srecords:
                uid = rows[0]
            scursor.close()

            scursor = con.cursor()
            scursor.execute(
                "SELECT CI_ID FROM CONFIGURATION_ITEM WHERE  CI_IPV4 = ? AND VERSION = ?", (toip, version))
            srecords = scursor.fetchall()
            for rows in srecords:
                ci = rows[0]
            scursor.close()

            scursor = con.cursor()
            scursor.execute(
                "SELECT CI_ID FROM CONFIGURATION_ITEM WHERE  CI_IPV4 = ? AND VERSION = ?", (fromip, version))
            srecords = scursor.fetchall()
            for rows in srecords:
                cf = rows[0]
            scursor.close()

            ins_cursor = con.cursor()
            sqlite_insert = """INSERT INTO APP_DEVICE_FLOW_MAP
                                       (ADF_ID, U_ID, U_LEVEL, CI_ID_FROM, CI_ID_TO, VERSION, TIME) 
                                       VALUES (?, ?, ?, ?, ?, ?, ?);"""

            data_tuple = (aid, uid, level, cf, ci, version, currentDateTime)
            ins_cursor.execute(sqlite_insert, data_tuple)
            ins_cursor.close()
            aid = aid + 1
        cursor.close()
        con.commit()
        logging.info(" .    Loaded ADF table.")

    except sqlite3.Error as error:
        logging.error("Error loading ADF table." + error)
    finally:
        return


def loadAFHTable():
    try:
        logging.info(".     Loading AFH table")
        cursor = con.cursor()
        sql = """SELECT A_ID FROM APPLICATION WHERE VERSION = ? """
        cursor.execute(sql, (version,))
        records = cursor.fetchall()
        for row in records:
            aid = row[0]
            dcid = 0
            cid = 0
            cifid = 0
            tcifhash = 0

            scursor = con.cursor()
            scursor.execute(
                "SELECT DC_ID FROM DATA_CENTER WHERE VERSION = ? AND DC_NAME <> 'URL'", (version,))
            srecords = scursor.fetchall()
            for rows in srecords:
                tcifhash = 0
                dcid = rows[0]
                # print('dcid: ',dcid)
                s1cursor = con.cursor()
                s1cursor.execute("SELECT DISTINCT CIF_ID FROM CONFIGURATION_ITEM "
                                 "WHERE A_ID = ? AND DC_ID = ? AND VERSION = ? and CIF_ID <> 4", (aid, dcid, version))
                s1records = s1cursor.fetchall()
                for rows1 in s1records:
                    cifid = rows1[0]
                    # print('cifid: ', cifid)
                    s2cursor = con.cursor()
                    s2cursor.execute("SELECT CIF_HASH FROM CONFIGURATION_ITEM_FUNCTION "
                                     "WHERE CIF_ID = ? AND VERSION = ?", (cifid, version))
                    s2records = s2cursor.fetchall()
                    for rows2 in s2records:
                        # print('hash: ',rows2[0])
                        tcifhash = tcifhash + rows2[0]
                        # print('tcifhash: ', tcifhash)
                    s2cursor.close()
                s1cursor.close()
                # print('====')

                if tcifhash != 0:
                    ins_cursor = con.cursor()
                    sqlite_insert = """INSERT INTO APP_FUNCTION_HASH(AFH_ID, A_ID, DC_ID, AFH_APP_HASH, 
                    AFH_APP_DIMENSIONAL_HASH, VERSION, TIME) VALUES (?, ?, ?, ?, ?, ?, ?) """
                    afhid = fake.random_int(min=10000, max=99999)
                    dt = (afhid, aid, dcid, tcifhash, 0, version, currentDateTime)
                    # print('dt: ',dt)
                    ins_cursor.execute(sqlite_insert, dt)
                    ins_cursor.close()
                    con.commit()
            scursor.close()
        cursor.close()
        logging.info(" .    Loaded AFH table.")

    except sqlite3.Error as error:
        logging.error("Error loading AFH table." + error)
    finally:
        return


def loadARHTable():
    try:
        logging.info(".     Loading ARH table")
        cursor = con.cursor()
        sql = """SELECT A_ID FROM APPLICATION WHERE VERSION = ? """
        cursor.execute(sql, (version,))
        records = cursor.fetchall()
        for row in records:
            aid = row[0]
            dcid = 0
            cid = 0
            cirid = 0
            tcirhash = 0

            scursor = con.cursor()
            scursor.execute(
                "SELECT DC_ID FROM DATA_CENTER WHERE VERSION = ? AND DC_NAME <> 'URL'", (version,))
            srecords = scursor.fetchall()
            for rows in srecords:
                tcirhash = 0
                dcid = rows[0]
                # print('dcid: ',dcid)
                s1cursor = con.cursor()
                s1cursor.execute("SELECT DISTINCT CIR_ID FROM CONFIGURATION_ITEM "
                                 "WHERE A_ID = ? AND DC_ID = ? AND VERSION = ?", (aid, dcid, version))
                s1records = s1cursor.fetchall()
                for rows1 in s1records:
                    cirid = rows1[0]
                    # print('cirid: ', cirid)
                    s2cursor = con.cursor()
                    s2cursor.execute("SELECT CIR_HASH FROM CONFIGURATION_ITEM_ROLE "
                                     "WHERE CIR_ID = ? AND VERSION = ?", (cirid, version))
                    s2records = s2cursor.fetchall()
                    for rows2 in s2records:
                        # print('hash: ',rows2[0])
                        tcirhash = tcirhash + rows2[0]
                        # print('tcirhash: ', tcirhash)
                    s2cursor.close()
                s1cursor.close()
                # print('====')

                #if tcirhash != 0:
                ins_cursor = con.cursor()
                sqlite_insert = """INSERT INTO APP_ROLE_HASH(ARH_ID, A_ID, DC_ID, ARH_APP_HASH, 
                ARH_APP_DIMENSIONAL_HASH, VERSION, TIME) VALUES (?, ?, ?, ?, ?, ?, ?) """
                arhid = fake.random_int(min=10000, max=99999)
                dt = (arhid, aid, dcid, tcirhash, 0, version, currentDateTime)
                # print('dt: ',dt)
                ins_cursor.execute(sqlite_insert, dt)
                ins_cursor.close()
                con.commit()
            scursor.close()
        cursor.close()
        logging.info(" .    Loaded ARH table.")

    except sqlite3.Error as error:
        logging.error("Error loading ARH table." + error)
    finally:
        return


def loadFRHTable():
    try:
        logging.info(".     Loading FRH table")
        cursor = con.cursor()
        sql = """SELECT U_ID, A_ID FROM URL WHERE VERSION = ? """
        cursor.execute(sql, (version,))
        records = cursor.fetchall()
        for row in records:
            uid = row[0]
            a_id = row[1]
            dc_id = 0
            cirhash = 0
            thash = 0
            scursor = con.cursor()
            scursor.execute("SELECT DISTINCT DC_ID FROM V_ALL_NWFLOW_DEVICES_EXTENDED "
                            "WHERE U_ID = ?", (uid,))
            srecords = scursor.fetchall()
            for rows in srecords:
                dcid = rows[0]
                s1cursor = con.cursor()
                s1cursor.execute("SELECT DISTINCT CIR_HASH FROM V_ALL_NWFLOW_DEVICES_EXTENDED "
                                 "WHERE U_ID = ? AND DC_ID = ?", (uid, dcid))
                s1records = s1cursor.fetchall()
                for rows1 in s1records:
                    cirhash = rows1[0]
                    thash = thash + cirhash
                s1cursor.close()

                ins_cursor = con.cursor()
                sqlite_insert = """INSERT INTO FLOW_ROLE_HASH(FRH_ID, A_ID, U_ID, DC_ID, FRH_APP_HASH, FRH_APP_DIMENSIONAL_HASH, VERSION, TIME) 
                                           VALUES (?, ?, ?, ?, ?, ?, ?, ?) """
                arhid = fake.random_int(min=10000, max=99999)
                dt = (arhid, a_id, uid, dcid, thash, 0, version, currentDateTime)
                ins_cursor.execute(sqlite_insert, dt)
                ins_cursor.close()
                con.commit()
            scursor.close()
        cursor.close()
        logging.info(" .    Loaded FRH table.")

    except sqlite3.Error as error:
        logging.error("Error loading FRH table." + error)
    finally:
        return


def loadFFHTable():
    try:
        logging.info(".     Loading FFH table")
        cursor = con.cursor()
        sql = """SELECT U_ID, A_ID FROM URL WHERE VERSION = ? """
        cursor.execute(sql, (version,))
        records = cursor.fetchall()
        for row in records:
            uid = row[0]
            a_id = row[1]
            dc_id = 0
            cifhash = 0
            thash = 0
            scursor = con.cursor()
            scursor.execute(
                "SELECT DISTINCT DC_ID FROM V_ALL_NWFLOW_DEVICES_EXTENDED WHERE U_ID = ?", (uid,))
            srecords = scursor.fetchall()
            for rows in srecords:
                dcid = rows[0]
                s1cursor = con.cursor()
                s1cursor.execute("SELECT DISTINCT CIF_HASH FROM V_ALL_NWFLOW_DEVICES_EXTENDED "
                                 "WHERE U_ID = ? AND DC_ID = ?", (uid, dcid))
                s1records = s1cursor.fetchall()
                for rows1 in s1records:
                    cifhash = rows1[0]
                    thash = thash + cifhash
                s1cursor.close()

                if thash != 0:
                    ins_cursor = con.cursor()
                    sqlite_insert = """INSERT INTO FLOW_FUNCTION_HASH(FFH_ID, A_ID, U_ID, DC_ID, FFH_APP_HASH, FFH_APP_DIMENSIONAL_HASH, VERSION, TIME) 
                                               VALUES (?, ?, ?, ?, ?, ?, ?, ?) """
                    arhid = fake.random_int(min=10000, max=99999)
                    dt = (arhid, a_id, uid, dcid, thash, 0, version, currentDateTime)
                    ins_cursor.execute(sqlite_insert, dt)
                    ins_cursor.close()
                    con.commit()
            scursor.close()
        cursor.close()
        logging.info(" .    Loaded FFH table.")

    except sqlite3.Error as error:
        logging.error("Error loading FFH table." + error)
    finally:
        return


def loadDistTable():
    try:
        logging.info(".     Loading DATA_CENTER_DISTANCES table")
        cursor = con.cursor()
        sql = """SELECT DC_NAME FROM DATA_CENTER WHERE VERSION = ? """
        cursor.execute(sql, (version,))
        records = cursor.fetchall()
        dcs = []
        dcc = []
        for row in records:
            dcR = row[0]
            if dcR != 'URL':
                dcs.append(dcR)
        dcc = rSubset(dcs)
        for element in dcc:

            lat1 = 0
            lon1 = 0
            lat2 = 0
            lon2 = 0

            sqlc1 = """SELECT LAT, LON FROM CITY_DATA WHERE UPPER(CITY) = ? AND VERSION = ? """
            cursor.execute(sqlc1, (element[0], version))
            recordsc1 = cursor.fetchall()
            for rowa in recordsc1:
                lat1 = rowa[0]
                lon1 = rowa[1]

            sqlc2 = """SELECT LAT, LON FROM CITY_DATA WHERE UPPER(CITY) = ? AND VERSION = ? """
            cursor.execute(sqlc2, (element[1], version))
            recordsc2 = cursor.fetchall()
            for rowb in recordsc2:
                lat2 = rowb[0]
                lon2 = rowb[1]

            distance = distancebetween(lat1, lat2, lon1, lon2)
            sqli = ''' INSERT INTO DATA_CENTER_DISTANCES(DCD_NAME1, DCD_NAME2, DCD_DISTANCE, VERSION, TIME)
                        VALUES(?,?,?,?,?) '''
            rec = (element[0], element[1], distance, version, currentDateTime)
            cursor.execute(sqli, rec)
            #sqli = ''' INSERT INTO DATA_CENTER_DISTANCES(DCD_NAME2, DCD_NAME1, DCD_DISTANCE, VERSION, TIME)
            #            VALUES(?,?,?,?,?) '''
            #rec = (element[0], element[1], distance, version, currentDateTime)
            #cursor.execute(sqli, rec)
        con.commit()
        logging.info(" .    Loaded DATA_CENTER_DISTANCES table.")

    except sqlite3.Error as error:
        logging.error("Error loading DATA_CENTER_DISTANCES table." + error)
    finally:
        return


def loadCapacityTable():
    global oorDist
    try:
        logging.info(".     Loading CAPACITY table")
        cursor = con.cursor()
        sql = '''SELECT A_ID,A_NAME,TA_ID FROM APPLICATION WHERE VERSION = ?'''
        cursor.execute(sql, (version,))
        records = cursor.fetchall()
        aid = 0
        for row in records:
            aid = row[0]
            aname = row[1]
            taid = row[2]

            sqldnd = '''SELECT  DISTINCT DC_NAME, A_NAME, A_ID FROM V_ALL_APP_DEVICES_EXTENDED WHERE DC_NAME <> 'URL' 
            AND A_NAME = ? ORDER BY A_NAME '''
            cursornd = con.cursor()
            cursornd.execute(sqldnd, (aname,))
            recordsnd = cursornd.fetchall()
            dcs = []
            dcc = []
            for rownd in recordsnd:
                dcnc = rownd[0]
                dcR = dcnc
                dcs.append(dcR)
            dcc = rSubset(dcs)
            cursornd.close()
            for element in dcc:
                dc1 = element[0]
                dc2 = element[1]

                sqldcd = '''SELECT DCD_NAME1, DCD_NAME2, DCD_DISTANCE FROM DATA_CENTER_DISTANCES WHERE DCD_NAME1 = ? AND 
                DCD_NAME2 = ? AND VERSION = ? '''
                cursordcd = con.cursor()
                cursordcd.execute(sqldcd, (dc1, dc2, version))
                recordsdcd = cursordcd.fetchall()
                dcdname1 = None
                dcdname2 = None
                dcddist = None
                for rowdcd in recordsdcd:
                    dcdname1 = rowdcd[0]
                    dcdname2 = rowdcd[1]
                    dcddist = rowdcd[2]
                    oor = None
                    cifid = None
                    sqlcif = '''SELECT DISTINCT CIF_ID, CIF_NAME FROM V_ALL_DEVICE_COUNTS WHERE CIF_NAME <> 'URL' AND 
                    A_ID = ? '''
                    cursorcif = con.cursor()
                    cursorcif.execute(sqlcif, (aid,))
                    recordscif = cursorcif.fetchall()
                    for rowscif in recordscif:
                        cifid = rowscif[0]
                        cifname = rowscif[1]
                        sqlcnt = '''SELECT A_NAME, A_ID, TA_ID, DC_NAME, DC_ID, CIF_NAME, CIF_ID, DEVICE_COUNT FROM 
                        V_ALL_DEVICE_COUNTS WHERE A_ID = ? AND CIF_ID = ? AND DC_NAME = ?'''
                        cursorcnt = con.cursor()
                        cursorcnt.execute(sqlcnt, (aid, cifid, dcdname1))
                        recordscnt = cursorcnt.fetchall()
                        dcount1 = 0
                        dcount2 = 0
                        dc1 = None
                        dc2 = None
                        ck1 = 0
                        ck2 = 0
                        for rowscnt in recordscnt:
                            ck1 = 1
                            dcount1 = rowscnt[7]
                            dc1 = rowscnt[3]
                        sqlcnt2 = '''SELECT A_NAME, A_ID, TA_ID, DC_NAME, DC_ID, CIF_NAME, CIF_ID, DEVICE_COUNT 
                        FROM V_ALL_DEVICE_COUNTS WHERE A_ID = ? AND CIF_ID = ? AND DC_NAME = ?'''
                        cursorcnt2 = con.cursor()
                        cursorcnt2.execute(sqlcnt2, (aid, cifid, dcdname2))
                        recordscnt2 = cursorcnt2.fetchall()
                        ck2 = 0
                        for rowscnt2 in recordscnt2:
                            ck2 = 1
                            dcount2 = rowscnt2[7]
                            dc2 = rowscnt2[3]
                        cursorcnt2.close()
                        cursorcnt.close()
                        small = 0
                        large = 0
                        pcap = 0
                        if dcount1 >= dcount2:
                            large = dcount1
                            small = dcount2
                        else:
                            large = dcount2
                            small = dcount1
                        if large != 0:
                            pcap = small / large * 100

                        if dcddist > oorDist:
                            oor = 1
                        else:
                            oor = 0

                        ##
                        #if ck1 == 0 and ck2 == 0:
                        #    x = 1
                        #else:
                        sqli = '''INSERT INTO APP_CAPACITY_INFORMATION(A_NAME, A_ID, TA_ID, CIF_NAME, CIF_ID, 
                        DATACENTER1, DEVICE_COUNT1, DATACENTER2, DEVICE_COUNT2, PERCENTCAPACITY, OUT_OF_REGION, 
                        VERSION, TIME) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) '''
                        rec = (aname, aid, taid, cifname, cifid, dcdname1, dcount1, dcdname2, dcount2, pcap, oor,
                               version, currentDateTime)
                        cursori = con.cursor()
                        cursori.execute(sqli, rec)
                        con.commit()
                        cursori.close()
                        ##
                    cursorcif.close()
                cursordcd.close()
        cursor.close()

        logging.info(" .    Loaded CAPACITY table.")

    except sqlite3.Error as error:
        logging.error("Error loading CAPACITY table." + error)
    finally:
        return


def loadFlowCapacityTable():
    global oorDist
    try:
        logging.info(".     Loading FLOW CAPACITY table")
        cursor = con.cursor()
        sql = '''SELECT U_ID,U_NAME,A_ID FROM URL WHERE VERSION = ?'''
        cursor.execute(sql, (version,))
        records = cursor.fetchall()
        aid = 0
        for row in records:
            uid = row[0]
            uname = row[1]
            aid = row[2]
            sqldnd = '''SELECT  DISTINCT DC_NAME, U_NAME, U_ID FROM V_ALL_NWFLOW_DEVICES_EXTENDED WHERE DC_NAME <> 'URL' 
             AND U_NAME = ? ORDER BY U_NAME '''
            cursornd = con.cursor()
            cursornd.execute(sqldnd, (uname,))
            recordsnd = cursornd.fetchall()
            dcs = []
            dcc = []
            for rownd in recordsnd:
                dcnc = rownd[0]
                dcR = dcnc
                dcs.append(dcR)
            dcc = rSubset(dcs)
            cursornd.close()
            for element in dcc:
                dc1 = element[0]
                dc2 = element[1]

                sqldcd = '''SELECT DCD_NAME1, DCD_NAME2, DCD_DISTANCE FROM DATA_CENTER_DISTANCES WHERE DCD_NAME1 = ? AND 
                DCD_NAME2 = ? AND VERSION = ? '''
                cursordcd = con.cursor()
                cursordcd.execute(sqldcd, (dc1, dc2, version))
                recordsdcd = cursordcd.fetchall()

                dcdname1 = None
                dcdname2 = None
                dcddist = None

                for rowdcd in recordsdcd:
                    dcdname1 = rowdcd[0]
                    dcdname2 = rowdcd[1]
                    dcddist = rowdcd[2]
                    oor = None
                    cifid = None
                    cifname = None
                    sqlcif = '''SELECT DISTINCT CIF_ID, CIF_NAME FROM V_NWFLOW_DEVICE_COUNTS WHERE CIF_NAME <> 'URL' AND 
                    U_ID = ? '''
                    cursorcif = con.cursor()
                    cursorcif.execute(sqlcif, (uid,))
                    recordscif = cursorcif.fetchall()

                    for rowscif in recordscif:
                        cifname = rowscif[1]
                        cifid = rowscif[0]
                        sqlcnt = '''SELECT U_NAME, U_ID, A_NAME, A_ID, TA_NAME, TA_ID, DC_NAME, DC_ID, CIF_NAME, CIF_ID, 
                        DEVICE_COUNT FROM V_NWFLOW_DEVICE_COUNTS WHERE U_ID = ? AND CIF_ID = ? AND DC_NAME = ? '''
                        cursorcnt = con.cursor()
                        cursorcnt.execute(sqlcnt, (uid, cifid, dcdname1))
                        recordscnt = cursorcnt.fetchall()
                        aname1 = None
                        tname1 = None
                        tid1 = None
                        aname2 = None
                        tname2 = None
                        tid2 = None
                        dcount1 = 0
                        dcount2 = 0
                        dc1 = None
                        dc2 = None
                        ck1 = 0
                        ck2 = 0
                        for rowscnt in recordscnt:
                            ck1 = 1
                            aname1 = rowscnt[2]
                            tname1 = rowscnt[4]
                            tid1 = rowscnt[5]
                            dcount1 = rowscnt[10]
                        sqlcnt2 = '''SELECT U_NAME, U_ID, A_NAME, A_ID, TA_NAME, TA_ID, DC_NAME, DC_ID, CIF_NAME, CIF_ID, 
                        DEVICE_COUNT FROM V_NWFLOW_DEVICE_COUNTS WHERE U_ID = ? AND CIF_ID = ? AND DC_NAME = ? '''
                        cursorcnt2 = con.cursor()
                        cursorcnt2.execute(sqlcnt2, (uid, cifid, dcdname2))
                        recordscnt2 = cursorcnt2.fetchall()
                        ck2 = 0
                        for rowscnt2 in recordscnt2:
                            ck2 = 1
                            aname2 = rowscnt2[2]
                            tname2 = rowscnt2[4]
                            tid2 = rowscnt2[5]
                            dcount2 = rowscnt2[10]
                        cursorcnt2.close()
                        cursorcnt.close()
                        small = 0
                        large = 0
                        pcap = 0
                        if dcount1 > dcount2:
                            large = dcount1
                            small = dcount2
                        else:
                            large = dcount2
                            small = dcount1
                        if large != 0:
                            pcap = small / large * 100

                        if aname1 is not None:
                            aname = aname1
                        if aname2 is not None:
                            aname = aname2
                        if tname1 is not None:
                            tname = tname1
                        if tname2 is not None:
                            tname = tname2
                        if tid1 is not None:
                            taid = tid1
                        if tid2 is not None:
                            taid = tid2

                        if dcddist > oorDist:
                            oor = 1
                        else:
                            oor = 0

                        sqli = '''INSERT INTO NWFLOW_CAPACITY_INFORMATION(U_NAME, U_ID, A_NAME, A_ID, TA_NAME, TA_ID, 
                        CIF_NAME, CIF_ID, DATACENTER1, DEVICE_COUNT1, DATACENTER2, DEVICE_COUNT2, PERCENTCAPACITY, 
                        OUT_OF_REGION, VERSION, TIME) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) '''
                        rec = (
                            uname, uid, aname, aid, tname, taid,
                            cifname, cifid, dcdname1, dcount1, dcdname2, dcount2,
                            pcap, oor,
                            version, currentDateTime)
                        cursori = con.cursor()
                        cursori.execute(sqli, rec)
                        con.commit()
                        cursori.close()
                    cursorcif.close()
                cursordcd.close()
        cursor.close()

        logging.info(" .    Loaded FLOW CAPACITY table.")

    except sqlite3.Error as error:
        logging.error("Error loading FLOW CAPACITY table." + error)
    finally:
        return


def loadSDCTable():
    try:
        logging.info(".     Loading SINGLE DATA CENTER table")
        cursor = con.cursor()
        sql = """create TABLE TSDC AS WITH SDC AS (SELECT A_NAME, A_ID,  TA_ID, 
        CIF_NAME, CIF_ID, SUM(PERCENTCAPACITY) AS SC 
        FROM APP_CAPACITY_INFORMATION  GROUP BY A_NAME, CIF_NAME)
        SELECT * FROM SDC"""
        cursor.execute(sql)
        con.commit()

        sqli = '''INSERT INTO SINGLE_DATA_CENTER SELECT A_NAME, A_ID, TA_ID, -15 AS SCORE, 0, 0 FROM TSDC WHERE SC = 0 
        group by A_NAME '''
        cursor.execute(sqli)
        con.commit()

        sql = """DROP TABLE TSDC"""
        cursor.execute(sql)

        sqli = '''UPDATE SINGLE_DATA_CENTER SET VERSION = ?, TIME = ?'''
        cursor = con.cursor()
        rec = (version, currentDateTime)
        cursor.execute(sqli, rec)

        cursor.close()
        con.commit()
        logging.info(" .    Loaded SINGLE DATA CENTER table.")

    except sqlite3.Error as error:
        logging.error("Error loading SINGLE DATA CENTER table." + error)
    finally:
        return


if __name__ == '__main__':
    start = time()
    if rebuildAll == 'True':
        delete_tables()
        create_tables()
    loadRAW()
    loadBaseTables()
    loadCITable()
    loadURLTable()
    loadADMTable()
    loadADFTable()
    loadAFHTable()
    loadARHTable()
    loadDistTable()
    loadFFHTable()
    loadFRHTable()
    loadCapacityTable()
    loadFlowCapacityTable()
    loadSDCTable()
    con.close()
    elapsed = time() - start
    logging.info('Created scoreDB: {}'.format(elapsed))
