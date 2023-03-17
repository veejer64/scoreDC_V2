
import logging
import sqlite3
import vdb.db

con = sqlite3.connect('/Users/veejer/dev/projects/scoreDC_V2/data/db/scoreDB.db')
cursor = con.cursor()


sqli = '''INSERT INTO SINGLE_DATA_CENTER SELECT A_NAME, A_ID, TA_ID, -15 AS SCORE,0 ,0 FROM TSDC WHERE SC = 0 
group by A_NAME '''
cursor.execute(sqli)
con.commit()

cursor.close()
con.commit()

