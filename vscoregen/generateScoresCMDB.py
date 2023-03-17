from configparser import ConfigParser
from itertools import combinations
from vscoregen import scoreSingleDataCenter, scoreDRTesting, scoreRoles
from time import time
import logging
from configparser import ConfigParser
import datetime

#  Load configuration items as Globals.
config_file = '/Users/veejer/dev/projects/scoreDC_V2/data/config/config.ini'
config = ConfigParser()
config.read(config_file)
loglevel = config.get('logging', 'loglevel')

#  Initialize logging.
logger = logging.getLogger()
logger.setLevel(eval(loglevel))
logging.info('scoreDC Generating CMDB Scores.')

#  Globals

if __name__ == '__main__':
    start = time()
    scoreSingleDataCenter.init()
    scoreDRTesting.init()
    scoreRoles.init()
    elapsed = time() - start
    logging.info('scoreDC CMDB Resiliency Scores Generated in: {}'.format(elapsed))
