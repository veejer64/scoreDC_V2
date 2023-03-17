import csv
import random
from configparser import ConfigParser
from time import time
import logging
from configparser import ConfigParser
from faker import Faker
from faker.providers import internet

#  Load configuration items as Globals.
config_file = '/Users/veejer/dev/projects/scoreDC/data/config/config.ini'
config = ConfigParser()
config.read(config_file)
db_file = config.get('database', 'db_file')
loglevel = config.get('logging', 'loglevel')
cio_file = config.get('input_files', 'cio_file')
nw_flow_file = config.get('input_files', 'nw_flow_file')
inventory_file = config.get('input_files', 'inventory_file')
dr_results_file = config.get('input_files','dr_results_file')
test_cio_file = config.get('input_files', 'test_cio_file')
test_nw_flow_file = config.get('input_files', 'test_nw_flow_file')
test_inventory_file = config.get('input_files', 'test_inventory_file')
test_dr_results_file = config.get('input_files','test_dr_results_file')
TXN_COUNT = int(config.get('input_files', 'test_txn_record_count'))

#  Initialize logging.
logger = logging.getLogger()
logger.setLevel(eval(loglevel))
logging.info('Generating test data for import as .csv file.')
logging.info('Number of transactions to simulate:' + str(TXN_COUNT))
logging.info('NW FLOW OUTPUT FILE: ' + test_nw_flow_file)
logging.info('CIO OUTPUT FILE: ' + test_cio_file)
logging.info('INVENTORY FILE: ' + test_inventory_file)
logging.info('DR RESULTS FILE: ' + test_dr_results_file)

fake = Faker()
fake.add_provider(internet)

#  Choices for random.
device_list = ['APP SERVER', 'DATABASE SERVER', 'WEB SERVER', 'SLB']
full_device_list = ['APP SERVER', 'DATABASE SERVER', 'WEB SERVER', 'URL', 'SLB', 'WAF', 'DNSEntryPoint', 'DP',
                    'MAINFRAME LPAR']
data_center = ['ATLANTA', 'KANSAS CITY', 'DES MOINES', 'LOS ANGELES']
#  Hack to weigh PROD higher, reduce to one for more even distribution
roles = ['PROD', 'PROD', 'PROD', 'PROD', 'PROD', 'PROD', 'PROD', 'PROD', 'PROD', 'PROD', 'PRODBCP']
tech_area = ['MORTGAGE', 'TECHNOLOGY', 'TELLER', 'CUSTOMER SERVICE', 'ADVISORS']
app_status = ['DEPLOYED', 'END OF LIFE']
dr_test_result = ['PASS', 'FAIL']
app_rto = ['0', '1']

#  Globals
RCOUNT = 0
CCOUNT = 0
ICOUNT = 0
DCOUNT = 0


def writerec(writer, name, shortname, url_id, url, level, fromip, frompsr, fromdvcname, fromdvcfunction,
             fromdvcdatacenter,
             fromdvcrole, toip, todvcname, topsr, todvcfunction, todvcdatacenter, todvcrole):
    global RCOUNT
    writer.writerow(
        {
            'name': name,
            'shortname': shortname,
            'url_id': url_id,
            'url': url,
            'level': level,
            'fromip': fromip,
            'frompsr': frompsr,
            'fromdvcname': fromdvcname,
            'fromdvcfunction': fromdvcfunction,
            'fromdvcdatacenter': fromdvcdatacenter,
            'fromdvcrole': fromdvcrole,
            'toip': toip,
            'topsr': topsr,
            'todvcname': todvcname,
            'todvcfunction': todvcfunction,
            'todvcdatacenter': todvcdatacenter,
            'todvcrole': todvcrole
        }
    )
    RCOUNT = RCOUNT + 1


def writeciorec(writer, name, techarea, url):
    global CCOUNT
    writer.writerow(
        {
            'Application_Name': name,
            'Technology_Area': techarea,
            'URL': url
        }
    )
    CCOUNT = CCOUNT + 1

def writedrrec(writer, name, result):
    global DCOUNT
    writer.writerow(
        {
            'Application_Name': name,
            'DR_Test_Result': result
        }
    )
    DCOUNT = DCOUNT + 1

def writeinvrec(writer, datacenter, shortname, name, app_guid, app_status, app_rto, device_name, device_guid, device_ip,
                device_function, environment):
    global ICOUNT
    writer.writerow(
        {
            'datacenter': datacenter,
            'shortname': shortname,
            'name': name,
            'app_guid': app_guid,
            'app_status': app_status,
            'app_rto': app_rto,
            'device_name': device_name,
            'device_guid': device_guid,
            'device_ip': device_ip,
            'device_function': device_function,
            'environment': environment
        }
    )
    ICOUNT = ICOUNT + 1


def geninvrec(writer, shortname, name, app_guid, app_status, app_rto):
    hostname = fake.hostname()
    function = random.choice(device_list)
    datacenter = random.choice(data_center)
    role = random.choice(roles)
    ip = fake.ipv4()
    dev_guid = fake.random_int(min=10000, max=99999)
    writeinvrec(writer, datacenter, shortname, name, app_guid, app_status, app_rto,
                hostname, dev_guid, ip, function, role)


def create_csv_file():
    with open(test_inventory_file, 'w', newline='') as invfile:
        invfieldnames = ['datacenter', 'shortname', 'name', 'app_guid', 'app_status', 'app_rto',
                         'device_name', 'device_guid', 'device_ip', 'device_function', 'environment']
        invwriter = csv.DictWriter(invfile, fieldnames=invfieldnames)
        invwriter.writeheader()

        with open(test_nw_flow_file, 'w', newline='') as csvfile:
            fieldnames = ['name', 'shortname', 'url_id', 'url', 'level',
                          'fromip', 'frompsr', 'fromdvcname', 'fromdvcfunction', 'fromdvcdatacenter',
                          'fromdvcrole', 'toip', 'todvcname', 'topsr', 'todvcfunction', 'todvcdatacenter', 'todvcrole']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            with open(test_cio_file, 'w', newline='') as ciofile:
                ciofieldnames = ['Application_Name', 'Technology_Area', 'URL']
                ciowriter = csv.DictWriter(ciofile, fieldnames=ciofieldnames)
                ciowriter.writeheader()
                with open(test_dr_results_file, 'w', newline='') as drfile:
                    drfieldnames = ['Application_Name', 'DR_Test_Result']
                    drwriter = csv.DictWriter(drfile, fieldnames=drfieldnames)
                    drwriter.writeheader()
                    r = 0
                    for i in range(0, TXN_COUNT):
                        r = r + 1
                        logging.debug("TOP LEVEL " + str(r))
                        name = fake.bs()
                        shortname = fake.slug()
                        ta = random.choice(tech_area)
                        uia = fake.random_int(min=1, max=3)
                        ucnt = 0
                        for urls_in_app in range(0, uia):
                            url_id = fake.random_int(min=100, max=199)
                            url = fake.uri()
                            ucnt = ucnt + 1
                            app_stat = random.choice(app_status)
                            app_guid = fake.random_int(min=10000, max=99999)
                            rto = random.choice(app_rto)
                            writeciorec(ciowriter, name, ta, url)
                            if ucnt == 1:
                                writedrrec(drwriter,name,random.choice(dr_test_result))
                            # r1End = fake.random_int(min=1, max=3)
                            # for datacenters in range(0, r1End):
                            ipA = fake.ipv4()
                            roleA = random.choice(roles)
                            r2End = fake.random_int(min=1, max=2)
                            for level0 in range(0, r2End):
                                ipB = fake.ipv4()
                                hostnameB = fake.hostname()
                                functionB = random.choice(device_list)
                                datacenterB = random.choice(data_center)
                                roleB = random.choice(roles)
                                dev_guidB = fake.random_int(min=10000, max=99999)

                                writerec(writer, name, shortname, url_id, url, 0, ipA,
                                         fake.random_int(min=100, max=199),
                                         url,
                                         'URL', 'URL', roleA, ipB, hostnameB, fake.random_int(min=100, max=199),
                                         functionB,
                                         datacenterB, roleB)
                                writeinvrec(invwriter, 'URL', shortname, name, app_guid, app_stat, rto,
                                            url, '0', ipA, 'URL', roleA)
                                writeinvrec(invwriter, datacenterB, shortname, name, app_guid, app_stat, rto,
                                            hostnameB, dev_guidB, ipB, functionB, roleB)
                                ipC = fake.ipv4()
                                hostnameC = fake.hostname()
                                functionC = random.choice(device_list)
                                datacenterC = random.choice(data_center)
                                roleC = random.choice(roles)
                                dev_guidC = fake.random_int(min=10000, max=99999)
                                writerec(writer, name, shortname, url_id, url, 1, ipB,
                                         fake.random_int(min=100, max=199),
                                         hostnameB,
                                         functionB, datacenterB, roleB, ipC, hostnameC,
                                         fake.random_int(min=100, max=199),
                                         functionC, datacenterC, roleC)
                                writeinvrec(invwriter, datacenterC, shortname, name, app_guid, app_stat, rto, hostnameC,
                                            dev_guidC, ipC, functionC, roleC)

                                r3End = fake.random_int(min=1, max=2)
                                for level1 in range(0, r3End):
                                    ipD = fake.ipv4()
                                    hostnameD = fake.hostname()
                                    functionD = random.choice(device_list)
                                    datacenterD = random.choice(data_center)
                                    roleD = random.choice(roles)
                                    dev_guidD = fake.random_int(min=10000, max=99999)
                                    writerec(writer, name, shortname, url_id, url, 2, ipC,
                                             fake.random_int(min=100, max=199),
                                             hostnameC,
                                             functionC, datacenterC, roleC, ipD, hostnameD,
                                             fake.random_int(min=100, max=199),
                                             functionD, datacenterD, roleD)
                                    writeinvrec(invwriter, datacenterD, shortname, name, app_guid, app_stat, rto,
                                                hostnameD, dev_guidD, ipD, functionD, roleD)
                                    ipE = fake.ipv4()
                                    hostnameE = fake.hostname()
                                    functionE = random.choice(device_list)
                                    datacenterE = random.choice(data_center)
                                    roleE = random.choice(roles)
                                    dev_guidE = fake.random_int(min=10000, max=99999)
                                    writerec(writer, name, shortname, url_id, url, 3, ipD,
                                             fake.random_int(min=100, max=199),
                                             hostnameD,
                                             functionD, datacenterD, roleD, ipE, hostnameE,
                                             fake.random_int(min=100, max=199),
                                             functionE, datacenterE, roleE)
                                    writeinvrec(invwriter, datacenterE, shortname, name, app_guid, app_stat, rto,
                                                hostnameE, dev_guidE, ipE, functionE, roleE)
                                    r4End = fake.random_int(min=1, max=2)
                                    for level2 in range(0, r4End):
                                        ipF = fake.ipv4()
                                        hostnameF = fake.hostname()
                                        functionF = random.choice(device_list)
                                        datacenterF = random.choice(data_center)
                                        roleF = random.choice(roles)
                                        dev_guidF = fake.random_int(min=10000, max=99999)
                                        writerec(writer, name, shortname, url_id, url, 4, ipE,
                                                 fake.random_int(min=100, max=199),
                                                 hostnameE,
                                                 functionE, datacenterE, roleE, ipF, hostnameF,
                                                 fake.random_int(min=100, max=199),
                                                 functionF, datacenterF, roleF)
                                        writeinvrec(invwriter, datacenterF, shortname, name, app_guid, app_stat, rto,
                                                    hostnameF, dev_guidF, ipF, functionF, roleF)
                                        ipG = fake.ipv4()
                                        hostnameG = fake.hostname()
                                        functionG = random.choice(device_list)
                                        datacenterG = random.choice(data_center)
                                        roleG = random.choice(roles)
                                        dev_guidG = fake.random_int(min=10000, max=99999)
                                        writerec(writer, name, shortname, url_id, url, 5, ipF,
                                                 fake.random_int(min=100, max=199),
                                                 hostnameF,
                                                 functionF, datacenterF, roleF, ipG, hostnameG,
                                                 fake.random_int(min=100, max=199),
                                                 functionG, datacenterG, roleG)
                                        writeinvrec(invwriter, datacenterG, shortname, name, app_guid, app_stat, rto,
                                                    hostnameG, dev_guidG, ipG, functionG, roleG)

                                values = range(fake.random_int(min=0, max=7))
                                for ii in values:
                                    geninvrec(invwriter, shortname, name, app_guid, app_stat, rto)


if __name__ == '__main__':
    start = time()
    create_csv_file()
    elapsed = time() - start
    logging.info('Created test transaction network flow csv file time: {}'.format(elapsed))
    logging.info('Transactions simulated:  ' + str(TXN_COUNT))
    logging.info('TXN Records written: ' + str(RCOUNT))
    logging.info('CIO Records written: ' + str(CCOUNT))
    logging.info('INVENTORY Records written: ' + str(ICOUNT))
    logging.info('DR RESULTS written: ' + str(DCOUNT))
