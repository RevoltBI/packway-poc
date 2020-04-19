# modules

import csv
import timeit
from snowflake.ingest import SimpleIngestManager
from snowflake.ingest import StagedFile
from snowflake.ingest.utils.uris import DEFAULT_SCHEME
import time
import datetime
import os
from datetime import timedelta

from requests import HTTPError
import logging
import logging.handlers
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from cryptography.hazmat.backends import default_backend
import random
import string
from datetime import datetime
import snowflake.connector
import re

import pandas as pd
#import pyarrow
import sys
import jaydebeapi

LOG_FILE_NAME="packway.application.log"

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

fh = logging.handlers.TimedRotatingFileHandler(LOG_FILE_NAME,when='midnight', backupCount=14)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.info("Starting up ...")
logger.info("Connecting to DB")
# set DB2 connection
c = jaydebeapi.connect(
    "com.ibm.as400.access.AS400JDBCDriver",
    "jdbc:as400://192.168.78.78;libraries=S0629D7R;",
    ["tableau", "tableau"],
    "C:\\Users\\revolt\\jt400-10.1.jar", )

# pozor na spravnou cestu k .jar

curs = c.cursor()


logger.info("Pripojeno k DB2")

# variables
snow_user = "ETL_PROCESS_PACKWAY_001"
snow_password = "Packway191919"
snow_rsa_key_location = "C:/Users/revolt/ETL_packway_rsa_key.p8"
# užít správnou cestu rsa_key
snow_rsa_password = "Packway123"

snow_account = 'hw22839'
snow_location = 'eu-central-1'
snow_database = "PACKWAY_POC"
snow_schema = "PACKWAY_POC"

tables = ["OBJEDNAVKA", "OBJLINSTAV"]

batch_limit = 190000

qr_DB2 = [
    "select  id, objednavka,spolecnost, projekt, dopravce,objradku, objkusu, balik, balikradku, balikkusu, stav,picker,packer from TABLEAU.objednavky WHERE ID >  {0} and  ID <=  {0} + " + str(
        batch_limit)
    ,
    "SELECT  ID, SPOLECNOST, OBJEDNAVKA, RADEK, DATUM, CAS, STAVDRIV, STAV, OSC,balik FROM tableau.OBJLINSTAV WHERE ID >  {0} and  ID <=  {0} + " + str(
        batch_limit)
    ]
# ,"select stav, popis from tableau.stavy WHeRE STAV > {0}"

# set snowflake connection
ctx = snowflake.connector.connect(user=snow_user,
                                  password=snow_password,
                                  account=snow_account + "." + snow_location)
cs = ctx.cursor()

# snowflake adjustments
cs.execute("use database {0}; ".format(snow_database))
cs.execute("use schema {0}; ".format(snow_schema))

# snowpipe
# setting up ingest manager
with open(snow_rsa_key_location, 'rb') as pem_in:
    pemlines = pem_in.read()
    private_key_obj = load_pem_private_key(pemlines, snow_rsa_password.encode(), default_backend())

private_key_text = private_key_obj.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                                                 serialization.NoEncryption()).decode('utf-8')

start = time.time()
last_write = []
last_id = []
write_num = []
first = []
to_print = []
for t1 in enumerate(tables):
    # setting up pipe
    cs.execute("create or replace  pipe PIPE_" + t1[1] + "  as copy into " + t1[1] + " from @PACKWAY_POC")
    to_print.append(False)
    last_write.append(start)
    write_num.append(0)
    last_id.append(float(0))
    first.append(1)

# parameters
run_time_sec = 40000
# testovaci interval-po vyprseni se skript ukonci !!!

snowpipe_interval = 15
write_batch_to_db2 = 100

loop = 0
x=[None]*2
write_id = ""
rows_piped = None

cs = ctx.cursor()
logger.info("Pripojeno k Snowflake")

while (time.time() - start) < run_time_sec:
    loop += 1
    for t1 in enumerate(tables):
        if t1[0] == 0:
            ids = "\"id\""
        else:
            ids = "\"id\""
 
        if first[t1[0]] == 1:   
            cs.execute("use warehouse ETL_WH")
            cs.execute("ALTER WAREHOUSE IF EXISTS ETL_WH RESUME IF SUSPENDED")
            #zde se ptam na posledi id polozky v snowflake
            x[t1[0]] = cs.execute("select max({3}) from  {0}.{1}.{2};".format(snow_database,snow_schema,t1[1],ids)).fetchone()[0]
        if x[t1[0]] == None :
                last_id[t1[0]] = 0 
        elif x[t1[0]] >= last_id[t1[0]]:
                last_id[t1[0]] = x[t1[0]]

                
        first[t1[0]] = 2

        if (time.time() - last_write[t1[0]]) > snowpipe_interval:
            logger.info("%s jde do kontroly", t1[1])
            last_write[t1[0]] = time.time()
            write_num[t1[0]] += 1
            write_id = datetime.now().strftime("%Y%m%d_%H%M%S")

            # read from DB2
            curs.execute(qr_DB2[t1[0]].format(last_id[t1[0]]))
            row = curs.fetchall()

            logger.info("Po DB2")

            # analyse batch
            batch_ids = []
            if row:
                logger.info("Mame radky: %s po poslednim id: %s", str(len(row)), str(last_id[t1[0]]))
                for r in row:
                    batch_ids.append(float(r[0]))

                # najde novou posledni hodnotu
                last_id[t1[0]] = max(max(batch_ids), last_id[t1[0]])

                df = pd.DataFrame(row)
                df.to_csv('batch_to_snowpipe_{0}_{1}.csv'.format(t1[1], write_id), index=False, header=False,
                          encoding="utf-8")
                logger.info("tisk")

                # break
                # upload file to the staga

                cs.execute(
                    "put file://c:\\Users\\revolt\\batch_to_snowpipe_{0}_{1}.csv @PACKWAY_POC;".format(t1[1], write_id))
                logger.info('%s.%s.PIPE_%s',snow_database, snow_schema, t1[1])
                logger.info("xx")
                # set up connection
                # print(snow_account)
                # print(snow_location)
                # print(snow_database)
                # print(snow_schema)


                # print(t1[1])
                ingest_manager = SimpleIngestManager(account=snow_account,
                                                     host='{0}.{1}.snowflakecomputing.com'.format(snow_account,
                                                                                                  snow_location),
                                                     user=snow_user,
                                                     pipe='{0}.{1}.PIPE_{2}'.format(snow_database, snow_schema, t1[1]),
                                                     private_key=private_key_text)

                # ingest using snowpipe
                ingest_manager.ingest_files(
                    [StagedFile("batch_to_snowpipe_{0}_{1}.csv.gz".format(t1[1], write_id), None)])
                os.remove('batch_to_snowpipe_{0}_{1}.csv'.format(t1[1], write_id))
    # Wait for 20 seconds
    time.sleep(65)
logger.info("Konec")
c.close()
logger.info("Connection closed")
