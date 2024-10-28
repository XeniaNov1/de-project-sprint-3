import psycopg2
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, format='[%(asctime)s] %(levelname)s %(message)s')

conn = psycopg2.connect(
    "host='localhost' port='5432' dbname='de' user='jovyan' password='jovyan'")
cur = conn.cursor()

with open('project/migrations/01-uol.sql', 'r') as uol01:
    uol_data = uol01.read()
    logger.debug(f'The file is read:\n{uol_data}')
    cur.execute(uol_data)
    conn.commit()
    logger.info('First transaction was commited')

with open('project/migrations/02-create-mart.f_customer_retention.sql', 'r') as custret03:
    ret_data = custret03.read()
    logger.debug(f'The file is read:\n{ret_data}')
    cur.execute(ret_data)
    conn.commit()
    logger.info('Second transaction was commited')

cur.close()
conn.close()
