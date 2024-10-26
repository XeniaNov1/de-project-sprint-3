import psycopg2
import pandas as pd
import numpy as np

conn = psycopg2.connect("host='localhost' port='5432' dbname='de' user='jovyan' password='jovyan'")
cur = conn.cursor()  

with  open('project/migrations/01-uol.sql','r') as uol01:
    uol_data = uol01.read()

    cur.execute(uol_data)
    conn.commit()


# with open('project/migrations/02-f_sales.sql','r') as fsales02:
#     f_sales_data = fsales02.read()

#     cur.execute(f_sales_data)
#     conn.commit()

#     fsales02.close()

with  open('project/migrations/03-create-mart.f_customer_retention.sql','r') as custret03:
    ret_data = custret03.read()

    cur.execute(ret_data)
    conn.commit()

cur.close()
conn.close()
