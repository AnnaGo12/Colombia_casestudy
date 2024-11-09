#!/usr/bin/python
import pandas
import psycopg2
from loguru import logger

from dbconfig import dbconfig, namedtuple_fetchall
import os
from pandas import DataFrame


def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = dbconfig()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)


        df = pandas.read_sql_query("""SELECT
        feedin."id" as "id",
        rp."id" raw_plant_id,
        feedin.date_time,
        feedin.cleaned_value as "energy_export"
        --  DISTINCT rp.plant_name
        FROM feedin
        LEFT JOIN (SELECT id, plant_name, plant_id  FROM raw_plant WHERE meter_type='GRID') as rp ON feedin.plant_id = rp.plant_id 
        WHERE feedin.date_time BETWEEN '2021-07-01 00:00:00' AND '2021-07-31 23:00:00' ORDER BY feedin.date_time""", conn)

        #logger.debug(df)

        # display the PostgreSQL database server version
        #db_version = cur.fetchone()
        #print(db_version)
       
	# close the communication with the PostgreSQL
        #cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


if __name__ == '__main__':
    print(os.getcwd())
    connect()


