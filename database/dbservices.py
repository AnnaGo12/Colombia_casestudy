import pandas
import psycopg2
from dbconfig import dbconfig


def get_energy_export(startdate, enddate,house_id):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = dbconfig()

        # connect to the PostgreSQL server
        #print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)

        df = pandas.read_sql_query("""SELECT
        feedin."id" as "id",
        rp."id" as "house_id",
        feedin.date_time as "datetime",
        feedin.cleaned_value as "energy_export"
        --  DISTINCT rp.plant_name
        FROM feedin
        LEFT JOIN (SELECT id, plant_name, plant_id  FROM raw_plant WHERE meter_type='GRID') as rp ON feedin.plant_id = rp.plant_id
        WHERE feedin.date_time BETWEEN '{}' AND '{}' AND rp."id"= {} ORDER BY feedin.date_time""".format(startdate, enddate, house_id), conn)

        # df = pandas.read_sql_query("""SELECT
        #      feedin."id" as "id",
        #      rp."id" as "house_id",
        #      feedin.date_time as "datetime",
        #      feedin.cleaned_value as "energy_export"
        #      --  DISTINCT rp.plant_name
        #      FROM feedin
        #      LEFT JOIN (SELECT id, plant_name, plant_id  FROM raw_plant WHERE meter_type='GRID') as rp ON feedin.plant_id = rp.plant_id
        #      WHERE feedin.date_time BETWEEN '{}' AND '{}' ORDER BY feedin.date_time""".format(startdate, enddate), conn)

        return df

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            #print('Database connection closed.')

def get_energy_export_all(startdate, enddate):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = dbconfig()

        # connect to the PostgreSQL server
        #print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)

        df = pandas.read_sql_query("""SELECT
             feedin."id" as "id",
             rp."id" as "house_id",
             feedin.date_time as "datetime",
             feedin.cleaned_value as "energy_export"
             --  DISTINCT rp.plant_name
             FROM feedin
             LEFT JOIN (SELECT id, plant_name, plant_id  FROM raw_plant WHERE meter_type='GRID') as rp ON feedin.plant_id = rp.plant_id
             WHERE feedin.date_time BETWEEN '{}' AND '{}' ORDER BY feedin.date_time""".format(startdate, enddate), conn)

        return df

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            #print('Database connection closed.')


def get_energy_import(startdate, enddate, house_id):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = dbconfig()

        # connect to the PostgreSQL server
        #print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)

        df = pandas.read_sql_query("""SELECT
        consumption."id" as "id",
        rp."id" as "house_id",
        consumption.date_time as "datetime",
        consumption.cleaned_value as "energy_import"
        --  DISTINCT rp.plant_name
        FROM consumption
        LEFT JOIN (SELECT id, plant_name, plant_id  FROM raw_plant WHERE meter_type='GRID') as rp ON consumption.plant_id = rp.plant_id
        WHERE consumption.date_time BETWEEN '{}' AND '{}' AND rp."id"= {} ORDER BY consumption.date_time""".format(startdate, enddate, house_id), conn)

        # df = pandas.read_sql_query("""SELECT
        # consumption."id" as "id",
        # rp."id" as "house_id",
        # consumption.date_time as "datetime",
        # consumption.cleaned_value as "energy_import"
        # --  DISTINCT rp.plant_name
        # FROM consumption
        # LEFT JOIN (SELECT id, plant_name, plant_id  FROM raw_plant WHERE meter_type='GRID') as rp ON consumption.plant_id = rp.plant_id
        # WHERE consumption.date_time BETWEEN '{}' AND '{}' ORDER BY consumption.date_time""".format(startdate, enddate), conn)

        return df

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            #print('Database connection closed.')


def get_energy_import_all(startdate, enddate):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = dbconfig()

        # connect to the PostgreSQL server
        #print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)


        df = pandas.read_sql_query("""SELECT
        consumption."id" as "id",
        rp."id" as "house_id",
        consumption.date_time as "datetime",
        consumption.cleaned_value as "energy_import"
        --  DISTINCT rp.plant_name
        FROM consumption
        LEFT JOIN (SELECT id, plant_name, plant_id  FROM raw_plant WHERE meter_type='GRID') as rp ON consumption.plant_id = rp.plant_id
        WHERE consumption.date_time BETWEEN '{}' AND '{}' ORDER BY consumption.date_time""".format(startdate, enddate), conn)

        return df

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            #print('Database connection closed.')

startdate = '2021-07-01 00:00:00'  # simulation start date
enddate = '2021-07-07 23:00:00'  # simulation end date


#df = get_energy_import(startdate, enddate, house_id)

#print(df['house_id'].unique())

#print(df[['energy_import', 'datetime']][df['house_id'] == 44])


# arr = [ 60,  73,  58,  51,  56,  75,  50,  57,  53, 255,  44,  49,  77]
#
# for i in arr:
#     print(f'this is house id {i}')
#     a = len(df[['energy_import', 'datetime']][df['house_id'] == i])
#     print(a)
#     if a < 168:
#         print('not enough date points')
#         b = 168-a
#         print(f'{b} datapoints missing')





# def get_energy_export(startdate,enddate):
#     """ Connect to the PostgreSQL database server """
#     conn = None
#     try:
#         # read connection parameters
#         params = dbconfig()
#
#         # connect to the PostgreSQL server
#         print('Connecting to the PostgreSQL database...')
#         conn = psycopg2.connect(**params)
#
#         df = pandas.read_sql_query("""SELECT
#         feedin."id" as "id",
#         rp."id" as "house_id",
#         feedin.date_time as "datetime",
#         feedin.cleaned_value as "energy_export"
#         --  DISTINCT rp.plant_name
#         FROM feedin
#         LEFT JOIN (SELECT id, plant_name, plant_id  FROM raw_plant WHERE meter_type='GRID') as rp ON feedin.plant_id = rp.plant_id
#         WHERE feedin.date_time BETWEEN '{}' AND '{}' ORDER BY feedin.date_time""".format(startdate, enddate), conn)
#         # logger.debug(df)
#         return df
#
#     except (Exception, psycopg2.DatabaseError) as error:
#         print(error)
#     finally:
#         if conn is not None:
#             conn.close()
#             print('Database connection closed.')