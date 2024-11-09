import random
import numpy as np
import csv
import datetime
import matplotlib.pyplot as plt
import pandas as pd
import os
import sys

from cluster import my_cluster_weeks, cluster_weeks_final, cluster_moments, count_moments_probabilities

from simulation import simulate_week, simulate_week_with_dates

from entity import Smart_meter, Bin_bounds_and_occurencies, Measurements

############ Settings
relative_path = "../Data/"  # Specify location of other files
df_strata = pd.read_csv(relative_path + "house_info.csv",
                        header=0)  # read strata information from strata info table
data_tables = ["energy_import", "energy_export"]  # table names for data extraction
tt = 0
############ Settings


def read_dates_start_finish( input_name ):
    with open(input_name) as f:
        lines = f.readlines()
        for j in range( len(lines) ):
            mark = 0
            for i in range( len( lines[j] ) ):
                if mark == 0 and lines[j][i] == '-':
                    year = int(lines[j][ i-4 :i ])
                    prev = i
                    mark = 1

                elif mark == 1 and lines[j][i] == '-':
                    month = int(lines[j][ prev + 1 :i ])
                    prev = i
                    mark = 2

                    day = int(lines[j][ i + 1 : i+3 ])
            if j == 0:
                date_start = datetime.datetime(year, month, day, 0, 0, 0)
            if j == 1:
                date_finish = datetime.datetime(year, month, day, 23, 0, 0)

    return [ date_start, date_finish ]



def read_weekly_groups( input_name ):
    with open(input_name) as f:
        lines = f.readlines()
        numbers = [ '0', '1', '2', '3', '4', '5', '6' ]
        day_groups = [  ]
        for j in range( len(lines) ):
            days = []
            for i in range( len( lines[j] ) ): 
                if lines[j][i] in numbers:
                    days += [ int( lines[j][i] ) ]
            day_groups += [ days ]
    
    return day_groups




def parce_data( input_name ):
    with open( input_name ) as csvfile:
        csvReader = csv.reader(csvfile, delimiter=',')
        i = 0
        houses = np.array( [] )
        smart_meter_arr = np.array( [] )
        count = 0
        for row in csvReader:
            if count > 0:
                house = int( float(row[ house_index ]) )
                my_datetime = row[ datetime_index ]
                j = 0
                for i in range( len( my_datetime ) ):
                    if my_datetime[i].isspace():
                        my_date = my_datetime[ :i ]
                        my_time = my_datetime[ i+1 : ]
                        j += 1
                assert j == 1
                
                mark = 0
                for i in range( len( my_date ) ):
                    if mark == 0 and my_date[i] == '-':
                        year = int(my_date[ :i ])
                        prev = i
                        mark = 1

                    elif mark == 1 and my_date[i] == '-':
                        month = int(my_date[ prev + 1 :i ])
                        prev = i
                        mark = 2

                        day = int(my_date[ i + 1 : ])
                assert mark == 2

                mark = 0
                for i in range( len( my_time ) ):
                    if mark == 0 and my_time[i] == ':':
                        hour = int(my_time[ :i ] )
                        prev = i
                        mark = 1

                    elif mark == 1 and my_time[i] == ':':
                        minute = int(my_time[ prev + 1 : i ])
                        prev = i
                        mark = 2

                        second = int(my_time[ i + 1 : ])

                assert mark == 2

                assert mark == 2
                    
                my_datetime = datetime.datetime( year = year, month = month, day = day, hour = hour, minute = minute, second = second )

                energy = float(row[ energy_export_index ])

                if house in houses:
                    number = np.where(houses == house)[0][0]
                    smart_meter_arr[number].datetime_arr = np.append( smart_meter_arr[number].datetime_arr, my_datetime )
                    smart_meter_arr[number].energy_export_arr = np.append( smart_meter_arr[number].energy_export_arr, energy )

                else:
                    my_meter = Smart_meter( np.array( [my_datetime] ), np.array( [energy] ), house)
                    houses = np.append( houses, house )
                    smart_meter_arr = np.append( smart_meter_arr, my_meter )
            
            else:

                for i in range( len( row ) ):
                    if row[i] == 'house_id':
                        house_index = i
                    elif row[i] == 'datetime':
                        datetime_index = i
                    elif row[i] == 'energy_export':
                        energy_export_index = i
            
            count += 1

        for meter in smart_meter_arr:
            meter = meter.sort_array_by_date( )
            assert len( meter.datetime_arr ) == len( meter.energy_export_arr )

            for i in range( len(  meter.datetime_arr  ) - 1 ):
                if (meter.datetime_arr[i+1] - meter.datetime_arr[i]).total_seconds() != 3600:
                    print( 'Problem in input data: after datetime', meter.datetime_arr[i], 'goes datetime', meter.datetime_arr[i+1], ' execution finished '  )
                assert (meter.datetime_arr[i+1] - meter.datetime_arr[i]).total_seconds()  == 3600
    return smart_meter_arr

        


nr_houses = 0

if __name__ == "__main__":


    while nr_houses <= 50: # Set here nr of energy_export profiles to be created
    
        t_1 = datetime.datetime.now()

        input_dir = 'data/inputs'
        for file_name in os.listdir(input_dir):
            input_path = os.path.join(input_dir, file_name)
            smart_meter_arr = parce_data( input_path)
            meters_out = []
            for meter in smart_meter_arr:


                print( 'house_number = ', meter.house_number )
                print( 'date_start, date_finish = ', meter.datetime_arr[0], meter.datetime_arr[-1] )

                week_arrays = meter.cut_energy_by_weeks()

                day_groups = read_weekly_groups( 'data/weekly_day_groups.txt' )
                min_length_day_group = 7
                for i in range( len( day_groups ) ):
                    if min_length_day_group >  len( day_groups[i] ) :
                        min_length_day_group = len( day_groups[i] )


                output = cluster_weeks_final( week_arrays, min_length_day_group )
                clustered_weeks = output[0]
                labels, occurencies, transitions = output[1], output[2], output[3]


                days_double_list = [  ]
                for i in range(len(clustered_weeks)):
                    days_of_week = clustered_weeks[i].cut_week_arr( day_groups )
                    days_double_list += [ days_of_week ]




                days_double_arr = np.array( days_double_list )
                days_double_arr_with_classes = cluster_moments( days_double_arr )

                output_2  = count_moments_probabilities( days_double_arr_with_classes  )

                group_meters_clasterized, bounds_daily_clusters, transition_matrices_clustered_by_weeks, bin_occurencies_clustered_by_weeks = output_2[0], output_2[1], output_2[2], output_2[3]

                house_number = meter.house_number

                datess = read_dates_start_finish( 'data/date_start_finish.txt' )
                date_start, date_finish =  datess[0], datess[1]
                meter_final = simulate_week_with_dates( date_start, date_finish, labels, occurencies, transitions, group_meters_clasterized, bounds_daily_clusters, transition_matrices_clustered_by_weeks, bin_occurencies_clustered_by_weeks, house_number, days_double_arr_with_classes, day_groups  )

                meters_out += [ meter_final ]



            output_dir = 'data/outputs'
            output_path = os.path.join(output_dir, file_name)

            with open(output_path, 'w', newline='') as f:
                # create the csv writer
                writer = csv.writer(f)
                counter = 1
                row = [ 'number', 'house_id', 'datetime', 'energy_export' ]
                writer.writerow(row)

                for meter in meters_out:
                    for i in range( len( meter.datetime_arr ) ):
                        row = [ str( counter ) , str( meter.house_number ), str( meter.datetime_arr[i] ), str( meter.energy_export_arr[i] ) ]
                        writer.writerow(row)

                        counter += 1



            t_2 = datetime.datetime.now()
            dt = (t_2 - t_1)
            print( 'instance ' + file_name + ' counted in' , dt.seconds, 'seconds' )

        user_id = df_strata.user_id.loc[df_strata['house_id'] == house_number]
        user_id = user_id.iloc[0]



        if nr_houses == 0:
            df = pd.read_csv(output_path)
            df.rename(columns={'energy_export': str(nr_houses+300)}, inplace=True)
            df = df.drop(['number'], axis=1)
            df = df.drop(['house_id'], axis=1)

            df.to_csv(output_dir + '/'+ data_tables[tt] + user_id +'__.csv', index=False, header=True)

        if nr_houses > 0:
            df = pd.read_csv(output_path)
            df_all = pd.read_csv(output_dir + '/'+ data_tables[tt] + user_id +'__.csv')
            df_all.rename(columns={'energy_export': str(nr_houses-1+300)}, inplace=True)



            append_column = df["energy_export"]

            df_all = df_all.join(append_column)
            df_all.rename(columns={'energy_export': str(nr_houses+300)}, inplace=True)
            df_all.to_csv(output_dir +'/'+ data_tables[tt] + user_id +'__.csv', index=False, header=True)

        nr_houses += 1
