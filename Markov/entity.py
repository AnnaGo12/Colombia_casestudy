import random
import numpy as np
import csv
import datetime
import matplotlib.pyplot as plt


class Bin_bounds_and_occurencies:
    def __init__( self, bounds, occurencies_matrix, num_clusters ):
        self.bounds = bounds
        self.occurencies_matrix = occurencies_matrix
        self.num_clusters = num_clusters



class Measurements:
    def __init__( self, energy, class_week, class_moment, num_moment_classes, week_day_next_indices, number_of_next_in_daily_meter, last_day ):
        self.energy = energy
        self.class_week = class_week
        self.class_moment = class_moment
        self.num_moment_classes = num_moment_classes
        self.week_day_next_indices = week_day_next_indices
        self.number_of_next_in_daily_meter = number_of_next_in_daily_meter
        self.last_day = last_day





class Smart_meter:
    def __init__(self, datetime_arr, energy_export_arr, house_number, L_M_or_H = None, hourly_cluster = None, hourly_cluster_num = None):
        assert len( datetime_arr ) == len( energy_export_arr )
        self.datetime_arr = datetime_arr
        self.energy_export_arr = energy_export_arr
        self.house_number = house_number
        self.L_M_or_H = L_M_or_H
        if hourly_cluster is not None:
            assert len( hourly_cluster ) == len( datetime_arr )
            self.hourly_cluster = hourly_cluster
        else:
            self.hourly_cluster = np.array( [] )
        
        if hourly_cluster_num is not None:
            assert len( hourly_cluster_num ) == len( datetime_arr )
            self.hourly_cluster_num = hourly_cluster_num
        else:
            self.hourly_cluster_num = np.array( [] )





    def get_sum_energy( self ):
        return np.sum( self.energy_export_arr )

    def sort_array_by_date( self ):
        dt_arr = np.array( [] )
        datetime_arr = self.datetime_arr
        for i in range( len( datetime_arr ) ):
            dt_arr = np.append( dt_arr, (datetime_arr[i] - datetime.datetime( 2019, 1, 1, 1, 0, 0 )).total_seconds() )
        en_arr = self.energy_export_arr
        arr_sort = np.vstack((dt_arr, en_arr, datetime_arr ))

        arr_sort = arr_sort [ :, arr_sort[0].argsort()]

        self.datetime_arr = arr_sort[2]
        self.energy_export_arr = arr_sort[1]
        return self

    def draw_graph(self):
        dt_arr = self.datetime_arr
        en_arr = self.energy_export_arr
        # Setting the figure size
        fig = plt.figure(figsize=(30,18))
        
        plt.plot_date(dt_arr, en_arr, ms = 1.5) 

        name = 'house_number' + str( self.house_number )
        plt.title(name , fontweight ="bold") 
        fig.savefig( name + '.jpg', bbox_inches='tight', dpi=300)
        #plt.show() 
        Smart_meter(datetime_arr, energy_export_arr, house_number)


    

    def copy_energy( self, times ):

        assert self.datetime_arr[0].hour == 0
        assert self.datetime_arr[-1].hour == 23
        assert self.datetime_arr[0].weekday() == 0
        assert self.datetime_arr[-1].weekday() == 6
        assert times > 0
        dt_arr = self.datetime_arr
        en_arr = self.energy_export_arr

        dt_out = np.copy( dt_arr )
        en_out = np.copy( en_arr )

        length = len( self.datetime_arr )

        for i in range( times - 1 ):
            dt_out = np.append( dt_out, dt_arr )
            en_out = np.append( en_out, en_arr )
            for j in range( length ):
                date_prev = dt_out[ -length - 1 + j ]
                date_next =  date_prev + datetime.timedelta(hours=1, minutes=0)
                dt_out[  -length  + j ] = date_next
                
                norm = np.random.normal(loc=0.0, scale=1.0, size=None)
                en_out[  -length  + j ] = max(0, en_out[  -length  + j ] + (norm/10)*en_out[  -length  + j ] )
                

        self.datetime_arr = np.copy(dt_out)
        self.energy_export_arr = np.copy(en_out)

        assert len( dt_out ) == len( en_out )

        return self



    
    def convert_to_array_of_weeks( self ):
        assert self.datetime_arr[0].hour == 0
        assert self.datetime_arr[-1].hour == 23
        assert self.datetime_arr[0].weekday() == 0
        assert self.datetime_arr[-1].weekday() == 6


        monday_array = np.array( [ ] )
        dt_arr = np.copy(self.datetime_arr)
        en_arr =  np.copy( self.energy_export_arr )
      
        assert len( dt_arr ) == len( en_arr )

        for i in range( len( dt_arr ) ):
            if dt_arr[i].hour == 0 and dt_arr[i].weekday() == 0:
                monday_array = np.append( monday_array, i )
        
        monday_array = np.append( monday_array, len( dt_arr ) )
        smart_meters_array = np.array( [] )
        for i in range( len( monday_array ) - 1 ):
            my_dt_arr = dt_arr[ int(monday_array[i]) : int(monday_array[i + 1]) ]
            my_en_arr = en_arr[ int(monday_array[i]) : int(monday_array[i + 1]) ]

            my_sm_meter = Smart_meter(my_dt_arr, my_en_arr, self.house_number)
            smart_meters_array = np.append( smart_meters_array, my_sm_meter )

        assert len( smart_meters_array ) > 0

        return smart_meters_array
            



    def cut_energy_by_weeks( self ):
        index_first = 0
        dt_arr = self.datetime_arr
        length = len( dt_arr )
        mark_found = 0
        index_start = -1
        i = 0
        while i < length and mark_found == 0:
            if dt_arr[i].hour == 0 and dt_arr[i].weekday() == 0:
                index_start = i
                mark_found = 1
            i += 1

        assert index_start >= 0

        i = length-1
        index_finish = -1
        mark_found_2 = 0
        while i >= 0 and mark_found_2 == 0:
            if dt_arr[i].hour == 23 and dt_arr[i].weekday() == 6:
                index_finish = i
                mark_found_2 = 1
            i -= 1

        assert index_finish >= 0
        assert index_finish > index_start

        date_after_finish = dt_arr[ index_finish ] + datetime.timedelta(hours=1, minutes=0)
        deltatime = date_after_finish - dt_arr[ index_start ]
        diff_days = deltatime.days

        self.datetime_arr = dt_arr[ index_start : index_finish + 1  ]
        self.energy_export_arr = np.copy( self.energy_export_arr[  index_start : index_finish + 1 ] )
        
        if diff_days  >= 35:
            

            week_meters = self.convert_to_array_of_weeks()

            return week_meters

        else:
            assert len( self.datetime_arr ) == len( self.energy_export_arr )
            array_multiplied = self.copy_energy( 5 )
            assert len(array_multiplied.datetime_arr ) == len( array_multiplied.energy_export_arr )
            week_meters = array_multiplied.convert_to_array_of_weeks()

            return week_meters


    def cut_week_arr( self, day_groups ):
        assert self.datetime_arr[0].hour == 0
        assert self.datetime_arr[-1].hour == 23
        assert self.datetime_arr[0].weekday() == 0
        assert self.datetime_arr[-1].weekday() == 6

        assert ( self.datetime_arr[-1] - self.datetime_arr[0] ).seconds == 23*3600
        assert ( self.datetime_arr[-1] - self.datetime_arr[0] ).days == 6
        
        days = [0.0]
        for i in range( len( day_groups ) - 1 ):
            days += [ float(day_groups[ i + 1 ][0]) ]
        days += [7.0]
        
        new_meters = np.array( [] )
        for i in range( len( days ) - 1 ):
            start_index = int( days[i] )*24
            finish_index = int( days[i + 1] )*24
            en_new = np.copy( self.energy_export_arr[ start_index: finish_index ] )
            dt_new = np.copy( self.datetime_arr[ start_index: finish_index ] )
            meter = Smart_meter( dt_new, en_new, self.house_number, L_M_or_H = self.L_M_or_H )
            new_meters = np.append( new_meters, meter )

        return new_meters



if __name__ == "__main__":
    print( 'please run main.py')