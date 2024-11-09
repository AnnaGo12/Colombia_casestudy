import random
import numpy as np
import datetime
from pandas import DataFrame
from sklearn.cluster import KMeans
from collections import Counter

from entity import Smart_meter, Measurements, Bin_bounds_and_occurencies


def summ_to_one( prob_array ):
    summ = np.sum( np.array( prob_array ) )
    if summ != 1.0:
        diff = 1.0 - summ
        if diff > 0:
            array_1 = np.arange( len( prob_array ) ) 
        else:
            array_1 = np.array( [] )
            for i in range( len( prob_array ) ):
                if prob_array[i] + diff >= 0:
                    array_1 = np.append( array_1, i )
        index = random.choice(array_1) 
        prob_array[ int(index) ] += diff 
    
    return prob_array



def simulate_week( date_start, weeks_num, labels, occurencies, transitions, group_meters_clasterized, bounds_daily_clusters, transition_matrices_clustered_by_weeks, bin_occurencies_clustered_by_weeks, house_number, days_double_arr_with_classes, day_groups  ): 

    num_clustrers = len( occurencies )
    prob_start = []
    summ = np.sum( occurencies )
    assert summ == len( labels )
    for i in range( len( occurencies ) ):
        prob_start += [ float( occurencies[i] )/float( summ ) ]

    summ_2 = np.sum( np.array( prob_start ) )
    assert summ_2 > 0.9999
    assert summ_2 < 1.0001
    
    start_state = np.random.choice(np.arange( len( occurencies ) ), p=summ_to_one(prob_start) )
    
    states = [start_state]
    curr_state = start_state
    for i in range( weeks_num - 1 ):
        prob_next = transitions[ int( curr_state ) ]
        #print( 'summ = ', sum( prob_next ) )
        next_state = np.random.choice(np.arange( len( occurencies ) ), p= summ_to_one(prob_next))
        states += [next_state]

    assert len( states ) == weeks_num
    weeks = np.array( [] )
    date = date_start
    for i in range( len( states ) ):
        out = create_empty_week( states[i], date, house_number )
        week = out[0]
        weeks = np.append( weeks, week )
        date = out[1]



    for week in weeks:
        for days in range( len( group_meters_clasterized[ 0 ] ) ):
            for day in range( len( day_groups[days] ) ):
                for hour in range( 24 ):
                    
                    day_current = 0
                    for i in range( days ):
                        day_current += len( day_groups[i] )
                    day_current += day
                   
                    hour_current = day_current*24 + hour

                    if hour == 0:
                        measurements = group_meters_clasterized[ int(week.L_M_or_H) ][ days ][ 0 ]
                        labels = []
                        for measure in measurements:
                            labels += [ measure.class_moment ]
                        num_labels = len( labels )
                        number_of_clusters = len(list(Counter(labels) ))
                        arr = np.zeros( number_of_clusters )
                        for measure in measurements:
                            arr[ int(measure.class_moment) ] += 1
                        

                        num_classes = len(list(Counter(labels) ))
                        assert num_labels == np.sum( arr )
                        
                        labels_count_norm = []
                        for i in range( number_of_clusters ):
                            if num_labels > 0:
                                labels_count_norm += [ float( arr[i] )/float( num_labels ) ]
                            else:
                                assert arr[i] == 0
                        summ = np.sum( np.array( labels_count_norm ) )
                        if summ <= 0.9999:
                            print( 'summ = ', summ )
                        assert summ > 0.9999
                        assert summ < 1.0001

                        moment_state = int( np.random.choice( np.arange( num_classes ), p= summ_to_one(labels_count_norm) ) )
                        week.hourly_cluster[ hour_current ] = int( moment_state )
                        week.hourly_cluster_num[ hour_current ] = int(number_of_clusters)
                        
                        
                            
                    else:

                        transition_prob = transition_matrices_clustered_by_weeks[ int(week.L_M_or_H) ][ days ][ hour - 1 ][ moment_state ]
                        
                        measurements = group_meters_clasterized[ int(week.L_M_or_H) ][ days ][ hour ]
                        number_of_classes = measurements[ 0 ].num_moment_classes
                        moment_state = int(np.random.choice(np.arange( number_of_classes ), p=summ_to_one(transition_prob) ) )
                    


                    week.hourly_cluster[ hour_current ] = int( moment_state )
                    week.hourly_cluster_num[ hour_current ] = int(number_of_clusters)

                    occurencies_bin = bin_occurencies_clustered_by_weeks[ int(week.L_M_or_H) ][ days ][ hour ]
                    prob_states = occurencies_bin.occurencies_matrix[ moment_state ]
                    bin_num = int( np.random.choice( np.arange( 10 ), p=summ_to_one(prob_states) ) )
                    bounds = occurencies_bin.bounds[ moment_state ][ bin_num ]

                    if bounds[0] == bounds[1]:
                        week.energy_export_arr[ hour_current ] = float( bounds[0] )

                    else:
                        week.energy_export_arr[ hour_current ] = float(np.random.uniform(bounds[0], bounds[1]) )



    for week in weeks:
        for i in range(len(week.hourly_cluster)):
            if week.hourly_cluster[i] < 0 or week.hourly_cluster_num[i] < 1:
                print( 'In simulation.py, 150. Hourly electricity is not written.  :week.hourly_cluster[i], week.hourly_cluster_num[i] ', week.hourly_cluster[i], week.hourly_cluster_num[i], 'i = ', i ) 
            assert week.hourly_cluster[i] >= 0 
            assert week.hourly_cluster_num[i] >= 1 
            assert week.energy_export_arr[i] >= 0 

    
    datitime_arr = np.array( [] )
    en_arr = np.array( [] )
    for week in weeks:
        datitime_arr = np.append( datitime_arr, week.datetime_arr )
        en_arr = np.append( en_arr, week.energy_export_arr )

    last_meter = Smart_meter( datitime_arr, en_arr, weeks[0].house_number )




    return last_meter

def create_empty_week( cluster, start_date, house_number ):
    assert start_date.weekday() == 0
    start_date = start_date.replace(hour=00, minute=00) 

    # replace(hour=23, minute=30).strftime('%Y-%m-%d')

    for i in range( 24*7 ):
        if i == 0:
            dt_arr = np.array( [ start_date ] )
            L_M_or_H = cluster
            energy_export_arr, hourly_cluster, hourly_cluster_num = np.array( [-1] ), np.array( [-1] ), np.array( [-1] )
            meter = Smart_meter(dt_arr, energy_export_arr, house_number, L_M_or_H, hourly_cluster, hourly_cluster_num)
            date = start_date + datetime.timedelta(hours=1, minutes=0)
        else:
            meter.datetime_arr = np.append( meter.datetime_arr, date )
            meter.energy_export_arr = np.append( meter.energy_export_arr,  np.array( [-1.0] )  )
            meter.hourly_cluster = np.append( meter.hourly_cluster,  np.array( [-1] )  )
            meter.hourly_cluster_num = np.append( meter.hourly_cluster_num,  np.array( [-1] )  )
            date = date + datetime.timedelta(hours=1, minutes=0)

    return [meter, date + datetime.timedelta(hours=1, minutes=0) ]


    # date_after_finish = dt_arr[ index_finish ] + datetime.timedelta(hours=1, minutes=0)



def simulate_week_with_dates( date_start, date_finish, labels, occurencies, transitions, group_meters_clasterized, bounds_daily_clusters, transition_matrices_clustered_by_weeks, bin_occurencies_clustered_by_weeks, house_number, days_double_arr_with_classes, day_groups  ):
    start_date = date_start.replace(hour=00, minute=00, second = 00)
    day_of_week = start_date.weekday() 
    first_monday = start_date - datetime.timedelta(days = int( day_of_week ), hours=0, minutes=0)

    finish_date = date_finish.replace(hour=23, minute=00, second = 00) 
    day_of_week = finish_date.weekday() 
    last_monday = finish_date + datetime.timedelta(days = 6 - int( day_of_week ), hours=1, minutes=0)

    weeks_num = int(( last_monday - first_monday ).days/7) + 1


    meter = simulate_week( first_monday, weeks_num, labels, occurencies, transitions, group_meters_clasterized, bounds_daily_clusters, transition_matrices_clustered_by_weeks, bin_occurencies_clustered_by_weeks, house_number, days_double_arr_with_classes, day_groups  ) 
    

    mark_found = False
    i = 0
    index_first = -1
    while i < len( meter.datetime_arr ) and (mark_found == False) :
        delta = meter.datetime_arr[i] - start_date
        if delta.seconds == 0 and delta.days == 0:
            mark_found = True
            index_first = i

        i += 1

    mark_found = False
    i = len( meter.datetime_arr ) - 1
    index_last = -1
    while i >= 0 and (mark_found == False) :
        delta = meter.datetime_arr[i] - finish_date
        if delta.seconds == 0 and delta.days == 0:
            mark_found = True
            index_last = i

        i -= 1
    
    assert index_first >= 0
    assert index_last >= 0

    meter.datetime_arr =  meter.datetime_arr[ index_first : index_last + 1  ]

    meter.energy_export_arr = meter.energy_export_arr[  index_first : index_last + 1   ]
    
    return meter



if __name__ == "__main__":
    print( 'please run main.py')