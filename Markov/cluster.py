import random
import numpy as np
import datetime
from pandas import DataFrame
from sklearn.cluster import KMeans
from collections import Counter

from entity import Smart_meter, Measurements, Bin_bounds_and_occurencies



def my_cluster_weeks( week_energy_export_list, k, number_of_iters, min_length_day_group  ):
    if len(week_energy_export_list)*min_length_day_group < 5:
        print( week_energy_export_list )
    assert len( week_energy_export_list )*min_length_day_group >= 5 
    assert k <= len( week_energy_export_list )

    zer_list = [0]*len( week_energy_export_list )

    Data = {'x': week_energy_export_list, 'y': zer_list }

    df = DataFrame(Data,columns=['x','y'])

    kmeans = KMeans(n_clusters=k, n_init = number_of_iters, tol = 1e-6, max_iter = 20000 ).fit(df) # n_init = number_of_iters -- number of initial trials, that are tested
    centroids = kmeans.cluster_centers_
    labels = kmeans.labels_ # clustering of peaks

    # there can be less than k clusters so k is adjusted based on number of actual clusters
    k = len(list(Counter(labels) ))

    max_0 = 0
    for i in range( len( labels ) ):
        if labels[i] > max_0:
            max_0 = labels[i]
    
    max_0 += 1
    
    if max_0 > k: # if the max nr of labels is not equal to clusters (i.e.[2 2 2 2 1 2 2 2 2 2 2 2 2 2 2 2 2] ), situation is adjusted
        pos_current = 0 # structuring of elements
        pos_current = 0 # structuring of elements
        while pos_current < k:
            # test whether there is the element in the label list pos_current
            mark_found = False
            for i in range( len( labels ) ):
                if labels[i] == pos_current:
                    mark_found = True
            if not mark_found: # if no element is found eual to pos_current, the min element is used. the next pos_current is used and replaced
                min_greater = float( 'inf' ) # min element, large pos current
                for i in range( len( labels ) ):
                    if labels[i] >= pos_current and labels[i] < min_greater:
                        min_greater = labels[i]
                assert pos_current < min_greater

                # labels adjusted from min_greater to pos_current
                for i in range( len( labels ) ):
                    if labels[i] == min_greater:
                        labels[i] = pos_current
            pos_current += 1
    
    found_zero_mark = False
    for i in range( len(labels) ):
        if labels[i] == 0.0:
            found_zero_mark = True
    if found_zero_mark == False:
        print( 'labels = ', labels )

    assert found_zero_mark == True



    # calculate max and min
    min_max = np.array( [[ float( 'inf' ), 0 ]]*k ) # list which stores all max and min for all elements from one cluster
    for i in range( len( week_energy_export_list ) ):
        j = int(labels[i])
        if j >= k:
            print( 'labels = ', labels, 'j = ', k, 'k = ', k )
        assert j < k
        if week_energy_export_list[i] > min_max[j][1]:
            min_max[j][1] = week_energy_export_list[i]
        
        if week_energy_export_list[i] < min_max[j][0]:
            min_max[j][0] = week_energy_export_list[i]

        
    # clusters separated based on higher getting max boundaries
    dtype = [('height', float), ('number', int)]
    sort_list = []
    for i in range( k ):
        tup = (min_max[i][1], i)
        sort_list += [ tup ]
    sort_arr = np.array(sort_list, dtype=dtype)       # create a structured array
    sorted_arr = np.copy(np.sort(sort_arr, order='height') )
    
    # sorted_arr contains indices in the other order,indices will contain of the direct transition
    indices = [0]*len( sorted_arr )
    for i in range( len(sorted_arr) ):
        j = sorted_arr[i][1]
        indices[j] = i
        
    for i in range( len( labels ) ):
        labels[i] = int(indices[ labels[i] ])
    
    found_zero_mark = False
    for i in range( len(labels) ):
        if labels[i] == 0.0:
            found_zero_mark = True
    assert found_zero_mark == True
    

    occurencies = np.array( [0.0]*k )  # count how often each element in one list appears
    for i in range( len( week_energy_export_list ) ):
        j = int(labels[i])
        if i < len( week_energy_export_list ) - 1:
            occurencies[j] += 1.0
        
    ##################### This is just to check correctness. Can be deleted
    min_max = np.array( [[ float( 'inf' ), 0 ]]*k ) # list which stores min an max for all elements from one cluster

    for i in range( len( week_energy_export_list ) ):
        j = int(labels[i])
        if week_energy_export_list[i] > min_max[j][1]:
            min_max[j][1] = week_energy_export_list[i]
        
        if week_energy_export_list[i] < min_max[j][0]:
            min_max[j][0] = week_energy_export_list[i]
    
    for i in range( len( min_max ) - 1 ):
        assert min_max[i+1][0] >= min_max[i][1]
    ####################### end of check


    transitions = np.array( [np.array( [0.0]*k )]*k ) # weekly transitions, transitions[i][k] equals transition from  i situations in k
    for i in range( len( week_energy_export_list ) - 1 ):
        j = int(labels[i])
        s = int( labels[i + 1] )
        transitions[j][s] += 1.0
    
    
    #  normalising transition matrix
    for i in range( k ):
        div = occurencies[i]
        if div > 0:
            for j in range( k ):
                transitions[i][j] = float(transitions[i][j])/float(div) 
                assert transitions[i][j] <= 1.0
        else:
            for j in range( k ):
                assert transitions[i][j] == 0

    for i in range( k ):
        if occurencies[i] > 0:
            summ = 0.0
            for j in range( k ):
                summ += transitions[i][j]
            
            assert summ < 1.0001
            if summ < 0.9999:
                print( 'labels = ', labels )
                print( 'occurencies = ', occurencies )
                print( 'transitions = ', transitions )
                print( 'summ = ', summ )
                print( 'transitions[i] = ', transitions[i] )

            assert summ > 0.9999
    
    occurencies[  int(labels[-1]) ] += 1 # occurencies[  int(labels[-1]) ] -- last element wasn't considered before
    assert np.sum( occurencies ) == len( labels )

    bad_matrix = False # way to detect whether the matrix is useful or no cluster has been found
    for i in range(k):
        if transitions[i][i] == 1.0 or occurencies[i]*min_length_day_group < 5:
            bad_matrix = True

    return [ week_energy_export_list, labels, occurencies, transitions, bad_matrix  ]





# final week clustering
def cluster_weeks_final( week_en_arr, min_length_day_group ):
    # Starting to create cluster_weeks( en_arr, k ), beginning with k = 3, reducing k until m*min_length_day_group = minimum number of
    # elements in on e cluster will be no less than 5
    # 
    # returning borders of the clusters, lists of clusters which each of the weeks belongs to
    # and  k = number of clusters
    assert len( week_en_arr ) >= 5 # number of weeks must be 5 or more

    week_energy_export_list = [ week.get_sum_energy() for week in week_en_arr ]

    div = len( week_en_arr )*min_length_day_group//5  # minimum number of chanfes in one short term clusters. There shouldn't be less than 5
    if div == 1:
        for i in range( len( week_en_arr ) ):
            week_en_arr[i].L_M_or_H = 0 # 1 cluster
        return [ week_en_arr, [0]*len( week_en_arr ), np.array( [ len( week_en_arr ) ] ), np.array( [ np.array( [1.0] ) ] ), False  ]
    else:
        iter_num = min(div, 3) # number of iterations
        mark_finished = 0
        while iter_num > 1 and mark_finished == 0:
            num_try = 8*iter_num # number of times we need to try to create a good cluster
            i = 0
            while i < num_try:
                output = my_cluster_weeks( week_energy_export_list, iter_num, 3, min_length_day_group ) # number_of_iters = 20
                if not output[4]: # if the matrix is good
                    for i in range( len( week_en_arr ) ):
                        week_en_arr[i].L_M_or_H = output[1][i] # 1 cluster
                    return [week_en_arr, output[1], output[2], output[3], output[4] ]
                i += 1

            iter_num -= 1

        if iter_num == 1: # when no good matrix was found
            for i in range( len( week_en_arr ) ):
                week_en_arr[i].L_M_or_H = 0 # 1 cluster
            return [ week_en_arr, [0]*len( week_en_arr ), np.array( [ len( week_en_arr ) ] ), np.array( [ np.array( [1.0] ) ] ), False  ]



def cluster_moments( days_double_arr ):      

    assert len( days_double_arr ) >= 5 
    lengths_week_period_arr = [] 
    for i in range( len(days_double_arr[0]) ):
        diff_days = (days_double_arr[0][i].datetime_arr[ -1 ] - days_double_arr[0][i].datetime_arr[ 0 ]).days + 1
        lengths_week_period_arr += [diff_days]

    num_week_groups = -1
    for week in days_double_arr:
        for days in week:
            days.hourly_cluster = np.array( [-1.0]*len( days.datetime_arr ) )
            days.hourly_cluster_num = np.array( [-1.0]*len( days.datetime_arr ) )
            if int(days.L_M_or_H) > num_week_groups:
                num_week_groups = int(days.L_M_or_H)
    num_week_groups += 1



    for cluster_num in range( num_week_groups ):
        for i in range( len( lengths_week_period_arr ) ):
            for j in range( 24 ):
                my_group_of_energies = np.array([])
                weeks = []
                num_in_dt_arr = []
                for d in range( lengths_week_period_arr[i] ):
                    for w in range( len( days_double_arr ) ):
                        if int(days_double_arr[w][i].L_M_or_H) == int( cluster_num ):  
                            my_group_of_energies = np.append(my_group_of_energies, days_double_arr[w][i].energy_export_arr[ j + 24*d  ] )
                            weeks += [ w ]
                            num_in_dt_arr += [ j + 24*d  ]
                output = my_cluster_weeks( my_group_of_energies, 5, 3, 1 )
                labels = output[1]
                
                num_of_clusters = len(list(Counter(labels) ))
                min_max = [  ]
                for fi in range( num_of_clusters ):
                    min_max += [[ float( 'inf' ), 0 ]]
                for phi in range( len( labels ) ):
                    b = int( labels[ phi] )
                    if min_max[ b ][0] > my_group_of_energies[ phi ]:
                       min_max[ b ][0] = my_group_of_energies[ phi ]
                    if min_max[ b ][1] < my_group_of_energies[ phi ]:
                       min_max[ b ][1] = my_group_of_energies[ phi ]
                
                for b in range( len( min_max ) - 1 ):
                    assert min_max[ b+1 ][0] >= min_max[b][1]
                    


                found_zero_mark = False
                for index in range( len(labels) ):
                   if labels[ index ] == 0.0:
                       found_zero_mark = True
                if found_zero_mark == False:
                    print( 'labels = ', labels )
                assert found_zero_mark == True


                num_of_clusters = len(list(Counter(labels) ))
                for m in range( len( labels ) ):
                    w_new = weeks[m]
                    num_in_dt = num_in_dt_arr[m]
                    days_double_arr[ w_new ][i].hourly_cluster[ num_in_dt ] =  labels[m] 
                    days_double_arr[ w_new ][i].hourly_cluster_num[ num_in_dt ] = num_of_clusters
                    assert days_double_arr[ w_new ][i].L_M_or_H == cluster_num
                    if labels[m] >= num_of_clusters:
                        print( 'Warning 296: index out of range' )
                        print( 'labels[m] >= num_of_clusters,  labels[m], num_of_clusters', labels[m], num_of_clusters )
                    assert labels[m] < num_of_clusters
                        

    for week in days_double_arr:
        for days in week:
            assert len( days.hourly_cluster ) == len( days.datetime_arr )
            assert len( days.hourly_cluster ) == len( days.hourly_cluster_num )
            for i in range( len( days.hourly_cluster ) ):
                assert days.hourly_cluster[i] >= 0
                assert days.hourly_cluster_num[i] >= 0

    return days_double_arr






def count_moments_probabilities( days_double_arr  ): 

    M = 0
    for i in range( len( days_double_arr ) ):
        if days_double_arr[i][0].L_M_or_H > M:
            M = days_double_arr[i][0].L_M_or_H

    M += 1

    group_meters_clasterized = []
    for m in range( M ):
        group_meters_weekly = [  ]
        for days in range( len( days_double_arr[0] ) ):
            diff_days = (days_double_arr[0][ days ].datetime_arr[ -1 ] - days_double_arr[0][ days ].datetime_arr[ 0 ]).days + 1
            group_meters_day_group = []
            for t in range( 24 ):
                group_meters_hourly = []
                for d in range( diff_days ):
                    for w in range( len( days_double_arr ) ):
                        needed_meter = days_double_arr[w][ days ]
                        class_week = needed_meter.L_M_or_H
                        if class_week == m:
                            energy = needed_meter.energy_export_arr[ d*24 + t ]
                            hourly_cluster = needed_meter.hourly_cluster[ d*24 + t ]
                            hourly_cluster_num = needed_meter.hourly_cluster_num[ d*24 + t ]
                            if (days == len( days_double_arr[0] ) - 1) and d == diff_days - 1 and t == 23:
                                week_day_next_indices = None
                                number_of_next_in_daily_meter = None
                                last_day = True
                            else:
                                last_day = False
                                if d == diff_days - 1 and t == 23:
                                    week_day_next_indices = [w, days + 1]
                                    number_of_next_in_daily_meter = 0
                                else:
                                    week_day_next_indices = [w, days ]
                                    number_of_next_in_daily_meter = d*24 + t + 1
                            group_meters_hourly += [ Measurements( energy, m, hourly_cluster, hourly_cluster_num, week_day_next_indices, number_of_next_in_daily_meter, last_day )  ]
                group_meters_day_group += [ group_meters_hourly ]
            
            group_meters_weekly += [ group_meters_day_group ]

        group_meters_clasterized += [group_meters_weekly]




    num_measures_1 = 0
    for week in days_double_arr:
        for days in week:
            num_measures_1 += len( days.datetime_arr )

    num_measures_2 = 0
    for weekly in group_meters_clasterized:
        for daily in weekly:
            for hourly in daily:
                num_measures_2 += len( hourly )

    assert num_measures_1 == num_measures_2


    bounds_weekly_clusters = []
    for weekly in group_meters_clasterized:
        mim_max = [ float( 'inf' ), 0 ]
        #for i in range( len( weekly ) ):
        for daily in weekly:
            for hourly in daily:
                for measurement in hourly:
                    if measurement.energy < mim_max[ 0 ]:
                        mim_max[ 0 ] = measurement.energy
                
                    if measurement.energy > mim_max[ 1 ]:
                        mim_max[ 1 ] = measurement.energy
        bounds_weekly_clusters += [ mim_max ]


    for weekly in group_meters_clasterized:
        for daily in weekly:
            for hourly in daily:
                num_clust_hourly = hourly[0].num_moment_classes
                for measurement in hourly:
                    assert measurement.num_moment_classes == num_clust_hourly



    bounds_daily_clusters = []
    for weekly in group_meters_clasterized:
        weekly_min_max = [ ]
        for days in weekly:
            days_min_max = [  ]
            for hourly in days:
                day_clusters_num = hourly[0].num_moment_classes
                min_max_arr = []
                for i in range( int(day_clusters_num) ):
                    min_max_arr += [[ float( 'inf' ), 0 ]]
                for measurement in hourly:
                    j = int( measurement.class_moment )
                    if measurement.energy < min_max_arr[j][ 0 ]:
                        min_max_arr[j][ 0 ] = measurement.energy
                
                    if measurement.energy > min_max_arr[j][ 1 ]:
                        min_max_arr[j][ 1 ] = measurement.energy
                days_min_max += [ min_max_arr ]
            weekly_min_max += [ days_min_max ]
        bounds_daily_clusters += [ weekly_min_max ]


 
    for weekly_min_max in bounds_daily_clusters:
        for days_min_max in weekly_min_max:
            for min_max_arr in days_min_max:
                for i in range( len( min_max_arr ) - 1 ):
                    assert min_max_arr[i+1][0] >= min_max_arr[i][1]
                    assert min_max_arr[i][1] >= min_max_arr[i][0]


    instances_in_clusters = []
    consumption_in_clusters = []
    for weekly in group_meters_clasterized:
        inst_in_cluster = 0
        cons_in_cluster = 0
        for i in range( len( weekly ) ):
            for daily in weekly:
                for hourly in daily:
                    for measurement in hourly:
                        inst_in_cluster += 1
                        cons_in_cluster += measurement.energy
        instances_in_clusters += [ inst_in_cluster ]
        consumption_in_clusters += [cons_in_cluster]

    
    average_consumption = []
    for i in range( len( instances_in_clusters ) ):
        av_cons = float(consumption_in_clusters[i])/float(instances_in_clusters[i])
        average_consumption += [ av_cons ]


    
    for i in range( len(average_consumption) - 1 ):
        assert average_consumption[i+1] >= average_consumption[i]
     


    transition_matrices_clustered_by_weeks = []
    bin_occurencies_clustered_by_weeks = []
    for w in range(len(group_meters_clasterized)):
        weekly_transition = [ ]
        bin_weekly_occurencies_list = [] 
        for days in range(len(group_meters_clasterized[w])):
            daily_transition = [  ]
            bin_daily_occurencies_list = [] 
            for h in range(len(group_meters_clasterized[w][days])):
                num_cl_0 = int(group_meters_clasterized[w][days][h][0].num_moment_classes)
                if h != 23:
                    num_cl_1 = int(group_meters_clasterized[w][days][h + 1][0].num_moment_classes)
                    occurencies = [0.0]*num_cl_0
                    for measurement in group_meters_clasterized[w][days][h]:
                        j = int( measurement.class_moment )
                        occurencies[j] += 1.0

                    transition_abs = np.zeros( (num_cl_0, num_cl_1) )
                    for mes in range(len(group_meters_clasterized[w][days][h])):
                        j = int( group_meters_clasterized[w][days][h][mes].class_moment )
                        k = int( group_meters_clasterized[w][days][h + 1][mes].class_moment )
                        transition_abs[j][k] += 1.0

                    transition_norm =  transition_abs 
                    for x in range(num_cl_0 ):
                        for y in range(num_cl_1):
                            if occurencies[x] > 0:
                                transition_norm[x][y] = float(transition_norm[x][y])/float(occurencies[x])
                            else:
                                assert transition_norm[x][y] == 0

                    for i in range(num_cl_0 ):
                        if occurencies[i] > 0:
                            summ = 0.0
                            for j in range(num_cl_1):
                                summ +=  transition_norm[i][j]
                            
                            assert summ > 0.9999
                            assert summ < 1.0001

                    
                    daily_transition += [transition_norm]
                



                occurencies = [0.0]*num_cl_0
                for measurement in group_meters_clasterized[w][days][h]:
                    j = int( measurement.class_moment )
                    occurencies[j] += 1.0
                    
                bounds_of_all_daily_clusters = []
                cluster_occurencies = np.zeros( ( num_cl_0, 10 ) )
                for i in range( num_cl_0 ):
                    bounds = bounds_daily_clusters[w][days][h][i]
                    step = (bounds[1] - bounds[0])/10.0
                    assert step >= 0
                    bounds_one_cluster = []
                    for s in range( 10 ):
                        bounds_one_cluster += [ [bounds[0] + s*step, bounds[0] + (s+1)*step  ] ]

                    for mes in group_meters_clasterized[w][days][h]:
                        if int( mes.class_moment ) == i:
                            interval_found = False
                            assert bounds_one_cluster[0][0] <= mes.energy
                            if bounds_one_cluster[0][0] <= mes.energy and bounds_one_cluster[0][1] >= mes.energy:
                                cluster_occurencies[i][0] += 1.0
                                interval_found = True
                            else:
                                for s in range( 9 ):
                                    if bounds_one_cluster[s+1][0] < mes.energy and bounds_one_cluster[s+1][1] >= mes.energy:
                                        cluster_occurencies[i][s + 1] += 1.0
                                        interval_found = True

                                if (interval_found == False) and ( mes.energy > bounds_one_cluster[9][1] ) and (  mes.energy - bounds_one_cluster[9][1] < 0.0001 ):
                                    cluster_occurencies[i][9] += 1.0
                                    interval_found = True
                            if interval_found == False:
                                print( 'bounds_one_cluster = ', bounds_one_cluster, 'mes.energy = ', mes.energy )
                                
                            assert interval_found == True

                    bounds_of_all_daily_clusters += [ bounds_one_cluster ]
                
                assert len( bounds_of_all_daily_clusters ) == num_cl_0


                cluster_occurencies_norm = np.copy(cluster_occurencies)
                for i in range( num_cl_0 ):
                    if int(occurencies[i]) != 0.0:   
                        for s in range(10):
                            cluster_occurencies_norm[i][s] = float( cluster_occurencies_norm[i][s] )/float( occurencies[i] )
                    else:
                        for s in range(10):
                            assert cluster_occurencies_norm[i][s] == 0

                    summ = 0.0
                    for s in range(10):
                        summ += cluster_occurencies_norm[i][s]
                    
                    if summ <= 0.9999:
                        print( 'summ = ', summ, 'cluster_occurencies_norm = ', cluster_occurencies_norm )
                    assert summ > 0.9999
                    assert summ < 1.0001

                bound_occurencies = Bin_bounds_and_occurencies(bounds_of_all_daily_clusters, cluster_occurencies_norm, num_cl_0 )
                bin_daily_occurencies_list += [ bound_occurencies ] 


            weekly_transition += [ daily_transition ]
            bin_weekly_occurencies_list += [ bin_daily_occurencies_list ]
        
        transition_matrices_clustered_by_weeks += [ weekly_transition ]
        bin_occurencies_clustered_by_weeks += [ bin_weekly_occurencies_list ]
    


    return [ group_meters_clasterized, bounds_daily_clusters, transition_matrices_clustered_by_weeks, bin_occurencies_clustered_by_weeks ]








if __name__ == "__main__":
    print( 'please run main.py')