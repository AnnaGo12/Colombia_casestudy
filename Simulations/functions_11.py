import numpy as np
import math
import matplotlib.pyplot as plt
import pandas as pd
from collections import defaultdict
import sys
from requests import head

path = 'PricePlots/'


# def data_analysis_absolute(df_0, df_1, df_strata):
#     """
#     Function to compare absolute tariff changes not relative (i.e. in percent)
#
#     :param df_0:
#     :param df_1:
#     :param df_strata:
#     :return:
#     """
#     df_0_s = pd.merge(df_0, df_strata, left_on='house_id', right_on='house_id')
#     df_1_s = pd.merge(df_1, df_strata, left_on='house_id', right_on='house_id')
#
#     df_0_s[['type', 'number']] = df_0_s['user_id'].str.extract('([A-Za-z]+)(\d+\.?\d*)', expand=True)
#     df_1_s[['type', 'number']] = df_1_s['user_id'].str.extract('([A-Za-z]+)(\d+\.?\d*)', expand=True)
#
#     # creates dict by taking the mean of the price (no unique value) whole community considered but in absolute value
#     abs_import_comm, abs_export_comm = scenario_difference_community_absolute(df_0_s, df_1_s, 'price')
#
#     # creates dict by taking the mean of the price based on the strata
#     import_res_strata, export_res_strata, all_res_strata, import_res_strata_load, export_res_strata_load, \
#     all_res_strata_load = scenario_difference(df_0_s, df_1_s, 'strata', 'price')
#
#     # creates dict by taking the mean of the price based on the strata
#     import_res_house, export_res_house, all_res_house, import_res_house_load, export_res_house_load, \
#     all_res_house_load = scenario_difference(df_0_s, df_1_s, 'house_id', 'price')
#
#     import_res_type, export_res_type, all_res_type, import_res_type_load, export_res_type_load, \
#     all_res_type_load = scenario_difference(df_0_s, df_1_s, 'type', 'price')
#
#     return import_res_community, export_res_community, all_res_community, import_res_community_load, \
#            export_res_community_load, all_res_community_load, import_res_strata, export_res_strata, all_res_strata, \
#            import_res_strata_load, export_res_strata_load, all_res_strata_load, import_res_house, export_res_house, \
#            all_res_house, import_res_house_load, export_res_house_load, all_res_house_load, import_res_type, \
#            export_res_type, all_res_type, import_res_type_load, export_res_type_load, all_res_type_load
#
# def scenario_difference_community_absolute(df_0_comm, df_1_comm, result_value):
#     """
#     This function takes the results table form P2P and non-P2P market and returns the absolute average price differences
#
#     :param df_0_comm:
#     :param df_1_comm:
#     :param result_value:
#     :return:
#     """
#     # Call function create_array to create lists
#     energy_export_0, energy_import_0, energy_import_load_0, energy_export_load_0 = create_array_import_export_comm_absolute(df_0_comm, result_value)
#     energy_export_1, energy_import_1, energy_import_load_1, energy_export_load_1 = create_array_import_export_comm_absolute(df_1_comm, result_value)
#
#     #### res all
#     energy_all_0, energy_total_load_0 = create_array_all_comm(df_0_comm, result_value)
#     energy_all_1, energy_total_load_1 = create_array_all_comm(df_1_comm, result_value)
#
#     # calculate difference in import
#     res_import = [i / j for i, j in zip(energy_import_1, energy_import_0)]
#     res_import[:] = [1 - res_import for res_import in res_import]
#     # calculate difference in export
#     res_export = [i / j for i, j in zip(energy_export_0, energy_export_1)]
#     res_export[:] = [1 - res_export for res_export in res_export]
#     # calculate total difference
#     ####res all
#     res_all = [i / j for i, j in zip(energy_all_1, energy_all_0)]
#     res_all[:] = [1 - res_all for res_all in res_all]
#
#     res_import = [0 if math.isnan(x) else x for x in res_import]
#     #res_import = [round(num, 3) for num in res_import]
#     res_export = [0 if math.isnan(x) else x for x in res_export]
#     #res_export = [round(num, 3) for num in res_export]
#     res_all = [0 if math.isnan(x) else x for x in res_all]
#     #res_all = [round(num, 3) for num in res_all]
#
#     dict_import = dict(zip([1], res_import))
#     dict_export = dict(zip([1], res_export))
#     dict_all = dict(zip([1], res_all))
#
#     #energy_import_load_0 = [round(num, 0) for num in energy_import_load_0]
#     #energy_import_load_1 =[round(num, 0) for num in energy_import_load_1]
#     #energy_export_load_0  = [round(num, 0) for num in energy_export_load_0]
#     #energy_export_load_1 = [round(num, 0) for num in energy_export_load_1]
# #    energy_total_load_0 =[round(num, 0) for num in energy_total_load_0]
# #    energy_total_load_1= [round(num, 0) for num in energy_total_load_1]
#
#     dict_import_load = dict(zip([0, 1], list(zip(energy_import_load_0, energy_import_load_1))))
#     dict_export_load = dict(zip([0, 1], list(zip(energy_export_load_0, energy_export_load_1))))
#     list_all_load = [energy_total_load_0, energy_total_load_1]
#
#     #print('list_all_load')
#     #print(list_all_load)
#
#     return abs_import_comm, abs_export_comm
#
# def create_array_import_export_comm_absolute(df, result_value):
#
#     """
#     This function creates new lists where it saves the mean for every new scalability run
#
#     :param df:
#     :param result_value:
#     :return:
#     """
#     energy_import_abs = [0] * 1
#     energy_export_abs = [0] * 1
#
#     energy_export_abs[0] = abs(df.loc[(df['energy_bought'] == False), result_value]).mean()
#     energy_import_abs[0] = abs(df.loc[(df['energy_bought'] == True), result_value]).mean()
#
#     return energy_import_abs, energy_export_abs
#


########################################################################################################################
########################################################################################################################
########################################################################################################################

def consumption_values(df_all_periods_1):
    # Energy bought in the market
    sum1 = (df_all_periods_1.loc[df_all_periods_1['energy_bought'] == True, ['quantity']])

    total_imp_0 = sum1['quantity'].sum()

    #sum2 = (df_all_periods_1.loc[
    #    (df_all_periods_1['energy_bought'] == True) & (df_all_periods_1['peer_transaction'] == False), ['quantity']])
    #total_imp_grid = sum2['quantity'].sum()

    sum3 = (df_all_periods_1.loc[
        (df_all_periods_1['energy_bought'] == True) & (df_all_periods_1['peer_transaction'] == True), ['quantity']])
    total_imp_peer = sum3['quantity'].sum()

    # Energy sold in the market
    sum4 = (df_all_periods_1.loc[df_all_periods_1['energy_bought'] == False, ['quantity']])
    total_exp_0 = sum4['quantity'].sum()

    #sum5 = (df_all_periods_1.loc[
    #    (df_all_periods_1['energy_bought'] == False) & (df_all_periods_1['peer_transaction'] == False), ['quantity']])
    #total_exp_grid = sum5['quantity'].sum()

    sum6 = (df_all_periods_1.loc[
        (df_all_periods_1['energy_bought'] == False) & (df_all_periods_1['peer_transaction'] == True), ['quantity']])
    total_exp_peer = sum6['quantity'].sum()

    percent_bought_from_peers = total_imp_peer / total_imp_0

    percent_sold_to_peers = total_exp_peer / total_exp_0

    list_load = [percent_bought_from_peers, percent_sold_to_peers]
    #print('list_load')
    #print(list_load)
    return list_load


# Is in use
def  data_analysis_2(df_0, df_1, df_strata):
    df_0_s = pd.merge(df_0, df_strata, left_on='house_id', right_on='house_id')
    df_1_s = pd.merge(df_1, df_strata, left_on='house_id', right_on='house_id')

    df_0_s[['type', 'number']] = df_0_s['user_id'].str.extract('([A-Za-z]+)(\d+\.?\d*)', expand=True)
    df_1_s[['type', 'number']] = df_1_s['user_id'].str.extract('([A-Za-z]+)(\d+\.?\d*)', expand=True)

    df_0_s['price_kwh'] = df_0_s['quantity'] * df_0_s['price']
    df_1_s['price_kwh'] = df_1_s['quantity'] * df_1_s['price']

    # ------------------------------------------------------------------------------------------------------------------
    # Calculate average bill per participant

    average_community_bill = ((df_1_s.loc[(df_1_s['energy_bought'] == True), 'price_kwh']).sum()) - (
        (df_1_s.loc[(df_1_s['energy_bought'] == False), 'price_kwh']).sum())

    average_community_bill = average_community_bill / len(df_1_s.house_id.unique())

    # print(f'this is average community bill: {average_community_bill}')
    # print('---------------------------------------')
    # ------------------------------------------------------------------------------------------------------------------
    # Calculate average bill for low-income strata

    energy_imported = (df_1_s.loc[(df_1_s['energy_bought'] == True) & (df_1_s['strata'].isin([2, 3])), 'price_kwh'].sum())
    energy_exported = (df_1_s.loc[(df_1_s['energy_bought'] == False) & (df_1_s['strata'].isin([2, 3])), 'price_kwh'].sum())

    participating_houses = (df_1_s.loc[(df_1_s['energy_bought'] == True) & (df_1_s['strata'].isin([2, 3])), 'house_id'].unique())

    average_low_income_bill = (energy_imported-energy_exported) / len(participating_houses)

    # print(f'this is average low-income: {average_low_income_bill}')
    # print('---------------------------------------')
    # # ------------------------------------------------------------------------------------------------------------------
    # Calculate average bill for high-income strata

    energy_imported = (
        df_1_s.loc[(df_1_s['energy_bought'] == True) & (df_1_s['strata'].isin([4, 5, 6])), 'price_kwh'].sum())
    energy_exported = (
        df_1_s.loc[(df_1_s['energy_bought'] == False) & (df_1_s['strata'].isin([4, 5, 6])), 'price_kwh'].sum())

    participating_houses = (
        df_1_s.loc[(df_1_s['energy_bought'] == True) & (df_1_s['strata'].isin([4, 5, 6])), 'house_id'].unique())

    average_high_income_bill = (energy_imported - energy_exported) / len(participating_houses)

    # print(f'this is average low-income: {average_high_income_bill}')
    # print('---------------------------------------')
    # ------------------------------------------------------------------------------------------------------------------
    arr_df = df_0_s['user_id'].loc[(df_0_s['energy_bought'] == True)].unique()
    arr_df_high = df_0_s['user_id'].loc[(df_0_s['energy_bought'] == True) & (df_0_s['strata'].isin([4, 5, 6]))].unique()
    arr_df_low = df_0_s['user_id'].loc[(df_0_s['energy_bought'] == True) & (df_0_s['strata'].isin([2, 3]))].unique()

    # Calculate savings comparing BAU and P2P market

    savings_list = []

    for user_id in arr_df:
        total_cost_energy_import_BAU = sum(
            df_0_s['price_kwh'].loc[(df_0_s['user_id'] == user_id) & (df_0_s['energy_bought'] == True)])
        total_cost_energy_export_BAU = sum(
            df_0_s['price_kwh'].loc[(df_0_s['user_id'] == user_id) & (df_0_s['energy_bought'] == False)])
        total_cost_of_energy_BAU = total_cost_energy_import_BAU - total_cost_energy_export_BAU

        total_cost_energy_import_P2P = sum(
            df_1_s['price_kwh'].loc[(df_1_s['user_id'] == user_id) & (df_1_s['energy_bought'] == True)])
        total_cost_energy_export_P2P = sum(
            df_1_s['price_kwh'].loc[(df_1_s['user_id'] == user_id) & (df_1_s['energy_bought'] == False)])
        total_cost_of_energy_P2P = total_cost_energy_import_P2P - total_cost_energy_export_P2P

        if total_cost_of_energy_P2P - total_cost_of_energy_BAU > 0.1:
            print(total_cost_of_energy_BAU)
            print(total_cost_of_energy_P2P)
            sys.exit('BAU better than P2P -> Not possible')

        income_value = abs((1 - total_cost_of_energy_P2P / total_cost_of_energy_BAU) * 100)
        savings_list.append(income_value)

    savings_community = sum(savings_list) / len(savings_list)

    # print(f'These are the community savings in %: {savings_community}')
    # print('---------------------------------------')
    # ------------------------------------------------------------------------------------------------------------------
    # Calculate savings comparing BAU and P2P market for low-income

    savings_list = []

    for user_id in arr_df_low:
        total_cost_energy_import_BAU = sum(
            df_0_s['price_kwh'].loc[(df_0_s['user_id'] == user_id) & (df_0_s['energy_bought'] == True)])
        total_cost_energy_export_BAU = sum(
            df_0_s['price_kwh'].loc[(df_0_s['user_id'] == user_id) & (df_0_s['energy_bought'] == False)])
        total_cost_of_energy_BAU = total_cost_energy_import_BAU - total_cost_energy_export_BAU

        total_cost_energy_import_P2P = sum(
            df_1_s['price_kwh'].loc[(df_1_s['user_id'] == user_id) & (df_1_s['energy_bought'] == True)])
        total_cost_energy_export_P2P = sum(
            df_1_s['price_kwh'].loc[(df_1_s['user_id'] == user_id) & (df_1_s['energy_bought'] == False)])
        total_cost_of_energy_P2P = total_cost_energy_import_P2P - total_cost_energy_export_P2P

        if total_cost_of_energy_P2P - total_cost_of_energy_BAU > 0.1:
            print(total_cost_of_energy_BAU)
            print(total_cost_of_energy_P2P)
            sys.exit('BAU better than P2P -> Not possible')

        income_value = abs((1 - total_cost_of_energy_P2P / total_cost_of_energy_BAU) * 100)
        savings_list.append(income_value)

    low_income_savings = sum(savings_list) / len(savings_list)

    # print(f'These are the low-income community savings in %: {low_income_savings}')
    # print('---------------------------------------')
    # ------------------------------------------------------------------------------------------------------------------
    # Calculate savings comparing BAU and P2P market for high-income

    savings_list = []

    for user_id in arr_df_high:
        total_cost_energy_import_BAU = sum(
            df_0_s['price_kwh'].loc[(df_0_s['user_id'] == user_id) & (df_0_s['energy_bought'] == True)])
        total_cost_energy_export_BAU = sum(
            df_0_s['price_kwh'].loc[(df_0_s['user_id'] == user_id) & (df_0_s['energy_bought'] == False)])
        total_cost_of_energy_BAU = total_cost_energy_import_BAU - total_cost_energy_export_BAU

        total_cost_energy_import_P2P = sum(
            df_1_s['price_kwh'].loc[(df_1_s['user_id'] == user_id) & (df_1_s['energy_bought'] == True)])
        total_cost_energy_export_P2P = sum(
            df_1_s['price_kwh'].loc[(df_1_s['user_id'] == user_id) & (df_1_s['energy_bought'] == False)])
        total_cost_of_energy_P2P = total_cost_energy_import_P2P - total_cost_energy_export_P2P

        if total_cost_of_energy_P2P - total_cost_of_energy_BAU > 0.1:
            print(total_cost_of_energy_BAU)
            print(total_cost_of_energy_P2P)
            sys.exit('BAU better than P2P -> Not possible')

        income_value = abs((1 - total_cost_of_energy_P2P / total_cost_of_energy_BAU) * 100)
        savings_list.append(income_value)

    high_income_savings = sum(savings_list) / len(savings_list)

    # print(f'These are the high-income community savings in %: {high_income_savings}')
    # print('---------------------------------------')
    # ------------------------------------------------------------------------------------------------------------------
    # CALCULATE SELF CONSUMPTION

    # share of locally generated electricity that is being consumed in-house

    # total peer consumption
    community_peer_consumption = (
        df_1_s.loc[(df_1_s['energy_bought'] == True) & (df_1_s['peer_transaction'] == True), 'quantity'].sum())

    # total peer generation
    community_peer_generation = (
        df_1_s.loc[(df_1_s['energy_bought'] == False), 'quantity'].sum())

    self_consumption = community_peer_consumption / community_peer_generation

    # print(f'This is self_consumption: {self_consumption}')
    # print('---------------------------------------')

    # ------------------------------------------------------------------------------------------------------------------
    # CALCULATE SELF SUFFICIENCY

    # self-sufficiency is defined as the share of total demand that is being supplied by in-house-generated electricity

    # total peer consumption
    community_peer_consumption = (
        df_1_s.loc[(df_1_s['energy_bought'] == True) & (df_1_s['peer_transaction'] == True), 'quantity'].sum())

    # total peer generation
    community_conumption = (df_1_s.loc[(df_1_s['energy_bought'] == True), 'quantity'].sum())

    self_sufficiency = community_peer_consumption / community_conumption

    # print(f'This is  self_sufficiency: {self_sufficiency}')
    # print('---------------------------------------')
    # ------------------------------------------------------------------------------------------------------------------
    # Calculate average price per kWh

    cost_import_1 = (df_1_s.loc[(df_1_s['energy_bought'] == True), 'price_kwh'].sum())
    cost_export_1 = (df_1_s.loc[(df_1_s['energy_bought'] == False), 'price_kwh'].sum())

    quantity_imp_1 = (df_1_s.loc[(df_1_s['energy_bought'] == True), 'quantity'].sum())
    quantity_exp_1 = (df_1_s.loc[(df_1_s['energy_bought'] == False), 'quantity'].sum())

    avg_price_per_kwh = (cost_import_1-cost_export_1)/(quantity_imp_1)
    # print(f'this is average price per kWh: {avg_price_per_kwh}')
    # print('---------------------------------------')

    # ------------------------------------------------------------------------------------------------------------------
    # Calculate average price per kWh for low-income strata

    # for strata 2 and 3

    cost_import_1 = (df_1_s.loc[(df_1_s['energy_bought'] == True) & (df_1_s['strata'].isin([2,3])), 'price_kwh'].sum())
    cost_export_1 = (df_1_s.loc[(df_1_s['energy_bought'] == False) & (df_1_s['strata'].isin([2,3])), 'price_kwh'].sum())

    quantity_imp_1 = (df_1_s.loc[(df_1_s['energy_bought'] == True) & (df_1_s['strata'].isin([2,3])), 'quantity'].sum())
    quantity_exp_1 = (df_1_s.loc[(df_1_s['energy_bought'] == False) & (df_1_s['strata'].isin([2,3])), 'quantity'].sum())

    participating_houses = (df_1_s.loc[(df_1_s['energy_bought'] == True) & (df_1_s['strata'].isin([2,3])), 'house_id'].unique())


    low_income_avg_price_per_kwh = ((cost_import_1-cost_export_1)/(quantity_imp_1))

    # print(f'This is average kWh price of low-income participants: {low_income_avg_price_per_kwh}')
    # print('---------------------------------------')

    # ------------------------------------------------------------------------------------------------------------------
    # Calculate average price per kWh for high-income strata

    # for strata 4, 5 and 6

    cost_import_1 = (df_1_s.loc[(df_1_s['energy_bought'] == True) & (df_1_s['strata'].isin([4, 5, 6])), 'price_kwh'].sum())
    cost_export_1 = (df_1_s.loc[(df_1_s['energy_bought'] == False) & (df_1_s['strata'].isin([4, 5, 6])), 'price_kwh'].sum())

    quantity_imp_1 = (df_1_s.loc[(df_1_s['energy_bought'] == True) & (df_1_s['strata'].isin([4, 5, 6])), 'quantity'].sum())
    quantity_exp_1 = (df_1_s.loc[(df_1_s['energy_bought'] == False) & (df_1_s['strata'].isin([4, 5, 6])), 'quantity'].sum())

    participating_houses = (df_1_s.loc[(df_1_s['energy_bought'] == True) & (df_1_s['strata'].isin([4, 5, 6])), 'house_id'].unique())

    high_income_avg_price_per_kwh = ((cost_import_1 - cost_export_1) / (quantity_imp_1))

    # print(f'This is average kWh price of low-income participants: {high_income_avg_price_per_kwh}')
    # print('---------------------------------------')

    return average_community_bill, average_low_income_bill, average_high_income_bill, savings_community, low_income_savings, high_income_savings, self_consumption, self_sufficiency, avg_price_per_kwh, low_income_avg_price_per_kwh, high_income_avg_price_per_kwh



def scenario_difference_community(df_0_comm, df_1_comm, result_value):
    # Call function create_array to create lists
    energy_export_0, energy_import_0, energy_import_load_0, energy_export_load_0 = create_array_import_export_comm(df_0_comm, result_value)
    energy_export_1, energy_import_1, energy_import_load_1, energy_export_load_1 = create_array_import_export_comm(df_1_comm, result_value)

    #### res all
    energy_all_0, energy_total_load_0 = create_array_all_comm(df_0_comm, result_value)
    energy_all_1, energy_total_load_1 = create_array_all_comm(df_1_comm, result_value)

    # calculate difference in import
    res_import = [i / j for i, j in zip(energy_import_1, energy_import_0)]
    # res_import[:] = [1 - res_import for res_import in res_import]
    # calculate difference in export
    res_export = [i / j for i, j in zip(energy_export_1, energy_export_0)]
    #res_export[:] = [1 - res_export for res_export in res_export]
    # calculate total difference
    ####res all
    res_all = [i / j for i, j in zip(energy_all_1, energy_all_0)]
    #res_all[:] = [1 - res_all for res_all in res_all]

    res_import = [0 if math.isnan(x) else x for x in res_import]
    #res_import = [round(num, 3) for num in res_import]
    res_export = [0 if math.isnan(x) else x for x in res_export]
    #res_export = [round(num, 3) for num in res_export]
    res_all = [0 if math.isnan(x) else x for x in res_all]
    #res_all = [round(num, 3) for num in res_all]

    dict_import = dict(zip([1], res_import))
    dict_export = dict(zip([1], res_export))
    dict_all = dict(zip([1], res_all))

    #energy_import_load_0 = [round(num, 0) for num in energy_import_load_0]
    #energy_import_load_1 =[round(num, 0) for num in energy_import_load_1]
    #energy_export_load_0  = [round(num, 0) for num in energy_export_load_0]
    #energy_export_load_1 = [round(num, 0) for num in energy_export_load_1]
#    energy_total_load_0 =[round(num, 0) for num in energy_total_load_0]
#    energy_total_load_1= [round(num, 0) for num in energy_total_load_1]

    dict_import_load = dict(zip([0, 1], list(zip(energy_import_load_0, energy_import_load_1))))
    dict_export_load = dict(zip([0, 1], list(zip(energy_export_load_0, energy_export_load_1))))
    list_all_load = [energy_total_load_0, energy_total_load_1]

    #print('list_all_load')
    #print(list_all_load)

    return dict_import, dict_export, dict_all, dict_import_load, dict_export_load, list_all_load

def create_array_import_export_comm(df, result_value):
    energy_import = [0] * 1
    energy_export = [0] * 1

    energy_import_load = [0] * 1
    energy_export_load = [0] * 1

    energy_export[0] = abs(df.loc[(df['energy_bought'] == False), result_value]).mean()
    energy_import[0] = abs(df.loc[(df['energy_bought'] == True), result_value]).mean()

    energy_import_load[0] = (abs(df.loc[(df['energy_bought'] == True), result_value]) *
                            (df.loc[(df['energy_bought'] == True), 'quantity'])).sum()
    energy_export_load[0] = (abs(df.loc[(df['energy_bought'] == False), result_value]) *
                            (df.loc[(df['energy_bought'] == False), 'quantity'])).sum()

    # energy_import_load[0] = ((df.loc[(df['energy_bought'] == True), 'quantity'])).sum()
    # energy_export_load[0] = ((df.loc[(df['energy_bought'] == False), 'quantity'])).sum()

    return energy_export, energy_import, energy_import_load, energy_export_load

def create_array_all_comm(df_comm, result_value):
    # Create empty arrays
    #rint(result_value)
    energy_total = [0] * 1
   #print(df_comm)
    df_comm['price'] = np.where(df_comm['energy_bought'], df_comm['price'], abs(df_comm['price']) * (-1))
    #rint(df_comm['price'])
    ## Res all
    energy_total[0] = df_comm.loc[:, result_value].mean()
   #print(energy_total[0])
    energy_total_load= (df_comm.loc[:, result_value] * df_comm.loc[:, 'quantity']).sum()
    # energy_total_load = (df_comm.loc[:, 'quantity']).sum()

    return energy_total, energy_total_load

def scenario_difference(df_0, df_1, unique_value, result_value):
    arr_df = df_0[unique_value].unique()
    arr_df = np.sort(arr_df)
    arr_df_0 = arr_df
    arr_df_1 = arr_df
    ind = np.arange(len(arr_df))  # the x locations for the groups
    width = 0.35  # the width of the bars

    # Call function create_array to create lists
    energy_export_0, energy_import_0, energy_export_load_0, energy_import_load_0 = \
        create_array_import_export(df_0, arr_df, unique_value, result_value)
    energy_export_1, energy_import_1, energy_export_load_1, energy_import_load_1 = \
        create_array_import_export(df_1, arr_df, unique_value, result_value)
    energy_all_0, energy_total_load_0 = create_array_all(df_0, arr_df, unique_value, result_value)
    energy_all_1, energy_total_load_1 = create_array_all(df_1, arr_df, unique_value, result_value)

    # calculate difference in import
    res_import = [i / j for i, j in zip(energy_import_1, energy_import_0)]
    # res_import[:] = [1 - res_import for res_import in res_import]
    # calculate difference in export
    res_export = [i / j for i, j in zip(energy_export_1, energy_export_0)]
    # res_export[:] = [1 - res_export for res_export in res_export]
    # calculate total difference
    res_all = [i / j for i, j in zip(energy_all_1, energy_all_0)]
    # res_all[:] = [1 - res_all for res_all in res_all]

    res_import = [0 if math.isnan(x) else x for x in res_import]
    # res_import = [round(num, 3) for num in res_import]
    res_export = [0 if math.isnan(x) else x for x in res_export]
    # res_export = [round(num, 3) for num in res_export]
    res_all = [0 if math.isnan(x) else x for x in res_all]
    # res_all = [round(num, 3) for num in res_all]

    dict_import = dict(zip(arr_df, res_import))
    dict_export = dict(zip(arr_df, res_export))
    dict_all = dict(zip(arr_df, res_all))

    # energy_import_load_0 = [round(num, 0) for num in energy_import_load_0]
    # energy_import_load_1 =[round(num, 0) for num in energy_import_load_1]
    # energy_export_load_0 = [round(num, 0) for num in energy_export_load_0]
    # energy_export_load_1 = [round(num, 0) for num in energy_export_load_1]
    # energy_total_load_0 =[round(num, 0) for num in energy_total_load_0]
    # energy_total_load_1= [round(num, 0) for num in energy_total_load_1]

    dict_import_load = dict(zip(arr_df_0, list(zip(energy_import_load_0, energy_import_load_1))))
    dict_export_load = dict(zip(arr_df_0, list(zip(energy_export_load_0, energy_export_load_1))))
    dict_all_load = dict(zip(arr_df_0, list(zip(energy_total_load_0, energy_total_load_1))))

    return dict_import, dict_export, dict_all, dict_import_load, dict_export_load, dict_all_load

def create_array_import_export(df, arr_df, unique_value, result_value):
    # Create empty arrays
    energy_import = [0] * len(arr_df)
    energy_export = [0] * len(arr_df)

    energy_import_load = [0] * len(arr_df)
    energy_export_load = [0] * len(arr_df)

    # Data for % change in price
    for i in range(len(arr_df)):
        energy_export[i] = abs(df.loc[
                                   (df['energy_bought'] == False) & (
                                               df[unique_value] == arr_df[i]), result_value]).mean()
        energy_import[i] = abs(df.loc[
                                   (df['energy_bought'] == True) & (
                                               df[unique_value] == arr_df[i]), result_value]).mean()

    # Data for cost of total load
    for i in range(len(arr_df)):
        energy_export_load[i] = abs(df.loc[
                                        (df['energy_bought'] == False) & (
                                                df[unique_value] == arr_df[i]), result_value] * df.loc[
                                        (df['energy_bought'] == False) & (
                                                df[unique_value] == arr_df[i]), 'quantity']).sum()

        energy_import_load[i] = abs(df.loc[
                                        (df['energy_bought'] == True) & (df[unique_value] == arr_df[i]), result_value] *
                                    df.loc[
                                        (df['energy_bought'] == True) & (
                                                df[unique_value] == arr_df[i]), 'quantity']).sum()

        # energy_export_load[i] = abs(df.loc[(df['energy_bought'] == False) & (df[unique_value] == arr_df[i]), 'quantity']).sum()
        #
        # energy_import_load[i] = abs(df.loc[(df['energy_bought'] == True) & ( df[unique_value] == arr_df[i]), 'quantity']).sum()

    return energy_export, energy_import, energy_export_load, energy_import_load

def create_array_all(df_individ, arr_df, unique_value, result_value):
    # Create empty arrays
    energy_total = [0] * len(arr_df)
    energy_total_load = [0] * len(arr_df)

    df_individ['price'] = np.where(df_individ['energy_bought'], df_individ['price'], abs(df_individ['price']) * (-1))

    for i in range(len(arr_df)):
        energy_total[i] = df_individ.loc[df_individ[unique_value] == arr_df[i], result_value].mean()

    for i in range(len(arr_df)):
        energy_total_load[i] = (df_individ.loc[df_individ[unique_value] == arr_df[i], result_value] * \
                                df_individ.loc[df_individ[unique_value] == arr_df[i], 'quantity']).sum()
        # energy_total_load[i] = (df_individ.loc[df_individ[unique_value] == arr_df[i], 'quantity']).sum()

    return energy_total, energy_total_load


def add_new_participants(df, df_participant, participant_type, participant_strata, participant_per_loop, trading_period,
                         data_tables):


    df_selected_period = df_participant.loc[df_participant['datetime'] == str(trading_period)]
    df_selected = df_selected_period.iloc[:, 1:participant_per_loop + 1]
    df_selected = df_selected.T
    df_selected.columns = [data_tables]
    df_selected['house_id'] = df_selected.index
    df_selected['id'] = df_selected.index

    df_selected['id'] = df_selected['id'].astype(float)
    df_selected['house_id'] = df_selected['house_id'].astype(float)

    df = df.append([df_selected])
    df['datetime'] = df['datetime'].fillna(trading_period)


    return df


def get_data_by_datetime_11(table_name, trading_period, i):
    """Function to call load data from database"""
    colnames = ['id', 'house_id', 'datetime', table_name]
    df = pd.read_csv('../Data/' + table_name + '.csv', names=colnames, header=0)
    mask = df['datetime'] == str(trading_period)
    df = df.loc[mask]

    if i == 0:
        df = df
    else:
        start = 3
        end = start
        while start <= i + end:
            colnames = ['id', 'house_id', 'datetime', table_name]
            df_SP3 = pd.read_csv('../Data/' + table_name + '_C6.csv', usecols=[0, 1, 2, start], names=colnames,
                                 header=0)
            mask = df_SP3['datetime'] == str(trading_period)
            df_SP3 = df_SP3.loc[mask]
            df_SP3['house_id'] = start - 2
            df = pd.concat([df, df_SP3], ignore_index=False)
            start += 1
    return df


def plot_line_graph(df, dict_labels):
    fig, ax = plt.subplots()

    x = df['trading_period'].to_list()
    y = df['mcp'].to_list()

    ax.set_xlabel(dict_labels['x_label'])
    ax.set_xlabel(dict_labels['y_label'])
    ax.set_title(dict_labels['title'])

    plt.plot(x, y)

    # fig.savefig(path+dict_labels['figname'])


def plot_vertical_bar_chart(dict_in, dict_labels, im_ex):
    plt.rcdefaults()
    fig, ax = plt.subplots()

    keys = dict_in.keys()
    values = dict_in.values()

    y_pos = np.arange(len(keys))

    ax.barh(y_pos, values, align='center')
    ax.set_yticks(y_pos)
    ax.set_yticklabels(keys)
    ax.invert_yaxis()  # labels read top-to-bottom
    ax.set_xlabel(dict_labels['x_label'])
    ax.set_title(dict_labels['title'] + im_ex)

    # fig.savefig(path+dict_labels['figname'] + im_ex)


def bar_plot_sum(df, unique_value, return_value, label, title, figname):
    arr_df = df[unique_value].unique()
    arr_df = np.sort(arr_df)

    ind = np.arange(len(arr_df))  # the x locations for the groups
    width = 0.35  # the width of the bars

    # Create empty arrays
    energy_import = [0] * len(arr_df)
    energy_export = [0] * len(arr_df)

    for i in range(len(arr_df)):
        energy_export[i] = df.loc[(df['energy_bought'] == False) & (df[unique_value] == arr_df[i]), return_value].sum()
        energy_import[i] = df.loc[(df['energy_bought'] == True) & (df[unique_value] == arr_df[i]), return_value].sum()

    energy_import = [0 if math.isnan(x) else x for x in energy_import]
    energy_import = [round(num) for num in energy_import]
    energy_export = [0 if math.isnan(x) else x for x in energy_export]
    energy_export = [round(num) for num in energy_export]

    fig, ax = plt.subplots()
    rects1 = ax.bar(ind - width / 2, energy_import, width, label='import')
    rects2 = ax.bar(ind + width / 2, energy_export, width, label='export')

    # Add some text for labels, title and custom x-axis tick labels, etc.
    ax.set_ylabel(label)
    ax.set_title(title)
    ax.set_xticks(ind)
    ax.set_xticklabels(arr_df)
    ax.legend()

    def autolabel(rects, xpos='center'):
        """
        Attach a text label above each bar in *rects*, displaying its height.
        *xpos* indicates which side to place the text w.r.t. the center of
        the bar. It can be one of the following {'center', 'right', 'left'}.
        """

        ha = {'center': 'center', 'right': 'left', 'left': 'right'}
        offset = {'center': 0, 'right': 1, 'left': -1}

        for rect in rects:
            height = rect.get_height()
            ax.annotate('{}'.format(height),
                        xy=(rect.get_x() + rect.get_width() / 2, height),
                        xytext=(offset[xpos] * 3, 3),  # use 3 points offset
                        textcoords="offset points",  # in both directions
                        ha=ha[xpos], va='bottom')

    autolabel(rects1, "left")
    autolabel(rects2, "right")

    fig.tight_layout()

    # plt.show()

    # fig.savefig(path+figname)


def bar_plot_mean(df, unique_value, return_value, label, title, figname):
    arr_df = df[unique_value].unique()
    arr_df = np.sort(arr_df)

    ind = np.arange(len(arr_df))  # the x locations for the groups
    width = 0.35  # the width of the bars

    # Create empty arrays
    energy_import = [0] * len(arr_df)
    energy_export = [0] * len(arr_df)

    for i in range(len(arr_df)):
        energy_export[i] = df.loc[(df['energy_bought'] == False) & (df[unique_value] == arr_df[i]), return_value].mean()
        energy_import[i] = df.loc[(df['energy_bought'] == True) & (df[unique_value] == arr_df[i]), return_value].mean()

    energy_import = [0 if math.isnan(x) else x for x in energy_import]
    energy_import = [round(num) for num in energy_import]
    energy_export = [0 if math.isnan(x) else x for x in energy_export]
    energy_export = [round(num) for num in energy_export]

    fig, ax = plt.subplots()
    rects1 = ax.bar(ind - width / 2, energy_import, width, label='import')
    rects2 = ax.bar(ind + width / 2, energy_export, width, label='export')

    # Add some text for labels, title and custom x-axis tick labels, etc.
    ax.set_ylabel(label)
    ax.set_title(title)
    ax.set_xticks(ind)
    ax.set_xticklabels(arr_df)
    ax.legend()

    def autolabel(rects, xpos='center'):
        """
        Attach a text label above each bar in *rects*, displaying its height.
        *xpos* indicates which side to place the text w.r.t. the center of
        the bar. It can be one of the following {'center', 'right', 'left'}.
        """

        ha = {'center': 'center', 'right': 'left', 'left': 'right'}
        offset = {'center': 0, 'right': 1, 'left': -1}

        for rect in rects:
            height = rect.get_height()
            ax.annotate('{}'.format(height),
                        xy=(rect.get_x() + rect.get_width() / 2, height),
                        xytext=(offset[xpos] * 3, 3),  # use 3 points offset
                        textcoords="offset points",  # in both directions
                        ha=ha[xpos], va='bottom')

    autolabel(rects1, "left")
    autolabel(rects2, "right")

    fig.tight_layout()

    # plt.show()

    fig.savefig(path + figname)
