import sys

sys.path.insert(1, 'C:\Projects\colombiadata\database')
from dbservices import *
from pathlib import Path
from datetime import datetime
from functions_0and1 import *
import os
import pandas as pd
import math
from random import random

#####################################################################################################################
####################################### Running Market Scenario's 0 and 1 ###########################################
print('RUNNING')
#####################################################################################################################
#  CONFIGURATE INPUT DATA
#####################################################################################################################

relative_path = str(settings.BASE_DIR) + "/"  # Specify location of other files
startdate = '2021-07-01 00:00:00'  # simulation start date
enddate = '2021-07-31 23:00:00'  # simulation end date

euro_exchangerate = 0.00019

SC = round(594 * euro_exchangerate, 2)   # Standard energy tariff
#SC = round(92 * euro_exchangerate, 2)   # Standard energy tariff
#SP = round(594 * euro_exchangerate, 2)  # Spot market price
SP = round(92 * euro_exchangerate, 2)  # Spot market price





#####################################################################################################################
#  IMPORT INPUT DATA
#####################################################################################################################
# read strata information from strata info table
df_strata = pd.read_csv(os.path.join(settings.BASE_DIR, "Data/house_info.csv"), header=0)

df_tariff = calculate_tariff(SC, SP)  # Function to calculate tariff
print(df_tariff)
df_load_tracker = pd.DataFrame(0, index=np.arange(len(df_strata)),
                               columns=['house_id', 'total_im', 'total_ex', 'total_load',
                                        'self_cons'])  # create dataframe which keeps track of load for each time periYod

df_load_tracker['house_id'] = df_strata['house_id']
df_load_tracker['total_load'] = False
df_load_tracker['self_cons'] = False

df_tariff_strata = pd.merge(df_strata, df_tariff, left_on='strata', right_index=True)  # merge tariffs with stratas

date_range = pd.date_range(start=startdate, end=enddate, freq="H")

#####################################################################################################################
#  CREATE EMPTY FRAMES
#####################################################################################################################

data_dict = {}
df_all_mcp = pd.DataFrame(columns=['mcp'])
df_data_all_periods_0 = pd.DataFrame(columns=['bid', 'quantity', 'price', 'house_id', 'energy_bought', 'active'])
df_data_all_periods_1 = pd.DataFrame(columns=['bid', 'quantity', 'price', 'house_id', 'energy_bought', 'active'])

#####################################################################################################################
#  GET ENERGY IMPORT AND EXPORT DATA
#####################################################################################################################

# list_house_ids_export = [56, 60, 53, 73, 75, 255, 77]
list_house_ids_import = [44, 51, 49, 50, 57, 58, 56, 60, 53, 73, 75, 255, 77]
list_house_ids_export = [44, 51, 49, 50, 57, 58, 56, 60, 53, 73, 75, 255, 77]

# house import
import_full = {
    house_id: get_energy_import(startdate, enddate, house_id) for house_id in
    list_house_ids_import}

house_import_generator = {house_id: None for house_id in list_house_ids_import}

for house_id in import_full.keys():
    import_full[house_id]['datetime'] = pd.to_datetime(import_full[house_id]['datetime'])
    import_full[house_id]['datetime'] = import_full[house_id]['datetime'].astype('datetime64[s]')
    import_full[house_id] = import_full[house_id].values
    house_import_generator[house_id] = (val for val in import_full[house_id])
try:
    house_import_curr_value = {
        house_id: next(house_import_generator[house_id])
        for house_id in list_house_ids_import
    }
except StopIteration:
    print('Stop iteration error')

# house export
export_full = {
    house_id: get_energy_export(startdate, enddate, house_id) for house_id in
    list_house_ids_export}

house_export_generator = {house_id: None for house_id in list_house_ids_export}

for house_id in export_full.keys():
    # Below if is for households that have no production
    if len(export_full[house_id]) == 0:
        export_full[house_id]['datetime'] = date_range
        export_full[house_id]['energy_export'] = 0.0

    export_full[house_id]['datetime'] = pd.to_datetime(export_full[house_id]['datetime'])
    export_full[house_id]['datetime'] = export_full[house_id]['datetime'].astype('datetime64[s]')
    export_full[house_id] = export_full[house_id].values
    house_export_generator[house_id] = (val for val in export_full[house_id])
try:
    house_export_curr_value = {
        house_id: next(house_export_generator[house_id])
        for house_id in list_house_ids_export
    }
except StopIteration:
    print('Stop iteration error')

#####################################################################################################################
#  RUN SIMULATION
#####################################################################################################################

for i, date_period in enumerate(date_range):
    trading_period = date_period

    participant_import_list = list()
    participant_export_list = list()

    for house_id in list_house_ids_import:
        try:
            while True:
                if house_import_curr_value[house_id][2] == date_period:
                    # this line does the same as the if not ... else statement below
                    # flats_consumption_list.append(flat_consumption_curr_value[flat][1] if not math.isnan(flat_consumption_curr_value[flat][1]) else 0)
                    if not math.isnan(house_import_curr_value[house_id][3]):
                        participant_import_list.append(house_import_curr_value[house_id][3])
                    else:
                        participant_import_list.append(0.0)
                        # print('0 added ##################################################################')
                    break
                elif house_import_curr_value[house_id][2] < date_period:
                    house_import_curr_value[house_id] = next(house_import_generator[house_id])
                elif house_import_curr_value[house_id][2] > date_period:
                    participant_import_list.append(0.0)
                    break
        except StopIteration:
            participant_import_list.append(0.0)
        except KeyError:
            participant_import_list.append(0.0)

    for house_id in list_house_ids_export:
        try:
            while True:
                if house_export_curr_value[house_id][2] == date_period:
                    # this line does the same as the if not ... else statement below
                    # flats_consumption_list.append(flat_consumption_curr_value[flat][1] if not math.isnan(flat_consumption_curr_value[flat][1]) else 0)
                    if not math.isnan(house_export_curr_value[house_id][3]):
                        participant_export_list.append(house_export_curr_value[house_id][3])
                    else:
                        participant_export_list.append(0.0)
                        # print('0 added ##################################################################')
                    break
                elif house_export_curr_value[house_id][2] < date_period:
                    house_export_curr_value[house_id] = next(house_export_generator[house_id])
                elif house_export_curr_value[house_id][2] > date_period:
                    participant_export_list.append(0.0)
                    break
        except StopIteration:
            participant_export_list.append(0.0)

        except KeyError:
            participant_export_list.append(0.0)

    buy_request_house = list()
    buy_request_house_ids = list()
    sell_request_house = list()
    sell_request_house_ids = list()

    for n, (buy_request, house_ids) in enumerate(zip(participant_import_list, list_house_ids_import)):
        buy_request_house.append(buy_request)
        buy_request_house_ids.append(house_ids)

    for n, (sell_request, house_ids) in enumerate(zip(participant_export_list, list_house_ids_export)):
        sell_request_house.append(sell_request)
        sell_request_house_ids.append(house_ids)

    energy_export = list()
    energy_import = list()
    energy_export_house = list()
    energy_import_house = list()

    for n in range(len(list_house_ids_export)):
        net_energy = buy_request_house[n] - sell_request_house[n]
        if net_energy < 0:
            energy_export.append(net_energy*(-1))
            energy_export_house.append(list_house_ids_export[n])
        elif net_energy > 0:
            energy_import.append(net_energy)
            energy_import_house.append(list_house_ids_export[n])
        else:
            continue


    energy_import_trading = pd.DataFrame(columns=['house_id', 'datetime', 'energy_import'])
    energy_import_trading['house_id'] = energy_import_house
    energy_import_trading['datetime'] = date_period
    energy_import_trading['energy_import'] = energy_import


    energy_export_trading = pd.DataFrame(columns=['house_id', 'datetime', 'energy_export'])
    energy_export_trading['house_id'] = energy_export_house
    energy_export_trading['datetime'] = date_period
    energy_export_trading['energy_export'] = energy_export

    # run market scenario 1
    df_results_scenario_1, df_all_mcp = run_market_scenario_1(energy_import_trading, energy_export_trading, df_load_tracker,
                                                  df_tariff_strata, trading_period, df_all_mcp)

    # run market Scenario 0
    df_load_tracker, df_results_scenario_0 = run_market_scenario_0(energy_import_trading, energy_export_trading,
                                                                   df_load_tracker, df_tariff_strata, trading_period)



    df_data_all_periods_0 = pd.concat([df_data_all_periods_0, df_results_scenario_0], ignore_index=False)

    df_data_all_periods_1 = pd.concat([df_data_all_periods_1, df_results_scenario_1], ignore_index=False)

print('CODE HAS FINISHED SUCCESSFULLY')

df_data_all_periods_0.to_csv(relative_path + 'Results/Scenario0/all_periods_scenario_0_BAU_NO_SUB_SPOT.csv')
df_data_all_periods_1.to_csv(relative_path + 'Results/Scenario1/all_periods_scenario_1_P2P_NO_SUB_SPOT.csv')
df_load_tracker.to_csv(relative_path + 'Results/Scenario0/total_load_overview_BAU_NO_SUB_SPOT.csv')
df_load_tracker.to_csv(relative_path + 'Results/Scenario1/total_load_overview_P2P_NO_SUB_SPOT.csv')
df_all_mcp.to_csv(relative_path + 'Results/Scenario1/all_mcp_P2P_NO_SUB_SPOT.csv')

