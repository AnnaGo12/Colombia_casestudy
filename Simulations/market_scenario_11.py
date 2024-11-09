import sys

sys.path.insert(1, 'C:\Projects\colombiadata\database')
from dbservices import *

from functions_0and1 import *
from functions_11 import *
import pandas as pd
import numpy as np
import os


###################################### RUN SIMULATION SCENARIO 1.1   ##############################################

print('START')

######################################   CONFIGURATE INPUT DATA      ##############################################

relative_path = str(settings.BASE_DIR) + "/"  # Specify location of other files
startdate = '2021-07-01 00:00:00'  # simulation start date
enddate = '2021-07-31 23:00:00'  # simulation end date
results_path = "../Results/results_11/csv/"

euro_exchangerate = 0.00019

SC = round(594 * euro_exchangerate, 2)   # Standard energy tariff
SP = round(92 * euro_exchangerate, 2)  # Spot market price

data_tables = ["energy_import", "energy_export"]  # table names for data extraction

df_tariff = calculate_tariff(SC, SP)  # Create df function
loop_house = 0 # starting number of new houses added to the market
loop_house_1 = 0

start_loop_house = loop_house
start_loop_house_1 = loop_house_1
max_house = 50  # max. number of houses added to the market
max_house_1 = 50  # max. number of houses added to the market

participant_type = "C"  # Specify what type of house is added
participant_type_1 = "SP"  # Specify what type of house is added

participant_strata = 5  # Specify the strata the participant is joining
participant_strata_1 = 2  # Specify the strata the participant is joining

participant_per_loop = 5 # Number of participants added in each loop
count = 0  # counts the columns to add the results in the loop
count_1 = 0

matrix_length = int((max_house-loop_house)/participant_per_loop) + 1
matrix_length_1 = int((max_house_1-loop_house_1)/participant_per_loop) + 1

print(f'this is matrix_length: {matrix_length}')


########################################     IMPORT INPUT DATA   ##################################################

#This is all energy import an energy export data

df_strata = pd.read_csv(os.path.join(settings.BASE_DIR, "Data/house_info.csv"),
                        header=0)  # read strata information from strata info table
df_tariff = calculate_tariff(SC, SP)  # Function to calculate tariff

#df_dates = pd.DataFrame(
#    {'datetime': pd.date_range(start=startdate, end=enddate, freq='H')})  # create dataframe for trading period
date_range = pd.date_range(start=startdate, end=enddate, freq="H")

###  --- This spection is specific to Scenario 1.1 ----------------------------------------------------------------

# House 0
df_new_participants_import = pd.read_csv(
    relative_path + 'Data/' + data_tables[0] + participant_type + str(participant_strata) + '.csv', header=0)
df_new_participants_import['datetime'] = pd.to_datetime(df_new_participants_import['datetime'], format="%d/%m/%Y %H:%M")

df_new_participants_export = pd.read_csv(
    relative_path + 'Data/' + data_tables[1] + participant_type + str(participant_strata) + '.csv', header=0)
df_new_participants_export['datetime'] = pd.to_datetime(df_new_participants_export['datetime'], format="%d/%m/%Y %H:%M")

house_id = df_new_participants_import.columns.tolist()
house_id = house_id[1:]
#strata = [participant_strata] * len(df_new_participants_import.iloc[0, 1:]) # here I could introduce random strata
# Calculate strata_1 list with random values between 2 and 3 for each element
strata = [random.choice([4,5,6]) for _ in range(len(df_new_participants_import.iloc[0, 1:]))]
user_id = [participant_type + s for s in house_id]
df_strata_new = pd.DataFrame(list(zip(house_id, strata, user_id)), columns=df_strata.columns.tolist())
df_strata = pd.concat([df_strata, df_strata_new], ignore_index=True)
df_strata['house_id'] = df_strata['house_id'].astype(int)

# House 1

df_new_participants_import_1 = pd.read_csv(
    relative_path + 'Data/' + data_tables[0] + participant_type_1 + str(participant_strata_1) + '.csv', header=0)
df_new_participants_import_1['datetime'] = pd.to_datetime(df_new_participants_import_1['datetime'], format="%d/%m/%Y %H:%M")

df_new_participants_export_1 = pd.read_csv(
    relative_path + 'Data/' + data_tables[1] + participant_type_1 + str(participant_strata_1) + '.csv', header=0)
df_new_participants_export_1['datetime'] = pd.to_datetime(df_new_participants_export_1['datetime'], format="%d/%m/%Y %H:%M")

house_id_1 = df_new_participants_import_1.columns.tolist()
house_id_1 = house_id_1[1:]

# Calculate strata_1 list with random values between 2 and 3 for each element
strata_1 = [random.choice([2, 3]) for _ in range(len(df_new_participants_import_1.iloc[0, 1:]))]
user_id_1 = [participant_type_1 + s for  s in house_id_1]
df_strata_new_1 = pd.DataFrame(list(zip(house_id_1, strata_1, user_id_1)), columns=df_strata.columns.tolist())
df_strata = pd.concat([df_strata, df_strata_new_1], ignore_index=True)
df_strata['house_id'] = df_strata['house_id'].astype(int)


###  --- End ------------------------------------------------------------------------------------------------------

df_tariff_strata = pd.merge(df_strata, df_tariff, left_on='strata', right_index=True)  # merge tariffs with stratas



#######################################     CREATE EMPTY FRAMES   #################################################

data_dict = {}
df_all_mcp = pd.DataFrame(columns=['mcp'])
df_data_all_periods_0 = pd.DataFrame(columns=['bid', 'quantity', 'price', 'house_id', 'energy_bought', 'active'])
df_data_all_periods_1 = pd.DataFrame(columns=['bid', 'quantity', 'price', 'house_id', 'energy_bought', 'active'])


# import_res_strata = {2: 0, 3: 0, 4: 0, 6: 0}
# export_res_strata = {2: 0, 3: 0, 4: 0, 6: 0}

uniq_strata = set(df_strata['strata'])
uniq_houseid = set(df_strata['house_id'])

df_strata[['type', 'number']] = df_strata['user_id'].str.extract('([A-Za-z]+)(\d+\.?\d*)', expand=True)

uniq_type = set(df_strata['type'])

#df_strata.to_csv(relative_path + 'Results/2023-11-15_scale_20-1_Strata/strata_list.csv')

#df_strata.to_csv(os.path.join(settings.BASE_DIR, "Data/strata_list.csv"))


#this is a predefined strata list so that all households have the same strata in all 4 scenarios
df_strata = pd.read_csv(os.path.join(settings.BASE_DIR, "Data/strata_list.csv"))

#####################################################################################################################
#  GET ENERGY IMPORT AND EXPORT DATA
#####################################################################################################################

# list_house_ids_export = [56, 60, 53, 73, 75, 255, 77]
list_house_ids_import = [44, 51, 49, 50, 57, 58, 56, 60, 53, 73, 75, 255, 77]
list_house_ids_export = [44, 51, 49, 50, 57, 58, 56, 60, 53, 73, 75, 255, 77]

###################################################################################################################
#  RUN SIMULATION

print('RUNNING')

while loop_house_1 <= max_house_1:  # Checks how many houses are added per loup of house type "C"

    while loop_house <= max_house:  # Checks how many houses are added per loup of house type "SP"


        print(f'{loop_house_1} participants have been added in loop 1 of participant type {participant_type_1}')
        print(f'{loop_house} participants have been added in loop 0 of participant type {participant_type}')


        df_load_tracker = pd.DataFrame(0, index=np.arange(len(df_strata)),
                                       columns=['house_id', 'total_im', 'total_ex', 'total_load',
                                                'self_cons'])  # create dataframe which keeps track of load for each time period

        df_load_tracker['house_id'] = df_strata['house_id']
        df_load_tracker['total_load'] = False
        df_load_tracker['self_cons'] = False

        df_data_all_periods_0.drop(df_data_all_periods_0.index, inplace=True)
        df_data_all_periods_1.drop(df_data_all_periods_1.index, inplace=True)


        #---------------------

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

        #---------------------


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

            energy_import_house = pd.DataFrame(columns=['house_id', 'datetime', 'energy_import'])
            energy_import_house['house_id'] = buy_request_house_ids
            energy_import_house['datetime'] = date_period
            energy_import_house['energy_import'] = buy_request_house

            energy_export_house = pd.DataFrame(columns=['house_id', 'datetime', 'energy_export'])
            energy_export_house['house_id'] = sell_request_house_ids
            energy_export_house['datetime'] = date_period
            energy_export_house['energy_export'] = sell_request_house



            #--------------------------------------

            # loads new participants to import dataframe

            if loop_house_1 == 0 and loop_house == 0:

                column1_values = energy_import_house['energy_import'].values
                column2_values = energy_export_house['energy_export'].values

                # Calculate the difference using NumPy
                difference = column1_values - column2_values

                # Create df3 and df4 DataFrames with additional columns
                energy_import_trading = energy_import_house.copy()
                energy_import_trading['energy_import'] = np.where(difference > 0, difference, np.nan)

                energy_export_trading = energy_export_house.copy()
                energy_export_trading['energy_export'] = np.where(difference < 0, difference * (-1), np.nan)

                # Remove rows with NaN values in 'Positive_Differences' or 'Negative_Differences' columns
                energy_import_trading = energy_import_trading.dropna(subset=['energy_import'], how='any')
                energy_export_trading = energy_export_trading.dropna(subset=['energy_export'], how='any')


            # I substract export - import before I imported the data so I don't have to do it again
            elif loop_house_1 == 0 and loop_house > 0:



                energy_import_house = add_new_participants(energy_import_house, df_new_participants_import, participant_type,
                                                 participant_strata, loop_house, trading_period, data_tables[0])
                # loads new participants to export dataframe
                energy_export_house = add_new_participants(energy_export_house, df_new_participants_export, participant_type,
                                                 participant_strata, loop_house, trading_period, data_tables[1])


                column1_values = energy_import_house['energy_import'].values
                column2_values = energy_export_house['energy_export'].values

                # Calculate the difference using NumPy
                difference = column1_values - column2_values

                # Create df3 and df4 DataFrames with additional columns
                energy_import_trading = energy_import_house.copy()
                energy_import_trading['energy_import'] = np.where(difference > 0, difference, np.nan)

                energy_export_trading = energy_export_house.copy()
                energy_export_trading['energy_export'] = np.where(difference < 0, difference * (-1), np.nan)

                # Remove rows with NaN values in 'Positive_Differences' or 'Negative_Differences' columns
                energy_import_trading = energy_import_trading.dropna(subset=['energy_import'], how='any')
                energy_export_trading = energy_export_trading.dropna(subset=['energy_export'], how='any')




            elif loop_house_1 > 0 and loop_house == 0:




                energy_import_house = add_new_participants(energy_import_house, df_new_participants_import_1, participant_type_1,
                                                 participant_strata_1, loop_house_1, trading_period, data_tables[0])
                # loads new participants to export dataframe
                energy_export_house = add_new_participants(energy_export_house, df_new_participants_export_1, participant_type_1,
                                                 participant_strata_1, loop_house_1, trading_period, data_tables[1])



                column1_values = energy_import_house['energy_import'].values
                column2_values = energy_export_house['energy_export'].values

                # Calculate the difference using NumPy
                difference = column1_values - column2_values

                # Create df3 and df4 DataFrames with additional columns
                energy_import_trading = energy_import_house.copy()
                energy_import_trading['energy_import'] = np.where(difference > 0, difference, np.nan)

                energy_export_trading = energy_export_house.copy()
                energy_export_trading['energy_export'] = np.where(difference < 0, difference * (-1), np.nan)

                # Remove rows with NaN values in 'Positive_Differences' or 'Negative_Differences' columns
                energy_import_trading = energy_import_trading.dropna(subset=['energy_import'], how='any')
                energy_export_trading = energy_export_trading.dropna(subset=['energy_export'], how='any')



            elif loop_house_1 > 0 and loop_house > 0:
                # Participant 0



                energy_import_house = add_new_participants(energy_import_house, df_new_participants_import, participant_type,
                                                 participant_strata, loop_house, trading_period, data_tables[0])

                # loads new participants to export dataframe
                energy_export_house = add_new_participants(energy_export_house, df_new_participants_export, participant_type,
                                                 participant_strata, loop_house, trading_period, data_tables[1])


                # Participant 1
                energy_import_house = add_new_participants(energy_import_house, df_new_participants_import_1, participant_type_1,
                                                 participant_strata_1, loop_house_1, trading_period, data_tables[0])

                # loads new participants to export dataframe
                energy_export_house = add_new_participants(energy_export_house, df_new_participants_export_1, participant_type_1,
                                                 participant_strata_1, loop_house_1, trading_period, data_tables[1])



                column1_values = energy_import_house['energy_import'].values
                column2_values = energy_export_house['energy_export'].values

                # Calculate the difference using NumPy
                difference = column1_values - column2_values

                # Create df3 and df4 DataFrames with additional columns
                energy_import_trading = energy_import_house.copy()
                energy_import_trading['energy_import'] = np.where(difference > 0, difference, np.nan)

                energy_export_trading = energy_export_house.copy()
                energy_export_trading['energy_export'] = np.where(difference < 0, difference * (-1), np.nan)

                # Remove rows with NaN values in 'Positive_Differences' or 'Negative_Differences' columns
                energy_import_trading = energy_import_trading.dropna(subset=['energy_import'], how='any')
                energy_export_trading = energy_export_trading.dropna(subset=['energy_export'], how='any')



            # Function to create net import and export dfs

            #energy_import_trading, energy_export_trading, df_load_level_battery, battery_settings =\
            #    data_cleaning(df_import, df_export, df_load_level_battery, trading_period, battery_settings)

            # run market scenario 1
            df_results_scenario_1, df_all_mcp = run_market_scenario_1(energy_import_trading, energy_export_trading,
                                                                      df_load_tracker, df_tariff_strata, trading_period,
                                                                      df_all_mcp)

            # run market Scenario 0
            df_load_tracker, df_results_scenario_0 = run_market_scenario_0(energy_import_trading, energy_export_trading,
                                                                           df_load_tracker, df_tariff_strata,
                                                                           trading_period)

            df_data_all_periods_0 = pd.concat([df_data_all_periods_0, df_results_scenario_0], ignore_index=False)

            df_data_all_periods_1 = pd.concat([df_data_all_periods_1, df_results_scenario_1], ignore_index=False)

        del import_full
        del export_full



        df_data_all_periods_0.to_csv(relative_path + 'Results/Scaling_scenarios/BAU-wo_P2P-wo/Scenario0/all_periods_scenario0_BAU_NO_SUB_SPOT_' + str(count) + '_' +str(count_1) +'.csv')
        df_data_all_periods_1.to_csv(relative_path + 'Results/Scaling_scenarios/BAU-wo_P2P-wo/Scenario1/all_periods_scenario1_P2P_NO_SUB_SPOT_' + str(count) + '_' + str(count_1) +'.csv')
        df_load_tracker.to_csv(relative_path + 'Results/Scaling_scenarios/BAU-wo_P2P-wo/Load/total_load_overview_P2P_NO_SUB_SPOT_' + str(count) + '_' + str(count_1) +'.csv')


        print(f'this is count {count}')
        print(f'this is count_1 {count_1}')

        count += 1
        loop_house += participant_per_loop

    else:
    ###############################              PLOTS                ##################################################

        loop_house_1 += participant_per_loop
        loop_house = start_loop_house
        count = 0
        count_1 += 1

else:
    print('CODE HAS FINISHED SUCCESSFULLY')

print('FINISHED')