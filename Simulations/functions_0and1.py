# Imports
import sys
from turtle import end_fill
import numpy as np
import pandas as pd
import pymarket as pm
import cProfile, pstats, io
import os
from loguru import logger
import settings
import random

logger.remove(0)

logger.add(sys.stderr, format="{time} {level} {message}", level="INFO")


####################################### Functions Market SHARED ###########################################

def profile(fnc):
    """A decorator that uses cProfile to profile a function"""

    def inner(*args, **kwargs):
        pr = cProfile.Profile()
        pr.enable()
        retval = fnc(*args, **kwargs)
        pr.disable()
        s = io.StringIO()
        sortby = 'cumulative'
        ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
        ps.print_stats()
        print(s.getvalue())
        return retval

    return inner


# @profile
def track_monthly_consumption_2(df_load_tracker, df_all_energy_traded):
    """

    :type df_load_tracker: object
    """
    """"
    This function creates a dataframe which gives an overview on all energy traded  how much energy is consumed
    and how much energy is generated. This information is used in the loop to identify when the price to a
    transaction has to be adjusted based on overall load and self-consumption
    :param
    df_load_tracker: dataframe which stores the information
    df_all_energy_traded: dataframe which provides the information for every single trading period and updates
                        the df_load_tracker
    :return:
    energy_export_trading: cleaned energy export data
    energy_import_trading: cleaned energy export data
    """

    aggregation_functions = {'bid': 'first', 'quantity': 'sum', 'price': 'first',
                             'house_id': 'first', 'energy_bought': 'first', 'active': 'first',
                             'peer_transaction': 'first'}
    df_data_aggregated = df_all_energy_traded.groupby(df_all_energy_traded['house_id']).aggregate(aggregation_functions)

    add_load = df_load_tracker['house_id'].map(df_data_aggregated.set_index('house_id')['quantity'])
    add_direction = df_load_tracker['house_id'].map(df_data_aggregated.set_index('house_id')['energy_bought'])

    # Replace NaN values with 0
    add_load = [0 if x != x else x for x in add_load]
    add_direction = [0 if x != x else x for x in add_direction]

    # Add to total import if add_direction True
    df_load_tracker['total_im'] = np.where(add_direction, df_load_tracker['total_im'] + add_load,
                                           df_load_tracker['total_im'])

    # Add to total export if add_direction False
    df_load_tracker['total_ex'] = np.where(add_direction, df_load_tracker['total_ex'],
                                           df_load_tracker['total_ex'] + add_load)

    df_load_tracker['self_cons'] = np.where(df_load_tracker['total_im'] - df_load_tracker['total_ex'] < 0, True, False)

    arr = [73, 75, 255,400,401,402,403,404,405,406,407,408,409,410,411,412,413,414,415,416,417,418,419,420,421,422,423,424,425,426,427,428,429,430,431,432,433,434,435,436,437,438,439,440,441,442,443,444,445,446,447,448,449]  # array with house ids that are in stratas lower than 4

    # ##########################################################################################################
    df_load_tracker['total_load'] = np.where((df_load_tracker['total_im'] > 130) &
                                             (df_load_tracker['house_id'].isin(arr)), True,
                                             False)


    return df_load_tracker



def assign_prices(df_transactions, df_load_tracker, df_st_tariff, buy_energy):
    """
    Function assigns prices to transactions by comparing
    1)  Total energy load > 130
        If total energy load is > 130, then select non_subsidised tariff
        else select subsidised tariff
    2)  Self-consumption rate
        If energy produced > energy consumed, then select spot market price
        else use FiT

    INPUT:
    df_transactions: list with transaction information with columns
        ['bid', 'quantity', 'price', 'user', 'source', 'active']
    df_tracker: Record of transactions made per house_id with columns
        ['house_id', 'total_im', 'total_ex', 'total_load', 'self_cons']
    df_st_tariff: df with house_id and tariff information including
        ['subsidised', 'non_subsidised', 'fit', 'spot']

    RETURN:
    df_transactions: with additional column ['prices']
    """

    df_transactions_info = pd.merge(df_transactions, df_load_tracker, left_on='user', right_on='house_id', how='left')
    df_transactions_info = pd.merge(df_transactions_info, df_st_tariff, left_on='user', right_on='house_id', how='left')

    df_transactions = df_transactions.reset_index()
    df_transactions_info = df_transactions_info.reset_index()

    if buy_energy:
        for index, row in df_transactions.iterrows():

            if df_transactions_info.loc[index, 'total_load']:
                df_transactions.loc[index, 'price'] = df_transactions_info.loc[index, 'non_subsidised']
                print("-#-#-#-#-#-#-#-#-#-#-#-#")
                print(df_transactions_info)
                print(df_transactions)
                print(df_load_tracker)
                print(df_st_tariff)
            else:
                df_transactions.loc[index, 'price'] = df_transactions_info.loc[index, 'subsidised']



    else:
        for index, row in df_transactions.iterrows():
            if df_transactions_info.loc[index, 'self_cons']:
                df_transactions.loc[index, 'price'] = df_transactions_info.loc[index, 'spot']
            else:
                df_transactions.loc[index, 'price'] = df_transactions_info.loc[index, 'fit']

    return df_transactions


def energy_as_bid(df_house_transaction, energy_import):
    df_house_bid = pd.DataFrame(columns=['bid', 'quantity', 'price', 'user', 'source', 'active'])
    df_house_bid['bid'] = 0
    df_house_bid['price'] = 0
    df_house_bid['user'] = df_house_transaction['house_id']
    df_house_bid['active'] = False  # Not sure why false

    if energy_import:  # if energy is bought
        df_house_bid['quantity'] = df_house_transaction['energy_import']
        df_house_bid['source'] = True  # True if energy bought
    else:  # if energy is sold
        df_house_bid['quantity'] = df_house_transaction['energy_export']
        df_house_bid['source'] = False  # False if energy sold

    return df_house_bid


# @profile
def data_cleaning(df_import, df_export):
    """"
    This function cleans the energy import and export data extracted from the database by removing
    overlap of import and export during a time period
    :param
    df_import: raw energy import data from database
    df_export raw energy export data from database
    :return:
    energy_export_trading: cleaned energy export data
    energy_import_trading: cleaned energy export data

    """

    # drop all values with import or export = 0
    # df_import = df_import[df_import != 0].dropna()
    # f_export = df_export[df_export != 0].dropna()

    # concatenate tables left or right depending on if seller list larger than buyer list or vice versa
    # if len(df_import) >= len(df_export):
    df_net = pd.merge(df_import, df_export, left_on='house_id', right_on='house_id', how='left')
    # else:
    #    df_net = pd.merge(df_export, df_import, left_on='house_id', right_on='house_id', how='left')

    # Replace nan values with 0
    df_net['energy_export'] = df_net['energy_export'].fillna(0)
    df_net['energy_import'] = df_net['energy_import'].fillna(0)

    # clean table based on  values of import and export
    df_net['energy_net'] = df_net['energy_import'] - df_net['energy_export']

    # Energy battery function
    # df_net, df_load_level_battery, battery_settings = bat_ind_opt(df_net, df_load_level_battery, trading_period, battery_settings)

    df_net.loc[df_net.energy_net <= 0, 'energy_import'] = 0
    df_net.loc[df_net.energy_net <= 0, 'energy_export'] = -1 * df_net.energy_net
    df_net.loc[df_net.energy_net > 0, 'energy_import'] = df_net.energy_net
    df_net.loc[df_net.energy_net > 0, 'energy_export'] = 0

    # logger.debug(df_net)

    # energy import bids
    energy_import_trading = df_net.iloc[:, :4]
    # energy export bids
    energy_export_trading = df_net.iloc[:, [4, 1, 5, 6]]

    # remove NaN values
    # energy_export_trading = energy_export_trading.dropna()
    # energy_import_trading = energy_import_trading.dropna()

    # remove rows with 0 import or export
    energy_export_trading = energy_export_trading[energy_export_trading.energy_export != 0]
    energy_import_trading = energy_import_trading[energy_import_trading.energy_import != 0]

    return energy_import_trading, energy_export_trading


def calculate_tariff(sc, sp):
    """
    :param
    SC: unsubsidised/standard energy tariff
    SP: spot market price
    :return:
    df_tariff: with actual prices assigned to df
    """
    # Energy tariff ratios defined in CSV file
    # logger.debug(os.path.join(settings.BASE_DIR, "Data/price_subsidies.csv"))
    df_tariff = pd.read_csv(os.path.join(settings.BASE_DIR, "Data/price_subsidies.csv"))
    df_tariff = df_tariff.set_index('idx')

    df_tariff['subsidised'] = df_tariff['subsidised'].mul(sc)
    df_tariff['non_subsidised'] = df_tariff['non_subsidised'].mul(sc)
    df_tariff['fit'] = df_tariff['fit'].mul(sp)  # CHANGE TO SP here
    df_tariff['spot'] = df_tariff['spot'].mul(sp)

    return df_tariff


def get_data_for_trading_period(df, trading_period):
    mask = df['datetime'].isin([str(trading_period)])
    df = df[mask]
    return df


####################################### Functions Market Scenario 0 ###########################################

def run_market_scenario_0(energy_import_trading, energy_export_trading, df_load_tracker, df_tariff_strata,
                          trading_period):

    df_results_scenario_0 = pd.DataFrame(columns=['bid', 'quantity', 'price', 'house_id', 'energy_bought',
                                                  'active'])  # create empty dataframe for scenario 0

    # Create buy bid table
    df_grid_energy_buy = energy_as_bid(energy_import_trading, True)

    # Call function to assign prices
    df_grid_energy_buy = assign_prices(df_grid_energy_buy, df_load_tracker, df_tariff_strata, True)
    # Add column for gird or peer transaction
    df_grid_energy_buy['peer_transaction'] = False
    # Create sell bid table
    df_grid_energy_sell = energy_as_bid(energy_export_trading, False)
    # Call function to assign prices

    df_grid_energy_sell = assign_prices(df_grid_energy_sell, df_load_tracker, df_tariff_strata, False)
    # Add column for gird or peer transaction
    df_grid_energy_sell['peer_transaction'] = False
    # Create df of all energy traded
    df_all_energy_traded = pd.concat([df_grid_energy_buy, df_grid_energy_sell], ignore_index=False)

    df_all_energy_traded = df_all_energy_traded.rename(columns={"source": "energy_bought", "user": "house_id"})

    df_all_energy_traded['trading_period'] = trading_period  # Set df index to datetime
    df_all_energy_traded = df_all_energy_traded.set_index(['trading_period'])

    df_results_scenario_0 = pd.concat([df_results_scenario_0, df_all_energy_traded], ignore_index=False)

    # Track monthly consumption
    df_load_tracker = track_monthly_consumption_2(df_load_tracker, df_results_scenario_0)  # Function

    return df_load_tracker, df_results_scenario_0


####################################### Functions Market Scenario 1 ###########################################


# @profile
def run_market_scenario_1(energy_import_trading, energy_export_trading, df_load_tracker, df_tariff_strata,
                          trading_period, df_all_mcp):
    df_results_scenario_1 = pd.DataFrame(columns=['bid', 'quantity', 'price', 'house_id', 'energy_bought',
                                                  'active'])  # create empty dataframe for scenario 0

    if energy_import_trading.empty or energy_export_trading.empty:
        if energy_export_trading.empty:
            df_grid_energy_buy = energy_as_bid(energy_import_trading, True)

            df_grid_energy_buy = assign_prices(df_grid_energy_buy, df_load_tracker, df_tariff_strata, True)

            # df_grid_energy_buy['price'] = df_grid_energy_buy['user'].map(
            # df_tariff_strata.set_index('house_id')['subsidised'])

            # Combine both grid sell and grid buy table
            df_grid_energy = df_grid_energy_buy

            # Add column for gird or peer transaction
            df_grid_energy['peer_transaction'] = False

            df_all_energy_traded = df_grid_energy

        else:
            df_grid_energy_sell = energy_as_bid(energy_export_trading, False)

            df_grid_energy_sell = assign_prices(df_grid_energy_sell, df_load_tracker, df_tariff_strata, False)

            # Combine both grid sell and grid buy table
            df_grid_energy = df_grid_energy_sell

            # Add column for gird or peer transaction
            df_grid_energy['peer_transaction'] = False

            df_all_energy_traded = df_grid_energy

        mcp = 0

    else:
        # merge import export data with tariff info
        energy_import_trading = energy_import_trading.merge(df_tariff_strata, left_on='house_id', right_on='house_id')
        energy_export_trading = energy_export_trading.merge(df_tariff_strata, left_on='house_id', right_on='house_id')

        # energy_import_trading['energy_import'].sum()
        # energy_export_trading['energy_export'].sum()
        pm.market.MECHANISM['uniform'] = UniformPrice

        r = np.random.RandomState(1234)
        mar = pm.Market()  # Creates a new market

        energy_import_trading_info = pd.merge(energy_import_trading, df_load_tracker, left_on='house_id',
                                              right_on='house_id',
                                              how='left')
        energy_export_trading_info = pd.merge(energy_export_trading, df_load_tracker, left_on='house_id',
                                              right_on='house_id',
                                              how='left')



        # Shuffle the rows in both DataFrames with a new random seed each time
        random_seed = random.randint(0, 1000)  # Generate a new random seed
        energy_import_trading = energy_import_trading.sample(frac=1, random_state=random_seed).reset_index(drop=True)
        energy_import_trading_info = energy_import_trading_info.sample(frac=1, random_state=random_seed).reset_index(drop=True)


        # Shuffle the rows in both DataFrames with a new random seed each time
        random_seed = random.randint(0, 1000)  # Generate a new random seed
        energy_export_trading = energy_export_trading.sample(frac=1, random_state=random_seed).reset_index(drop=True)
        energy_export_trading_info = energy_export_trading_info.sample(frac=1, random_state=random_seed).reset_index(drop=True)





        for i in range(len(energy_import_trading_info)):

            if energy_import_trading_info.loc[i, 'total_load']:


                # price_rand =  int(energy_import_trading.loc[i, 'non_subsidised'] * 100)


                price_rand = random.randint(int(energy_import_trading.loc[i, 'spot'] * 100),
                                               int(energy_import_trading.loc[i, 'non_subsidised'] * 100))

                if price_rand == True:
                    exit()

                mar.accept_bid(energy_import_trading.loc[i, 'energy_import'],
                               price_rand / 100,
                               energy_import_trading.loc[i, 'house_id'], True)
            else:


                # price_rand = int(energy_import_trading.loc[i, 'subsidised'] * 100)



                if int(energy_import_trading.loc[i, 'spot'] * 100) < int(energy_import_trading.loc[i, 'subsidised'] * 100):
                    price_rand = random.randint(int(energy_import_trading.loc[i, 'spot'] * 100),
                                           int(energy_import_trading.loc[i, 'subsidised'] * 100))
                else:
                    price_rand = int(energy_import_trading.loc[i, 'subsidised'] * 100)



                if price_rand == True:
                    exit()


                mar.accept_bid(energy_import_trading.loc[i, 'energy_import'],
                               price_rand / 100,
                               energy_import_trading.loc[i, 'house_id'], True)



        for i in range(len(energy_export_trading_info)):

            if energy_export_trading_info.loc[i, 'self_cons']:

                # price_rand = int(energy_export_trading.loc[i, 'spot'] * 100)

                price_rand = random.randint(int(energy_export_trading.loc[i, 'spot'] * 100),
                                            int(energy_export_trading.loc[i, 'non_subsidised'] * 100))

                if price_rand == True:
                    exit()

                mar.accept_bid(energy_export_trading.loc[i, 'energy_export'],
                               price_rand / 100,
                               energy_export_trading.loc[i, 'house_id'], False)
            else:

                # price_rand = int(energy_export_trading.loc[i, 'spot'] * 100)

                price_rand = random.randint(int(energy_export_trading.loc[i, 'fit'] * 100),
                                            int(energy_export_trading.loc[i, 'non_subsidised'] * 100))

                # price_rand = random.randint(int(energy_export_trading.loc[i, 'spot'] * 100),
                #                             int(energy_export_trading.loc[i, 'non_subsidised'] * 100))

                # energy_export_trading.loc[i, 'fit'],

                if price_rand == True:
                    exit()

                mar.accept_bid(energy_export_trading.loc[i, 'energy_export'],
                               price_rand / 100,
                               energy_export_trading.loc[i, 'house_id'], False)

        bids = mar.bm.get_df()

        #print(bids)


        transactions, extras = mar.run('uniform')
        # stats = mar.statistics()

        df_all_transactions = transactions.get_df()

        #print(df_all_transactions)

        # Create list with market clearing price
        mcp = extras['clearing price']

        if mcp == True:
            print(trading_period)


        df_grid_energy = df_all_transactions.loc[df_all_transactions.price == True]
        df_peer_energy = df_all_transactions.loc[df_all_transactions.price != True]

        # Separate tables for energy bought from grid and energy sold to grid
        df_grid_energy_sell = df_grid_energy.loc[df_grid_energy.source == False]
        df_grid_energy_buy = df_grid_energy.loc[df_grid_energy.source == True]

        # Call function to assign prices

        df_grid_energy_sell = assign_prices(df_grid_energy_sell, df_load_tracker, df_tariff_strata, False)

        df_grid_energy_buy = assign_prices(df_grid_energy_buy, df_load_tracker, df_tariff_strata, True)

        # Combine both grid sell and grid buy table
        df_grid_energy = pd.concat([df_grid_energy_sell, df_grid_energy_buy], ignore_index=True)

        # Add column for gird or peer transaction
        df_peer_energy = df_peer_energy.assign(peer_transaction=True)
        df_grid_energy = df_grid_energy.assign(peer_transaction=False)

        df_all_energy_traded = pd.concat([df_grid_energy, df_peer_energy], ignore_index=True)

        if True in df_all_energy_traded['price'].values:
            print('############################################################################')
            print('############################################################################')
            print('############################################################################')

            print(df_all_energy_traded[['price', 'source']])
            exit()

    df_all_energy_traded = df_all_energy_traded.rename(columns={"source": "energy_bought", "user": "house_id"})

    df_all_energy_traded['trading_period'] = trading_period
    df_all_energy_traded = df_all_energy_traded.set_index(['trading_period'])

    df_mcp = pd.DataFrame([mcp], columns=['mcp'])
    df_mcp['trading_period'] = trading_period
    frames = [df_all_mcp, df_mcp]
    df_all_mcp = pd.concat(frames, ignore_index=False)

    df_results_scenario_1 = pd.concat([df_results_scenario_1, df_all_energy_traded], ignore_index=False)



    return df_results_scenario_1, df_all_mcp


def uniform_price_mechanism(bids: pd.DataFrame) -> (pm.TransactionManager, dict):
    trans = pm.TransactionManager()

    # print(bids)

    # returns demand and supply curve with index of prosumer
    buy, _ = pm.bids.demand_curve_from_bids(bids)  # Creates demand curve from bids
    sell, _ = pm.bids.supply_curve_from_bids(bids)  # Creates supply curve from bids

    # q_ is the quantity at which supply and demand meet
    # price is the price at which that happens
    # b_ is the index of the buyer in that position
    # s_ is the index of the seller in that position
    q_, b_, s_, price = intersect_stepwise(buy, sell)

    # print('this is bids buy = ', buy)
    # print('this is bids sell= ', sell)

    # print('this is q_ value = ', q_)
    # print('this is b_ value = ', b_)
    # print('this is s_ value = ', s_)
    # print('this is price value = ', price)

    buying_bids = bids.loc[bids['buying']].sort_values('price', ascending=False)
    selling_bids = bids.loc[~bids['buying']].sort_values('price', ascending=True)

    # Saving unchanged selling bids
    buying_bids_full = buying_bids
    selling_bids_full = selling_bids

    """if no trading can be place this if statement is executed,
    which autmatically assigns the bids the grid tariff"""

    if b_ is None:
        for i, x in buying_bids_full.iterrows():
            buy_or_sell = True
            tg = (i, x.quantity, True, x.user, buy_or_sell, False)
            trans.add_transaction(*tg)  ################################################
        for i, x in selling_bids_full.iterrows():
            buy_or_sell = False
            tg = (i, x.quantity, True, x.user, buy_or_sell, False)
            trans.add_transaction(*tg)  ################################################

        extra = {
            'clearing quantity': None,
            'clearing price': True
        }

    else:
        # Filter only the trading bids.
        buying_bids = buying_bids.iloc[: b_ + 1, :]
        selling_bids = selling_bids.iloc[: s_ + 1, :]

        # print('SELECTED buying short')
        # print(buying_bids)
        # print('SELECTED selling')
        # print(selling_bids)

        # Find the long side of the market
        buying_quantity = buying_bids.quantity.sum()
        selling_quantity = selling_bids.quantity.sum()

        if buying_quantity > selling_quantity:
            long_side_full = buying_bids_full
            short_side_full = selling_bids_full
            long_side = buying_bids
            short_side = selling_bids
        else:
            long_side_full = selling_bids_full
            short_side_full = buying_bids_full
            long_side = selling_bids
            short_side = buying_bids

        traded_quantity = short_side.quantity.sum()

        """All the short side will trade at `price`
        # The -1 is there because there is no clear 1 to 1 trade."""
        for i, x in short_side.iterrows():
            if buying_quantity > selling_quantity:
                buy_or_sell = False
            else:
                buy_or_sell = True
            t = (i, x.quantity, price, x.user, buy_or_sell, False)
            trans.add_transaction(*t)

        ## The long side has to trade only up to the short side
        quantity_added = 0
        for i, x in long_side.iterrows():
            if x.quantity + quantity_added <= traded_quantity:
                x_quantity = x.quantity
                x_user = x.user
            else:
                x_quantity = traded_quantity - quantity_added
                x_user = x.user
            if buying_quantity > selling_quantity:
                buy_or_sell = True
            else:
                buy_or_sell = False
            t = (i, x_quantity, price, x_user, buy_or_sell, False)
            trans.add_transaction(*t)
            quantity_added += x.quantity

        i_long_list = len(long_side)
        i_short_list = len(short_side)

        # print('This is i after first loop: ')
        # print(i_long_list)

        # print('transactions IN MARKET ')
        # print(trans.get_df())
        # print('LONG LIST ')
        # print(long_side_full)

        market_transactions_full = trans.get_df()

        # Add non-traders to grid transaction list
        for i, x in long_side_full.iloc[i_long_list - 1:].iterrows():
            # print('This is first row of grid list ')
            # market_transactions_full.iloc[-1, 0]
            if i == market_transactions_full.iloc[-1, 0]:
                # print(market_transactions_full.iloc[-1, 1])
                x.quantity = x.quantity - market_transactions_full.iloc[-1, 1]
                # print('this is tg in if statement:')
                # print(x.quantity)
            if x.quantity == 0:
                pass
            else:
                if buying_quantity > selling_quantity:
                    buy_or_sell = True
                else:
                    buy_or_sell = False
                tg = (i, x.quantity, True, x.user, buy_or_sell, False)
                trans.add_transaction(*tg)  ##############################################

        for i, x in short_side_full.iloc[i_short_list:].iterrows():
            if i == market_transactions_full.iloc[-1, 0]:
                # market_transactions_full.iloc[-1, 1]
                x.quantity = x.quantity - market_transactions_full.iloc[-1, 1]
                # print('this is tg in if statement:')
                # print(x.quantity)
            if x.quantity == 0:
                pass
            else:
                if buying_quantity > selling_quantity:
                    buy_or_sell = False
                else:
                    buy_or_sell = True
                tg = (i, x.quantity, True, x.user, buy_or_sell, False)
                trans.add_transaction(*tg)  ##############################################

        extra = {
            'clearing quantity': q_,
            'clearing price': price
        }

        #print(price)

    # print('THESE ARE PEER TRASNACTIONS')
    # traded_transactions = (trans.get_df())

    return trans, extra


def demand_curve_from_bids(bids):
    """
    Creates a demand curve from a set of buying bids.
    It is the inverse cumulative distribution of quantity
    as a function of price.

    Parameters
    Parameters
    ----------
    bids
        Collection of all the bids in the market. The algorithm
        filters only the buying bids.

    Returns
    ---------
    demand_curve: np.ndarray
       Stepwise constant demand curve represented as a collection
       of the N rightmost points of each interval (N-1 bids). It is stored
       as a (N, 2) matrix where the first column is the x-coordinate
       and the second column is the y-coordinate.
       An extra point is a))dded with x coordinate at infinity and
       price at 0 to represent the end of the curve.

    index : np.ndarray
        The order of the identifier of each bid in the demand
        curve.

    Examples
    ---------

    A minimal example, selling bid is ignored:

    >> bm = pm.BidManager()
    >> bm.add_bid(1, 1, 0, buying=True)
    0
    >> bm.add_bid(1, 1, 1, buying=False)
    1
    >> dc, index = pm.demand_curve_from_bids(bm.get_df())
    >> dc
    array([[ 1.,  1.],
           [inf,  0.]])
    >> index
    array([0])

    A larger example with reordering of bids:

    >> bm = pm.BidManager()
    >> bm.add_bid(1, 1, 0, buying=True)
    0
    >> bm.add_bid(1, 1, 1, buying=False)
    1
    >> bm.add_bid(3, 0.5, 2, buying=True)
    2
    >> bm.add_bid(2.3, 0.1, 3, buying=True)
    3
    >> dc, index = pm.demand_curve_from_bids(bm.get_df())
    >> dc
    array([[1. , 1. ],
           [4. , 0.5],
           [6.3, 0.1],
           [inf, 0. ]])
    >> index
    array([0, 2, 3])

    """
    buying = bids[bids.buying]
    buying = buying.sort_values('price', ascending=False)
    buying['acum'] = buying.quantity.cumsum()
    demand_curve = buying[['acum', 'price']].values
    demand_curve = np.vstack([demand_curve, [np.inf, 0]])
    index = buying.index.values.astype('int64')
    return demand_curve, index


def supply_curve_from_bids(bids):
    """
    Creates a supply curve from a set of selling bids.
    It is the cumulative distribution of quantity
    as a function of price.

    Parameters
    ----------
    bids: pd.DataFrame
        Collection of all the bids in the market. The algorithm
        filters only the selling bids.

    Returns
    ---------
    supply_curve: np.ndarray
       Stepwise constant demand curve represented as a collection
       of the N rightmost points of each interval (N-1 bids). It is stored
       as a (N, 2) matrix where the first column is the x-coordinate
       and the second column is the y-coordinate.
       An extra point is added with x coordinate at infinity and
       price at infinity to represent the end of the curve.

    index : np.ndarray
        The order of the identifier of each bid in the supply
        curve.

    Examples
    ---------

    A minimal example, selling bid is ignored:

    >> bm = pm.BidManager()
    >> bm.add_bid(1, 3, 0, False)
    0
    >> bm.add_bid(2.1, 3, 3, True)
    1
    >> sc, index = pm.supply_curve_from_bids(bm.get_df())
    >> sc
    array([[ 1.,  3.],
           [inf, inf]])
    >> index
    array([0])

    A larger example with reordering:

    >> bm = pm.BidManager()
    >> bm.add_bid(1, 3, 0, False)
    0
    >> bm.add_bid(2.1, 3, 3, True)
    1
    >> bm.add_bid(0.2, 1, 3, False)
    2
    >> bm.add_bid(1.7, 6, 4, False)
    3
    >> sc, index = pm.supply_curve_from_bids(bm.get_df())
    >> sc
    array([[0.2, 1. ],
           [1.2, 3. ],
           [2.9, 6. ],
           [inf, inf]])
    >> index
    array([2, 0, 3])


    """
    selling = bids[bids.buying == False]
    selling = selling.sort_values('price')
    selling['acum'] = selling.quantity.cumsum()
    supply_curve = selling[['acum', 'price']].values
    supply_curve = np.vstack([supply_curve, [np.inf, np.inf]])
    index = selling.index.values.astype('int64')
    return supply_curve, index


def get_value_stepwise(x, f):
    """
    Returns the value of a stepwise constant
    function defined by the right extrems
    of its interval
    Functions are assumed to be defined
    in (0, inf).

    Parameters
    ----------
    x: float
        Value in which the function is to be
        evaluated
    f: np.ndarray
        Stepwise function represented as a 2 column
        matrix. Each row is the rightmost extreme
        point of each constant interval. The first column
        contains the x coordinate and is sorted increasingly.
        f is assumed to be defined only in the interval
        :math: (0, \infty)
    Returns
    --------
    float or None
        The image of x under f: `f(x)`. If `x` is negative,
        then None is returned instead. If x is outside
        the range of the function (greater than `f[-1, 0]`),
        then the method returns None.

    Examples
    ---------
    >> f = np.array([
    ...     [1, 1],
    ...     [3, 4]])
    >> [pm.get_value_stepwise(x, f)
    ...     for x in [-1, 0, 0.5, 1, 2, 3, 4]]
    [None, 1, 1, 1, 4, 4, None]

    """
    if x < 0:
        return None

    for step in f:
        if x <= step[0]:
            return step[1]


def intersect_stepwise(
        f,
        g,
        k=0.5
):

    """
    Finds the intersection of
    two stepwise constants functions
    where f is assumed to be bigger at 0
    than g.
    If no intersection is found, None is returned.

    Parameters  (Anna's comment: f is import and g is export; sorted by price)
    ----------
    f: np.ndarray
        Stepwise constant function represented as
        a 2 column matrix where each row is the rightmost
        point of the constat interval. The first column
        is sorted increasingly.
        Preconditions: f is non-increasing.

    g: np.ndarray
        Stepwise constant function represented as
        a 2 column matrix where each row is the rightmost
        point of the constat interval. The first column
        is sorted increasingly.
        Preconditions: g is non-decreasing and
        `f[0, 0] > g[0, 0]`
    k : float
        If the intersection is empty or an interval,
        a convex combination of the y-values of f and g
        will be returned and k will be used to determine
        the final value. `k=1` will be the value of g
        while `k=0` will be the value of f.

    Returns
    --------
    x_ast : float or None
        Axis coordinate of the intersection of both
        functions. If the intersection is empty,
        then it returns None.
    f_ast : int or None
        Index of the rightmost extreme
        of the interval of `f` involved in the
        intersection. If the intersection is
        empty, returns None
    g_ast : int or None
        Index of the rightmost extreme
        of the interval of `g` involved in the
        intersection. If the intersection is
        empty, returns None.
    v : float or None
        Ordinate of the intersection if it
        is uniquely identified, otherwise
        the k-convex combination of the
        y values of `f` and `g` in the last
        point when they were both defined.

    Examples
    ---------
    Simple intersection with diferent domains

    >> f = np.array([[1, 3], [3, 1]])
    >> g = np.array([[2,2]])
    >> pm.intersect_stepwise(f, g)
    (1, 0, 0, 2)

    Empty intersection, returning the middle value

    >> f = np.array([[1,3], [2, 2.5]])
    >> g = np.array([[1,1], [2, 2]])
    >> pm.intersect_stepwise(f, g)
    (None, None, None, 2.25)
    """

    x_max = np.min([f.max(axis=0)[0], g.max(axis=0)[0]])
    xs = sorted([x for x in set(g[:, 0]).union(set(f[:, 0])) if x <= x_max])
    fext = [get_value_stepwise(x, f) for x in xs]
    gext = [get_value_stepwise(x, g) for x in xs]
    x_ast = None
    for i in range(len(xs) - 1):
        if (fext[i] >= gext[i]) and (fext[i + 1] < gext[i + 1]):  # hier egentlich ohne =
            x_ast = xs[i]

    f_ast = np.argmax(f[:, 0] >= x_ast) if x_ast is not None else None
    g_ast = np.argmax(g[:, 0] >= x_ast) if x_ast is not None else None



    g_val = g[g_ast, 1] if g_ast is not None else get_value_stepwise(xs[-1], g)
    f_val = f[f_ast, 1] if f_ast is not None else get_value_stepwise(xs[-1], f)



    intersect_domain_both = x_ast in f[:, 0] and x_ast in g[:, 0]


    if not (intersect_domain_both) and (x_ast is not None):

        if g_val < f_val:
            #print(g_val)
            #print(f_val)

            v = random.randint(int(g_val*100), int(f_val*100))/100
        else:
            v = g_val if x_ast in f[:, 0] else f_val
            #print('#-#-#-#-#-#-#-#-#-#-#-#-#-#-#')
        #print('v1')
        #print(g_val)
        #print(f_val)
    else:
        v = g_val * k + (1 - k) * f_val
        print('v2')


    return x_ast, f_ast, g_ast, v


class UniformPrice(pm.Mechanism):
    """
    Interface for our new
     price mechanism.

    Parameters
    -----------
    bids
        Collection of bids to run the mechanism
        with.
    """

    def __init__(self, bids, *args, **kwargs):
        """TODO: to be defined1. """
        pm.Mechanism.__init__(self, uniform_price_mechanism, bids, *args, **kwargs)
