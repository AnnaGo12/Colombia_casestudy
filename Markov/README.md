# simulate_energy_consumption

The goal is to simulate energy consumption using Markov chains.
The method used is mostly from [1].

The differrence is that days of week may be clustered into groups abd simulated independently. The example of groups: [ [Monday, Tuesday, Wendesday, Thursday], [Friday], [Saturday, Sunday]]

Execute the program by running executing main.py file on python.

The input data should be placed in data/inputs folder.
After executing, the simulated scenario will appear in data/outputs folder as .csv files.


Requirements:

    Python 3.5 | 3.6 | 3.7 | 3.8 | 3.9
    

To choose date, at which simulation will start, write start and finish dates into file date_start_finish.txt in form:
date_start = 2021-03-25
date_finish  = 2022-08-12

There is also an option to choose daygroups in weekly_day_groups.txt file. The days from different groups are simulated separately.
The default case (as in [1]), where there is only one group of days and all weekdays are simulated together, is when the weekly_day_groups.txt file contains single line

0 1 2 3 4 5 6

1. In the case of groups The example of groups: [ [Monday, Tuesday, Wendesday, Thursday], [Friday], [Saturday, Sunday]],  the weekly_day_groups.txt file contains 3 lines

0 1 2 3

4

5 6

2. The first line stands for [Monday, Tuesday, Wendesday, Thursday], the second line stands for [Friday], the last line stands for the weekend [Saturday, Sunday].



The input data is in folder data/inputs, the result of the simulation is in folder data/outputs. More than one csv file can be given as inputs



[1] Toffanin, D., 2016. Generation of customer load profiles based on smart-metering time series, building-level data and aggregated measurements.
