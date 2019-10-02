#!/usr/bin/env python3


''' 
Takes as input the quantile files for each collector 

Includes for each <monitor_ip,prefix> pair the clock offset error estimation computed for the collector and beacon generation.
Also includes remote and local collector names.

Result goes to a single file per experiment, 'quantiles_with_clock.csv'

monitor_ip,prefix,minA_q0,minA_q50_UP,maxA_q50_UP,minA_q90_UP,maxA_q90_UP,count_A,count_UP_events,min_q50_DOWN,maxW_q50_DOWN,min_q90_DOWN,maxW_q90_DOWN,count_W,count_DOWN_events,as_path_count_DOWN,ases_different_one_event_DOWN,last_as_path_length_DOWN,zombie_count,rfd_count_UP,rfd_count_DOWN,collector,remote_collector,clock_p_50,clock_p_90
195.66.224.111,84.205.64.0/24,6,36.0,62.0,54.0,88.0,389,88,56.0,139.0,153.0,281.0,88.0,87,171.0,506.0,408.0,1,1,0,rrc01,rrc00,2.0,6.0
195.66.224.111,84.205.65.0/24,3,29.0,32.0,45.0,52.0,259,87,18.0,103.0,29.0,224.0,106.0,76,90.0,271.0,261.0,0,0,0,rrc01,rrc01,0.0,0.0


Removes entries without clock information.
'''


import pandas as pd
import numpy as np
from argparse import ArgumentParser
from typing import List

from filenames_directories import per_experiment_clock_synch_filename, quantile_filename, quantiles_with_clock_filename
from experiments import collector_list, beacon_list, beacon2collector

# Returns collector corresponding to a beacon
def beacon_pref2collector(exp_name: str, beacon_serie: pd.Series) -> pd.Series:
    for beacon in beacon_list(exp_name):
        collector = beacon2collector(exp_name, beacon)
        beacon_serie = beacon_serie.str.replace(beacon, collector)
    return beacon_serie

min_count = 45
# read quantiles and filter some info
def read_quantiles(exp_name: str, collectors: List[str]) -> pd.DataFrame:
    q_list = []
    for collector in collectors:
        filename =  quantile_filename(exp_name, collector)
        try:
            q_list.append(pd.read_csv(filename))
        except:
            print('could not read data for {}'.format(collector))
    qdf = pd.concat(q_list, ignore_index=True)

    # remove rows with Nan (e.g., there is no W)
    qdf = qdf.dropna()

    total_pairs = len(qdf)
    # Select only monitor/prefix pairs with more than a number of events
    qdf = qdf[(qdf['count_A'] > min_count) & (qdf['count_W'] > min_count)]

    if (total_pairs != len(qdf)):
        print('Removed {} pairs (too few data) for {}'.format(total_pairs-len(qdf), collector))


    return qdf

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("exp_name")

    args= parser.parse_args()
    exp_name = args.exp_name

    collectors = collector_list(exp_name)

    qdf = read_quantiles(exp_name, collectors)
    # monitor_ip,prefix,minA_q0,minA_q50,minA_q90,minA_q100,maxA_q0,maxA_q50,maxA_q90,maxA_q100,count_A,minW_q0,minW_q50,minW_q90,minW_q100,maxW_q0,maxW_q50,maxW_q90,maxW_q100,count_W,zombie_count,rfd_count_UP,rfd_count_DOWN,collector
    # 195.66.224.121,84.205.64.0/24,4,34,49,65,9,55,79,550,281,40.0,95.0,121.0,581.0,40.0,95.0,121.0,581.0,81.0,0,1,rrc01


    # Read DOWN clock information
    clock_synch_fn = per_experiment_clock_synch_filename(exp_name, False, True)
    clock_df = pd.read_csv(clock_synch_fn)
    # collector_1,collector_2,p_0,p_50,p_90,p_100,event_count
    # rrc00,rrc04,3.0,17.0,25.0,73.0,83
    # rrc00,rrc05,1.0,4.0,17.0,44.0,83
    
    qdf['remote_collector'] = beacon_pref2collector(exp_name, qdf['prefix'])
    qdf['clock_p_50'] = np.NaN
    qdf['clock_p_90'] = np.NaN


    for index, row in qdf.iterrows():
        collector = row['collector']
        remote_collector = row['remote_collector']

        # same collector as prefix origin
        if collector == remote_collector:
            qdf.at[index, 'clock_p_50'] = 0
            qdf.at[index, 'clock_p_90'] = 0
        else:
            # clock collectors could be switched
            clock_row1 = clock_df[ (clock_df['collector_1'] == collector) & (clock_df['collector_2']== remote_collector) ]
            clock_row2 = clock_df[ (clock_df['collector_2'] == collector) & (clock_df['collector_1']== remote_collector) ]
            
            if (len(clock_row1)==0) & (len(clock_row2)==0):
                #print('Warning, Should be a clock entry for ', collector, remote_collector)
                pass
                
            elif (len(clock_row1)>0) & (len(clock_row2)>0):
                raise Exception('Should only be ONE clock entry, not two, for ', collector, remote_collector)
            else:
                if len(clock_row1):
                    clock_row = clock_row1
                else:
                    clock_row = clock_row2
            
                # Modify dataframe
                qdf.at[index, 'clock_p_50'] = clock_row['p_50']
                qdf.at[index, 'clock_p_90'] = clock_row['p_90']
        
    # Remove collector pairs without clock measurement
    # Note that this may reduce the number of pairs to compare
    qdf = qdf.dropna()

    
    fn = quantiles_with_clock_filename(exp_name)
    qdf.to_csv(fn, index=False)
