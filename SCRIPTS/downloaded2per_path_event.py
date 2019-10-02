#!/usr/bin/env python3

'''
Reads 'download/' data, resulting from download.py (one line per BGP message).

Generates single file per collector, in 'per_path_event/' directory.
Files are named as: /per_path_event_0.csv
    ./per_path_event_EVENT_NUMBER.csv
Contains ONE LINE PER PATH (monitor_ip, prefix) and event number
Each line contains the timestamp (relative to the start of the beacon event) 
at which
- first advertisement message for the prefix was observed
- last advertisement message was observed
- first withdrawn message for the prefix was observed
- last withdrawn message.
Also contains
- count_A: number of advertisement messages
- count_W number of wd messages
- as_path_count_A: number of different aspaths observed for monitor/beacon pair
- different_ases_count_A: count of different ASes observed in the event
- last_as_path_length_A: as path length of the last advertisement observed.

(note that there may not be advertisement/wd messages depending on the beacon type.)

Example of data:
monitor_ip,prefix,min_ts_A,max_ts_A,count_A,min_ts_W,max_ts_W,count_W,as_path_count_A,different_ases_count_A,last_as_path_length_A,event_number
12.0.1.63,84.205.64.0/24,7,78,8,41,101,2,2,6,5,1
12.0.1.63,84.205.65.0/24,3,108,9,275,275,1,4,9,4,1
12.0.1.63,84.205.68.0/24,18,31,4,35,35,1,1,4,4,1


'''

from argparse import ArgumentParser
from experiment_specs import result_directory
import pandas as pd
from typing import List

from filenames_directories import download_directory, per_path_event_filename
from experiments import event_number2timestamp_tuple, events_in_experiment
from collections import OrderedDict


def as_path_remove_prepending(as_path: str) -> str:
    as_path = as_path.strip()
    if ' ' in as_path:
        elements = as_path.split(' ')
        return ' '.join(list(OrderedDict.fromkeys(elements)))
    else:
        return as_path

def get_as_path_set(as_path: str) -> List:
    as_path = as_path.strip()
    if ' ' in as_path:
        list_result= as_path.split(' ')
        result=[int(x) for x in list_result]
    else:
        result= [int(as_path)]
    return result


def count_aspaths(df: pd.DataFrame) -> int:
    dfna=df.dropna()
    as_path_wo_prepending = dfna.apply(as_path_remove_prepending)
    count = len(as_path_wo_prepending.unique())
    return count
    
def count_different_ases(df: pd.DataFrame) -> int:
    dfna=df.dropna()
    as_paths= dfna.apply(get_as_path_set)
    # operate elements of series to get a single Set    
    
    # nested list
    # transform
    # 86     6667 3257 29208 6881 12654
    # 87          6667 29208 6881 12654
    # into
    # [6667, 3257, 29208, 6881, 12654, 6667, 29208, 6881, 12654,...

    as_paths_set = set([element for list_ in as_paths for element in list_])

    return len(as_paths_set)

def length_last_path_observed(df: pd.DataFrame) -> int:
    dfna=df.dropna()
    if len(dfna) > 0:
        # Take last path
        last_as_path = dfna.iloc[-1]
        last_as_path = str(last_as_path).strip()
        elements = last_as_path.split(' ')
        last_as_path_no_prep = set(elements)
        result = len(last_as_path_no_prep)

    else:
        result = 0
    return result

# ./downloaded2per_path_event.py 20120101_30d rrc00
if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("exp_name")
    parser.add_argument("collector")

    args= parser.parse_args()
    exp_name = args.exp_name
    collector = args.collector

    directory = download_directory(exp_name, collector)
    
    for event_number in range(events_in_experiment(exp_name)):
        filename = directory + 'beacon_'+str(event_number)+'.csv'

        first_ts, _ = event_number2timestamp_tuple(exp_name, event_number)

        try:
            update_df = pd.read_csv(filename, names=['AW', 'timestamp','monitor_ip', 'monitor_as', 'prefix', 'as_path'])
        except:
            # debug
            # print('filename does not exist: ', filename)
            continue
        if len(update_df) == 0:
            # debug
            # print('Warning, empty file {}, exiting (can continue with further processing steps)'.format(filename))
            continue

        update_df['timestamp'] = update_df['timestamp'] - first_ts
        
        grouped = update_df.groupby(['monitor_ip', 'prefix', 'AW'])

        extremes = grouped.aggregate({'timestamp': lambda serie: serie.min()})
        extremes.rename(index=str, columns={'timestamp': 'min_ts'}, inplace = True)
        extremes['max_ts'] = grouped.aggregate({'timestamp': lambda serie: serie.max()})
        extremes['count'] = grouped.aggregate({'timestamp': lambda serie: len(serie)})


        extremes['as_path_count'] = grouped.aggregate({'as_path': count_aspaths})
        extremes['different_ases_count'] = grouped.aggregate({'as_path': count_different_ases})
        extremes['last_as_path_length'] = grouped.aggregate({'as_path': length_last_path_observed})
        
        extremes.reset_index(inplace = True)


        extremes_A = extremes[extremes['AW']=='A']
        extremes_W = extremes[extremes['AW']=='W']
        

        merged = extremes_A.merge(extremes_W[['monitor_ip', 'prefix' , 'min_ts', 'max_ts', 'count', 'as_path_count', 'different_ases_count', 'last_as_path_length']], 
        left_on=['monitor_ip', 'prefix'], 
        right_on=['monitor_ip', 'prefix'],
        how='left', suffixes=('_A', '_W'), copy=True)

        merged['event_number']=event_number
       

        filename = per_path_event_filename(exp_name, collector, event_number)
        # Remove as_path_count_W, different_ases_count_W, last_as_path_length_W:
        # aspaths only appear in advertisement, i.e., 'A' messages
        merged[['monitor_ip', 'prefix' , 
        'min_ts_A', 'max_ts_A', 'count_A', 
        'min_ts_W', 'max_ts_W', 'count_W', 
        'as_path_count_A', 'different_ases_count_A', 'last_as_path_length_A',
        'event_number']].to_csv(filename, index=False)

        #print(merged[['event_number', 'as_path_count_A', 'as_path_count_W']])