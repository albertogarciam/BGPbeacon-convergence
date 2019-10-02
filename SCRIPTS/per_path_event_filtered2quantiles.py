#!/usr/bin/env python3

'''
Take as input the per_path_event_filtered files for the events of a collector.

Generates a single file per collector, placed in 'quantiles/' directory.
Each file contains one entry per <monitor_ip,prefix> pair. For each pair, it computes
several quantiles (quantile 0, quantile 50, etc.) of first advertisement (minA),
last advertisement (maxA), last withdrawn observed for the route (maxW).
It separates the values for UP events and for DOWN events.

Detects BGP zombie events, prefixes which are not withdrawn in 90 mins (or not wd in the whole 120 min period).
Detects rfd events, i.e., convergence longer than 20 min
Quantiles are computed WITHOUT BGP zombie and rfd events.
Provides counts for these events


monitor_ip,prefix,minA_q0,minA_q50_UP,maxA_q50_UP,minA_q90_UP,maxA_q90_UP,count_A,count_UP_events,min_q50_DOWN,maxW_q50_DOWN,min_q90_DOWN,maxW_q90_DOWN,count_W,count_DOWN_events,as_path_count_DOWN,ases_different_one_event_DOWN,last_as_path_length_DOWN,zombie_count,rfd_count_UP,rfd_count_DOWN,collector
194.68.123.136,84.205.64.0/24,2,12,28,28,46,972,76,16.0,65.0,29.0,82.0,81.0,74,516.0,919.0,436.0,0,0,0,rrc07
194.68.123.136,84.205.65.0/24,1,11,17,27,30,1082,74,2.0,112.0,5.0,148.0,79.0,72,592.0,711.0,449.0,0,0,0,rrc07
194.68.123.136,84.205.69.0/24,1,15,15,29,30,880,78,2.0,66.0,2.0,103.0,90.0,72,384.0,602.0,415.0,0,0,0,rrc07

'''

from argparse import ArgumentParser
import pandas as pd

from filenames_directories import per_path_event_filtered_directory, quantile_filename, zombies_filename
from experiments import events_in_experiment

RFD_THR = 20*60
ZOMBIE_THR = 90 *60

def zombie_count(df: pd.DataFrame) -> int:
    zombie_condition = ((df['event_number']%2) == 1) & (df['max_ts_W'].isnull() | (df['max_ts_W'] > ZOMBIE_THR))
    return len(df[zombie_condition])

def rfd_count_UP(df: pd.DataFrame) -> int:
    zombie_condition = ((df['event_number']%2) == 1) & (df['max_ts_W'].isnull() | df['max_ts_W'] > ZOMBIE_THR)
    alive = df[~ zombie_condition ]
    
    rfd_condition = ((alive['event_number']%2) == 0) & ((alive['max_ts_W'] > RFD_THR) | (alive['max_ts_A'] > RFD_THR))
    return len(alive[rfd_condition])

def rfd_count_DOWN(df: pd.DataFrame) -> int:
    zombie_condition = ((df['event_number']%2) == 1) & (df['max_ts_W'].isnull() | df['max_ts_W'] > ZOMBIE_THR)
    alive = df[~ zombie_condition ]
    
    rfd_condition = ((alive['event_number']%2) == 1) & ((alive['max_ts_W'] > RFD_THR) | (alive['max_ts_A'] > RFD_THR))
    return len(alive[rfd_condition])

# quantile  quant of min value for A update messages for UP events
def minA_q_UP(df: pd.DataFrame, quant:float) -> int:
    df = df[(df['event_number']%2) == 0]
    return df['min_ts_A'].quantile(quant, 'lower')

def maxA_q_UP(df: pd.DataFrame, quant:float) -> int:
    df = df[(df['event_number']%2) == 0]
    return df['max_ts_A'].quantile(quant, 'lower')

def min_q_DOWN(df: pd.DataFrame, quant:float) -> int:
    df = df[(df['event_number']%2) == 1]
    df['min'] = df[['min_ts_A','min_ts_W']].min(axis=1)
    return df['min'].quantile(quant, 'lower')

def maxW_q_DOWN(df: pd.DataFrame, quant:float) -> int:
    df = df[(df['event_number']%2) == 1]
    return df['max_ts_W'].quantile(quant, 'lower')


# ./per_path_event_filtered2quantiles.py 20090101_30d rrc00
if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("exp_name")
    parser.add_argument("collector")

    args= parser.parse_args()
    exp_name = args.exp_name
    collector = args.collector

    directory = per_path_event_filtered_directory(exp_name, collector)

    frames = []
    for event_number in range(events_in_experiment(exp_name)):
        filename = directory + '/per_path_event_filtered_' + str(event_number) + '.csv'
        try:
            frames.append(pd.read_csv(filename))
        except Exception as e:
            # print('Problem reading file {} ; continue operation'.format(filename))
            pass

    # ignore_index = the dataframes read may have the SAME index
    try:
        df = pd.concat(frames, ignore_index = True)
    except:
        print('no data for this collector, exiting ', collector )
        exit(0)

    if len(df)==0: 
        print('no data for this collector, exiting ', collector )
        exit(0)

    grouped_all = df.groupby(['monitor_ip', 'prefix'])

    # zombies if it is a DOWN event (%2 ==1), 
    # and either there is no W or W arrived later than 1h30
    zombie_condition = ((df['event_number']%2) == 1) & (df['max_ts_W'].isnull() | (df['max_ts_W'] > ZOMBIE_THR))
    zombies = df[zombie_condition]

    alive = df[~ zombie_condition ]

    rfd_condition = (alive['max_ts_W'] > RFD_THR) | (alive['max_ts_A'] > RFD_THR)
    rfd_df = alive[rfd_condition]

    normal = alive[~rfd_condition]
    grouped = normal.groupby(['monitor_ip', 'prefix'])

    grouped_DOWN = normal[(normal['event_number']%2) == 1].groupby(['monitor_ip', 'prefix'])


    qdf = grouped.aggregate({'min_ts_A': lambda serie: serie.min()})
    qdf.rename(index=str, columns={'min_ts_A': 'minA_q0'}, inplace = True)
    

    # Use lambda to insert quantile argument
    qdf['minA_q50_UP'] = grouped.apply(lambda x: minA_q_UP(x, 0.5))
    qdf['maxA_q50_UP'] = grouped.apply(lambda x: maxA_q_UP(x, 0.5))
    qdf['minA_q90_UP'] = grouped.apply(lambda x: minA_q_UP(x, 0.9))
    qdf['maxA_q90_UP'] = grouped.apply(lambda x: maxA_q_UP(x, 0.9))


    qdf['count_A'] = grouped.aggregate({'count_A': lambda serie: serie.sum()})
    # To count the number of true values in the serie, just sum them
    qdf['count_UP_events'] = grouped.aggregate({'event_number': lambda serie: sum( (serie %2)==0)})
    

    qdf['min_q50_DOWN'] = grouped.apply(lambda x: min_q_DOWN(x, 0.5))
    qdf['maxW_q50_DOWN'] = grouped.apply(lambda x: maxW_q_DOWN(x, 0.5))

    qdf['min_q90_DOWN'] = grouped.apply(lambda x: min_q_DOWN(x, 0.9))
    qdf['maxW_q90_DOWN'] = grouped.apply(lambda x: maxW_q_DOWN(x, 0.9))


    qdf['count_W'] = grouped.aggregate({'count_W': lambda serie: serie.sum()})
    qdf['count_DOWN_events'] = grouped.aggregate({'event_number': lambda serie: sum( (serie %2)==1)})

    # the same as_path counts many times if it appears in different events
    # Only for DOWN events, using grouped_DOWN
    qdf['as_path_count_DOWN'] = grouped_DOWN.aggregate({'as_path_count_A': lambda serie: serie.sum()})
    qdf['ases_different_one_event_DOWN'] = grouped_DOWN.aggregate({'different_ases_count_A': lambda serie: serie.sum()})
    qdf['last_as_path_length_DOWN'] = grouped_DOWN.aggregate({'last_as_path_length_A': lambda serie: serie.sum()})

    qdf['zombie_count'] = grouped_all.apply(zombie_count)
    qdf['rfd_count_UP'] = grouped_all.apply(rfd_count_UP)
    qdf['rfd_count_DOWN'] = grouped_all.apply(rfd_count_DOWN)
    qdf['collector']=collector

    filename =  quantile_filename(exp_name, collector)
    qdf.to_csv(filename)
