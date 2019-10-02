#!/usr/bin/env python3

'''
For clock offset error analysis.

Reads 'per_collector_event_mins/' for all the collectors. 
Computes min time from each collector to any other collector. 
Takes the maximum time of every pair (the max for each direction), call this 'weight'.
It also looks if there is a shortest sum of maximum times going through other collectors 
(i.e., maybe sum of maximum times for c1 - c2 -c3 is less than c1-c3). 
For this computation, uses networkx functions.

1000000 (s) is a maximum value used to populate pairs for which there is no data.

Results go a single file for the whole experiment, 'per_event_shortest_distance.csv'

Has one entry per <collector_1, collector_2, event_number>.
min_time_1 and min_time_2 is the min time at the event for each of the 
directions (collector_1 -> collector_2, and collector_2 -> collector_1).
Weight = max(min_time_1, min_time_2)

collector_1,collector_2,event_number,min_time_1,min_time_2,weight,shortest_distance
rrc13,rrc10,0,19.0,19.0,19.0,19.0
rrc13,rrc10,1,1.0,31.0,31.0,9.0

'''

import pandas as pd
from argparse import ArgumentParser
import itertools
import networkx as nx

from experiments import collector_list
from filenames_directories import per_collector_event_mins_filename, per_event_shortest_distance_filename



def fill_non_recorded_events(collector1: str, collector2:str, df: pd.DataFrame) -> pd.DataFrame:
    for event_number in range(180):
        # collector_src,collector_dst,event_number,min_time
        # rrc00,rrc00,0,20.0
        # rrc00,rrc00,1,1.0

        if len(df[ (df['collector_1']==collector1) &
                    (df['collector_2']==collector2) &
                    (df['event_number'] == event_number)] ) != 1:
            # There is no value for this event, set 'infinite' value
            row = pd.DataFrame(data={
                'collector_1':collector1, 
                'collector_2': collector2,
                'event_number': event_number,
                'min_time_1': 1000000,
                'min_time_2': 1000000,
                'weight': 1000000,
            }, index=[0])
            df = df.append(row, ignore_index=True)
    
    return df


def max_time_collector_pair_df(collector1: str, collector2:str) -> pd.DataFrame:
    clock_synch_fn_1 = per_collector_event_mins_filename(args.exp_name, collector1)
    clock_synch_fn_2 = per_collector_event_mins_filename(args.exp_name, collector2)

    try:
        min_collector_1 = pd.read_csv(clock_synch_fn_1)
        min_collector_2 = pd.read_csv(clock_synch_fn_2)
    except:
        print('no data for either {} or {}'.format(clock_synch_fn_1, clock_synch_fn_2))
        return pd.DataFrame()

        # collector_src,collector_dst,event_number,min_time
        # rrc00,rrc00,0,20.0
        # rrc00,rrc00,1,1.0


    # reorder collectors for second DataFrame (to match them in opposition)
    min_collector_2 = min_collector_2[['collector_src','collector_dst','event_number','min_time']]

    # rename names to be uniform, but for min_time, 1 and 2
    min_collector_1.columns = ['collector_1','collector_2','event_number','min_time_1']
    min_collector_2.columns = ['collector_2','collector_1','event_number','min_time_2']    


    # concat values with equal values in some column... 

    merged = min_collector_1.merge(min_collector_2, left_on=['collector_1','collector_2','event_number'], right_on=['collector_1','collector_2','event_number'], how='inner')

    merged['weight'] = merged[['min_time_1','min_time_2']].max(axis=1)
    merged = fill_non_recorded_events(collector2, collector1, merged)
    
    return merged

# Computes shortest distance between rrc's with weight = 'max_time'
# collector_1 collector_2  event_number  min_time_1  min_time_2  max_time
def shortest_distance(one_event_df: pd.DataFrame) -> pd.DataFrame:
    G = nx.from_pandas_edgelist(one_event_df, 'collector_1', 'collector_2', ['weight'])
    
    # returns a generator
    p = nx.all_pairs_dijkstra_path_length(G)
    p_dict = dict(p)
    # print((p_dict))
    # [('rrc06', {'rrc06': 0, 'rrc07': 2.0, 'rrc10': 2.0,...
    

    for index, row in one_event_df.iterrows():
        collec1 = row['collector_1']
        collec2 = row['collector_2']

        # Modify dataframe
        one_event_df.at[index,'shortest_distance'] = p_dict[collec1][collec2]
        #print(one_event_df.loc[index])
        
    return one_event_df

# Generates a single file with all info
# ./per_collector_event_mins2per_event_shortest_distance.py 20180401_30d
if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("exp_name")

    args= parser.parse_args()

    exp_name = args.exp_name

    accumulated_result = pd.DataFrame()
    direct_collector_distance_per_event = []
    collectors = collector_list(exp_name)

    for collector_pair in itertools.combinations(collectors, 2):
        #pair_df = max_time_collector_pair_df(*collector_pair)
        direct_collector_distance_per_event.append(max_time_collector_pair_df(*collector_pair))

    
    # Distance using the direct path between the collectors
    direct_collector_distance_per_event_df = pd.concat(direct_collector_distance_per_event, ignore_index = True)
    #print(direct_collector_distance_per_event_df.head(190))

    #        collector_1 collector_2  event_number  min_time_1  min_time_2  max_time
    # 0         rrc11       rrc05             0         2.0         2.0       2.0
    # 180         rrc12       rrc05             0         2.0         2.0       2.0

    by_event_number = direct_collector_distance_per_event_df.groupby('event_number')
    distance_df =   by_event_number.apply(shortest_distance)
    
    worse_d = distance_df[distance_df['weight'] != distance_df['shortest_distance']]
    print('Total entries {}, with worse direct distance: {} (fraction {})'.format(len(distance_df), len(worse_d), len(worse_d)/len(distance_df)))

    fn = per_event_shortest_distance_filename(exp_name)
    distance_df.to_csv(fn, index=False)

    