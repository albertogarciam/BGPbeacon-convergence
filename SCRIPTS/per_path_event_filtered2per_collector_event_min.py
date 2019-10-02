#!/usr/bin/env python3

''' 
Estimates clock offset error

For a given collector, takes 'per_path_event_filtered/' data and
generates a file with the minimum time of the propagation of data for a given event from this to any other collector. 
(This is the time for the DIRECT communication.)

Results go to 'per_collector_event_mins/', one file per collector ('rrc00.csv', etc.) 
One entry per <src collector, dst collector> pair:

collector_src,collector_dst,event_number,min_time
rrc00,rrc01,0,19.0
rrc00,rrc01,1,2.0
...


- collector_dst is the local collector
- collector_src is the collector originating the beacon
- minTime is the min time observed for this interval number for each of the destination collectors. The min is the minimum of ANY advertisement received (through ANY path).
It is also the minimum of both IPv4 and IPv6 beacons coming from collector_src.
'''

from argparse import ArgumentParser
import pandas as pd

from filenames_directories import per_path_event_filtered_directory, per_collector_event_mins_filename
from experiments import events_in_experiment, beacon2collector, beacon_list

# replaces a beacon by its collector
def beacon_pref2collector(expName: str, beacon_serie: pd.Series) -> pd.Series:
    for beacon in beacon_list(expName):
        collector = beacon2collector(expName, beacon)
        beacon_serie = beacon_serie.str.replace(beacon, collector)
    return beacon_serie

def generateTimeDf(expName: str, this_collector:str) -> pd.DataFrame:
    directory = per_path_event_filtered_directory(exp_name, this_collector)

    # Reads all event files into a dataframe
    # (then process this big dataframe, write to file just once)
    frames = []
    for event_number in range(events_in_experiment(exp_name)):
        filename = directory + '/per_path_event_filtered_' + str(event_number) + '.csv'
        try:
            frames.append(pd.read_csv(filename))
        except Exception as e:
            print('Problem reading file {} ; continue operation'.format(filename))

    # ignore_index = the dataframes read may have the SAME index
    try:
        df = pd.concat(frames, ignore_index = True)
    except:
        print('no data for {}, exiting'.format(this_collector))
        return pd.DataFrame()
    # monitor_ip              prefix AW  min_ts_A  max_ts_A  count_A   min_ts_W  max_ts_W  count_W  event_number
    # 21708  218.189.6.2      84.205.64.0/24  A        51        51        1  96.0      96.0      1.0           179 
    df['min_time'] = df[['min_ts_A','min_ts_W']].min(axis=1)
    df['collector_src'] = beacon_pref2collector(expName, df['prefix'])

    grouped_df = df.groupby(['collector_src', 'event_number'])
    min_df = df.loc[grouped_df['min_time'].idxmin()]

    # insert the name of this collector at the beginning of every row
    min_df.insert(0, 'collector_dst', this_collector)

    return min_df[['collector_src', 'collector_dst', 'event_number', 'min_time']]

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("exp_name")
    parser.add_argument("collector",type=str)

    args= parser.parse_args()

    exp_name = args.exp_name
    collector = args.collector

    result = generateTimeDf(exp_name, collector)
    filename = per_collector_event_mins_filename(exp_name, collector)
    result.to_csv(filename, index=False)
