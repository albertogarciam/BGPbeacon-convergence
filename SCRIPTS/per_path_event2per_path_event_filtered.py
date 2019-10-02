#!/usr/bin/env python3

''' 
Reads beacon info from "per_path_event/" and anchor info from "download/".
Removes entries for (beacon, prefix) with activity in
(corresponding anchor, prefix), i.e., for the path, 
in the same event or in the last 10 minutes of the previous event.

Also performs a simple clock_synch test: looks for activity in the 
beacon file for the last minute of the period, and prints it if it exists.
If they exist, it removes them.

Filters prefixes as 0/0, ::/0 (these prefixes appear even if 
pybgpstream has been configured not to download them.)
Also excludes data from rrc16 (if downloaded)

Results go to 'per_path_event_filtered/'
Generates a file (per collector) with same format as per_path_event:

monitor_ip,prefix,min_ts_A,max_ts_A,count_A,min_ts_W,max_ts_W,count_W,as_path_count_A,different_ases_count_A,last_as_path_length_A,event_number
194.68.123.136,84.205.64.0/24,3,56,14,63,63,1,9,13,6,1
194.68.123.136,84.205.65.0/24,3,77,14,94,94,1,8,10,6,1

'''

from argparse import ArgumentParser
import pandas as pd


from filenames_directories import download_filename, per_path_event_directory, per_path_event_filtered_filename
from experiments import beacons_corresponding_to_anchor, beacon_list, events_in_experiment, event_number2timestamp_tuple 

# ./per_path_event2per_path_event_filtered.py 20181001_30d rrc00
if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("exp_name")
    parser.add_argument("collector")

    args= parser.parse_args()
    exp_name = args.exp_name
    collector = args.collector

    beacon_directory = per_path_event_directory(exp_name, collector)
    # Initialize with empty dataframe, with len == 0
    anchor_for_next_event_df = pd.DataFrame()

    for event_number in range(events_in_experiment(exp_name)):
        beacon_filename = beacon_directory + 'per_path_event_' + str(event_number) +'.csv'

        beacon_filtered_filename = per_path_event_filtered_filename(exp_name, collector, event_number)

        # Try to read corresponding anchor file. It may not exist 
        # (this means there was no anchor activity in this period)
        anchor_filename = download_filename(exp_name, collector, True, event_number)


        try:
            anchor_df = pd.read_csv(anchor_filename, names=['AW', 'timestamp','monitor_ip', 'monitor_as', 'prefix', 'as_path'])

            anchor_this_event_df = anchor_df
            if len(anchor_for_next_event_df) > 0:
                anchor_this_event_df = pd.concat([anchor_this_event_df, anchor_for_next_event_df], ignore_index=True)

            anchor_this_event_df = anchor_df.drop_duplicates(['monitor_ip', 'prefix']).copy()

            # replace anchor by its corresponding beacon
            anchor_this_event_df['beacon_prefix'] = anchor_this_event_df['prefix'].map(lambda x: beacons_corresponding_to_anchor(exp_name, x))

            # file has column headers
            beacon_df = pd.read_csv(beacon_filename)

            # Default routes appear sometimes in pybgpstream data, 
            # Eg. 0.0.0.0/0, 0.0.0.0/1, ::/0
            # Ensure only prefixes configured in the experiment specification are processed
            beacon_df = beacon_df[beacon_df['prefix'].isin(beacon_list(exp_name))]

            _, last_ts = event_number2timestamp_tuple(exp_name, event_number)

            # Look for activity in beacons in the last minute of the period. 
            # This could be beacons arriving 'before' being sent, indicating clock synch problems
            beacon_last_min_condition = (beacon_df['max_ts_A']> (last_ts-60)) | (beacon_df['max_ts_W']> (last_ts-60))
            if len(beacon_df[beacon_last_min_condition]) > 0:
                print('Beacon activity in last minute of period: ',beacon_filename)

            # remove events with activity in the last minute
            beacon_df = beacon_df[~beacon_last_min_condition]
            

            # Merge to remove unwanted rows
            # second solution, https://stackoverflow.com/questions/28901683/pandas-get-rows-which-are-not-in-other-dataframe
            df_all = beacon_df.merge(anchor_this_event_df,left_on=['monitor_ip','prefix'], right_on=['monitor_ip','beacon_prefix'], how='left', suffixes=('', '_anchor'), indicator=True)

            # select
            filtered_beacon = df_all[df_all['_merge'] == 'left_only']
            filtered_beacon[['monitor_ip','prefix','min_ts_A','max_ts_A','count_A','min_ts_W','max_ts_W','count_W',
            'as_path_count_A', 
            'different_ases_count_A',
            'last_as_path_length_A',
            'event_number'
            ]].to_csv(beacon_filtered_filename, index=False)


            # Look for updates in the last 10 mins (600 secs)
            anchor_for_next_event_df = anchor_df[anchor_df['timestamp'] > (last_ts - 600)]

        except IOError:
            print('Warning, could not read file {}'.format(beacon_filename))
            
