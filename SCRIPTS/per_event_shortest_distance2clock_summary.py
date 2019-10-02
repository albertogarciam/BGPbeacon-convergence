#!/usr/bin/env python3
'''
Used for clock error estimation.

Generates one file per experiment, File: 'per_experiment_clock_synch_DOWN.csv' (or UP, if only_UP )
that contains different percentile values per collector_1/collector_2 pairs, only for DOWN events.

    example of the result:
    $ head per_experiment_clock_synch_DOWN.csv 
    collector_1,collector_2,p_0,p_50,p_90,p_100,event_count
    rrc00,rrc04,4.0,16.0,26.0,56.0,54
    rrc00,rrc05,1.0,2.0,6.0,46.0,54
    ...

Also print some general stats.

'''
import pandas as pd
from argparse import ArgumentParser

from filenames_directories import per_event_shortest_distance_filename, per_experiment_clock_synch_filename

# ./per_event_shortest_distance2clock_summary.py 20090101_30d --only_DOWN
if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("exp_name")
    # order is not relevant

    # Use all events if there is no optional filter
    parser.add_argument("--only_UP", action='store_true')
    parser.add_argument("--only_DOWN", action='store_true')

    args= parser.parse_args()

    exp_name = args.exp_name

    time_fn = per_event_shortest_distance_filename(exp_name)
    time_df = pd.read_csv(time_fn)

    time_df = time_df[time_df['shortest_distance'] < 100]

    #    collector_1 collector_2  event_number  min_time_1  min_time_2  weight  shortest_distance
    #     rrc13       rrc14            68        23.0      2350.0  2350.0             2350.0
    #     rrc00       rrc14            75         1.0        60.0    60.0               60.0
    if args.only_UP:
        time_df = time_df[time_df['event_number']%2 == 0]
    elif args.only_DOWN:
        time_df = time_df[time_df['event_number']%2 == 1]

    grouped = time_df.groupby(['collector_1', 'collector_2']) 
    res_df = grouped.aggregate({'shortest_distance': lambda serie: serie.min()})

    res_df['p_50'] = grouped.aggregate({'shortest_distance': lambda serie: serie.quantile(0.5, 'lower')})

    res_df['p_90'] = grouped.aggregate({'shortest_distance': lambda serie: serie.quantile(0.9, 'lower')})

    res_df['p_100'] = grouped.aggregate({'shortest_distance': lambda serie: serie.max()})
    res_df['event_count'] = grouped.size()

    res_df = res_df.rename(index=str, columns={"shortest_distance": "p_0"})

    
    out_fn = per_experiment_clock_synch_filename(exp_name, args.only_UP, args.only_DOWN)
    res_df.to_csv(out_fn)

    # general stats
    print('Total number of pairs {}'.format(len(res_df)))
    print('Pairs with less or eq than 50 events: {}'.format(len(res_df[res_df['event_count']<=50])))

    res_df = res_df[res_df['event_count']> 50]
    print('Mean (over all collector pairs, more than 50 events) of')
    print('min: ', res_df['p_0'].mean())
    print('p_50: ', res_df['p_50'].mean())
    print('p_90: ', res_df['p_90'].mean())
    print('p_100: ', res_df['p_100'].mean())
    print('number of pairs over 50 event count: ', len(res_df))