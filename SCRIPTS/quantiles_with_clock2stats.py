#!/usr/bin/env python3

'''
Plots prefix reachability, preferred route interval, and prefix withdrawn interval for 
a given experiment. 
Also prints summary stats.
'''

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from argparse import ArgumentParser
from filenames_directories import quantiles_with_clock_filename

clock_error = 'clock_p_90'


# Adds to a dataframe columns with quantiles corrected with clock offset error
def add_clock_info(qdf: pd.DataFrame) -> pd.DataFrame:

    qdf['minA_q50_minus_clock'] = qdf['minA_q50_UP'] - qdf[clock_error]
    # set to 0 negative values, otherwise, same as before
    qdf['minA_q50_minus_clock']= qdf['minA_q50_minus_clock'].where(qdf['minA_q50_minus_clock'] > 0, 0)
    qdf['minA_q50_plus_clock'] = qdf['minA_q50_UP'] + qdf[clock_error]
    # Rename for consistent processing (get rit of '_UP' in the name)
    qdf['minA_q50'] = qdf['minA_q50_UP']


    ###########
    # MaxA

    qdf['maxA_q50_minus_clock'] = qdf['maxA_q50_UP'] - qdf[clock_error]
    # set to 0 negative values, otherwise, same as before
    qdf['maxA_q50_minus_clock']= qdf['maxA_q50_minus_clock'].where(qdf['maxA_q50_minus_clock'] > 0, 0)
    qdf['maxA_q50_plus_clock'] = qdf['maxA_q50_UP'] + qdf[clock_error]
    qdf['maxA_q50'] = qdf['maxA_q50_UP'] 


    ###########
    qdf['maxW_q50_minus_clock'] = qdf['maxW_q50_DOWN'] - qdf[clock_error]
    # set to 0 negative values, otherwise, same as before
    qdf['maxW_q50_minus_clock']= qdf['maxW_q50_minus_clock'].where(qdf['maxW_q50_minus_clock'] > 0, 0)
    qdf['maxW_q50_plus_clock'] = qdf['maxW_q50_DOWN'] + qdf[clock_error]
    qdf['maxW_q50'] = qdf['maxW_q50_DOWN']

    return qdf

# Only v4 beacons remain
def only_ipv4(qdf: pd.DataFrame) -> pd.DataFrame:
    qdf = qdf[~qdf.prefix.str.contains(':')]
    return qdf

# Only v6 beacons remain
def only_ipv6(qdf: pd.DataFrame) -> pd.DataFrame:
    qdf = qdf[qdf.prefix.str.contains(':')]
    return qdf


def print_stats(qdf: pd.DataFrame) -> None:
    print('Total number of pairs: ', len(qdf))
    print('Number of unique monitors', len(qdf_4['monitor_ip'].unique()))
    print()

    # Zombies and rfd are counted but then removed from the rest of the analysis
    print('Fraction of zombies: {:.3f}'.format(qdf['zombie_count'].sum()/qdf['count_DOWN_events'].sum()))
    print('     Total number of zombie route events: {:d}'.format(qdf['zombie_count'].sum()))
    rfd_count = qdf['rfd_count_UP'].sum() + qdf['rfd_count_DOWN'].sum()
    print('Fraction of rfd events over total events: {:.3f}'.format(rfd_count/(qdf['count_DOWN_events'].sum() +qdf['count_UP_events'].sum())))
    print('     Fraction of RFD in DOWN over total: {:.3f}'.format(qdf['rfd_count_DOWN'].sum()/rfd_count))
    print('     Fraction of monitor/beacon pairs observing at least one rfd event: {:.3f}'.format(len(qdf[(qdf['rfd_count_UP']>0) | (qdf['rfd_count_DOWN']>0)])/len(qdf)))
    RFD_THR=10
    print('     Fraction of monitor/beacon pairs observing more than {} rfd events: {:.3f}'.format(RFD_THR, len(qdf[(qdf['rfd_count_UP']> RFD_THR) | (qdf['rfd_count_DOWN']> RFD_THR)])/len(qdf)))
    print()

    print('Mean clock error q90 {:.3f}'.format(qdf[clock_error].mean()))

    print()
    print('Mean of Prefix reachability interval, q50: {:.3f}'.format(qdf['minA_q50_UP'].mean()))
    print('...q50 + error 90th perc: {:.3f}'.format(qdf['minA_q50_plus_clock'].mean()))
    # print('...q90', qdf['minA_q90_UP'].mean())
    print('Mean of Preferred route interval, q50: {:.3f}'.format(qdf['maxA_q50_UP'].mean()))
    print('...Mean of q50 + error 90th perc: {:.3f}'.format(qdf['maxA_q50_plus_clock'].mean()))
    # print('...Mean of q90', qdf['maxA_q90_UP'].mean())
    print('Mean of Prefix withdrawn interval, q50: {:.3f}'.format(qdf['maxW_q50_DOWN'].mean()))
    print('...Mean of q50 + error 90th perc: {:.3f}'.format(qdf['maxW_q50_plus_clock'].mean()))
    # print('...Mean of q90', qdf['maxW_q90_DOWN'].mean())
    
    print('Mean number of messages in UP events :{:.3f}'.format(qdf['count_A'].sum()/qdf['count_UP_events'].sum()))
    print('Mean number of messages in DOWN events :{:.3f}'.format((qdf['count_W'].sum() + qdf['count_A'].sum())/qdf['count_DOWN_events'].sum()))

    print()
    print('As_path_count in DOWN: Mean as_path_count (different AS_PATHS observed in a single event) in DOWN events :{:.3f}'.format(qdf['as_path_count_DOWN'].sum()/qdf['count_DOWN_events'].sum()))

    print('Different ASes count in DOWN: Mean count of different ASes (different ASes observed in a single event) in DOWN events: {:.3f}'.format(qdf['ases_different_one_event_DOWN'].sum()/qdf['count_DOWN_events'].sum()))
    print('Last AS length in DOWN: Mean of the length of the last path observed in  DOWN events: {:.3f}'.format(qdf['last_as_path_length_DOWN'].sum()/qdf['count_DOWN_events'].sum()))

    return

#./quantiles2stats.py 20181001_30d
if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("exp_name")
    
    args= parser.parse_args()
    exp_name = args.exp_name
    
    fn = quantiles_with_clock_filename(exp_name)
    qdf = pd.read_csv(fn)

    min_events =  45
    
    qdf = qdf[(qdf['count_UP_events'] > min_events) & (qdf['count_DOWN_events'] > min_events)]
    qdf = add_clock_info(qdf)
    qdf_4 = only_ipv4(qdf)
    qdf_6 = only_ipv6(qdf)


    print('\n\n------------------\nIPv4')
    print_stats(qdf_4)
    print('\n\n------------------\nIPv6')
    print_stats(qdf_6)
