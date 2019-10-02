#!/usr/bin/env python3

''' 
Functions to access to experiment parameters.
List of beacons, anchor, collectors. 
Access to dates, etc.

The main parameter is the experiment name, which refers to the 
initial and final day of the experiment.
'''


from typing import List, Tuple
import calendar
import pandas as pd

from experiment_specs import experiments, beacons_2009


def beacon_list(exp_name:str) -> List[str]:
    return [x[0][0] for x in beacons_2009]


def anchor_list(exp_name:str) -> List[str]:
    # position [1] is anchor
    return [x[1] for x in beacons_2009]


def collector_list(exp_name:str) -> List[str]:
    unique = set([x[2].lower() for x in beacons_2009])
    return list(unique)

def beacons_corresponding_to_anchor(exp_name, anchor):
    for pair in  beacons_2009:
        if pair[1] == anchor:
            return pair[0][0]


# beacon2collector('20100401_30d', '84.205.71.0/24') -> rrc07
def beacon2collector(exp_name, beacon) -> str:
    df = pd.DataFrame(list(beacons_2009))
    # remove the tuple in ('xxxx/pp',)
    df[0] = df[0].map(lambda x: x[0])
    # df[df[0] == '2001:7fb:fe04::/48'][2].values
    # array(['RRC04'], dtype=object)
    return df[df[0] == beacon][2].values[0].lower()

# split('20181002')
#       (2018, 10, 2)
def split_yyyymmdd(yyyymmdd: str) -> Tuple[int, int, int]:
    yyyy = yyyymmdd[:4]
    mm = yyyymmdd[4:6]
    dd = yyyymmdd[6:8]

    if int(yyyy) < 1995 or int(yyyy) > 2030:
        print('Invalid year {}'.format(yyyy))
        exit(1)

    if int(mm) < 0 or int(mm)>12:
        print('Invalid month {}'.format(mm))
        exit(1)
    
    if int(dd) <0 or int(dd)>31:
        print('Invalid day {}'.format(dd))
        exit(1)

    return (int(yyyy), int(mm), int(dd))


# Number of experiments obtained from ['end_day'] - ['init_day']
def events_in_experiment(exp_name:str) -> int:
    yyyy_i, mm_i, dd_i = split_yyyymmdd(experiments[exp_name]['init_day'])
    yyyy_e, mm_e, dd_e = split_yyyymmdd(experiments[exp_name]['end_day'])

    diff_secs = calendar.timegm((yyyy_e, mm_e, dd_e, 0, 0, 0, 0))- calendar.timegm((yyyy_i, mm_i, dd_i, 0, 0, 0, 0))
    # 6 events per day, count both days
    num_events = 6 + int(diff_secs) / (3600*24) *6
    return int(num_events)



# Returns first timestamp, last timestamp corresponding to the 2h period 
# of an event
def event_number2timestamp_tuple(exp_name:str, event_number:int) -> Tuple[int, int]:
    year, month, init_day = split_yyyymmdd(experiments[exp_name]['init_day'])

    year = int(year)
    month = int(month)
    init_day = int(init_day)

    # six events per day
    # up_time_list =['0400', '1200', '2000']
    # down_time_list = ['0600', '1400', '2200']
    day = init_day + int(event_number / 6)
    if (event_number % 6) == 0:
        hour = 4
    elif (event_number % 6) == 1:
        hour = 6
    elif (event_number % 6) == 2:
        hour = 12
    elif (event_number % 6) == 3:
        hour = 14
    elif (event_number % 6) == 4:
        hour = 20
    elif (event_number % 6) == 5:
        hour = 22
    else:
        print('Error')
        exit(0)
    # print(year, month, day, hour)
    return ( int(calendar.timegm((year, month, day, hour, 0, 0, 0))),
        int(calendar.timegm((year, month, day, hour+2, 0, 0, 0))))


# returns first update filename for a given event
# 	updates.20121001.0600
# Without .gz
def event_number2update_filename_no_gz(exp_name:str, event_number:int) -> str:
    year = experiments[exp_name]['init_day'][:4]
    month = experiments[exp_name]['init_day'][4:6]
    init_day = int(experiments[exp_name]['init_day'][6:8])

    # six events per day
    # up_time_list =['0400', '1200', '2000']
    # down_time_list = ['0600', '1400', '2200']
    day = init_day + int(event_number / 6)
    if (event_number % 6) == 0:
        hour = '04'
    elif (event_number % 6) == 1:
        hour = '06'
    elif (event_number % 6) == 2:
        hour = '12'
    elif (event_number % 6) == 3:
        hour = '14'
    elif (event_number % 6) == 4:
        hour = '20'
    elif (event_number % 6) == 5:
        hour = '22'
    else:
        print('Error')
        exit(0)

    if (month == '02') and (day > 28):
        print('Error: cannot process more than 28 days in february')
        exit(0)

    day_str = "{:02d}".format(day)

    filename = 'updates.' + year + month + day_str + '.' + hour + '00'
    return filename

# returns year month for first update file
def event_number2year_month(exp_name:str, event_number:int) -> Tuple[str, str]:
    year = experiments[exp_name]['init_day'][:4]
    month = experiments[exp_name]['init_day'][4:6]
    return year, month