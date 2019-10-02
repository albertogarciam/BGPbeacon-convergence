#!/usr/bin/env python3

''' Returns directory names (and creates if they didn't exist)
and filenames
'''

import os

from experiment_specs import experiments, result_directory, alternative_result_directory


# This directory must exist beforehand (this is the way to check that the code
# is running in the appropriate system)
# '/srv/agarcia/beacon_mrai/ris_beacons/20190501_4w'
def experiment_base_result_dir(exp_name):
    target_dir = result_directory + '/' 
    if not os.path.isdir(target_dir):
        if alternative_result_directory:
            alt_dir =  alternative_result_directory + '/'
            if not os.path.isdir(alt_dir):
                raise Exception('Neither base result directory ' + target_dir + ' nor alternative ' + alt_dir + ' did not existed.')
            target_dir = alt_dir
        else:         
            raise Exception('Base result directory ' + target_dir + ' did not existed, no alternative directory. Check if the system is ok, etc.')
    
    result_dir = test_and_create_dir_absolute_path(target_dir, exp_name)
    return result_dir


def test_and_create_dir(exp_name: str, directory_string: str) -> str:
    directory = experiment_base_result_dir(exp_name) + directory_string + '/'
    if not os.path.isdir(directory):
        os.mkdir(directory)
    return directory

def test_and_create_dir_absolute_path(base:str, directory_string: str) -> str:
    directory = base + directory_string + '/'
    if not os.path.isdir(directory):
        os.mkdir(directory)
    return directory
    

def download_directory(exp_name:str, collector:str) -> str:
    download_dir = test_and_create_dir(exp_name, 'download/')
    collector_dwn_dir = test_and_create_dir_absolute_path(download_dir, collector)
    return collector_dwn_dir

def download_filename(exp_name:str, collector:str, anchor:bool, event_number:int) -> str:
    directory = download_directory(exp_name, collector)
    if anchor:
        return(directory + 'anchor_'+str(event_number)+'.csv')
    else:
        return(directory + 'beacon_'+str(event_number)+'.csv')

def state_filename(exp_name:str, collector:str, anchor:bool, event_number:int) -> str:
    directory = download_directory(exp_name, collector)
    if anchor:
        return(directory + 'anchor_STATE_'+str(event_number)+'.csv')
    else:
        return(directory + 'beacon_STATE_'+str(event_number)+'.csv')

###
def per_path_event_basedir(exp_name:str) -> str:
    base_directory = test_and_create_dir(exp_name, 'per_path_event/')
    return base_directory

def per_path_event_directory(exp_name:str, collector:str) -> str:
    base_directory = per_path_event_basedir(exp_name)
    directory = test_and_create_dir_absolute_path(base_directory, collector)
    return directory

# def per_src_dst_anchor_filename(exp_name: str, collector:str) -> str:
#     directory = test_and_create_dir(exp_name, 'per_src_dest_anchor')
#     return directory + collector + '.csv'

def per_path_event_filename(exp_name: str, collector:str, event_number:int) -> str:
    directory = per_path_event_directory(exp_name, collector)
    return directory + 'per_path_event_' + str(event_number) + '.csv'

####
def per_path_event_filtered_directory(exp_name:str, collector:str) -> str:
    base_directory = test_and_create_dir(exp_name, 'per_path_event_filtered/')
    directory = test_and_create_dir_absolute_path(base_directory, collector)
    return directory

def per_path_event_filtered_filename(exp_name:str, collector:str, event_number:int) -> str:
    directory = per_path_event_filtered_directory(exp_name, collector)
    return directory + 'per_path_event_filtered_' + str(event_number) + '.csv'

    
####
def per_collector_event_mins_filename(exp_name:str, collector) -> str:
    base_directory = test_and_create_dir(exp_name, 'per_collector_event_mins')
    return base_directory + collector + '.csv'

# A single file per experiment
def per_event_shortest_distance_filename(exp_name:str) -> str:
    directory = experiment_base_result_dir(exp_name)
    return directory + 'per_event_shortest_distance.csv'

####
# A single file per experiment
def per_experiment_clock_synch_filename(exp_name:str, UP:bool = False, DOWN:bool = False) -> str:
    directory = experiment_base_result_dir(exp_name)
    if UP and DOWN:
        raise Exception('Cannot select both UP and DOWN')
    if UP:
        return directory + 'per_experiment_clock_synch_UP.csv'    
    elif DOWN:
        return directory + 'per_experiment_clock_synch_DOWN.csv'    
    else:
        return directory + 'per_experiment_clock_synch.csv'


####
def quantile_filename(exp_name: str, collector:str) -> str:
    directory  = test_and_create_dir(exp_name, 'quantiles/')
    return directory + collector + '.csv'

def zombies_filename(exp_name: str, collector:str) -> str:
    directory  = test_and_create_dir(exp_name, 'zombies/')
    return directory + collector + '.csv'

# should be called in both orders for the experiment name (so that the result is written in both directories)
def common_monitor_prefix_filename(exp_name1, exp_name2:str) -> str:
    directory = experiment_base_result_dir(exp_name1)
    return directory + 'common_monitor_prefix_' + exp_name1 + '_' + exp_name2 + '.csv'

def quantiles_with_clock_filename(exp_name: str) -> str:
    directory = experiment_base_result_dir(exp_name)
    return directory + 'quantiles_with_clock.csv'

# Directory containing ris update files
def download_updates_directory(exp_name:str, collector:str) -> str:
    download_dir = test_and_create_dir(exp_name, 'download_updates/')
    collector_dwn_dir = test_and_create_dir_absolute_path(download_dir, collector)
    return collector_dwn_dir


def download_updates_filename(exp_name:str, collector:str, anchor:bool, event_number:int) -> str:
    directory = download_directory(exp_name, collector)
    if anchor:
        return(directory + 'anchor_'+str(event_number)+'.csv')
    else:
        return(directory + 'beacon_'+str(event_number)+'.csv')