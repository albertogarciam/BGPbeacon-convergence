#!/usr/bin/env python3

''' 
Downloads traces for 
    beacons (by default) and 
    anchors (if started with --anchor) 
Uses pybgpstream.
Downloads data for a single beacon_event (2 hour period), identified 
by a number (0..MAX_EVENT) related to the experiment init_time and end_time.
If the file already existed (and has more than 0 bytes), 
it does not attempt to download it.

Generates a different file per collector, beacon/anchor and event_number.
The resulting file is a csv, goes to '../download/' directory.
.../download/COLLECTOR/beacon_EVENT_NUMBER.csv
                      /anchor_EVENT_NUMBER.csv
.../download/rrc00/beacon_0.csv
.../download/rrc00/beacon_1.csv

Each line contains:
BGP_message_type,timestamp,monitor_IP,monitor_AS,prefix,AS_PATH
However, the header is not included

Example of the contents 
A,1270094405,2001:610:1e08:4::5,196613,2001:7fb:fe03::/48,196613 1125 1103 12859 12654
A,1270094405,2001:610:1e08:4::5,196613,2001:7fb:fe03::/48,196613 1125 1103 12654
A,1270094409,145.125.80.5,196613,84.205.79.0/24,196613 1125 1103 11537 1916 1916 1916 1916 12654

'''

from _pybgpstream import BGPStream, BGPRecord
from argparse import ArgumentParser
import os

from experiments import anchor_list, beacon_list, event_number2timestamp_tuple 
from filenames_directories import download_filename


def dump_elem(output_file, elem):
    if elem.type == 'rib':
        raise Exception('Found RIB info')
        
    elif elem.type == 'A':
        info = ','.join((elem.type, str(elem.time), elem.peer_address, str(elem.peer_asn), elem.fields['prefix'], elem.fields['as-path']))
        output_file.write(info)
        output_file.write('\n')
    elif elem.type == 'W':
        info = ','.join((elem.type, str(elem.time), elem.peer_address, str(elem.peer_asn), elem.fields['prefix'], ''))
        output_file.write(info)
        output_file.write('\n')


# ./download.py 20181001_30d rrc00 0
if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("exp_name")
    parser.add_argument("collector")
    parser.add_argument("event_number", type=int)
    parser.add_argument("--anchors", help="if set, downloads the anchor, otherwise downloads beacons", action='store_true')

    args= parser.parse_args()
    exp_name = args.exp_name
    collector = args.collector
    event_number = args.event_number

    stream = BGPStream()
    rec = BGPRecord()
    stream.add_filter('collector', collector)

    if args.anchors:
        prefixes = anchor_list(exp_name)
    else:
        prefixes = beacon_list(exp_name)
    
    for prefix in prefixes:
        # print('Adding filter for prefix {}'.format(prefix))
        stream.add_filter('prefix', prefix)

    fn = download_filename(exp_name, collector, args.anchors, event_number)
    if os.path.isfile(fn) and os.stat(fn).st_size > 0:
        # To debug
        # print('File {} already exists, with size larger than 0, exiting'.format(fn))
        exit(1)

    output_file = open(fn, 'w')

    init_timestamp, end_timestamp = event_number2timestamp_tuple(exp_name, event_number)
    stream.add_interval_filter(init_timestamp, end_timestamp -1)
    stream.start()

    while(stream.get_next_record(rec)):
        if rec is None:
            print('None type read while processing {}'.format(collector))
        elif rec.status != "valid":
            print("{} {} {} {} {}".format(rec.project, rec.collector, rec.type, rec.time, rec.status, rec))
        else:
            elem = rec.get_next_elem()

            while(elem):
                dump_elem(output_file,  elem)
                elem = rec.get_next_elem()
