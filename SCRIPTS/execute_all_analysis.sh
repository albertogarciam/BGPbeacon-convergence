#!/usr/bin/env bash

# ./execute_all_analysis.sh 20181001_30d

# Assumes data is already downloaded
# Performs all processing required.
set -euo pipefail

if [ $# -ne 1 ]; then
    echo -e "indicate experiment name"
    exit 1
fi

EXP_NAME=$1

# Uses .execute_for_each_collector to expand the task defined for 
# each *.py script to each collector
./execute_for_each_collector.sh downloaded2per_path_event.py $EXP_NAME
./execute_for_each_collector.sh per_path_event2per_path_event_filtered.py $EXP_NAME

# Clock synch analysis
./execute_for_each_collector.sh per_path_event_filtered2per_collector_event_min.py $EXP_NAME
./per_collector_event_mins2per_event_shortest_distance.py $EXP_NAME
./per_event_shortest_distance2clock_summary.py $EXP_NAME --only_DOWN
# When 20091001_20d, 20121001_30d, 20151001_30d and 20181001_30d have been processed,
# execute   ./plot_error_per_collector_pair.py

# analysis
./execute_for_each_collector.sh per_path_event_filtered2quantiles.py $EXP_NAME
./quantiles2quantiles_with_clock.py $EXP_NAME
# ./plot_quantiles_with_clock.py 20121001_30d 20181001_30d
