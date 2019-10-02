#!/usr/bin/env bash

# Executes the same command for each collector, with CONCURRENT_PROCESSES in parallel
# Example:
# ./execute_for_each_collector.sh downloaded2per_path_event.py 20100401_30d
# ./downloaded2per_path_event.py 20100401_30d rrc00


# To debug this file, 
# bash -x ./execute_for_each_collector.sh

# strict mode, http://redsymbol.net/articles/unofficial-bash-strict-mode/
set -euo pipefail

# Number of max concurrent processes (one per collector) allowed
CONCURRENT_PROCESSES=2

if [ $# -ne 2 ]; then
    echo -e $TEST_VECTOR_HELP
    exit 1
fi

COMMAND_NAME=$1
EXP_NAME=$2
# removed rrc16 for problems with pybgpstream
COLLECTOR_NAMES="rrc00 rrc01 rrc04 rrc05 rrc06 rrc07 rrc10 rrc11 rrc12 rrc13 rrc14 rrc15"



# Be nice, do not execute more than K processes at the same time, e.g.,:
# max 20        - K=20 processes
function max () {
   # Check running jobs only (not Done), show pids, count number of pids
   while [ `jobs -rp| wc -w` -gt $1 ]
   do
      sleep 5
   done
}

for collector in $COLLECTOR_NAMES
do
    C1=' "./$COMMAND_NAME $EXP_NAME $collector" ' 
    COMMAND=" $(eval echo "$C1" ) "
    max $CONCURRENT_PROCESSES; $COMMAND &
    
done

# Ensure all processes have finished before returning
# (to be able to serialize the execution of other tasks)
max 0