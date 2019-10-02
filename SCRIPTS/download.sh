#!/usr/bin/env bash

# Downloads all beacon and anchors for one experiment

# To debug this file, 
# bash -x ./donwload.sh

# starts the following number of CONCURRENT_PROCESSES
CONCURRENT_PROCESSES=2

set -euo pipefail

HELP_INFO="
run as\n
   nohup ./download.sh EXP_NAME  &\n
   nohup ./download.sh 20181001_30d &\n
\n
"

if [ $# -ne 1 ]; then
    echo -e $HELP_INFO
    exit 1
fi

EXP_NAME=$1
COLLECTOR_NAMES="rrc00 rrc01 rrc04 rrc05 rrc06 rrc07 rrc10 rrc11 rrc12 rrc13 rrc14 rrc15"


# Ensure a maximum of K processes at the same time with 'max K'
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
    for i in {0..179}
    do
    echo "./download.py $EXP_NAME $collector $i"
    max $CONCURRENT_PROCESSES; ./download.py $EXP_NAME $collector $i &
    done
done

for collector in $COLLECTOR_NAMES
do
    for i in {0..179}
    do
    echo "./download.py $EXP_NAME $collector $i --anchors"
    max $CONCURRENT_PROCESSES; ./download.py $EXP_NAME $collector $i --anchors &
    done
done