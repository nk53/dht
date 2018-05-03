#!/bin/bash

num_nodes=$1

if [ -z "$num_nodes" ]; then
    num_nodes=3
fi

# maximum sleep time
max_sleep=60
# time per sleep
sleep_time=3
# total time we've slept so far
total_sleep=0

echo "Starting monitor"
while [ $(./ends.sh $num_nodes | wc -l) -lt $num_nodes ]; do
    if [ $total_sleep -ge $max_sleep ]; then
        echo "Quitting early"
        exit
    fi
    sleep $sleep_time
    let "total_sleep += $sleep_time"
    ./collect_outputs > /dev/null
done

echo Done
