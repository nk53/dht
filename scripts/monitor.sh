#!/bin/bash

num_nodes=$1

if [ -z "$num_nodes" ]; then
    num_nodes=3
fi

echo "Starting monitor"
while [ $(./ends.sh $num_nodes | wc -l) -lt $num_nodes ]; do
    sleep 3
    ./collect_outputs > /dev/null
done

echo Done
