#!/bin/bash

source setup_env.sh

if [ -e "$PIDS" ]; then
    echo "Processes on all nodes:"
    cat $PIDS
    ./$KILL_ALL_PYTHONS
    rm $PIDS
else
    echo "No processes running"
    echo "Did you mean $(basename $START_NODES)?"
fi

