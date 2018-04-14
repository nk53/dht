#!/bin/bash
# If you have to use this, you did something wrong. Shame on you.

source setup_env.sh

hnames=($(cat $HOST_LIST))

for hname in ${hnames[@]}; do
    echo "Checking $hname"
    ssh -q -T $hname < $KILL_THIS_PYTHON &
done
