#!/bin/bash
# If you have to use this, you did something wrong. Shame on you.

source setup_env.sh

awk '{print $1}' $ADDRESSES | while read hname; do
    echo "Checking $hname"
    ssh -q -T $hname < $KILL_THIS_PYTHON &
done
