#!/bin/bash
# If you have to use this, you did something wrong. Shame on you.

source setup_env.sh

hnames=($(cat $HOST_LIST))

user=$(whoami)

for hname in ${hnames[@]}; do
    pid=$(ssh -q -T $hname "ps -u $user" | grep -iE "[^_]python" |
        awk '{print $1}')
    if [ -n "$pid" ]; then
        echo "$hname: $pid"
    else
        echo "$hname: no pythons"
    fi
done
